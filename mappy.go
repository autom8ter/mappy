package mappy

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/autom8ter/mappy/rafty"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
	"net"
	"os"
	"os/user"
	"strings"
	"sync"
	"time"
)

func init() {
	gob.Register(&sBucket{})
	gob.Register(&mappy{})
	gob.Register(&Record{})
}

type Op int

const (
	EXPIRE Op = 1
	DELETE Op = 2
	SET    Op = 3
)

var Done = errors.New("mappy: done")

type Record struct {
	bucketPath []string
	Key        interface{}   `json:"key"`
	Val        interface{}   `json:"val"`
	Exp        time.Duration `json:"exp"`
}

func (r *Record) toStored() *storedRecord {
	return &storedRecord{
		BucketPath: r.bucketPath,
		Key:        r.Key,
		Val:        r.Val,
		Exp:        r.Exp,
	}
}

func (r *Record) BucketPath() []string {
	return r.bucketPath
}

type storedRecord struct {
	BucketPath []string      `json:"bucketPath"`
	Key        interface{}   `json:"key"`
	Val        interface{}   `json:"val"`
	Exp        time.Duration `json:"exp"`
}

func (r *Record) toLog(term uint64) (*raft.Log, error) {
	buffer, err := rafty.EncodeMsgPack(r.toStored())
	if err != nil {
		return nil, err
	}
	return &raft.Log{
		Index:      rafty.BytesToUint64([]byte(strings.Join(r.bucketPath, ","))),
		Term:       term,
		Type:       raft.LogCommand,
		Data:       buffer.Bytes(),
		Extensions: nil,
	}, nil
}

func fromLog(lg *raft.Log) (*Record, error) {
	var stored storedRecord
	if err := rafty.ReadMsgPack(&stored, bytes.NewBuffer(lg.Data)); err != nil {
		return nil, err
	}
	return &Record{
		bucketPath: stored.BucketPath,
		Key:        stored.Key,
		Val:        stored.Val,
		Exp:        stored.Exp,
	}, nil
}

type ViewFunc func(record *Record) error

type ChangeHandlerFunc func(op Op, old, new *Record) error

type Bucket interface {
	Nested(key string) Bucket
	Delete(key interface{}) error
	Len() int
	OnChange(fns ...ChangeHandlerFunc)
	Get(key interface{}) (value *Record, ok bool)
	Set(record *Record) error
	View(fn ViewFunc) error
	Decode(r io.Reader) error
	Encode(w io.Writer) error
}

type sBucket struct {
	BucketPath []string `json:"bucketPath"`
	onChange   []ChangeHandlerFunc
	Records    *sync.Map `json:"records"`
}

func (s *sBucket) Encode(w io.Writer) error {
	return gob.NewEncoder(w).Encode(s)
}

func (s *sBucket) Decode(r io.Reader) error {
	return gob.NewDecoder(r).Decode(s)
}

func (s *sBucket) Nested(key string) Bucket {
	val, ok := s.Records.Load(key)
	if !ok {
		return nil
	}
	bucket, ok := val.(Bucket)
	bucketPath := s.BucketPath
	bucketPath = append(bucketPath, key)
	if !ok {
		return &sBucket{
			BucketPath: bucketPath,
			onChange:   nil,
			Records:    &sync.Map{},
		}
	}
	return bucket
}

func (s *sBucket) bucket() Bucket {
	return s
}

func (s *sBucket) Len() int {
	counter := 0
	_ = s.View(func(record *Record) error {
		if record != nil {
			counter++
		}
		return nil
	})
	return counter
}

func (s *sBucket) getRecord(key interface{}) (*Record, bool) {
	val, ok := s.Records.Load(key)
	if !ok {
		return nil, false
	}
	record, ok := val.(*Record)
	if !ok {
		return nil, false
	}
	return record, true
}

func (s *sBucket) Delete(key interface{}) error {
	record, ok := s.getRecord(key)
	if ok {
		s.Records.Delete(key)
		for _, fn := range s.onChange {
			if err := fn(DELETE, record, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sBucket) OnChange(fns ...ChangeHandlerFunc) {
	s.onChange = append(s.onChange, fns...)
}

func (s *sBucket) Get(key interface{}) (*Record, bool) {
	return s.getRecord(key)
}

func (s *sBucket) Set(record *Record) error {
	record.bucketPath = s.BucketPath
	before, _ := s.getRecord(record.Key)
	s.Records.Store(record.Key, record)
	for _, fn := range s.onChange {
		if err := fn(SET, before, record); err != nil {
			return err
		}
	}
	return nil
}

func (s *sBucket) View(fn ViewFunc) error {
	var errs []error
	s.Records.Range(func(key, value interface{}) bool {
		record, _ := value.(*Record)
		if err := fn(record); err != nil {
			if err == Done {
				return false
			}
			errs = append(errs, err)
			return false
		}
		return true
	})
	if len(errs) > 0 {
		var err error
		for i, e := range errs {
			err = fmt.Errorf("mappy error %v: %s", i, e.Error())
		}
		return err
	}
	return nil
}

type Mappy interface {
	Bucket
	Bucket(r *Record) Bucket
	AddPeer(addr string) error
	Close() error
}

type mappy struct {
	*sBucket
	rft    *raft.Raft
	logger hclog.Logger
}

func (m *mappy) Persist(sink raft.SnapshotSink) error {
	if err := m.Encode(sink); err != nil {
		return err
	}
	return sink.Close()
}

func (m *mappy) Release() {

}

func (m *mappy) Apply(log *raft.Log) interface{} {
	record, err := fromLog(log)
	if err != nil {
		return err
	}
	bucket := m.bucket()
	for _, b := range record.bucketPath {
		bucket = bucket.Nested(b)
	}
	if err := bucket.Set(record); err != nil {
		return err
	}
	return nil
}

func (m *mappy) Snapshot() (raft.FSMSnapshot, error) {
	return m, nil
}

func (m *mappy) Restore(closer io.ReadCloser) error {
	return m.Decode(closer)
}

type Opts struct {
	Path     string
	LogLevel string
	Listen string
	MaxPool int
}

var DefaultOpts = &Opts{
	Path:     "/tmp/mappy",
	LogLevel: "DEBUG",
	Listen: fmt.Sprintf("127.0.0.1:8765"),
	MaxPool: 5,
}

func Open(opts *Opts) (Mappy, error) {
	if _, err := os.Stat(opts.Path); os.IsNotExist(err) {
		os.MkdirAll(opts.Path, 0777)
	}
	config := raft.DefaultConfig()
	logStore, err := rafty.NewboltRaft(opts.Path + "/mappy.db")
	if err != nil {
		return nil, err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(opts.Path, 1, os.Stdout)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveTCPAddr("tcp", opts.Listen)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(opts.Listen, addr, opts.MaxPool, time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}
	config.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "mappy",
		Level:  hclog.LevelFromString(opts.LogLevel),
		Output: os.Stderr,
	})
	usr, err := user.Current()
	if err != nil {
		return nil, err
	}
	config.LocalID = raft.ServerID(fmt.Sprintf("%s %s", usr.Name, opts.Listen))
	m := &mappy{
		logger: config.Logger,
		sBucket: &sBucket{
			BucketPath: nil,
			onChange:   nil,
			Records:    &sync.Map{},
		},
	}
	r, err := raft.NewRaft(config, m, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}
	m.rft = r
	return m, nil
}

func (m mappy) Close() error {
	return m.rft.Shutdown().Error()
}

func (m *mappy) Bucket(r *Record) Bucket {
	bucket := Bucket(m)
	nested := bucket
	for _, nest := range r.bucketPath {
		nested = nested.Nested(nest)
	}
	return nested
}

func (m *mappy) AddPeer(addr string) error {
	return m.rft.AddPeer(raft.ServerAddress(addr)).Error()
}