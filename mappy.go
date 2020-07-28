package mappy

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/autom8ter/mappy/cmap"
	"github.com/autom8ter/mappy/rafty"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
	"io"
	"net"
	"os"
	"time"
)

func init() {
	gob.Register(&sBucket{})
	gob.Register(&mappy{})
	gob.Register(&Record{})
	gob.Register(&storedRecord{})
}

type Op int

const (
	EXPIRE Op = 1
	DELETE Op = 2
	SET    Op = 3
)

var Done = errors.New("mappy: done")

type Record struct {
	Key        string      `json:"key"`
	Val        interface{} `json:"val"`
	Exp        int64       `json:"exp"`
	bucketPath []string
	updatedAt  int64
}

func (r *Record) toStored() *storedRecord {
	return &storedRecord{
		BucketPath: r.bucketPath,
		Key:        r.Key,
		Val:        r.Val,
		Exp:        r.Exp,
		UpdatedAt:  r.updatedAt,
	}
}

func (r *Record) BucketPath() []string {
	return r.bucketPath
}

func (r *Record) UpdatedAt() time.Time {
	return time.Unix(0, r.updatedAt)
}

func (r *Record) JSON() string {
	bits, _ := json.MarshalIndent(r, "", "    ")
	return fmt.Sprintf("%s", bits)
}

type storedRecord struct {
	BucketPath []string    `json:"bucketPath"`
	Key        string      `json:"key"`
	Val        interface{} `json:"val"`
	Exp        int64       `json:"exp"`
	UpdatedAt  int64       `json:"updatedAt"`
}

type Log struct {
	Op        Op
	New       *Record
	Old       *Record
	CreatedAt int64
}

func fromLog(lg *raft.Log) (*Log, error) {
	var stored Log
	if err := gob.NewDecoder(bytes.NewBuffer(lg.Data)).Decode(&stored); err != nil {
		return nil, err
	}
	return &stored, nil
}

func (l *Log) encode() (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	return buf, gob.NewEncoder(buf).Encode(buf)
}

type ViewFunc func(record *Record) error

type ChangeHandlerFunc func(log *Log) error

type Bucket interface {
	Nested(key string) Bucket
	Delete(key string) error
	Len() int
	OnChange(fns ...ChangeHandlerFunc)
	HandleChange(log *Log) error
	Get(key string) (value *Record, ok bool)
	Set(record *Record) *Result
	View(fn ViewFunc) error
	Decode(r io.Reader) error
	Encode(w io.Writer) error
	Store() *cmap.Map
}

type sBucket struct {
	logz *bbolt.Bucket
	BucketPath []string `json:"bucketPath"`
	onChange   []ChangeHandlerFunc
	Records    *cmap.Map `json:"records"`
}

func (s *sBucket) Store() *cmap.Map {
	return s.Records
}

func (s *sBucket) HandleChange(log *Log) error {
	for _, fn := range s.onChange {
		if err := fn(log); err != nil {
			return err
		}
	}
	return nil
}

func (s *sBucket) Encode(w io.Writer) error {
	return s.Encode(w)
}

func (s *sBucket) Decode(r io.Reader) error {
	return s.Decode(r)
}

func (s *sBucket) Nested(key string) Bucket {
	val, ok := s.Records.Load(key)
	bucketPath := s.BucketPath
	bucketPath = append(bucketPath, key)
	if !ok {
		return &sBucket{
			BucketPath: bucketPath,
			onChange:   nil,
			Records:    &cmap.Map{},
		}
	}
	bucket, ok := val.(Bucket)
	if !ok {
		return &sBucket{
			BucketPath: bucketPath,
			onChange:   nil,
			Records:    &cmap.Map{},
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

func (s *sBucket) getRecord(key string) (*Record, bool) {
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

func (s *sBucket) Delete(key string) error {
	before, _ := s.getRecord(key)
	lg := &Log{
		Op:        DELETE,
		New:       nil,
		Old:       before,
		CreatedAt: time.Now().Unix(),
	}
	buf, err := lg.encode()
	if err != nil {
		return err
	}
	seq, _ := s.logz.NextSequence()
	if err := s.logz.Put(rafty.Uint64ToBytes(seq), buf.Bytes()); err != nil {
		return err
	}
	s.Records.Delete(key)
	return nil
}

func (s *sBucket) OnChange(fns ...ChangeHandlerFunc) {
	s.onChange = append(s.onChange, fns...)
}

func (s *sBucket) Get(key string) (*Record, bool) {
	return s.getRecord(key)
}

func (s *sBucket) Set(record *Record) error {
	before, _ := s.getRecord(record.Key)
	lg := &Log{
		Op:        SET,
		New:       record,
		Old:       before,
		CreatedAt: time.Now().Unix(),
	}
	buf, err := lg.encode()
	if err != nil {
		return err
	}
	seq, _ := s.logz.NextSequence()
	if err := s.logz.Put(rafty.Uint64ToBytes(seq), buf.Bytes()); err != nil {
		return err
	}
	s.Records.Store(record.Key, record)

}

func (s *sBucket) View(fn ViewFunc) error {
	var errs []error
	s.Records.Range(func(key string, value interface{}) bool {
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
	Close() error
	Destroy() error
}

type mappy struct {
	*sBucket
	o        *Opts
	rft      *raft.Raft
	logger   hclog.Logger
	shutdown bool
}

func (m *mappy) Release() {

}

type Opts struct {
	Leader            bool
	Path              string
	LocalID           string
	LogLevel          string
	Listen            string
	MaxPool           int
	SnapshotRetention int
	Timeout           time.Duration
}

var DefaultOpts = &Opts{
	Leader:            true,
	Path:              "/tmp/mappy",
	LocalID:           "node0",
	LogLevel:          "DEBUG",
	Listen:            fmt.Sprintf("127.0.0.1:8765"),
	MaxPool:           5,
	SnapshotRetention: 1,
	Timeout:           5 * time.Second,
}

func Open(opts *Opts) (Mappy, error) {
	if _, err := os.Stat(opts.Path); os.IsNotExist(err) {
		os.MkdirAll(opts.Path, 0777)
	}
	config := raft.DefaultConfig()
	logStore, err := bbolt.Open(opts.Path + "/mappy.db", 0700, bbolt.DefaultOptions)
	if err != nil {
		return nil, err
	}
	var bucket *bbolt.Bucket
	logStore.Update(func(tx *bbolt.Tx) error {
		var err error
		bucket, err = tx.CreateBucketIfNotExists([]byte("logs/"))
		if err != nil {
			return err
		}
		return nil
	})
	m := &mappy{
		logger: config.Logger,
		sBucket: &sBucket{
			logz: bucket,
			BucketPath: nil,
			onChange:   nil,
			Records:    &cmap.Map{},
		},
	}
	m.o = opts
	return m, nil
}

func (m *mappy) Close() error {
	if err := m.rft.Shutdown().Error(); err != nil {
		return err
	}
	m.shutdown = true
	return nil
}

func (m *mappy) Bucket(r *Record) Bucket {
	bucket := Bucket(m)
	nested := bucket
	for _, nest := range r.bucketPath {
		nested = nested.Nested(nest)
	}
	return nested
}

type Raft interface {
	AddPeer(addr string) error
	Stats() map[string]string
	State() string
}

type rft struct {
	impl *raft.Raft
}

func (m *rft) AddPeer(addr string) error {
	return m.impl.AddPeer(raft.ServerAddress(addr)).Error()
}

func (m *rft) Stats() map[string]string {
	return m.impl.Stats()
}

func (m *rft) State() string {
	return m.impl.State().String()
}

type Result struct {
	Index uint64
	Log   *Log
	Err   error
}

func (m *mappy) Destroy() error {
	if !m.shutdown {
		if err := m.Close(); err != nil {
			return err
		}
	}
	return os.RemoveAll(m.o.Path)
}
