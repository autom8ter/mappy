package mappy

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/autom8ter/mappy/rafty"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
	"net"
	"os"
	"sync"
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
	Key        string   `json:"key"`
	Val        interface{}   `json:"val"`
	Exp        int64 `json:"exp"`
	bucketPath []string
	updatedAt int64
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
	BucketPath []string      `json:"bucketPath"`
	Key        string   `json:"key"`
	Val        interface{}   `json:"val"`
	Exp        int64 `json:"exp"`
	UpdatedAt  int64 `json:"updatedAt"`
}

type Log struct {
	Op Op
	New *Record
	Old *Record
	CreatedAt int64
}

func fromLog(lg *raft.Log) (*Log, error) {
	var stored Log
	if err := rafty.ReadMsgPack(&stored, bytes.NewBuffer(lg.Data)); err != nil {
		return nil, err
	}
	return &stored, nil
}

func (l *Log) encode() (*bytes.Buffer, error) {
	return rafty.EncodeMsgPack(l)
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
	Set(record *Record) error
	View(fn ViewFunc) error
	Decode(r io.Reader) error
	Encode(w io.Writer) error
	Store() *sync.Map
}

type sBucket struct {
	applyFn func(data []byte) *Result
	BucketPath []string `json:"bucketPath"`
	onChange   []ChangeHandlerFunc
	Records    *sync.Map `json:"records"`
}

func (s *sBucket) Store() *sync.Map {
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
	return gob.NewEncoder(w).Encode(s)
}

func (s *sBucket) Decode(r io.Reader) error {
	return gob.NewDecoder(r).Decode(s)
}

func (s *sBucket) Nested(key string) Bucket {
	val, ok := s.Records.Load(key)
	bucketPath := s.BucketPath
	bucketPath = append(bucketPath, key)
	if !ok {
		return &sBucket{
			applyFn: s.applyFn,
			BucketPath: bucketPath,
			onChange:   nil,
			Records:    &sync.Map{},
		}
	}
	bucket, ok := val.(Bucket)
	if !ok {
		return &sBucket{
			applyFn: s.applyFn,
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
	return s.applyFn(buf.Bytes()).Err
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
	return s.applyFn(buf.Bytes()).Err
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
	Close() error
	Destroy() error
	Raft() Raft
}

type mappy struct {
	*sBucket
	o *Opts
	rft    *raft.Raft
	logger hclog.Logger
	shutdown bool
}

func (m *mappy) Raft() Raft {
	return &rft{impl: m.rft}
}

func (m *mappy) Persist(sink raft.SnapshotSink) error {
	if err := m.Encode(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (m *mappy) Release() {

}

func (m *mappy) Apply(log *raft.Log) interface{} {
	lg, err := fromLog(log)
	if err != nil {
		return err
	}
	switch lg.Op {
	case DELETE:
		m.Bucket(lg.Old).Store().Delete(lg)
		return m.Bucket(lg.Old).HandleChange(lg)
	case EXPIRE:
		m.Bucket(lg.Old).Store().Delete(lg)
		return m.Bucket(lg.Old).HandleChange(lg)
	case SET:
		m.Bucket(lg.Old).Store().Store(lg.New.Key, lg.New)
		return m.Bucket(lg.Old).HandleChange(lg)
	default:
		return m.Bucket(lg.Old).HandleChange(lg)
	}
}

func (m *mappy) Snapshot() (raft.FSMSnapshot, error) {
	return m, nil
}

func (m *mappy) Restore(closer io.ReadCloser) error {
	return m.Decode(closer)
}

type Opts struct {
	Leader bool
	Path     string
	LocalID  string
	LogLevel string
	Listen string
	MaxPool int
	SnapshotRetention int
	Timeout time.Duration
}

var DefaultOpts = &Opts{
	Leader: true,
	Path:     "/tmp/mappy",
	LocalID: fmt.Sprintf("127.0.0.1:8765"),
	LogLevel: "DEBUG",
	Listen: fmt.Sprintf("127.0.0.1:8765"),
	MaxPool: 5,
	SnapshotRetention: 1,
	Timeout: 5 *time.Second,
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
	snapshotStore, err := raft.NewFileSnapshotStore(opts.Path, opts.SnapshotRetention, os.Stdout)
	if err != nil {
		return nil, err
	}
	addr, err := net.ResolveTCPAddr("tcp", opts.Listen)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(opts.Listen, addr, opts.MaxPool, opts.Timeout, os.Stderr)
	if err != nil {
		return nil, err
	}
	config.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "mappy",
		Level:  hclog.LevelFromString(opts.LogLevel),
		Output: os.Stderr,
	})
	config.LocalID = raft.ServerID(opts.LocalID)
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
	m.applyFn = func(data []byte) *Result {
		fut := r.Apply(data, opts.Timeout)
		if fut.Error() != nil {
			return &Result{
				Err:   fut.Error(),
			}
		}
		r := &Result{
			Index: fut.Index(),
		}
		if l, ok := fut.Response().(*Log); ok {
			r.Log = l
		}
		return r
	}
	if opts.Leader {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		if err := r.BootstrapCluster(configuration).Error(); err != nil && err != raft.ErrCantBootstrap {
			return nil, err
		}
	}
	m.o = opts
	m.rft = r
	return m, nil
}

func (m *mappy) Close() error {
	if err :=  m.rft.Shutdown().Error(); err != nil {
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
	Log *Log
	Err error
}

func (m *mappy) Destroy() error {
	if !m.shutdown {
		if err := m.Close(); err != nil {
			return err
		}
	}
	return os.RemoveAll(m.o.Path)
}