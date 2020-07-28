package mappy

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/autom8ter/mappy/cmap"
	"go.etcd.io/bbolt"
	"io"
	"log"
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
	Exp        time.Time   `json:"exp"`
	bucketPath []string
	updatedAt  int64
}

func (r *Record) toStored() *storedRecord {
	s := &storedRecord{
		BucketPath: r.bucketPath,
		Key:        r.Key,
		Val:        r.Val,
		UpdatedAt:  r.updatedAt,
	}
	if r.Exp.After(time.Now()) {
		s.Exp = r.Exp.Unix()
	}
	return s
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
	Sequence  int
	Op        Op
	New       *Record
	Old       *Record
	CreatedAt int64
}

func (l *Log) encode() (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	return buf, gob.NewEncoder(buf).Encode(l)
}

func (l *Log) decode(r io.Reader) error {
	return gob.NewDecoder(r).Decode(l)
}

type ViewFunc func(record *Record) error
type ReplayFunc func(lg *Log) error
type ChangeHandlerFunc func(log *Log) error

type Bucket interface {
	Nested(key string) Bucket
	Delete(key string) error
	Len() int
	Get(key string) (value *Record, ok bool)
	Set(record *Record) error
	View(fn ViewFunc) error
	Decode(r io.Reader) error
	Encode(w io.Writer) error
	Store() *cmap.Map
}

type sBucket struct {
	BucketPath []string
	onChange   []ChangeHandlerFunc
	Records    *cmap.Map
	logChan    chan *Log
}

func (s *sBucket) Store() *cmap.Map {
	return s.Records
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
			logChan:    s.logChan,
			BucketPath: bucketPath,
			onChange:   nil,
			Records:    &cmap.Map{},
		}
	}
	bucket, ok := val.(Bucket)
	if !ok {
		return &sBucket{
			logChan:    s.logChan,
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
	s.logChan <- lg
	s.Records.Delete(key)
	for _, fn := range s.onChange {
		if err := fn(lg); err != nil {
			return err
		}
	}

	return nil
}

func (s *sBucket) Get(key string) (*Record, bool) {
	return s.getRecord(key)
}

func (s *sBucket) Set(record *Record) error {
	record.updatedAt = time.Now().Unix()
	before, _ := s.getRecord(record.Key)
	lg := &Log{
		Op:        SET,
		New:       record,
		Old:       before,
		CreatedAt: time.Now().Unix(),
	}
	s.logChan <- lg
	s.Records.Store(record.Key, record)
	for _, fn := range s.onChange {
		if err := fn(lg); err != nil {
			return err
		}
	}
	return nil
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
	Replay(min, max time.Time, fn ReplayFunc) error
	Bucket(r *Record) Bucket
	Close() error
	Destroy() error
}

type mappy struct {
	db *bbolt.DB
	*sBucket
	o    *Opts
	done chan (struct{})
}

type Opts struct {
	Path string
}

var DefaultOpts = &Opts{
	Path: "/tmp/mappy",
}

func Open(opts *Opts) (Mappy, error) {
	if _, err := os.Stat(opts.Path); os.IsNotExist(err) {
		os.MkdirAll(opts.Path, 0777)
	}
	logStore, err := bbolt.Open(opts.Path+"/mappy.db", 0700, bbolt.DefaultOptions)
	if err != nil {
		return nil, err
	}
	if err := logStore.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("logs"))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	m := &mappy{
		db:   logStore,
		o:    opts,
		done: make(chan struct{}, 1),
		sBucket: &sBucket{
			logChan:    make(chan *Log),
			BucketPath: nil,
			onChange:   nil,
			Records:    &cmap.Map{},
		},
	}
	go func() {
		for {
			select {
			case <-m.done:
				log.Println("mappy: closing...")
				if err := m.db.Close(); err != nil {
					log.Printf("mappy: %s\n", err.Error())
				}
				break
			case lg := <-m.logChan:
				if err := m.db.Update(func(tx *bbolt.Tx) error {
					bucket := tx.Bucket([]byte("logs"))
					seq, _ := bucket.NextSequence()
					lg.Sequence = int(seq)
					buf, err := lg.encode()
					if err != nil {
						return err
					}
					if err := bucket.Put(uint64ToBytes(uint64(time.Now().UnixNano())), buf.Bytes()); err != nil {
						return err
					}
					return nil
				}); err != nil {
					log.Printf("mappy: %s\n", err.Error())
				}
			}
		}
	}()
	return m, nil
}

func (m *mappy) Close() error {
	m.done <- struct{}{}
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

func (m *mappy) Replay(min, max time.Time, fn ReplayFunc) error {
	return m.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		c := bucket.Cursor()
		// Iterate over the 90's.
		for k, v := c.Seek(uint64ToBytes(uint64(min.UnixNano()))); k != nil && bytes.Compare(k, uint64ToBytes(uint64(max.UnixNano()))) <= 0; k, v = c.Next() {
			lg := &Log{}
			if err := lg.decode(bytes.NewBuffer(v)); err != nil {
				return err
			}
			if err := fn(lg); err != nil {
				return err
			}
		}
		return nil
	})

}

func (m *mappy) Destroy() error {
	return os.RemoveAll(m.o.Path)
}
