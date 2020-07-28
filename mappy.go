package mappy

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"go.etcd.io/bbolt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

func init() {
	gob.Register(&StoredRecord{})
	gob.Register(&Log{})
}

type Op int

const (
	DELETE Op = 2
	SET    Op = 3
)

var Done = errors.New("mappy: done")

type Record struct {
	Key        string      `json:"key"`
	Val        interface{} `json:"val"`
	bucketPath []string
	updatedAt  time.Time
}

func (r *Record) toStored() *StoredRecord {
	s := &StoredRecord{
		BucketPath: r.bucketPath,
		Key:        r.Key,
		Val:        r.Val,
		UpdatedAt:  r.updatedAt,
	}
	return s
}

func (r *Record) BucketPath() []string {
	return r.bucketPath
}

func (r *Record) UpdatedAt() time.Time {
	return r.updatedAt
}

func (r *Record) JSON() string {
	bits, _ := json.MarshalIndent(r, "", "    ")
	return fmt.Sprintf("%s", bits)
}

type StoredRecord struct {
	BucketPath []string    `json:"bucketPath"`
	Key        string      `json:"key"`
	Val        interface{} `json:"val"`
	UpdatedAt  time.Time   `json:"updatedAt"`
}

func (s *StoredRecord) Record() *Record {
	return &Record{
		Key:        s.Key,
		Val:        s.Val,
		bucketPath: s.BucketPath,
		updatedAt:  s.UpdatedAt,
	}
}

type Log struct {
	Sequence  int
	Op        Op
	New       *StoredRecord
	Old       *StoredRecord
	CreatedAt time.Time
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
	Store() *sync.Map
}

type sBucket struct {
	restoring  bool
	BucketPath []string
	onChange   []ChangeHandlerFunc
	Records    *sync.Map
	logChan    chan *Log
}

func (s *sBucket) Store() *sync.Map {
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
			restoring:  s.restoring,
			logChan:    s.logChan,
			BucketPath: bucketPath,
			onChange:   nil,
			Records:    &sync.Map{},
		}
	}
	bucket, ok := val.(Bucket)
	if !ok {
		return &sBucket{
			restoring:  s.restoring,
			logChan:    s.logChan,
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
	s.Records.Delete(key)
	if !s.restoring {
		before, _ := s.getRecord(key)
		lg := &Log{
			Op:        DELETE,
			New:       nil,
			Old:       before.toStored(),
			CreatedAt: time.Now(),
		}
		s.logChan <- lg
		for _, fn := range s.onChange {
			if err := fn(lg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sBucket) Get(key string) (*Record, bool) {
	return s.getRecord(key)
}

func (s *sBucket) Set(record *Record) error {
	if !s.restoring {
		record.updatedAt = time.Now()
	}
	s.Records.Store(record.Key, record)
	if !s.restoring {
		before, _ := s.getRecord(record.Key)
		lg := &Log{
			Op:        SET,
			New:       record.toStored(),
			Old:       before.toStored(),
			CreatedAt: time.Now(),
		}
		s.logChan <- lg

		for _, fn := range s.onChange {
			if err := fn(lg); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *sBucket) View(fn ViewFunc) error {
	var errs []error
	s.Records.Range(func(key interface{}, value interface{}) bool {
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
	Replay(min, max int, fn ReplayFunc) error
	Restore() (func(), error)
	Bucket(r *Record) Bucket
	Close() error
	Destroy() error
}

type mappy struct {
	mu *sync.Mutex
	db *bbolt.DB
	*sBucket
	o         *Opts
	done      chan (struct{})
	restoring bool
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
			restoring:  false,
			logChan:    make(chan *Log),
			BucketPath: nil,
			onChange:   nil,
			Records:    &sync.Map{},
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
				if !m.restoring {
					if err := m.db.Update(func(tx *bbolt.Tx) error {
						bucket := tx.Bucket([]byte("logs"))
						seq, _ := bucket.NextSequence()
						lg.Sequence = int(seq)
						buf, err := lg.encode()
						if err != nil {
							return err
						}
						if err := bucket.Put(uint64ToBytes(seq), buf.Bytes()); err != nil {
							return err
						}
						return nil
					}); err != nil {
						log.Printf("mappy: %s\n", err.Error())
					}
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

func (m *mappy) Replay(min, max int, fn ReplayFunc) error {
	return m.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		c := bucket.Cursor()
		// Iterate over the 90's.
		for k, v := c.Seek(uint64ToBytes(uint64(min))); k != nil && bytes.Compare(k, uint64ToBytes(uint64(max))) <= 0; k, v = c.Next() {
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

func (m *mappy) Restore() (func(), error) {
	m.restoring = true
	return func() {
			m.restoring = false
		}, m.db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket([]byte("logs"))
			c := bucket.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				fmt.Println(string(k))
				lg := &Log{}
				if err := lg.decode(bytes.NewBuffer(v)); err != nil {
					return err
				}
				switch lg.Op {
				case DELETE:
					if err := m.Bucket(lg.Old.Record()).Delete(lg.Old.Key); err != nil {
						return err
					}
				case SET:
					if err := m.Bucket(lg.New.Record()).Set(lg.New.Record()); err != nil {
						return err
					}
				}
			}
			return nil
		})
}

func (m *mappy) Destroy() error {
	return os.RemoveAll(m.o.Path)
}
