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
	gob.Register(&Record{})
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
	BucketPath []string    `json:"bucketPath"`
	UpdatedAt  time.Time   `json:"updatedAt"`
}

func (r *Record) JSON() string {
	bits, _ := json.MarshalIndent(r, "", "    ")
	return fmt.Sprintf("%s", bits)
}

type Log struct {
	Sequence  int
	Op        Op
	Record    *Record
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
	Path() []string
	NewRecord(key string, val interface{}) *Record
	Nest(key string) Bucket
	Del(key string) error
	Flush() error
	Len() int
	Get(key string) (value *Record, ok bool)
	Set(record *Record) error
	View(fn ViewFunc) error
	History(bool)
}

type sBucket struct {
	BucketPath []string
	onChange   []ChangeHandlerFunc
	disableLogs bool
	Records    *sync.Map
	logChan    chan *Log
	nested     *sync.Map
}

func (s *sBucket) Path() []string {
	return s.BucketPath
}

func (s *sBucket) History(enable bool) {
	s.disableLogs = enable
}

func (s *sBucket) NewRecord(key string, val interface{}) *Record {
	return &Record{
		Key:        key,
		Val:        val,
		BucketPath: s.BucketPath,
		UpdatedAt:  time.Now(),
	}
}

func (s *sBucket) Store() *sync.Map {
	return s.Records
}

func (s *sBucket) Nest(key string) Bucket {
	v, ok := s.nested.Load(key)
	if ok {
		b, ok := v.(*sBucket)
		if ok {
			return b
		}
	}
	bucketPath := s.BucketPath
	bucketPath = append(bucketPath, key)
	b := &sBucket{
		disableLogs:  s.disableLogs,
		logChan:    s.logChan,
		BucketPath: bucketPath,
		onChange:   nil,
		Records:    &sync.Map{},
		nested:     &sync.Map{},
	}
	s.nested.Store(key, b)
	return b
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

func (s *sBucket) Del(key string) error {
	s.Records.Delete(key)
	before, _ := s.getRecord(key)
	lg := &Log{
		Op:        DELETE,
		Record:    before,
		CreatedAt: time.Now(),
	}
	s.logChan <- lg
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
	if !s.disableLogs {
		record.UpdatedAt = time.Now()
	}
	record.BucketPath = s.BucketPath
	s.Records.Store(record.Key, record)
	lg := &Log{
		Op:        SET,
		Record:    record,
		CreatedAt: time.Now(),
	}
	s.logChan <- lg
	for _, fn := range s.onChange {
		if err := fn(lg); err != nil {
			return err
		}
	}
	return nil
}

func (s *sBucket) View(fn ViewFunc) error {
	var errs []error
	s.Records.Range(func(key interface{}, value interface{}) bool {
		record, ok := value.(*Record)
		if ok {
			if err := fn(record); err != nil {
				if err == Done {
					return false
				}
				errs = append(errs, err)
				return false
			}
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
	Restore() error
	Bucket(path []string) Bucket
	Close() error
	Destroy() error
}

type mappy struct {
	mu *sync.Mutex
	db *bbolt.DB
	*sBucket
	o         *Opts
	closing   bool
	done      bool
}

type Opts struct {
	Path    string
	Restore bool
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
		done: false,
		sBucket: &sBucket{
			disableLogs:  false,
			logChan:    make(chan *Log),
			BucketPath: nil,
			onChange:   nil,
			Records:    &sync.Map{},
			nested:     &sync.Map{},
		},
	}
	go func() {
		wg := sync.WaitGroup{}
		for !m.closing {
			select {
			case msg := <-m.logChan:
				if msg == nil || m.disableLogs{
					continue
				}
				wg.Add(1)
				go func(lg *Log) {
					defer wg.Done()
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
				}(msg)
			}
		}
		wg.Wait()
		m.done = true
	}()
	if opts.Restore {
		return m, m.Restore()
	}
	return m, nil
}

func (m *sBucket) Flush() error {
	m.nested.Range(func(key, value interface{}) bool {
		if b, ok := value.(*sBucket); ok {
			b.Flush()
		} else {
			m.nested.Delete(key)
		}
		return true
	})
	m.Records.Range(func(key, value interface{}) bool {
		m.Records.Delete(key)
		return true
	})
	return nil
}

func (m *mappy) Close() error {
	log.Println("mappy: closing...")
	close(m.logChan)
	m.closing = true
	for  {
		if m.done {
			if err := m.db.Close(); err != nil {
				log.Printf("mappy: %s\n", err.Error())
			}
			break
		}
	}


	return nil
}

func (m *mappy) Bucket(path []string) Bucket {
	var bucket = m.Nest(path[0])
	if len(path) >= 2 {
		for _, p := range path[1:] {
			bucket = bucket.Nest(p)
		}
	}
	return bucket
}

func (m *mappy) Replay(min, max int, fn ReplayFunc) error {
	return m.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		c := bucket.Cursor()
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

func (m *mappy) Restore() error {
	m.History(false)
	defer m.History(true)
	if err := m.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			lg := &Log{}
			if err := lg.decode(bytes.NewBuffer(v)); err != nil {
				return err
			}
			b := m.Bucket(lg.Record.BucketPath)
			defer b.History(true)
			switch lg.Op {
			case DELETE:
				if err := b.Del(lg.Record.Key); err != nil {
					return err
				}
			case SET:
				if err := b.Set(lg.Record); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (m *mappy) Destroy() error {
	if !m.done {
		if err := m.Close(); err != nil {
			return err
		}
	}
	return os.RemoveAll(m.o.Path)
}
