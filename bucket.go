package mappy

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

const pathSeparator = "##"

type Bucket interface {
	Key() interface{}
	Path() []interface{}
	NewRecord(opts *RecordOpts) *Record
	Nest(key interface{}) Bucket
	NestedBuckets() []Bucket
	Del(opts *DelOpts) error
	Flush(opts *FlushOpts) error
	Count(opts *LenOpts) int
	Get(opts *GetOpts) (value *Record, ok bool)
	Set(opts *SetOpts) (*Record, error)
	View(opts *ViewOpts) error
	OnChange(fns ...ChangeHandlerFunc)
}

type sBucket struct {
	BucketPath  []interface{}
	onChange    []ChangeHandlerFunc
	disableLogs bool
	Records     *sync.Map
	logChan     chan *Log
	nested      *sync.Map
}

func (s *sBucket) Key() interface{} {
	return s.BucketPath[0]
}

func (s *sBucket) Path() []interface{} {
	return s.BucketPath
}

func (s *sBucket) NewRecord(opts *RecordOpts) *Record {
	return &Record{
		Key:        opts.Key,
		Val:        opts.Val,
		BucketPath: s.BucketPath,
		UpdatedAt:  time.Now(),
	}
}

func (s *sBucket) Nest(key interface{}) Bucket {
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
		disableLogs: s.disableLogs,
		logChan:     s.logChan,
		BucketPath:  bucketPath,
		onChange:    nil,
		Records:     &sync.Map{},
		nested:      &sync.Map{},
	}
	s.nested.Store(key, b)
	return b
}

func (s *sBucket) bucket() Bucket {
	return s
}

func (s *sBucket) Count(opts *LenOpts) int {
	counter := 0
	_ = s.View(&ViewOpts{
		ViewFn: func(bucket Bucket, record *Record) error {
			if record != nil {
				counter++
			}
			return nil
		}})
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
	if record.BucketPath == nil {
		record.BucketPath = s.BucketPath
	}
	return record, true
}

func (s *sBucket) Del(opts *DelOpts) error {
	s.Records.Delete(opts.Key)
	before, _ := s.getRecord(opts.Key)
	lg := &Log{
		Op:        DELETE,
		Record:    before,
		CreatedAt: time.Now(),
	}
	s.logChan <- lg
	for _, fn := range s.onChange {
		if err := fn(s, lg); err != nil {
			return err
		}
	}
	return nil
}

func (s *sBucket) Get(opts *GetOpts) (*Record, bool) {
	return s.getRecord(opts.Key)
}

func (s *sBucket) Set(opts *SetOpts) (*Record, error) {
	if !s.disableLogs {
		opts.Record.UpdatedAt = time.Now()
	}
	opts.Record.BucketPath = s.BucketPath
	var path []string
	for _, i := range opts.Record.BucketPath {
		path = append(path, fmt.Sprintf("%v", i))
	}
	path = append(path, fmt.Sprintf("%v", opts.Record.Key))
	opts.Record.GloablId = strings.Join(path, pathSeparator)
	s.Records.Store(opts.Record.Key, opts.Record)
	lg := &Log{
		Op:        SET,
		Record:    opts.Record,
		CreatedAt: time.Now(),
	}
	s.logChan <- lg
	for _, fn := range s.onChange {
		if err := fn(s, lg); err != nil {
			return nil, err
		}
	}
	return opts.Record, nil
}

func (s *sBucket) View(opts *ViewOpts) error {
	var errs []error
	s.Records.Range(func(key interface{}, value interface{}) bool {
		record, ok := value.(*Record)
		if ok {
			if err := opts.ViewFn(s, record); err != nil {
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

func (m *sBucket) Flush(opts *FlushOpts) error {
	m.nested.Range(func(key, value interface{}) bool {
		if b, ok := value.(*sBucket); ok {
			b.Flush(opts)
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

func (s *sBucket) OnChange(fns ...ChangeHandlerFunc) {
	s.onChange = fns
}
func (s *sBucket) NestedBuckets() []Bucket {
	var buckets = []Bucket{}
	s.nested.Range(func(key, value interface{}) bool {
		if b, ok := value.(*sBucket); ok && b != nil {
			buckets = append(buckets, b)
		}
		return true
	})
	return buckets
}
