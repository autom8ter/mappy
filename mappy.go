//go:generate godocdown -o README.md

package mappy

import (
	"bytes"
	"go.etcd.io/bbolt"
	"log"
	"os"
	"strings"
	"sync"
)

type Mappy interface {
	Bucket
	GetRecord(globalId string) (*Record, bool)
	GetBucket(path []interface{}) Bucket
	Close(opts *CloseOpts) error
	DestroyLogs(opts *DestroyOpts) error
	ReplayLogs(opts *ReplayOpts) error
	BackupLogs(opts *BackupOpts) (int64, error)
}

type mappy struct {
	mu *sync.Mutex
	db *bbolt.DB
	*sBucket
	o       *Opts
	closing bool
	done    bool
}

type Opts struct {
	Path    string
	Restore bool
}

var DefaultOpts = &Opts{
	Path:    "/tmp/mappy",
	Restore: true,
}

func Open(opts *Opts) (Mappy, error) {
	if _, err := os.Stat(opts.Path); os.IsNotExist(err) {
		os.MkdirAll(opts.Path, 0777)
	}
	logStore, err := bbolt.Open(opts.Path+"/logs.db", 0700, bbolt.DefaultOptions)
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
			disableLogs: false,
			logChan:     make(chan *Log),
			BucketPath:  nil,
			onChange:    nil,
			Records:     &sync.Map{},
			nested:      &sync.Map{},
		},
	}
	go func() {
		wg := sync.WaitGroup{}
		for !m.closing {
			select {
			case msg := <-m.logChan:
				if msg == nil || m.disableLogs {
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
		return m, m.restore(nil)
	}
	return m, nil
}

func (m *mappy) Close(opts *CloseOpts) error {
	log.Println("mappy: closing...")
	close(m.logChan)
	m.closing = true
	for {
		if m.done {
			if err := m.db.Close(); err != nil {
				log.Printf("mappy: %s\n", err.Error())
			}
			break
		}
	}

	return nil
}

func (m *mappy) GetBucket(path []interface{}) Bucket {
	var bucket = m.Nest(path[0])
	if len(path) >= 2 {
		for _, p := range path[1:] {
			bucket = bucket.Nest(p)
		}
	}
	return bucket
}

func (m *mappy) GetRecord(globalId string) (*Record, bool) {
	path := strings.Split(globalId, pathSeparator)
	var iSlice []interface{}
	for _, p := range path[:len(path)-1] {
		iSlice = append(iSlice, p)
	}
	bucket := m.GetBucket(iSlice)
	return bucket.Get(&GetOpts{Key: path[len(path)-1]})
}

func (m *mappy) ReplayLogs(opts *ReplayOpts) error {
	return m.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		c := bucket.Cursor()
		for k, v := c.Seek(uint64ToBytes(uint64(opts.Min))); k != nil && bytes.Compare(k, uint64ToBytes(uint64(opts.Max))) <= 0; k, v = c.Next() {
			lg := &Log{}
			if err := lg.decode(bytes.NewBuffer(v)); err != nil {
				return err
			}
			if err := opts.Fn(m, lg); err != nil {
				return err
			}
		}
		return nil
	})

}

func (m *mappy) Snapshot() {

}

func (m *mappy) restore(opts *RestoreOpts) error {
	if err := m.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			lg := &Log{}
			if err := lg.decode(bytes.NewBuffer(v)); err != nil {
				return err
			}
			b := m.GetBucket(lg.Record.BucketPath)
			switch lg.Op {
			case DELETE:
				if err := b.Del(&DelOpts{Key: lg.Record.Key}); err != nil {
					return err
				}
			case SET:
				if _, err := b.Set(&SetOpts{Record: lg.Record}); err != nil {
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

func (m *mappy) DestroyLogs(opts *DestroyOpts) error {
	if !m.done {
		if err := m.Close(&CloseOpts{}); err != nil {
			return err
		}
	}
	return os.RemoveAll(m.o.Path)
}

func (m *mappy) BackupLogs(opts *BackupOpts) (int64, error) {
	var count int64
	if err := m.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		bits, err := bucket.Tx().WriteTo(opts.Dest)
		count = bits
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return count, err
	}
	return count, nil
}
