package mappy

import (
	"bytes"
	"go.etcd.io/bbolt"
	"log"
	"os"
	"sync"
)

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
	o       *Opts
	closing bool
	done    bool
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
		return m, m.Restore()
	}
	return m, nil
}

func (m *mappy) Close() error {
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

func (m *mappy) Bucket(path []string) Bucket {
	var bucket = m.Nest(&NestOpts{Key: path[0]})
	if len(path) >= 2 {
		for _, p := range path[1:] {
			bucket = bucket.Nest(&NestOpts{Key: p})
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
	if err := m.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("logs"))
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			lg := &Log{}
			if err := lg.decode(bytes.NewBuffer(v)); err != nil {
				return err
			}
			b := m.Bucket(lg.Record.BucketPath)
			switch lg.Op {
			case DELETE:
				if err := b.Del(&DelOpts{Key: lg.Record.Key}); err != nil {
					return err
				}
			case SET:
				if err := b.Set(&SetOpts{Record: lg.Record}); err != nil {
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
