package cmap_test

import (
	"bytes"
	"github.com/autom8ter/mappy/cmap"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)


func TestMap_Encoding(t *testing.T) {
	cm := &cmap.Map{}
	cm.Store("name", "Coleman")

	cm.Range(func(key string, value interface{}) bool {
		t.Log(key, value)
		return true
	})
	buf := bytes.NewBuffer(nil)
	if err := cm.Encode(buf); err != nil {
		t.Fatal(err.Error())
	}
	cm2 := &cmap.Map{}
	if err := cm2.Decode(buf); err != nil {
		t.Fatal(err.Error())
	}
	cm2.Range(func(key string, value interface{}) bool {
		t.Log(key, value)
		return true
	})
}

func TestConcurrentRange(t *testing.T) {
	const mapSize = 1 << 10

	m := new(sync.Map)
	for n := int64(1); n <= mapSize; n++ {
		m.Store(n, int64(n))
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(done)
		wg.Wait()
	}()
	for g := int64(runtime.GOMAXPROCS(0)); g > 0; g-- {
		r := rand.New(rand.NewSource(g))
		wg.Add(1)
		go func(g int64) {
			defer wg.Done()
			for i := int64(0); ; i++ {
				select {
				case <-done:
					return
				default:
				}
				for n := int64(1); n < mapSize; n++ {
					if r.Int63n(mapSize) == 0 {
						m.Store(n, n*i*g)
					} else {
						m.Load(n)
					}
				}
			}
		}(g)
	}

	iters := 1 << 10
	if testing.Short() {
		iters = 16
	}
	for n := iters; n > 0; n-- {
		seen := make(map[int64]bool, mapSize)

		m.Range(func(ki, vi interface{}) bool {
			k, v := ki.(int64), vi.(int64)
			if v%k != 0 {
				t.Fatalf("while Storing multiples of %v, Range saw value %v", k, v)
			}
			if seen[k] {
				t.Fatalf("Range visited key %v twice", k)
			}
			seen[k] = true
			return true
		})

		if len(seen) != mapSize {
			t.Fatalf("Range visited %v elements of %v-element Map", len(seen), mapSize)
		}
	}
}
