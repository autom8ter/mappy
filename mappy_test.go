package mappy_test

import (
	"github.com/autom8ter/mappy"
	"math/rand"
	"os"
	"testing"
	"time"
)

func Test(t *testing.T) {
	mapp, err := mappy.Open(mappy.DefaultOpts)
	if err != nil {
		t.Fatal(err.Error())
	}
	bucket := mapp.Nest(
		&mappy.NestOpts{
			Key: "users",
		}).Nest(&mappy.NestOpts{
		Key: "colemanword@gmail.com",
	})
	t.Log(bucket.Path())
	now := time.Now()
	if err := bucket.Set(&mappy.SetOpts{
		Record: &mappy.Record{
			Key: "name",
			Val: "Coleman Word",
		},
	}); err != nil {
		t.Fatal(err.Error())
	}

	t.Logf("set name : %s", time.Since(now).String())
	now = time.Now()
	res, _ := bucket.Get(&mappy.GetOpts{
		Key: "name",
	})
	t.Logf("get name : %s", time.Since(now).String())
	if res.Val != "Coleman Word" {
		t.Fatalf("expected: %s got: %s", "Coleman Word", res.Val)
	}
	if err := mapp.Flush(nil); err != nil {
		t.Fatal(err.Error())
	}
	time.Sleep(1 * time.Second)
	err = mapp.Restore(&mappy.RestoreOpts{})
	if err != nil {
		t.Fatal(err.Error())
	}
	counter := 0
	if err := bucket.View(&mappy.ViewOpts{
		Fn: func(record *mappy.Record) error {
			counter++
			t.Logf("%v %s\n", counter, record.JSON())
			return nil
		},
	}); err != nil {
		t.Fatal(err.Error())
	}
	if counter == 0 {
		t.Fatal("failed restore")
	}
	if err := mapp.Replay(&mappy.ReplayOpts{
		Min: 0,
		Max: 5,
		Fn: func(lg *mappy.Log) error {
			switch lg.Op {
			case mappy.DELETE:
				t.Logf("DELETE: %v %v\n", lg.Sequence, lg.Record.JSON())
			case mappy.SET:
				t.Logf("SET: %v %v\n", lg.Sequence, lg.Record.JSON())
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err.Error())
	}
	if err := mapp.Close(&mappy.CloseOpts{}); err != nil {
		t.Fatal(err.Error())
	}
	mapp.Destroy(&mappy.DestroyOpts{})
}

/* go test -bench Benchmark
goos: darwin
goarch: amd64
pkg: github.com/autom8ter/mappy
Benchmark/SETUP-8               19399074                52.7 ns/op
Benchmark/SET-8                 1000000000               0.000002 ns/op
Benchmark/GET-8                 1000000000               0.000001 ns/op
*/
func Benchmark(b *testing.B) {
	os.RemoveAll(mappy.DefaultOpts.Path)
	db, err := mappy.Open(mappy.DefaultOpts)
	if err != nil {
		b.Fatal(err.Error())
	}
	var vals = map[string]string{}
	defer db.Destroy(&mappy.DestroyOpts{})
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ReportAllocs()
	b.Run("SETUP", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			vals[string(seededRand.Int())] = string(seededRand.Int())
		}
	})
	b.Run("SET", func(b *testing.B) {
		for k, v := range vals {
			if err := db.Nest(&mappy.NestOpts{
				Key: "testing",
			}).Set(&mappy.SetOpts{
				Record: db.NewRecord(&mappy.RecordOpts{
					Key: k,
					Val: v,
				}),
			}); err != nil {
				b.Fatal(err.Error())
			}
		}
	})
	b.Run("GET", func(b *testing.B) {
		for k, v := range vals {
			res, ok := db.Nest(&mappy.NestOpts{
				Key: "testing",
			}).Get(&mappy.GetOpts{
				Key: k,
			})
			if !ok {
				b.Fatal("value not found")
			}
			if res.Val != v {
				b.Fatal("value mismatch")
			}
		}
	})
}
