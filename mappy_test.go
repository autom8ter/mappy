package mappy_test

import (
	"encoding/json"
	"fmt"
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
	bucket := mapp.Nest("users").Nest("colemanword@gmail.com")
	t.Log(bucket.Path())
	now := time.Now()
	record, err := bucket.Set(&mappy.SetOpts{
		Record: &mappy.Record{
			Key: "name",
			Val: "Coleman Word",
		},
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Logf("set name : %s", time.Since(now).String())
	now2 := time.Now()
	res, _ := mapp.GetRecord(record.GloablId)
	t.Logf("get globalid : %s", time.Since(now2).String())
	if res != record {
		t.Fatalf("expected match: %s %s", res.GloablId, record.GloablId)
	}
	now3 := time.Now()
	res, _ = bucket.Get(&mappy.GetOpts{
		Key: "name",
	})
	t.Logf("get name : %s", time.Since(now3).String())
	if res.Val != "Coleman Word" {
		t.Fatalf("expected: %s got: %s", "Coleman Word", res.Val)
	}
	if err := mapp.Flush(nil); err != nil {
		t.Fatal(err.Error())
	}
	mapp.Close(nil)
	time.Sleep(1 * time.Second)
	mapp2, err := mappy.Open(mappy.DefaultOpts)
	if err != nil {
		t.Fatal(err.Error())
	}
	bucket2 := mapp2.Nest("users").Nest("colemanword@gmail.com")
	t.Log(bucket2.Path())
	counter := 0
	if err := bucket2.View(&mappy.ViewOpts{
		Fn: func(b mappy.Bucket, record *mappy.Record) error {
			counter++
			t.Logf("after restore: %v %s\n", counter, jSON(record))
			return nil
		},
	}); err != nil {
		t.Fatal(err.Error())
	}
	if counter == 0 {
		t.Fatal("failed restore")
	}
	if err := mapp2.ReplayLogs(&mappy.ReplayOpts{
		Min: 0,
		Max: 5,
		Fn: func(bucket mappy.Bucket, lg *mappy.Log) error {
			switch lg.Op {
			case mappy.DELETE:
				t.Logf("DELETE: %v %v\n", lg.Sequence, jSON(lg.Record))
			case mappy.SET:
				t.Logf("SET: %v %v\n", lg.Sequence, jSON(lg.Record))
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err.Error())
	}
	f, _ := os.Create("backup.txt")
	defer f.Close()
	bits, err := mapp2.BackupLogs(&mappy.BackupOpts{
		Dest: f,
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Logf("wrote: %v", bits)
	if err := mapp2.Close(&mappy.CloseOpts{}); err != nil {
		t.Fatal(err.Error())
	}
	mapp2.DestroyLogs(&mappy.DestroyOpts{})
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
	defer db.DestroyLogs(&mappy.DestroyOpts{})
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ReportAllocs()
	b.Run("SETUP", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			vals[string(seededRand.Int())] = string(seededRand.Int())
		}
	})
	b.Run("SET", func(b *testing.B) {
		for k, v := range vals {
			if _, err := db.Nest("testing").Set(&mappy.SetOpts{
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
			res, ok := db.Nest("testing").Get(&mappy.GetOpts{
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

func jSON(obj interface{}) string {
	bits, _ := json.MarshalIndent(obj, "", "    ")
	return fmt.Sprintf("%s", bits)
}
