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
	bucket := mapp.Nest("users").Nest("colemanword@gmail.com")
	t.Log(bucket.Path())
	now := time.Now()
	if err := bucket.Set(&mappy.Record{
		Key: "name",
		Val: "Coleman Word",
	}); err != nil {
		t.Fatal(err.Error())
	}

	t.Logf("set name : %s", time.Since(now).String())
	now = time.Now()
	res, _ := bucket.Get("name")
	t.Logf("get name : %s", time.Since(now).String())
	if res.Val != "Coleman Word" {
		t.Fatalf("expected: %s got: %s", "Coleman Word", res.Val)
	}
	if err := mapp.Flush(); err != nil {
		t.Fatal(err.Error())
	}
	time.Sleep(1 *time.Second)
	err = mapp.Restore()
	if err != nil {
		t.Fatal(err.Error())
	}
	counter := 0
	if err := bucket.View(func(record *mappy.Record) error {
		counter++
		t.Logf("%v %s\n", counter, record.JSON())
		return nil
	}); err != nil {
		t.Fatal(err.Error())
	}
	if counter == 0 {
		t.Fatal("failed restore")
	}
	if err := mapp.Replay(0, 2, func(lg *mappy.Log) error {
		switch lg.Op {
		case mappy.DELETE:
			t.Logf("DELETE: %v\n", lg.Record.JSON())
		case mappy.SET:
			t.Logf("SET: %v\n", lg.Record.JSON())
		}
		return nil
	}); err != nil {
		t.Fatal(err.Error())
	}
	if err := mapp.Close(); err != nil {
		t.Fatal(err.Error())
	}
	mapp.Destroy()
}

func BenchmarkSet(b *testing.B) {
	os.RemoveAll(mappy.DefaultOpts.Path)
	db, err := mappy.Open(mappy.DefaultOpts)
	if err != nil {
		b.Fatal(err.Error())
	}
	defer db.Destroy()
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := db.Nest("testing").Set(db.NewRecord(string(seededRand.Int()), string(seededRand.Int()))); err != nil {
			b.Fatal(err.Error())
		}
	}
}
