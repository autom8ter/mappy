package mappy_test

import (
	"github.com/autom8ter/mappy"
	"testing"
	"time"
)

func Test(t *testing.T) {
	mapp, err := mappy.Open(mappy.DefaultOpts)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = mapp.Restore()
	if err != nil {
		t.Fatal(err.Error())
	}

	bucket := mapp.Nested("users").Nested("colemanword@gmail.com")
	t.Log(bucket.Path())
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

	if err := bucket.Set(&mappy.Record{
		Key: "name",
		Val: "Coleman Word",
	}); err != nil {
		t.Fatal(err.Error())
	}
	time.Sleep(1 * time.Second)
	res, _ := bucket.Get("name")
	if res.Val != "Coleman Word" {
		t.Fatalf("expected: %s got: %s", "Coleman Word", res.Val)
	}
	if err := bucket.View(func(record *mappy.Record) error {
		t.Logf("record: %s\n", record.JSON())
		return nil
	}); err != nil {
		t.Fatal(err.Error())
	}
	time.Sleep(1 * time.Second)
	if err := mapp.Replay(0, 5, func(lg *mappy.Log) error {
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
	//mapp.Destroy()
}
