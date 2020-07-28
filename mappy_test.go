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
	if closer, err := mapp.Restore(); err != nil {
		t.Fatal(err.Error())
	} else {
		closer()
	}
	bucket := mapp.Nested("users").Nested("colemanword@gmail.com")
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
	if err := mapp.Replay(0,3, func(lg *mappy.Log) error {
		switch lg.Op {
		case mappy.DELETE:
			t.Logf("DELETE: %s\n", lg.Old.Key)
		case mappy.SET:
			t.Logf("SET: %s\n", lg.New.Key)
		}
		return nil
	}); err != nil {
		t.Fatal(err.Error())
	}
	if err := mapp.Close(); err != nil {
		t.Fatal(err.Error())
	}
}
