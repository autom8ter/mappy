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
	bucket := mapp.Nested("users").Nested("colemanword@gmail.com")
	if err := bucket.Set(&mappy.Record{
		Key: "name",
		Val: "Coleman Word",
		Exp: time.Now().Add(5 *time.Minute).Unix(),
	}); err != nil {
		t.Fatal(err.Error())
	}
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
	if err := mapp.Close(); err != nil {
		t.Fatal(err.Error())
	}
}