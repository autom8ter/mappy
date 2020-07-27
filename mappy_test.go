package mappy_test

import (
	"github.com/autom8ter/mappy"
	"testing"
)

func Test(t *testing.T) {
	mapp, err := mappy.Open(mappy.DefaultOpts)
	if err != nil {
		t.Fatal(err.Error())
	}

	if err := mapp.Close(); err != nil {
		t.Fatal(err.Error())
	}
}