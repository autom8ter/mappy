package mappy

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

func init() {
	gob.Register(&Record{})
	gob.Register(&Log{})
}

type Op int

const (
	DELETE Op = 2
	SET    Op = 3
)

var Done = errors.New("mappy: done")

type Record struct {
	Key        string      `json:"key"`
	Val        interface{} `json:"val"`
	BucketPath []string    `json:"bucketPath"`
	UpdatedAt  time.Time   `json:"updatedAt"`
}

func (r *Record) JSON() string {
	bits, _ := json.MarshalIndent(r, "", "    ")
	return fmt.Sprintf("%s", bits)
}

type Log struct {
	Sequence  int
	Op        Op
	Record    *Record
	CreatedAt time.Time
}

func (l *Log) encode() (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	return buf, gob.NewEncoder(buf).Encode(l)
}

func (l *Log) decode(r io.Reader) error {
	return gob.NewDecoder(r).Decode(l)
}

type ViewFunc func(record *Record) error
type ReplayFunc func(lg *Log) error
type ChangeHandlerFunc func(log *Log) error

type ViewOpts struct {
	Fn ViewFunc
}

type SetOpts struct {
	Record *Record
}

type GetOpts struct {
	Key string
}

type DelOpts struct {
	Key string
}

type RecordOpts struct {
	Key string
	Val interface{}
}

type NestOpts struct {
	Key string
}

type LenOpts struct {
}

type FlushOpts struct {
}

type CloseOpts struct {
}

type DestroyOpts struct {
}

type RestoreOpts struct {
}

type ReplayOpts struct {
	Min int
	Max int
	Fn  ReplayFunc
}
