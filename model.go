package mappy

import (
	"bytes"
	"encoding/gob"
	"errors"
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
	Key        interface{}   `json:"key"`
	Val        interface{}   `json:"val"`
	BucketPath []interface{} `json:"bucketPath"`
	GloablId   string        `json:"globalId"`
	UpdatedAt  time.Time     `json:"updatedAt"`
}

func NewRecord(opts *RecordOpts) *Record {
	return &Record{
		Key: opts.Key,
		Val: opts.Val,
	}
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

type ViewFunc func(bucket Bucket, record *Record) error
type ReplayFunc func(bucket Bucket, lg *Log) error
type ChangeHandlerFunc func(bucket Bucket, log *Log) error

type ViewOpts struct {
	ViewFn ViewFunc
}

type SetOpts struct {
	Record *Record
}

type GetOpts struct {
	Key string
}

type DelOpts struct {
	Key interface{}
}

type RecordOpts struct {
	Key interface{}
	Val interface{}
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

type BackupOpts struct {
	Dest io.Writer
}

type BucketOpts struct {
	Path     []string
	GlobalId string
}