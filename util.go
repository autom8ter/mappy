package mappy

import (
	"bytes"
	"encoding/binary"
	"github.com/hashicorp/go-msgpack/codec"
	"io"
)

// Decode reverses the encode operation on a byte slice input
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

func writeMsgPack(from interface{}, w io.Writer) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(w, &hd)
	return enc.Encode(from)
}

func readMsgPack(to interface{}, r io.Reader) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewDecoder(r, &hd)
	return enc.Decode(to)
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
