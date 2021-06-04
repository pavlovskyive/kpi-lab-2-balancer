package datastore

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	key := "key"
	val := "value"
	hash := sha1.Sum([]byte(val))

	e := entry{key, val, hash[:]}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
	if !bytes.Equal(e.hash, hash[:]) {
		t.Error("incorrect hash")
	}
}

func TestReadValue(t *testing.T) {
	key := "key"
	val := "value"
	hash := sha1.Sum([]byte(val))

	e := entry{key, val, hash[:]}
	data := e.Encode()
	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v != "value" {
		t.Errorf("Got bad value [%s]", v)
	}
}
