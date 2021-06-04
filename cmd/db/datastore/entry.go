package datastore

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key, value string
	hash       []byte
}

type entryChan struct {
	entry   *entry
	resChan chan error
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	hl := len(e.hash)

	size := kl + vl + hl + 16

	res := make([]byte, size)

	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl+12:], e.value)
	binary.LittleEndian.PutUint32(res[kl+vl+12:], uint32(hl))
	copy(res[kl+vl+16:], e.hash)
	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	vl := binary.LittleEndian.Uint32(input[kl+8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+12:kl+12+vl])
	e.value = string(valBuf)

	hl := binary.LittleEndian.Uint32(input[kl+vl+12:])
	hashBuf := make([]byte, hl)
	copy(hashBuf, input[kl+vl+16:kl+vl+16+hl])
	e.hash = hashBuf
}

func readValue(in *bufio.Reader) (string, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	value := string(data)
	hash := sha1.Sum([]byte(value))

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	hashSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	hashSaved := make([]byte, hashSize)
	n, err = in.Read(hashSaved)
	if err != nil {
		return "", err
	}
	if n != hashSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	if !bytes.Equal(hash[:], hashSaved) {
		return "", fmt.Errorf("hashes not equal")
	}

	return string(data), nil
}
