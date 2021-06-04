package datastore

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

const segPreffix = "segment-"
const segActive = "active"
const bufSize = 8192

type segment struct {
	outputPath string
	outOffset  int64
	index      hashIndex
}

func (s *segment) recover() error {
	input, err := os.Open(s.outputPath)
	if err != nil {
		return err
	}
	defer input.Close()
	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)
		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)
		if err == nil {
			if n != int(size) {
				return ErrReadFile
			}
			var e entry
			e.Decode(data)
			s.index[e.key] = s.outOffset
			s.outOffset += int64(n)
		}
	}
	return err
}

func (s *segment) get(key string) (string, error) {
	position, ok := s.index[key]
	if !ok {
		return "", ErrNotFound
	}
	file, err := os.Open(s.outputPath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}
	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}
