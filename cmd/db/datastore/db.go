package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const segPreffix = "segment-"
const segActive = "active"
const segSize = 10 * 1024 * 1024

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out      *os.File
	dir      string
	segments []*segment

	mux *sync.Mutex
}

type segment struct {
	outputPath string
	outOffset  int64
	index      hashIndex
}

func NewDb(dir string) (*Db, error) {
	outputPath := filepath.Join(dir, segPreffix+segActive)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	var segments []*segment
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		fileName := file.Name()
		if strings.HasPrefix(fileName, segPreffix) {
			seg := &segment{
				outputPath: filepath.Join(dir, fileName),
				index:      make(hashIndex),
			}
			err := seg.recover()
			if err != io.EOF {
				return nil, err
			}
			segments = append(segments, seg)
		}
	}
	sort.Slice(segments, func(l, r int) bool {
		lSuffixString := segments[l].outputPath[len(dir+segPreffix)+1:]
		rSuffixString := segments[r].outputPath[len(dir+segPreffix)+1:]
		if lSuffixString == segActive {
			return true
		}
		if rSuffixString == segActive {
			return false
		}

		lSuffix, lErr := strconv.Atoi(lSuffixString)
		rSuffix, rErr := strconv.Atoi(rSuffixString)

		return rErr != nil || (lErr == nil && lSuffix < rSuffix)
	})
	db := &Db{
		out:      f,
		dir:      dir,
		segments: segments,
		mux:      new(sync.Mutex),
	}
	return db, nil
}

const bufSize = 8192

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
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			s.index[e.key] = s.outOffset
			s.outOffset += int64(n)
		}
	}
	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	var s *segment
	var position int64
	for _, seg := range db.segments {
		var ok bool
		position, ok = seg.index[key]
		if ok {
			s = seg
			break
		}
	}
	if s == nil {
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

func (db *Db) NewSeg() (*segment, error) {
	err := db.out.Close()
	if err != nil {
		return nil, err
	}
	outputPath := filepath.Join(db.dir, segPreffix+segActive)
	segOutputPath := filepath.Join(db.dir, fmt.Sprintf("%v_%v", segPreffix, len(db.segments)))
	err = os.Rename(outputPath, segOutputPath)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db.out = f
	seg := &segment{
		outputPath: outputPath,
		index:      make(hashIndex),
	}
	db.segments = append(db.segments, seg)
	return seg, nil
}

func (db *Db) lastSeg() *segment {
	return db.segments[len(db.segments)-1]
}

func (db *Db) Put(key, value string) error {
	e := &entry{
		key:   key,
		value: value,
	}
	activeSeg := db.lastSeg()
	f, err := os.Stat(activeSeg.outputPath)
	if err != nil {
		return err
	}
	if f.Size()+int64(len(e.Encode())) >= segSize {
		activeSeg, err = db.NewSeg()
		if err != nil {
			return err
		}
	}
	n, err := db.out.Write(e.Encode())
	if err == nil {
		activeSeg.index[key] = activeSeg.outOffset
		activeSeg.outOffset += int64(n)
	}
	return err
}
