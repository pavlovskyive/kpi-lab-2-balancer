package datastore

import (
	"crypto/sha1"
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

const segSize = 10 * 1024 * 1024 // 10 Mb
const autoMerge = true

var ErrNotFound = fmt.Errorf("record does not exist")
var ErrReadFile = fmt.Errorf("error reading file")
var ErrControllSum = fmt.Errorf("error controll sum")

type hashIndex map[string]int64

type Db struct {
	out *os.File
	dir string

	segSize  int64
	segments []*segment

	mergeChan chan int
	entryChan chan entryChan

	autoMerge bool

	mux *sync.RWMutex
}

func NewDb(dir string) (*Db, error) {
	return NewDBSized(dir, segSize, autoMerge)
}

func NewDBSized(dir string, segSize int64, autoMerge bool) (*Db, error) {
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
		out: f,
		dir: dir,

		segSize:  segSize,
		segments: segments,

		mergeChan: make(chan int),
		entryChan: make(chan entryChan),

		autoMerge: autoMerge,

		mux: new(sync.RWMutex),
	}
	if autoMerge {
		go db.runMerge()
	}

	go db.runPut()

	return db, nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Put(key, value string) error {
	resChan := make(chan error)
	hash := sha1.Sum([]byte(value))
	e := &entry{key: key, value: value, hash: hash[:]}

	db.entryChan <- entryChan{
		entry:   e,
		resChan: resChan,
	}
	res := <-resChan
	return res
}

func (db *Db) Get(key string) (string, error) {
	db.mux.RLock()
	defer db.mux.RUnlock()
	var err error
	for _, segment := range db.segments {
		val, err := segment.get(key)
		if err == nil {
			return val, nil
		}
	}
	return "", err
}

func (db *Db) put(ec entryChan) {
	if len(db.segments) > 2 && db.autoMerge {
		go func() {
			db.mergeChan <- 1
		}()
	}
	e := ec.entry
	n, err := db.out.Write(e.Encode())
	if err != nil {
		ec.resChan <- err
		return
	}
	db.mux.Lock()
	activeSeg := db.segments[0]
	activeSeg.index[e.key] = activeSeg.outOffset
	activeSeg.outOffset += int64(n)
	db.mux.Unlock()
	fi, err := os.Stat(activeSeg.outputPath)
	if err != nil {
		ec.resChan <- nil
		return
	}
	if fi.Size() >= db.segSize {
		_, err = db.newSeg()
		if err != nil {
			ec.resChan <- nil
			return
		}
	}
	ec.resChan <- nil
}

func (db *Db) runPut() {
	for el := range db.entryChan {
		if el.entry == nil {
			return
		}
		db.put(el)
	}
}

func (db *Db) runMerge() {
	for el := range db.mergeChan {
		if el == 0 {
			return
		}
		db.merge()
	}
}

func (db *Db) merge() {
	segments := db.segments[1:]
	segmentsCpy := make([]*segment, len(segments))
	copy(segmentsCpy, segments)
	if len(segments) < 2 {
		return
	}
	keysSegments := make(map[string]*segment)
	for i := len(segments) - 1; i >= 0; i-- {
		s := segments[i]
		for k := range segments[i].index {
			keysSegments[k] = s
		}
	}
	segPath := filepath.Join(db.dir, segPreffix)
	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	segment := &segment{
		outputPath: segPath,
		index:      make(hashIndex),
	}
	for k, s := range keysSegments {
		value, _ := s.get(k)
		hash := sha1.Sum([]byte(value))
		e := &entry{
			key:   k,
			value: value,
			hash:  hash[:],
		}
		n, err := f.Write(e.Encode())
		if err != nil {
			return
		}
		segment.index[k] = segment.outOffset
		segment.outOffset += int64(n)
	}
	db.mux.Lock()
	to := len(db.segments) - len(segmentsCpy)
	db.segments = append(db.segments[:to], segment)
	db.mux.Unlock()
	for _, s := range segmentsCpy {
		if segPath != s.outputPath {
			os.Remove(s.outputPath)
		}
	}
}

func (db *Db) newSeg() (*segment, error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	err := db.out.Close()
	if err != nil {
		return nil, err
	}
	segmentSuffix := 0
	if len(db.segments) > 1 {
		lastSavedSegmentSuffix := db.segments[1].outputPath[len(db.dir+segPreffix)+1:]
		if prevSegmentSuffix, err := strconv.Atoi(lastSavedSegmentSuffix); err == nil {
			segmentSuffix = prevSegmentSuffix + 1
		}
	}
	segmentPath := filepath.Join(db.dir, fmt.Sprintf("%v%v", segPreffix, segmentSuffix))
	outputPath := filepath.Join(db.dir, segPreffix+segActive)

	err = os.Rename(outputPath, segmentPath)
	if err != nil {
		return nil, err
	}
	db.segments[0].outputPath = segmentPath

	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db.out = f

	s := &segment{
		outputPath: outputPath,
		index:      make(hashIndex),
	}
	db.segments = append([]*segment{s}, db.segments...)

	return s, nil
}
