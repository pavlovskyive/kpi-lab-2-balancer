package datastore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	outFile, err := os.Open(filepath.Join(dir, segPreffix+segActive))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("put/get", func(t *testing.T) {
		pairs := [][]string{
			{"key1", "val1"},
			{"key2", "val2"},
			{"key3", "val3"},
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})
	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()
	t.Run("file growth", func(t *testing.T) {
		pairs := [][]string{
			{"key1", "val1"},
			{"key2", "val2"},
			{"key3", "val3"},
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})
	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir)
		if err != nil {
			t.Fatal(err)
		}
		pairs := [][]string{
			{"key1", "val1"},
			{"key2", "val2"},
			{"key3", "val3"},
		}
		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})
}
func TestDb_Segmentation(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	db, err := NewDBSized(dir, 44, false)
	if err != nil {
		t.Fatal(err)
	}
	pairs := [][]string{
		{"key1", "val1"},
		{"key2", "val2"},
		{"key3", "val3"},
	}
	for _, pair := range pairs {
		err = db.Put(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
	}
	files, _ := ioutil.ReadDir(dir)
	if len(files) != 2 {
		t.Errorf("Unexpected segment count (%d vs %d)", len(files), 2)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDb_Merge(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	db, err := NewDBSized(dir, 40, false)
	if err != nil {
		t.Fatal(err)
	}
	pairs := [][]string{
		{"key1", "val1"},
		{"key2", "val2"},
		{"key3", "val3"},
	}
	for _, pair := range pairs {
		err = db.Put(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
	}
	pairs = [][]string{
		{"key2", "newVal2"},
		{"key3", "newVal3"},
	}
	for _, pair := range pairs {
		err = db.Put(pair[0], pair[1])
		if err != nil {
			t.Fatal(err)
		}
	}
	files, _ := ioutil.ReadDir(dir)
	if len(files) != 3 {
		t.Errorf("Unexpected segment count before merge (%d vs %d)", len(files), 3)
	}
	db.merge()
	files, _ = ioutil.ReadDir(dir)
	if len(files) != 2 {
		t.Errorf("Unexpected segment count after merge (%d vs %d)", len(files), 2)
	}
	mergedSegment := db.segments[1]
	expectedMergedSegment := [][]string{
		{"key1", "val1"},
		{"key2", "newVal2"},
		{"key3", "val3"},
	}
	for _, pair := range expectedMergedSegment {
		value, err := mergedSegment.get(pair[0])
		if err != nil {
			t.Errorf("Cannot get %s: %s", pair[0], err)
		}

		if value != pair[1] {
			t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDb_Concurrency(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	db, err := NewDBSized(dir, 44, false)
	if err != nil {
		t.Fatal(err)
	}
	resCh := make(chan int)
	var pairs [10][]string
	for i := range pairs {
		pairs[i] = []string{
			fmt.Sprintf("key%v", i), fmt.Sprintf("val%v", i),
		}
	}
	for _, pair := range pairs {
		pair := pair
		go func() {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
			resCh <- 1
		}()
	}
	for range pairs {
		<-resCh
	}
	for _, pair := range pairs {
		value, err := db.Get(pair[0])
		if err != nil {
			t.Errorf("Cannot get %s: %s", pair[0], err)
		}

		if value != pair[1] {
			t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
