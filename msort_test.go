package msort

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func encode(val ...int32) []byte {
	a := &bytes.Buffer{}
	for _, x := range val {
		writeBInt(a, x)
	}
	return a.Bytes()
}

func Test_doMerge(t *testing.T) {
	out := &bytes.Buffer{}
	r1 := bytes.NewReader(encode(1, 3, 5))
	r2 := bytes.NewReader(encode(2, 4))
	doMerge(out, r1, r2)
	assert.Equal(t, encode(1, 2, 3, 4, 5), out.Bytes())
}

func Test_doMergeOneToOne(t *testing.T) {
	out := &bytes.Buffer{}
	r1 := bytes.NewReader(encode(1))
	r2 := bytes.NewReader(encode(2))
	doMerge(out, r1, r2)
	assert.Equal(t, encode(1, 2), out.Bytes())
}

func Test_doMergeBadInputFile(t *testing.T) {
	err := doMerge(ioutil.Discard, errorReader(0), errorReader(0))
	assert.Error(t, err)
}

type errorWriter int

func (_ errorWriter) Write(_ []byte) (int, error) {
	return 0, fmt.Errorf("File system full")
}

type errorReader int

func (_ errorReader) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("Can not read")
}

func Test_doMergeErrorOutput(t *testing.T) {
	var out errorWriter
	r1 := bytes.NewReader(encode(1))
	r2 := bytes.NewReader(encode(2))
	err := doMerge(out, r1, r2)
	assert.Error(t, err)
}

func Test_doMergeErrorInput(t *testing.T) {
	r1 := errorReader(0)
	r2 := bytes.NewReader(encode(1, 2, 3))
	err := doMerge(ioutil.Discard, r1, r2)
	assert.Error(t, err)
}

func Test_doMergeTruncatedInput(t *testing.T) {
	r1 := bytes.NewReader([]byte{1, 2, 3, 4, 5, 6})
	r2 := bytes.NewReader(encode(1, 2, 3))
	err := doMerge(ioutil.Discard, r1, r2)
	assert.Error(t, err)
}

func checkContent(t *testing.T, expected []byte, fn string) {
	assert.NoError(t, isSorted(fn))
	contents, err := ioutil.ReadFile(fn)
	assert.NoError(t, err)
	assert.Equal(t, expected, contents)

}

//leafSort(r io.Reader, chunkSz int, chunks chan string, errors chan error, inFlight *int32)
func Test_leafSort(t *testing.T) {
	var err error
	assert.NoError(t, err)
	input := "10 8 6 4 2 0 1 3 7 9 99"
	s := newSorter()
	s.tmpDir, err = ioutil.TempDir(".", "tempdir")
	assert.NoError(t, err)
	defer os.RemoveAll(s.tmpDir)

	go s.leafSort(strings.NewReader(input), 4)

	chunk := <-s.fileChan
	checkContent(t, encode(4, 6, 8, 10), chunk)

	chunk = <-s.fileChan
	checkContent(t, encode(0, 1, 2, 3), chunk)

	chunk = <-s.fileChan
	checkContent(t, encode(7, 9, 99), chunk)
	assert.Zero(t, atomic.LoadInt32(&s.inFlight))
}

func Test_leafSort0(t *testing.T) {
	var err error
	input := "10 8 6 4 2"
	s := newSorter()
	s.tmpDir, err = ioutil.TempDir(".", "tempdir")
	assert.NoError(t, err)
	defer os.RemoveAll(s.tmpDir)

	go s.leafSort(strings.NewReader(input), 4)

	chunk := <-s.fileChan
	checkContent(t, encode(4, 6, 8, 10), chunk)

	chunk = <-s.fileChan
	checkContent(t, encode(2), chunk)
	assert.Zero(t, atomic.LoadInt32(&s.inFlight))
}

func Test_leafSortError(t *testing.T) {
	var err error
	input := "10 8 6 4 2 0 1 3 5 7 9  not_a_number"
	s := newSorter()
	s.tmpDir, err = ioutil.TempDir(".", "tempdir")
	assert.NoError(t, err)
	defer os.RemoveAll(s.tmpDir)

	go s.leafSort(strings.NewReader(input), 4)
	err = <-s.errors
	assert.Error(t, err)
}

func Test_sortFilezz(t *testing.T) {
	s := "10 8 6 4 2 0 1 3 5 7 9"
	out := new(bytes.Buffer)
	err := SortFile(out, strings.NewReader(s), 4)
	assert.NoError(t, err)
	assert.Equal(t, sortString(s), strings.TrimSpace(out.String()))
}

type randReader int

func (r *randReader) Read(buf []byte) (int, error) {
	if int(*r) == 0 {
		return 0, io.EOF
	}
	buf = buf[:0]
	for cap(buf)-len(buf) > 20 && int(*r) > 0 {
		buf = strconv.AppendInt(buf, int64(rand.Int31()), 10)
		buf = append(buf, '\n')
		*r--
	}
	return len(buf), nil
}

func isSorted(fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	a := newAStream(f)
	a.Next()
	prev := a.top
	for a.Next() {
		if prev > a.top {
			return errors.New("Unsorted")
		}
	}
	return nil
}

type inOrderCheckerWriter struct {
	t     *testing.T
	last  int
	count int
}

// Write consumes buf, parsing newline separated positive numbers, and raises an error if the numbers are
// not sorted.
func (w *inOrderCheckerWriter) Write(buf []byte) (int, error) {
	cur := 0
	for _, b := range buf {
		if '0' <= b && b <= '9' {
			cur = 10*cur + int(b-'0')
			continue
		}
		if w.last > cur {
			w.t.Error(w.last, ">", cur)
		}
		w.last = cur
		w.count++
		cur = 0
	}
	return len(buf), nil
}

// This tests sorts a 1000000 element file
func Test_sortFileMassive(t *testing.T) {
	const N = 1000000
	r := randReader(N)
	w := &inOrderCheckerWriter{t: t}
	err := SortFile(w, &r, 10000)
	assert.NoError(t, err)
	assert.Equal(t, N, w.count)
}

func sortString(in string) string {
	atoi := func(s string) int {
		i, _ := strconv.Atoi(s)
		return i
	}

	items := strings.Split(in, " ")
	sort.Slice(items, func(i, j int) bool {
		return atoi(items[i]) < atoi(items[j])
	})
	return strings.Join(items, "\n")
}

func Test_sortFile(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"one", "1"},
		{"sorted", "1 2 3"},
		{"long sorted", "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17"},
		{"long jumpbles", "11 7 16 1 2 3 4 5 6 8 9 10 12 13 14 15 17"},
	}
	out := new(bytes.Buffer)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out.Reset()
			err := SortFile(out, strings.NewReader(tt.input), 4)
			assert.NoError(t, err)
			want := sortString(tt.input)
			assert.Equal(t, want, strings.TrimSpace(out.String()))
		})
	}
}

func Test_sortFileMalformedInput(t *testing.T) {
	err := SortFile(ioutil.Discard, strings.NewReader("not_a_number"), 4)
	assert.Error(t, err)
}

func Test_sortFileReadError(t *testing.T) {
	err := SortFile(ioutil.Discard, errorReader(0), 4)
	assert.Error(t, err)
}

func Test_sortFileCantWriteOutput(t *testing.T) {
	err := SortFile(errorWriter(0), errorReader(0), 4)
	assert.Error(t, err)
}
