package msort

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

func checkContent(t *testing.T, expected interface{}, fn string) {
	contents, err := ioutil.ReadFile(fn)
	assert.NoError(t, err)
	switch expected.(type) {
	case string:
		actual := strings.ReplaceAll(string(contents), "\n", " ")
		actual = strings.TrimSpace(actual)

		assert.Equal(t, expected, actual)
	case []byte:
		assert.Equal(t, expected, contents)
	}
}

//leafSort(r io.Reader, chunkSz int, chunks chan string, errors chan error, inFlight *int32)
func Test_leafSort(t *testing.T) {
	var err error
	tmpDir, err = ioutil.TempDir(".", "tempdir")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	input := "10 8 6 4 2 0 1 3 7 9 99"
	s := newSorter()
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
	tmpDir, err = ioutil.TempDir(".", "tempdir")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	input := "10 8 6 4 2"
	s := newSorter()
	go s.leafSort(strings.NewReader(input), 4)

	chunk := <-s.fileChan
	checkContent(t, encode(4, 6, 8, 10), chunk)

	chunk = <-s.fileChan
	checkContent(t, encode(2), chunk)
	assert.Zero(t, atomic.LoadInt32(&s.inFlight))
}

func Test_leafSortError(t *testing.T) {
	var err error
	tmpDir, err = ioutil.TempDir(".", "tempdir")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	input := "10 8 6 4 2 0 1 3 5 7 9  not_a_number"
	s := newSorter()
	go s.leafSort(strings.NewReader(input), 4)
	err = <-s.errors
	assert.Error(t, err)
}

func Test_sortFilezz(t *testing.T) {
	s := "10 8 6 4 2 0 1 3 5 7 9"
	outFile := fmt.Sprintf("outfile%d.txt", time.Now().Nanosecond())
	defer os.Remove(outFile)
	err := SortFile(outFile, strings.NewReader(s), 4)
	assert.NoError(t, err)
	checkContent(t, "0 1 2 3 4 5 6 7 8 9 10", outFile)
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

func checkSorted(t *testing.T, fileName string) {
	f, err := os.Open(fileName)
	require.NoError(t, err)
	defer f.Close()
	a := newAStream(f)
	a.Next()
	prev := a.top
	for a.Next() {
		assert.True(t, prev <= a.top)
		prev = a.top
	}
}

// This tests sorts a 1000000 element file
func Test_sortFileMassive(t *testing.T) {
	r := randReader(1000000)
	outFile := fmt.Sprintf("outfile%d.txt", time.Now().Nanosecond())
	defer os.Remove(outFile)
	err := SortFile(outFile, &r, 10000)
	checkSorted(t, outFile)
	assert.NoError(t, err)
}

func Test_sortFile(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"one", "1", "1"},
		{"sorted", "1 2 3", "1 2 3"},
		{"long sorted", "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17", "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17"},
		{"long jumpbles", "11 7 16 1 2 3 4 5 6 8 9 10 12 13 14 15  17", "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outFile := fmt.Sprintf("outfile%d.txt", time.Now().Nanosecond())
			os.Remove(outFile)

			err := SortFile(outFile, strings.NewReader(tt.input), 4)
			assert.NoError(t, err)
			checkContent(t, tt.want, outFile)
			os.Remove(outFile)
		})
	}
}

func Test_sortFileMalformedInput(t *testing.T) {
	fn, err := ioutil.TempFile("", "softtest")
	defer os.Remove(fn.Name())
	err = SortFile(fn.Name(), strings.NewReader("not_a_number"), 4)
	assert.Error(t, err)
}

func Test_sortFileReadError(t *testing.T) {
	fn, err := ioutil.TempFile("", "softtest")
	defer os.Remove(fn.Name())
	err = SortFile(fn.Name(), errorReader(0), 4)
	assert.Error(t, err)
}
