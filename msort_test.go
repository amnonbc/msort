package msort

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func Test_astream_Next(t *testing.T) {
	a := newAStream(strings.NewReader("1\n2"))

	ok := a.Next()
	assert.True(t, ok)
	assert.False(t, a.eof)
	assert.NoError(t, a.err)
	assert.Equal(t, int32(1), a.top)

	ok = a.Next()
	assert.True(t, ok)
	assert.False(t, a.eof)
	assert.NoError(t, a.err)
	assert.Equal(t, int32(2), a.top)

	ok = a.Next()
	assert.False(t, ok)
	assert.True(t, a.eof)
	assert.NoError(t, a.err)

}

func Test_astream_NextError(t *testing.T) {
	a := newAStream(errorReader(0))
	ok := a.Next()
	assert.False(t, ok)
	assert.False(t, a.ok())
	assert.Error(t, a.err)
}

func Test_astream_ReadNums(t *testing.T) {
	a := newAStream(strings.NewReader("1\n2\n3\n"))
	nums := make([]int32, 2)
	n, err := a.readNums(nums)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []int32{1, 2}, nums)

	n, err = a.readNums(nums)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, int32(3), nums[0])

}

func Test_astream_ReadNumsError(t *testing.T) {
	a := newAStream(strings.NewReader("abc"))
	nums := make([]int32, 2)
	_, err := a.readNums(nums)
	assert.Error(t, err)
}

func Test_astream_ReadNumsErrorBadFile(t *testing.T) {
	a := newAStream(errorReader(0))
	nums := make([]int32, 2)
	_, err := a.readNums(nums)
	assert.Error(t, err)
}

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
	s := "10 8 6 4 2 0 1 3 7 9 99"
	files := make(chan string)
	errors := make(chan error)
	inFlight := int32(1)
	go leafSort(strings.NewReader(s), 4, files, errors, &inFlight)

	chunk := <-files
	checkContent(t, encode(4, 6, 8, 10), chunk)

	chunk = <-files
	checkContent(t, encode(0, 1, 2, 3), chunk)

	chunk = <-files
	checkContent(t, encode(7, 9, 99), chunk)
	assert.Zero(t, inFlight)
}

func Test_leafSort0(t *testing.T) {
	var err error
	tmpDir, err = ioutil.TempDir(".", "tempdir")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	s := "10 8 6 4 2"
	files := make(chan string)
	errors := make(chan error)
	inFlight := int32(1)
	go leafSort(strings.NewReader(s), 4, files, errors, &inFlight)

	chunk := <-files
	checkContent(t, encode(4, 6, 8, 10), chunk)

	chunk = <-files
	checkContent(t, encode(2), chunk)
	assert.Zero(t, inFlight)
}

func Test_leafSortError(t *testing.T) {
	var err error
	tmpDir, err = ioutil.TempDir(".", "tempdir")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	s := "10 8 6 4 2 0 1 3 5 7 9  not_a_number"
	files := make(chan string, 10)
	errors := make(chan error)
	inFlight := int32(1)
	go leafSort(strings.NewReader(s), 4, files, errors, &inFlight)
	err = <-errors
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
	w := &bytes.Buffer{}
	for len(buf)-w.Len() > 20 && int(*r) > 0 {
		fmt.Fprintln(w, rand.Int31())
		*r--
	}

	copy(buf, w.Bytes())
	return w.Len(), nil
}

func Test_sortFileMassive(t *testing.T) {
	r := randReader(1000000)
	outFile := fmt.Sprintf("outfile%d.txt", time.Now().Nanosecond())
	defer os.Remove(outFile)
	err := SortFile(outFile, &r, 10000)
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

func Test_iStream_NextEmpty(t *testing.T) {
	buf := &bytes.Buffer{}
	b := newIStream(buf)
	b.Next()
	assert.True(t, b.eof)
	assert.NoError(t, b.err)
}

func Test_writeBInt(t *testing.T) {
	h := &bytes.Buffer{}
	writeBInt(h, 0x7)
	assert.Equal(t, []byte{0x7, 0, 0, 0}, h.Bytes())
}

func Test_ReadBInt(t *testing.T) {
	h := bytes.NewReader([]byte{0x7, 0, 0, 0, 0, 0, 0, 0})
	b := newIStream(h)
	ok := b.Next()
	assert.True(t, ok)
	assert.False(t, b.eof)
	assert.NoError(t, b.err)
	assert.Equal(t, int32(7), b.top)
}

func Test_writeBInts(t *testing.T) {
	f := &bytes.Buffer{}
	err := writeBInts(f, []int32{1})
	assert.NoError(t, err)
	assert.Equal(t, encode(1), f.Bytes())
}

func Test_intWriter_Write(t *testing.T) {
	f := &bytes.Buffer{}
	w := newIntWriter(f, 2)
	w.writeInt32(1)
	w.flush()
	assert.Equal(t, encode(1), f.Bytes())
}

func Test_intWriter_Write3(t *testing.T) {
	f := &bytes.Buffer{}
	w := newIntWriter(f, 2)
	w.writeInt32(1)
	w.writeInt32(2)
	w.writeInt32(3)
	w.flush()
	assert.Equal(t, encode(1, 2, 3), f.Bytes())
}

func Test_readBInts(t *testing.T) {
	buf := bytes.NewBuffer(encode(1, 2, 3))
	a := make([]int32, 3)
	got, err := readBInts(buf, a)
	assert.NoError(t, err)
	assert.Equal(t, 3, got)
	assert.Equal(t, []int32{1, 2, 3}, a)
}

func Test_readBIntsShort(t *testing.T) {
	buf := bytes.NewBuffer(encode(1, 2, 3))
	a := make([]int32, 2)
	got, err := readBInts(buf, a)
	assert.NoError(t, err)
	assert.Equal(t, 2, got)
	assert.Equal(t, []int32{1, 2}, a)
}

func Test_readBIntsEof(t *testing.T) {
	buf := bytes.NewBuffer(encode(1, 2, 3))
	a := make([]int32, 4)
	got, err := readBInts(buf, a)
	assert.NoError(t, err)
	assert.Equal(t, 3, got)
	assert.Equal(t, []int32{1, 2, 3}, a[0:got])

	got, err = readBInts(buf, a)
	assert.Error(t, err)
	assert.Equal(t, 0, got)

}
