package msort

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
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
	assert.Equal(t, 1, a.top)

	ok = a.Next()
	assert.True(t, ok)
	assert.False(t, a.eof)
	assert.NoError(t, a.err)
	assert.Equal(t, 2, a.top)

	ok = a.Next()
	assert.False(t, ok)
	assert.True(t, a.eof)
	assert.NoError(t, a.err)

}

func Test_astream_ReadNums(t *testing.T) {
	a := newAStream(strings.NewReader("1\n2\n3\n"))
	nums := make([]int, 2)
	n, err := a.ReadNums(nums)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []int{1, 2}, nums)

	n, err = a.ReadNums(nums)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 3, nums[0])

}

func Test_astream_ReadNumsError(t *testing.T) {
	a := newAStream(strings.NewReader("abc"))
	nums := make([]int, 2)
	_, err := a.ReadNums(nums)
	assert.Error(t, err)
}

func Test_doMerge(t *testing.T) {
	out := &bytes.Buffer{}
	r1 := strings.NewReader("1 3 5")
	r2 := strings.NewReader("2 4")
	doMerge(out, r1, r2)
	assert.Equal(t, "1\n2\n3\n4\n5\n", out.String())
}

type errorWriter int

func (_ errorWriter) Write(_ []byte) (int, error) {
	return 0, fmt.Errorf("File system full")
}

func Test_doMergeErrorOutput(t *testing.T) {
	var out errorWriter
	h := bufio.NewWriter(out)
	r1 := strings.NewReader("1 3 5")
	r2 := strings.NewReader("2 4")
	doMerge(h, r1, r2)
	err := h.Flush()
	assert.Error(t, err)
}

func Test_doMergeErrorInput(t *testing.T) {
	r1 := strings.NewReader("1 not a number")
	r2 := strings.NewReader("2 4")
	err := doMerge(ioutil.Discard, r1, r2)
	assert.Error(t, err)
}

func checkContent(t *testing.T, expected string, fn string) {
	contents, err := ioutil.ReadFile(fn)
	assert.NoError(t, err)
	actual := strings.ReplaceAll(string(contents), "\n", " ")
	actual = strings.TrimSpace(actual)
	assert.Equal(t, expected, actual)
}

func Test_leafSort(t *testing.T) {
	var err error
	tmpDir, err = ioutil.TempDir(".", "tempdir")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	s := "10 8 6 4 2 0 1 3 5 7 9"
	gotChunks, err := leafSort(strings.NewReader(s), 4)
	assert.NoError(t, err)
	require.Equal(t, 3, len(gotChunks))
	checkContent(t, "4 6 8 10", gotChunks[0])
	checkContent(t, "0 1 2 3", gotChunks[1])
	checkContent(t, "5 7 9", gotChunks[2])
}

func Test_sortFilezz(t *testing.T) {
	s := "10 8 6 4 2 0 1 3 5 7 9"
	outFile := fmt.Sprintf("outfile%d.txt", time.Now().Nanosecond())
	defer os.Remove(outFile)
	err := SortFile(outFile, strings.NewReader(s), 4)
	assert.NoError(t, err)
	checkContent(t, "0 1 2 3 4 5 6 7 8 9 10", outFile)
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
