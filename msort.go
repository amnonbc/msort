package msort

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)

type aStream struct {
	top int
	r   *bufio.Scanner
	eof bool
	err error
}

var tmpDir = os.TempDir()

func newAStream(r io.Reader) aStream {
	i := aStream{r: bufio.NewScanner(r)}
	i.r.Split(bufio.ScanWords)
	return i
}

func (a *aStream) ok() bool {
	return a.err == nil && a.eof == false
}

func (a *aStream) Next() bool {
	if a.err != nil {
		return false
	}
	a.eof = !a.r.Scan()
	a.err = a.r.Err()
	if a.eof || a.err != nil {
		return false
	}
	a.top, a.err = strconv.Atoi(a.r.Text())
	return true
}

func (a *aStream) ReadNums(nums []int) (int, error) {
	n := 0
	for n < len(nums) && a.Next() {
		nums[n] = a.top
		n++
	}
	return n, a.err
}

func writeInt(w io.Writer, x int) error {
	_, err := fmt.Fprintln(w, x)
	return err
}

// writeInts writes a slice of numbers into a new temprary file, returning the name of the temporary file
func writeInts(a []int) (string, error) {
	f, err := ioutil.TempFile("", "sortchunk")
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := bufio.NewWriter(f)
	for _, x := range a {
		writeInt(h, x)
	}
	return f.Name(), h.Flush()
}

// leafsort reads numbers from r, breaks them into sorted chunks of length chunkSz and writes each chunk to a file.
// It returns a slice of the names of chunkfiles.
func leafSort(r io.Reader, chunkSz int) (chunks []string, err error) {
	buf := make([]int, chunkSz)
	a := newAStream(r)

	for !a.eof && a.err == nil {
		n, err := a.ReadNums(buf)
		if err != nil {
			return chunks, err
		}
		buf = buf[0:n]
		sort.Ints(buf)
		fn, err := writeInts(buf)
		if err != nil {
			return chunks, err
		}
		chunks = append(chunks, fn)
	}
	return chunks, a.err
}

// SortFile sorts numbers from r, saving the output to outFileName.
func SortFile(outFileName string, r io.Reader, chunkSz int) error {
	files, err := leafSort(r, chunkSz)
	if err != nil {
		return err
	}
	for len(files) > 1 {
		newFile, err := merge(files[0], files[1])
		if err != nil {
			return err
		}
		files = append(files[2:], newFile)
	}
	if len(files) == 1 {
		err = os.Rename(files[0], outFileName)
	}
	return err
}

// doMerge merges two sorted sequences of numbers from r1 and r2, and writes the merged output to w.
func doMerge(writer io.Writer, r1 io.Reader, r2 io.Reader) error {
	w := bufio.NewWriter(writer)
	a := newAStream(r1)
	b := newAStream(r2)
	a.Next()
	b.Next()
	for a.ok() && b.ok() {
		if a.top < b.top {
			writeInt(w, a.top)
			a.Next()
		} else {
			writeInt(w, b.top)
			b.Next()
		}
	}
	for a.ok() {
		writeInt(w, a.top)
		a.Next()
	}
	for b.ok() {
		writeInt(w, b.top)
		b.Next()
	}

	if a.err != nil {
		return a.err
	}
	if b.err != nil {
		return b.err
	}

	return w.Flush()
}

// merge merges fn1 and fn2, and writes the merged output into a new temporary file.
// it returns the name of the new temporary file.
func merge(fn1 string, fn2 string) (string, error) {
	f1, err := os.Open(fn1)
	if err != nil {
		return "", err
	}
	os.Remove(fn1)
	defer f1.Close()

	f2, err := os.Open(fn2)
	if err != nil {
		return "", err
	}
	os.Remove(fn2)
	defer f1.Close()

	fm, err := ioutil.TempFile("", "sortchunk")
	if err != nil {
		return "", err
	}
	defer fm.Close()
	err = doMerge(fm, f1, f2)
	return fm.Name(), err
}
