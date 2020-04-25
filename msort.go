package msort

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

var done sync.Mutex

type aStream struct {
	top int
	r   *bufio.Scanner
	eof bool
	err error
}

const bytesPerNumber = 8

type iStream struct {
	top int64
	r   bufio.Reader
	eof bool
	err error
}

func newIStream(r io.Reader) iStream {
	return iStream{r: *bufio.NewReader(r)}
	return iStream{r: *bufio.NewReader(r)}
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

func (a *iStream) ReadNums(nums []int) (int, error) {
	n := 0
	for n < len(nums) && a.Next() {
		nums[n] = int(a.top)
		n++
	}
	return n, a.err
}

func (a *iStream) ok() bool {
	return a.err == nil && a.eof == false
}

func (a *iStream) Next() bool {
	if a.err != nil {
		return false
	}
	buf, err := a.r.Peek(bytesPerNumber)
	if len(buf) > 0 && len(buf) != bytesPerNumber {
		a.err = fmt.Errorf("Input stream truncated, got %d bytes, need %d", len(buf), bytesPerNumber)
		return false
	}
	if err == io.EOF {
		a.eof = true
		a.err = nil
		return false
	}
	if err != nil {
		a.err = err
		return false
	}
	a.r.Discard(bytesPerNumber)
	a.top = int64(binary.LittleEndian.Uint64(buf))
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

func writeBInt(h io.Writer, x int64) {
	buf := make([]byte, bytesPerNumber)
	binary.LittleEndian.PutUint64(buf, uint64(x))
	h.Write(buf)
}

// writeInts writes a slice of numbers into a new temprary file, returning the name of the temporary file
func writeBInts(a []int) (string, error) {
	f, err := ioutil.TempFile("", "sortchunk")
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := bufio.NewWriter(f)
	for _, x := range a {
		writeBInt(h, int64(x))
	}
	return f.Name(), h.Flush()
}

// leafsort reads numbers from r, breaks them into sorted chunks of length chunkSz and writes each chunk to a file.
// It returns a slice of the names of chunkfiles.
func leafSort(r io.Reader, chunkSz int, chunks chan string, errors chan error, inFlight *int64) {
	buf := make([]int, chunkSz)
	a := newAStream(r)

	for !a.eof && a.err == nil {
		n, err := a.ReadNums(buf)
		if err != nil {
			errors <- err
			return
		}
		buf = buf[0:n]
		sort.Ints(buf)
		fn, err := writeBInts(buf)
		if err != nil {
			errors <- err
			return
		}
		chunks <- fn
	}
	done.Lock()
	atomic.AddInt64(inFlight, -1)
	done.Unlock()

}

func binToAscii(in string, out string) error {
	h, err := os.Open(in)
	if err != nil {
		return err
	}
	defer h.Close()
	a := newIStream(h)
	o, err := os.Create(out)
	if err != nil {
		return err
	}
	defer o.Close()
	ob := bufio.NewWriter(o)
	for a.Next() {
		writeInt(ob, int(a.top))
	}
	return ob.Flush()
}

// SortFile sorts numbers from r, saving the output to outFileName.
func SortFile(outFileName string, r io.Reader, chunkSz int) error {
	left, right := "", ""
	files := make(chan string, 1000)
	errors := make(chan error, 1000)
	inFlight := int64(1)
	go leafSort(r, chunkSz, files, errors, &inFlight)
	for {
		select {
		case left = <-files:
		case err := <-errors:
			return err
		}

		done.Lock()
		inF := atomic.LoadInt64(&inFlight)
		done.Unlock()
		if inF == 0 && len(files) == 0 {
			break
		}
		select {
		case right = <-files:

		case err := <-errors:
			return err
		}
		atomic.AddInt64(&inFlight, 1)
		go merge(left, right, files, errors, &inFlight)
	}
	return binToAscii(left, outFileName)
}

// doMerge merges two sorted sequences of numbers from r1 and r2, and writes the merged output to w.
func doMerge(writer io.Writer, r1 io.Reader, r2 io.Reader) error {
	w := bufio.NewWriter(writer)
	a := newIStream(r1)
	b := newIStream(r2)
	a.Next()
	b.Next()
	for a.ok() && b.ok() {
		if a.top < b.top {
			writeBInt(w, a.top)
			a.Next()
		} else {
			writeBInt(w, b.top)
			b.Next()
		}
	}
	for a.ok() {
		writeBInt(w, a.top)
		a.Next()
	}
	for b.ok() {
		writeBInt(w, b.top)
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
func merge(fn1 string, fn2 string, files chan string, errors chan error, inFlight *int64) {
	f1, err := os.Open(fn1)
	if err != nil {
		errors <- err
		return
	}
	os.Remove(fn1)
	defer f1.Close()

	f2, err := os.Open(fn2)
	if err != nil {
		errors <- err
		return
	}
	os.Remove(fn2)
	defer f1.Close()

	fm, err := ioutil.TempFile("", "sortchunk")
	if err != nil {
		errors <- err
		return

	}
	defer fm.Close()
	err = doMerge(fm, f1, f2)
	if err != nil {
		errors <- err
		return
	}
	done.Lock()
	atomic.AddInt64(inFlight, -1)
	files <- fm.Name()
	done.Unlock()
}
