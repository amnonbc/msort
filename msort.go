package msort

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
)

var done sync.Mutex

type aStream struct {
	top int64
	r   *bufio.Scanner
	eof bool
	err error
}

const bytesPerNumber = 8

type iStream struct {
	top  int64
	buf  []int64
	next int
	last int
	r    io.Reader
	eof  bool
	err  error
}

func newIStream(r io.Reader) iStream {
	return iStream{
		r:   r,
		buf: make([]int64, 16*1024),
	}
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
	x := 0
	x, a.err = strconv.Atoi(a.r.Text())
	a.top = int64(x)
	return true
}

func (a *iStream) ok() bool {
	return a.err == nil && a.eof == false
}

func (a *iStream) Next() bool {
	if a.err != nil {
		return false
	}
	if a.next >= a.last {
		n, err := readBInts(a.r, a.buf)
		if err == io.EOF {
			a.eof = true
		} else {
			a.err = err
		}
		a.last = n
		a.next = 0
	}
	if a.err != nil || a.eof {
		return false
	}
	a.top = a.buf[a.next]
	a.next++
	return true
}

func (a *aStream) ReadNums(nums []int64) (int, error) {
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
func writeBIntsToFile(a []int64) (string, error) {
	f, err := ioutil.TempFile("", "sortchunk")
	if err != nil {
		return "", err
	}
	defer f.Close()
	err = writeBInts(f, a)
	return f.Name(), err
}

func writeBInts(f io.Writer, a []int64) error {

	// Get the slice header
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&a))
	header.Len *= bytesPerNumber
	header.Cap *= bytesPerNumber

	// Convert slice header to an []byte
	data := *(*[]byte)(unsafe.Pointer(&header))

	_, err := f.Write(data)
	return err
}

func readBInts(f io.Reader, a []int64) (int, error) {

	// Get the slice header
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&a))
	header.Len *= bytesPerNumber
	header.Cap *= bytesPerNumber

	// Convert slice header to an []byte
	data := *(*[]byte)(unsafe.Pointer(&header))

	n, err := f.Read(data)
	if n%bytesPerNumber != 0 {
		err = errors.New("truncated input")
	}
	return n / bytesPerNumber, err
}

// leafsort reads numbers from r, breaks them into sorted chunks of length chunkSz and writes each chunk to a file.
// It returns a slice of the names of chunkfiles.
func leafSort(r io.Reader, chunkSz int, chunks chan string, errors chan error, inFlight *int64) {
	buf := make([]int64, chunkSz)
	a := newAStream(r)

	for !a.eof && a.err == nil {
		n, err := a.ReadNums(buf)
		if err != nil {
			errors <- err
			return
		}
		buf = buf[0:n]
		sort.Slice(buf, func(i, j int) bool {
			return buf[i] < buf[j]
		})
		fn, err := writeBIntsToFile(buf)
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

type intWriter struct {
	f      io.Writer
	maxLen int
	buf    []int64
	err    error
}

func newIntWriter(f io.Writer, maxLen int) *intWriter {
	return &intWriter{
		f:      f,
		maxLen: maxLen,
		buf:    make([]int64, 0, maxLen),
	}
}

func (w *intWriter) Flush() error {
	w.err = writeBInts(w.f, w.buf)
	w.buf = w.buf[0:0]
	return w.err
}

func (w *intWriter) Write(x int64) {
	if len(w.buf) >= w.maxLen {
		w.Flush()
	}
	w.buf = append(w.buf, x)
}

// doMerge merges two sorted sequences of numbers from r1 and r2, and writes the merged output to w.
func doMerge(writer io.Writer, r1 io.Reader, r2 io.Reader) error {
	w := newIntWriter(writer, 16*1024)
	a := newIStream(r1)
	b := newIStream(r2)
	a.Next()
	b.Next()
	for a.ok() && b.ok() {
		if a.top < b.top {
			w.Write(a.top)
			a.Next()
		} else {
			w.Write(b.top)
			b.Next()
		}
	}
	for a.ok() {
		w.Write(a.top)
		a.Next()
	}
	for b.ok() {
		w.Write(b.top)
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
