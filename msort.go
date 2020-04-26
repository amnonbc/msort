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
	top int32
	r   *bufio.Scanner
	eof bool
	err error
}

const bytesPerNumber = 4

type iStream struct {
	top  int32
	buf  []int32
	next int
	last int
	r    io.Reader
	eof  bool
	err  error
}

func newIStream(r io.Reader) iStream {
	return iStream{
		r:   r,
		buf: make([]int32, 16*1024),
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

// copy of stdlib strconv.Atoi, hacked to accept []byte input, avoiding allocation.
func atoi(s []byte) (int32, error) {
	s0 := s
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
		if len(s) < 1 {
			return 0, fmt.Errorf("bad int %q", s0)
		}
	}

	n := int32(0)
	for _, ch := range s {
		ch -= '0'
		if ch > 9 {
			return 0, fmt.Errorf("bad int %q", s0)
		}
		n = n*10 + int32(ch)
	}
	if s0[0] == '-' {
		n = -n
	}
	return n, nil
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
	buf := a.r.Bytes()
	a.top, a.err = atoi(buf)
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

func (a *aStream) ReadNums(nums []int32) (int, error) {
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

func writeBInt(h io.Writer, x int32) {
	buf := make([]byte, bytesPerNumber)
	binary.LittleEndian.PutUint32(buf, uint32(x))
	h.Write(buf)
}

// writeInts writes a slice of numbers into a new temprary file, returning the name of the temporary file
func writeBIntsToFile(a []int32) (string, error) {
	f, err := ioutil.TempFile("", "sortchunk")
	if err != nil {
		return "", err
	}
	defer f.Close()
	err = writeBInts(f, a)
	return f.Name(), err
}

func writeBInts(f io.Writer, a []int32) error {

	// Get the slice header
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&a))
	header.Len *= bytesPerNumber
	header.Cap *= bytesPerNumber

	// Convert slice header to an []byte
	data := *(*[]byte)(unsafe.Pointer(&header))

	_, err := f.Write(data)
	return err
}

func readBInts(f io.Reader, a []int32) (int, error) {

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
func leafSort(r io.Reader, chunkSz int, chunks chan string, errors chan error, inFlight *int32) {
	buf := make([]int32, chunkSz)
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
	atomic.AddInt32(inFlight, -1)
	done.Unlock()

}

func binToAscii(in string, out string) error {
	buf := make([]byte, 0, 64*1024)
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
	for a.Next() {
		if cap(buf)-len(buf) < 20 {
			_, err = o.Write(buf)
			if err != nil {
				return err
			}
			buf = buf[:0]
		}
		buf = strconv.AppendInt(buf, int64(a.top), 10)
		buf = append(buf, '\n')
	}
	_, err = o.Write(buf)
	return err
}

// SortFile sorts numbers from r, saving the output to outFileName.
func SortFile(outFileName string, r io.Reader, chunkSz int) error {
	left, right := "", ""
	files := make(chan string, 1000)
	errors := make(chan error, 1000)
	inFlight := int32(1)
	go leafSort(r, chunkSz, files, errors, &inFlight)
	for {
		select {
		case left = <-files:
		case err := <-errors:
			return err
		}

		done.Lock()
		inF := atomic.LoadInt32(&inFlight)
		done.Unlock()
		if inF == 0 && len(files) == 0 {
			break
		}
		select {
		case right = <-files:

		case err := <-errors:
			return err
		}
		atomic.AddInt32(&inFlight, 1)
		go merge(left, right, files, errors, &inFlight)
	}
	return binToAscii(left, outFileName)
}

type intWriter struct {
	f      io.Writer
	maxLen int
	buf    []int32
	err    error
}

func newIntWriter(f io.Writer, maxLen int) *intWriter {
	return &intWriter{
		f:      f,
		maxLen: maxLen,
		buf:    make([]int32, 0, maxLen),
	}
}

func (w *intWriter) Flush() error {
	w.err = writeBInts(w.f, w.buf)
	w.buf = w.buf[0:0]
	return w.err
}

func (w *intWriter) Write(x int32) {
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
func merge(fn1 string, fn2 string, files chan string, errors chan error, inFlight *int32) {
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
	atomic.AddInt32(inFlight, -1)
	files <- fm.Name()
	done.Unlock()
}
