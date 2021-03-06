// processing of files of 32 bit little endian numbers.

package msort

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"reflect"
	"strconv"
	"unsafe"
)

const bytesPerNumber = 4

// helper struct for reading a file of binary 4 byte little endian integers.
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
		if a.err != nil || a.eof {
			return false
		}
	}
	a.top = a.buf[a.next]
	a.next++
	return true
}

func writeBInt(h io.Writer, x int32) {
	buf := make([]byte, bytesPerNumber)
	binary.LittleEndian.PutUint32(buf, uint32(x))
	h.Write(buf)
}

// WriteBInts writes a slice of numbers to f.
func writeBInts(f io.Writer, a []int32) error {
	// Use unsafe cast to avoid unnecessary copy of data.
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&a))
	header.Len *= bytesPerNumber
	header.Cap *= bytesPerNumber

	// Convert slice header to an []byte.
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

// intWriter is a helper structure for writing binary numbers to a file.
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

func (w *intWriter) flush() error {
	if w.err != nil {
		return w.err
	}
	w.err = writeBInts(w.f, w.buf)
	w.buf = w.buf[0:0]
	return w.err
}

// writeInt32 writes an number into an intWriter.
// The intWriter stores any errors encoutered internally, and these should be checked
// when intWriter.flush is called.
func (w *intWriter) writeInt32(x int32) {
	if w.err != nil {
		return
	}
	if len(w.buf) >= w.maxLen {
		w.flush()
	}
	w.buf = append(w.buf, x)
}

// binToAscii reads binary encodes numbers from file inFile and writes them as text to outFile.
func binToAscii(inFile string, w io.Writer) error {
	h, err := os.Open(inFile)
	if err != nil {
		return err
	}
	defer h.Close()
	return doBinToAscii(w, h)
}

func doBinToAscii(w io.Writer, r io.Reader) error {
	const writeBufferSize = 64 * 1024
	buf := make([]byte, 0, writeBufferSize)
	a := newIStream(r)
	for a.Next() {
		// buffer nearly full, lets flush buf.
		if cap(buf)-len(buf) < 20 {
			_, err := w.Write(buf)
			if err != nil {
				return err
			}
			buf = buf[:0]
		}
		buf = strconv.AppendInt(buf, int64(a.top), 10)
		buf = append(buf, '\n')
	}
	_, err := w.Write(buf)
	return err
}
