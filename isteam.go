// processing of files of 32 bit little endian numbers.

package msort

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"unsafe"
)

const bytesPerNumber = 4

var tmpDir = os.TempDir()

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

// writeBIntsToFile writes a slice of numbers into a new temprary file, returning the name of the temporary file
func writeBIntsToFile(a []int32) (string, error) {
	f, err := ioutil.TempFile("", "sortchunk")
	if err != nil {
		return "", err
	}
	defer f.Close()
	err = writeBInts(f, a)
	return f.Name(), err
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
