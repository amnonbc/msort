// processing of ascii files containing a number on each line

package msort

import (
	"bufio"
	"fmt"
	"io"
)

type aStream struct {
	top int32
	r   *bufio.Scanner
	eof bool
	err error
}

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

// readNums reads at most len(nums) numbers from ascii stream a into array. It returns the number of numbers
// read.
func (a *aStream) readNums(array []int32) (int, error) {
	n := 0
	for n < len(array) && a.Next() {
		array[n] = a.top
		n++
	}
	return n, a.err
}
