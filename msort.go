package msort

import (
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

type sorter struct {
	fileChan     chan string
	errors       chan error
	activeWorker chan bool

	done     sync.Mutex
	inFlight int32
}

func newSorter() *sorter {
	return &sorter{
		fileChan:     make(chan string, 20),
		errors:       make(chan error),
		activeWorker: make(chan bool, runtime.NumCPU()),
		inFlight:     1,
	}
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
			w.writeInt32(a.top)
			a.Next()
		} else {
			w.writeInt32(b.top)
			b.Next()
		}
	}
	for a.ok() {
		w.writeInt32(a.top)
		a.Next()
	}
	for b.ok() {
		w.writeInt32(b.top)
		b.Next()
	}

	if a.err != nil {
		return a.err
	}
	if b.err != nil {
		return b.err
	}

	return w.flush()
}

// leafsort reads numbers from r, breaks them into sorted chunks of length chunkSz and writes each chunk to a file.
// The name of each chunk file is written to s.fileChan.
func (s *sorter) leafSort(r io.Reader, chunkSz int) {
	s.activeWorker <- true
	defer func() {
		_ = <-s.activeWorker
	}()

	buf := make([]int32, chunkSz)
	a := newAStream(r)

	for !a.eof && a.err == nil {
		n, err := a.readNums(buf)
		if err != nil {
			s.errors <- err
			return
		}
		buf = buf[0:n]
		sort.Slice(buf, func(i, j int) bool {
			return buf[i] < buf[j]
		})
		fn, err := writeBIntsToFile(buf)
		if err != nil {
			s.errors <- err
			return
		}
		s.fileChan <- fn
	}
	s.done.Lock()
	atomic.AddInt32(&s.inFlight, -1)
	s.done.Unlock()
}

// merge merges fn1 and fn2, and writes the merged output into a new temporary file.
// It deletes both of its input files.
// It writes the name of the newly created temporary file to filesChan
func (s *sorter) merge(fn1 string, fn2 string) {
	// limit number of workers to NumCPU()
	s.activeWorker <- true
	defer func() {
		_ = <-s.activeWorker
	}()

	f1, err := os.Open(fn1)
	if err != nil {
		s.errors <- err
		return
	}
	os.Remove(fn1)
	defer f1.Close()

	f2, err := os.Open(fn2)
	if err != nil {
		s.errors <- err
		return
	}
	os.Remove(fn2)
	defer f1.Close()

	fm, err := ioutil.TempFile("", "sortchunk")
	if err != nil {
		s.errors <- err
		return
	}
	defer fm.Close()
	err = doMerge(fm, f1, f2)
	if err != nil {
		s.errors <- err
		return
	}
	s.done.Lock()
	atomic.AddInt32(&s.inFlight, -1)
	s.fileChan <- fm.Name()
	s.done.Unlock()
}

// SortFile sorts numbers from r, saving the output to outFileName.
func SortFile(outFileName string, r io.Reader, chunkSz int) error {
	left, right := "", ""
	s := newSorter()
	go s.leafSort(r, chunkSz)
	for {
		select {
		case left = <-s.fileChan:
		case err := <-s.errors:
			return err
		}

		s.done.Lock()
		inF := atomic.LoadInt32(&s.inFlight)
		s.done.Unlock()
		if inF == 0 && len(s.fileChan) == 0 {
			// We only have one file, and no more in flight, so we are done!
			break
		}
		select {
		case right = <-s.fileChan:
		case err := <-s.errors:
			return err
		}
		atomic.AddInt32(&s.inFlight, 1)
		go s.merge(left, right)
	}
	defer os.Remove(left)
	return binToAscii(left, outFileName)
}
