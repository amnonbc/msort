package msort

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

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

type randIReader int

func (r *randIReader) Read(buf []byte) (int, error) {
	buf = buf[:0]
	n := int32(0)
	for cap(buf)-len(buf) > 4 {
		buf = append(buf, encode(n)...)
		n++
	}
	return len(buf), nil
}

func Benchmark_iStream_Next(b *testing.B) {
	r := randIReader(0)
	i := newIStream(&r)
	for n := 0; n < b.N; n++ {
		ok := i.Next()

		if !ok {
			b.Fatal(n, "failed", i)
		}
	}
}
