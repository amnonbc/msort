package msort

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func Test_astream_Next(t *testing.T) {
	a := newAStream(strings.NewReader("1\n2"))

	ok := a.Next()
	assert.True(t, ok)
	assert.False(t, a.eof)
	assert.NoError(t, a.err)
	assert.Equal(t, int32(1), a.top)

	ok = a.Next()
	assert.True(t, ok)
	assert.False(t, a.eof)
	assert.NoError(t, a.err)
	assert.Equal(t, int32(2), a.top)

	ok = a.Next()
	assert.False(t, ok)
	assert.True(t, a.eof)
	assert.NoError(t, a.err)

}

func Test_astream_NextError(t *testing.T) {
	a := newAStream(errorReader(0))
	ok := a.Next()
	assert.False(t, ok)
	assert.False(t, a.ok())
	assert.Error(t, a.err)
}

func Test_astream_ReadNums(t *testing.T) {
	a := newAStream(strings.NewReader("1\n2\n3\n"))
	nums := make([]int32, 2)
	n, err := a.readNums(nums)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, []int32{1, 2}, nums)

	n, err = a.readNums(nums)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, int32(3), nums[0])

}

func Test_astream_ReadNumsError(t *testing.T) {
	a := newAStream(strings.NewReader("abc"))
	nums := make([]int32, 2)
	_, err := a.readNums(nums)
	assert.Error(t, err)
}

func Test_astream_ReadNumsErrorBadFile(t *testing.T) {
	a := newAStream(errorReader(0))
	nums := make([]int32, 2)
	_, err := a.readNums(nums)
	assert.Error(t, err)
}

func Test_atoi(t *testing.T) {
	tests := []struct {
		arg     string
		want    int32
		wantErr bool
	}{
		{"0", 0, false},
		{"123", 123, false},
		{"-123", -123, false},
		{"not_a_number", 0, true},
		{"9999999999999999999999", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.arg, func(t *testing.T) {
			got, err := atoi([]byte(tt.arg))
			if (err != nil) != tt.wantErr {
				t.Errorf("atoi() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("atoi() got = %v, want %v", got, tt.want)
			}
		})
	}
}
