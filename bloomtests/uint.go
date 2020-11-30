package bloomtests

import (
	"unsafe"
)

type BitSets []int64

func Str2byte(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func Byte2Str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func NewBitSets(n uint) BitSets {
	bs := make(BitSets, n/64+1)
	return bs
}

func (bs BitSets) Set(index uint) {
	index, bit := index/64, index%64
	bs[index] |= 1 << bit
}

func (bs BitSets) Unset(index uint) {
	index, bit := index/64, index%64
	bs[index] ^= 1 << bit
}

func (bs BitSets) IsSet(index uint) bool {
	index, bit := index/64, index%64
	word := bs[index]
	return (word | (1 << bit)) == word
}
