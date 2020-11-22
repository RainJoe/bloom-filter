package bloom_filter

import (
	"errors"
	"sync"
)

const BitsPerUint32 uint32 = 1 << 5

type MemoryBitArray struct {
	mu   sync.RWMutex
	size uint32
	bits []uint32
}

func roundUpBitArraySize(size uint32) uint32 {
	bitsSize := size / BitsPerUint32
	if size%BitsPerUint32 > 0 {
		return bitsSize + 1
	}
	return bitsSize
}

func NewMemoryBitArray(size uint32) *MemoryBitArray {
	return &MemoryBitArray{
		size: size,
		bits: make([]uint32, roundUpBitArraySize(size)),
	}
}

func (m *MemoryBitArray) SetBits(offsets []uint32) error {
	for _, offset := range offsets {
		if offset > m.size {
			return errors.New("one of offset out of range")
		}
	}
	m.mu.Lock()
	for _, offset := range offsets {
		index := offset / BitsPerUint32
		offsetOfPerUint32 := offset % BitsPerUint32
		m.bits[index] |= 1 << offsetOfPerUint32
	}
	m.mu.Unlock()
	return nil
}

func (m *MemoryBitArray) GetBits(offsets []uint32) ([]uint32, error) {
	for _, offset := range offsets {
		if offset > m.size {
			return nil, errors.New("one of offset out of range")
		}
	}
	var exits []uint32
	m.mu.Lock()
	for _, offset := range offsets {
		index := offset / BitsPerUint32
		offsetOfPerUint32 := offset % BitsPerUint32
		if (m.bits[index] & (1 << offsetOfPerUint32)) != 0 {
			exits = append(exits, offset)
		}
	}
	m.mu.Unlock()
	return exits, nil
}
