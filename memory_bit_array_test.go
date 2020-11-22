package bloom_filter

import (
	"reflect"
	"sync"
	"testing"
)

func TestNewMemoryBitArray(t *testing.T) {
	mb := NewMemoryBitArray(DefaultSize)
	if mb.size != DefaultSize {
		t.Error("got wrong size")
	}
	if len(mb.bits) != int(roundUpBitArraySize(DefaultSize)) {
		t.Error("got wrong size of bits")
	}
}

func TestMemoryBitArray_GetBits(t *testing.T) {
	var getTests = []struct {
		name       string
		size       uint32
		bitsToAdd  []uint32
		bitsToGet  []uint32
		expectedOk bool
	}{
		{"hits", 100, []uint32{10}, []uint32{10}, true},
		{"miss", 100, []uint32{10}, []uint32{20}, false},
		{"out_ouf_range", 100, []uint32{101}, []uint32{101}, false},
	}
	// basics
	for _, tt := range getTests {
		mb := NewMemoryBitArray(tt.size)
		_ = mb.SetBits(tt.bitsToAdd)
		bits, _ := mb.GetBits(tt.bitsToGet)
		got := reflect.DeepEqual(bits, tt.bitsToGet)
		if got != tt.expectedOk {
			t.Fatalf("expected: %v, got: %v", tt.expectedOk, got)
		}
	}
}

func TestMemoryBitArray_GetBitsParallel(t *testing.T) {
	var getTestsParallel = []struct {
		name       string
		bitsToAdd  []uint32
		bitsToGet  []uint32
		expectedOk bool
	}{
		{"hits", []uint32{10}, []uint32{10}, true},
		{"miss", []uint32{10}, []uint32{20}, false},
	}
	// goroutine safe
	var wg sync.WaitGroup
	mb := NewMemoryBitArray(DefaultSize)
	for _, tt := range getTestsParallel {
		tt := tt
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = mb.SetBits(tt.bitsToAdd)
			bits, _ := mb.GetBits(tt.bitsToGet)
			got := reflect.DeepEqual(bits, tt.bitsToGet)
			if got != tt.expectedOk {
				t.Errorf("expected: %v, got: %v", tt.expectedOk, got)
			}
		}()
	}
}
