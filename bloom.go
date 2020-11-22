package bloom_filter

import (
	"hash"

	"github.com/twmb/murmur3"
)

// DefaultSize is the max length of bit array default is 1 << 32 - 1.
// It's 4.29 billion, 512M of memory occupied.
const DefaultSize uint32 = 1<<32 - 1
const DefaultHashCombinations uint32 = 14

type options struct {
	bitArray BitArray
	hash     hash.Hash32
	m        uint32
	k        uint32
}

var defaultOptions = options{
	m:    DefaultSize,
	k:    DefaultHashCombinations,
	hash: murmur3.New32(),
}

type Option interface {
	apply(*options)
}

type funcOption struct {
	f func(*options)
}

func newFuncOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

func (fo *funcOption) apply(opts *options) {
	fo.f(opts)
}

// WithSize set bloom filter bits size default is 4.29 billion occupied 512MB
func WithSize(size uint32) Option {
	return newFuncOption(func(o *options) {
		o.m = size
	})
}

// WithHashCombinations sets how many hash functions to use default is 14.
// When m/n = 20 (m is bitarray size, n is the elements to save), the false positive rate
// is 6.71e-05
func WithHashCombinations(combinations uint32) Option {
	return newFuncOption(func(o *options) {
		o.k = combinations
	})
}

// WithHash sets which hash function to use default is murmur3
func WithHash(h hash.Hash32) Option {
	return newFuncOption(func(o *options) {
		o.hash = h
	})
}

// WithBitArray set the underlying storage of bloom filter. default is memory storage.
func WithBitArray(ba BitArray) Option {
	return newFuncOption(func(o *options) {
		o.bitArray = ba
	})
}

type BitArray interface {
	GetBits(offsets []uint32) ([]uint32, error)
	SetBits(offsets []uint32) error
}

type BloomFilter struct {
	bitArray BitArray
	hash     hash.Hash32
	// m is bit array size
	m uint32
	// k means how many hash function
	k uint32
}

func NewBloomFilter(opts ...Option) *BloomFilter {
	for _, opt := range opts {
		opt.apply(&defaultOptions)
	}
	if defaultOptions.bitArray == nil {
		defaultOptions.bitArray = NewMemoryBitArray(defaultOptions.m)
	}
	return &BloomFilter{
		bitArray: defaultOptions.bitArray,
		hash:     defaultOptions.hash,
		m:        defaultOptions.m,
		k:        defaultOptions.k,
	}
}

func (b *BloomFilter) getIndexes(key []byte) []uint32 {
	indexes := make([]uint32, b.k)
	for i := uint32(0); i < b.k; i++ {
		_, _ = b.hash.Write(append(key, byte(i)))
		indexes[i] = b.hash.Sum32() % b.m
		b.hash.Reset()
	}
	return indexes
}

func (b *BloomFilter) Set(key []byte) error {
	indexes := b.getIndexes(key)
	return b.bitArray.SetBits(indexes)
}

func (b *BloomFilter) Exist(key []byte) (bool, error) {
	indexes := b.getIndexes(key)
	exits, err := b.bitArray.GetBits(indexes)
	if err != nil {
		return false, err
	}
	return len(exits) == len(indexes), nil
}
