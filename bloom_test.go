package bloom_filter

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
)

func TestBloomFilter_Exist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bitArray := NewMockBitArray(ctrl)
	bf := NewBloomFilter()
	key1 := []byte("key1")
	key2 := []byte("key2")
	bitArray.EXPECT().SetBits(gomock.Any()).Return(nil).AnyTimes()
	bitArray.EXPECT().GetBits(gomock.Any()).DoAndReturn(func(offsets []uint32) ([]uint32, error) {
		if reflect.DeepEqual(offsets, bf.getIndexes(key1)) {
			return offsets, nil
		} else {
			return nil, nil
		}
	}).AnyTimes()
	_ = bf.Set(key1)
	ok, _ := bf.Exist(key1)
	if !ok {
		t.Errorf("expected: %v, got: %v", true, ok)
	}
	ok, _ = bf.Exist(key2)
	if ok {
		t.Errorf("expected: %v, got: %v", false, ok)
	}
}
