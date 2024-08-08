package utils

import (
	"log"
	"math"
)

// isPowerOf2 checks if a number is a power of 2.
func isPowerOf2(x uint64) bool {
	return x > 0 && (x&(x-1)) == 0
}

// log2 calculates the base 2 logarithm for power of 2 numbers.
func log2(x uint64) uint64 {
	result := uint64(0)
	for x > 1 {
		x >>= 1
		result++
	}
	return result
}

// alignLeft aligns the offset to the left according to the block size.
func alignLeft(offset uint64, bs uint64) uint64 {
	if !isPowerOf2(bs) {
		panic("bs must be a power of 2")
	}
	return offset &^ (bs - 1)
}

// isAligned checks if a number is aligned to a given block size.
func isAligned(offset uint64, bs uint64) bool {
	return (offset & (bs - 1)) == 0
}

// blockIndex returns the index of the block that contains the given offset.
func blockIndex(offset uint64, bs uint64) uint64 {
	if !isPowerOf2(bs) {
		log.Fatal("bs must be a power of 2")
	}
	return alignLeft(offset, bs) >> log2(bs)
}

// blockCount returns the number of blocks that the range [offset, offset + size) spans.
func blockCount(offset uint64, size uint64, bs uint64) uint64 {
	if !isPowerOf2(bs) {
		log.Fatal("bs must be a power of 2")
	}

	// Check for overflow (simple version, you may want to handle more rigorously in production)
	if size > 0 && offset+size < offset {
		log.Fatal("overflow detected in offset + size")
	}

	firstBlock := alignLeft(offset, bs)
	finalBlock := alignLeft(offset+size, bs)

	mask := uint64(0)
	if size > 0 {
		mask = ^uint64(0)
	}

	return ((finalBlock >> log2(bs)) -
		(firstBlock >> log2(bs)) +
		boolToUint64(!isAligned(offset+size, bs))) &
		mask
}

// boolToUint64 converts a boolean to uint64 (true -> 1, false -> 0).
func boolToUint64(condition bool) uint64 {
	if condition {
		return 1
	}
	return 0
} // chnkLalign aligns the offset to the nearest lower multiple of chnkSize.
func chnkLalign(offset int64, chnkSize uint64) int64 {
	return offset & ^(int64(chnkSize) - 1)
}

// chnkCountForOffset computes the chunk count for a given offset and count.
func chnkCountForOffset(offset int64, count uint64, chnkSize uint64) uint64 {
	chnkStart := chnkLalign(offset, chnkSize)
	chnkEnd := chnkLalign(offset+int64(count)-1, chnkSize)

	return uint64((chnkEnd >> uint64(math.Log2(float64(chnkSize)))) -
		(chnkStart >> uint64(math.Log2(float64(chnkSize)))) + 1)
}
