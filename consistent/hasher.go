package consistent

import (
	"github.com/cespare/xxhash/v2"
	"github.com/spaolacci/murmur3"
)

// XXHasher .
type XXHasher struct{}

// MurmurHash3Hasher .
type MurmurHash3Hasher struct{}

// NewXXHasher creates a new xxHash hasher.
func NewXXHasher() *XXHasher {
	return &XXHasher{}
}

// Sum64 calculates the 64-bit hash of a byte slice using xxHash.
func (h *XXHasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// NewDefaultHasher creates a new default hasher (xxHash).
func NewDefaultHasher() Hasher {
	return NewXXHasher()
}

// NewMurmurHash3Hasher creates a new MurmurHash3 hasher.
func NewMurmurHash3Hasher() *MurmurHash3Hasher {
	return &MurmurHash3Hasher{}
}

// Sum64 calculates the 64-bit hash of a byte slice using MurmurHash3.
func (h *MurmurHash3Hasher) Sum64(data []byte) uint64 {
	return murmur3.Sum64(data)
}
