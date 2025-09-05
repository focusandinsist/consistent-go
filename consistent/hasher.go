package consistent

import (
	"hash/crc64"
	"hash/fnv"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/spaolacci/murmur3"
)

var (
	crc64Table *crc64.Table
	once       sync.Once
)

// XXHasher .
type XXHasher struct{}

// CRC64Hasher .
type CRC64Hasher struct {
	table *crc64.Table
}

// FNVHasher .
type FNVHasher struct{}

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

// NewCRC64Hasher creates a new CRC64 hasher.
func NewCRC64Hasher() *CRC64Hasher {
	return &CRC64Hasher{
		table: getCRC64Table(),
	}
}

// Sum64 calculates the 64-bit hash of a byte slice using CRC64.
func (h *CRC64Hasher) Sum64(data []byte) uint64 {
	return crc64.Checksum(data, h.table)
}

// NewFNVHasher creates a new FNV hasher.
func NewFNVHasher() *FNVHasher {
	return &FNVHasher{}
}

// Sum64 calculates the 64-bit hash of a byte slice using FNV-1a.
func (h *FNVHasher) Sum64(data []byte) uint64 {
	hash := fnv.New64a()
	hash.Write(data)
	return hash.Sum64()
}

// NewMurmurHash3Hasher creates a new MurmurHash3 hasher.
func NewMurmurHash3Hasher() *MurmurHash3Hasher {
	return &MurmurHash3Hasher{}
}

// Sum64 calculates the 64-bit hash of a byte slice using MurmurHash3.
func (h *MurmurHash3Hasher) Sum64(data []byte) uint64 {
	return murmur3.Sum64(data)
}

// getCRC64Table returns a CRC64 ISO table
func getCRC64Table() *crc64.Table {
	once.Do(func() {
		crc64Table = crc64.MakeTable(crc64.ISO)
	})
	return crc64Table
}
