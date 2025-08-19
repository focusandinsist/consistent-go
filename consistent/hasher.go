package consistent

import (
	"hash/crc64"
	"hash/fnv"
	"sync"
)

var (
	crc64Table *crc64.Table
	once       sync.Once
)

// getCRC64Table returns the shared CRC64 table, creating it only once.
func getCRC64Table() *crc64.Table {
	once.Do(func() {
		crc64Table = crc64.MakeTable(crc64.ISO)
	})
	return crc64Table
}

// CRC64Hasher is a hasher that uses the CRC64 algorithm.
type CRC64Hasher struct {
	table *crc64.Table
}

// NewCRC64Hasher creates a new CRC64 hasher.
func NewCRC64Hasher() *CRC64Hasher {
	return &CRC64Hasher{
		table: getCRC64Table(),
	}
}

// Sum64 calculates the 64-bit hash of a byte slice.
func (h *CRC64Hasher) Sum64(data []byte) uint64 {
	return crc64.Checksum(data, h.table)
}

// FNVHasher is a hasher that uses the FNV algorithm.
type FNVHasher struct{}

// NewFNVHasher creates a new FNV hasher.
func NewFNVHasher() *FNVHasher {
	return &FNVHasher{}
}

// Sum64 calculates the 64-bit hash of a byte slice.
func (h *FNVHasher) Sum64(data []byte) uint64 {
	hash := fnv.New64a()
	hash.Write(data)
	return hash.Sum64()
}
