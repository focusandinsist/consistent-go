package consistent

import (
	"bytes"
	"testing"
)

// TestXXHasher tests the XXHasher implementation
func TestXXHasher(t *testing.T) {
	hasher := NewXXHasher()

	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"single_byte", []byte{1}},
		{"small_string", []byte("hello")},
		{"medium_string", []byte("hello world this is a test")},
		{"large_string", bytes.Repeat([]byte("a"), 1000)},
		{"binary_data", []byte{0, 1, 2, 3, 255, 254, 253}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hash1 := hasher.Sum64(tc.data)
			hash2 := hasher.Sum64(tc.data)

			// Hash should be deterministic
			if hash1 != hash2 {
				t.Errorf("Hash not deterministic: %d != %d", hash1, hash2)
			}

			// Hash should not be zero for non-empty data (very unlikely)
			if len(tc.data) > 0 && hash1 == 0 {
				t.Errorf("Hash is zero for non-empty data")
			}
		})
	}
}

// TestMurmurHash3Hasher tests the MurmurHash3Hasher implementation
func TestMurmurHash3Hasher(t *testing.T) {
	hasher := NewMurmurHash3Hasher()

	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"single_byte", []byte{1}},
		{"small_string", []byte("hello")},
		{"medium_string", []byte("hello world this is a test")},
		{"large_string", bytes.Repeat([]byte("d"), 1000)},
		{"binary_data", []byte{0, 1, 2, 3, 255, 254, 253}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hash1 := hasher.Sum64(tc.data)
			hash2 := hasher.Sum64(tc.data)

			// Hash should be deterministic
			if hash1 != hash2 {
				t.Errorf("Hash not deterministic: %d != %d", hash1, hash2)
			}
		})
	}
}

// TestHasherConsistency tests that different hashers produce different results
func TestHasherConsistency(t *testing.T) {
	testData := []byte("test data for hashing")

	xxHasher := NewXXHasher()
	murmurHasher := NewMurmurHash3Hasher()

	xxHash := xxHasher.Sum64(testData)
	murmurHash := murmurHasher.Sum64(testData)

	// Different hashers should produce different results (very likely)
	if xxHash == murmurHash {
		t.Logf("Warning: Hash collision between XXHash and MurmurHash: %d", xxHash)
	}

	// Both hashes should be non-zero for non-empty data
	if xxHash == 0 {
		t.Error("XXHash returned zero for non-empty data")
	}
	if murmurHash == 0 {
		t.Error("MurmurHash returned zero for non-empty data")
	}
}

// TestDefaultHasher tests the default hasher creation
func TestDefaultHasher(t *testing.T) {
	hasher := NewDefaultHasher()
	if hasher == nil {
		t.Error("NewDefaultHasher() returned nil")
	}

	// Should be XXHasher by default
	if _, ok := hasher.(*XXHasher); !ok {
		t.Error("Default hasher is not XXHasher")
	}

	// Test basic functionality
	testData := []byte("test")
	hash := hasher.Sum64(testData)
	if hash == 0 {
		t.Error("Default hasher returned zero hash")
	}
}

// TestHashDistribution tests hash distribution quality
func TestHashDistribution(t *testing.T) {
	hasher := NewDefaultHasher()

	// Generate many hashes and check distribution
	const numHashes = 10000
	const numBuckets = 100

	buckets := make([]int, numBuckets)

	for i := 0; i < numHashes; i++ {
		data := []byte("key-" + string(rune(i)))
		hash := hasher.Sum64(data)
		bucket := hash % numBuckets
		buckets[bucket]++
	}

	// Check that distribution is reasonably uniform
	expectedPerBucket := numHashes / numBuckets
	tolerance := expectedPerBucket / 2 // 50% tolerance

	for i, count := range buckets {
		if count < expectedPerBucket-tolerance || count > expectedPerBucket+tolerance {
			t.Logf("Bucket %d has %d items (expected ~%d)", i, count, expectedPerBucket)
		}
	}

	// At least check that no bucket is completely empty
	emptyBuckets := 0
	for _, count := range buckets {
		if count == 0 {
			emptyBuckets++
		}
	}

	if emptyBuckets > numBuckets/10 { // More than 10% empty buckets is concerning
		t.Errorf("Too many empty buckets: %d/%d", emptyBuckets, numBuckets)
	}
}

// BenchmarkHashers benchmarks different hasher implementations
func BenchmarkXXHasher(b *testing.B) {
	hasher := NewXXHasher()
	data := []byte("benchmark test data for hashing performance")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hasher.Sum64(data)
	}
}

func BenchmarkMurmurHash3Hasher(b *testing.B) {
	hasher := NewMurmurHash3Hasher()
	data := []byte("benchmark test data for hashing performance")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hasher.Sum64(data)
	}
}

// BenchmarkHasherComparison compares the two recommended hashers with different data sizes
func BenchmarkHasherComparison(b *testing.B) {
	hashers := map[string]Hasher{
		"XXHash":     NewXXHasher(),
		"MurmurHash": NewMurmurHash3Hasher(),
	}

	dataSizes := []int{8, 64, 256, 1024}

	for name, hasher := range hashers {
		for _, size := range dataSizes {
			data := bytes.Repeat([]byte("x"), size)
			b.Run(name+"_"+string(rune(size))+"bytes", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					hasher.Sum64(data)
				}
			})
		}
	}
}
