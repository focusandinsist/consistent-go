package consistent

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
)

const (
	DefaultPartitionCount    = 271
	DefaultReplicationFactor = 20
	DefaultLoad              = 1.25
)

var (
	ErrInsufficientMemberCount = errors.New("insufficient number of members")
	ErrInsufficientSpace       = errors.New("not enough space to distribute partitions")
)

// Hasher generates a 64-bit unsigned hash for a given byte slice.
// A Hasher should minimize collisions (generating the same hash for different byte slices).
// Performance is also important, so fast functions are preferred.
type Hasher interface {
	Sum64([]byte) uint64
}

// Config represents the configuration that controls the consistent hashing package.
type Config struct {
	// Hasher is responsible for generating a 64-bit unsigned hash for a given byte slice.
	Hasher Hasher

	// Keys are distributed among partitions. A prime number is good for distributing keys uniformly.
	// If you have too many keys, choose a large PartitionCount.
	PartitionCount int

	// Members are replicated on the consistent hash ring.
	// This number represents how many times a member is replicated on the ring.
	ReplicationFactor int

	// Load is used to calculate the average load.
	Load float64
}

// Consistent holds information about the members of the consistent hash ring.
type Consistent struct {
	mu sync.RWMutex

	config              Config
	hasher              Hasher
	sortedSet           []uint64
	partitionCount      uint64
	loads               map[string]float64
	members             map[string]struct{}
	partitions          map[int]string
	ring                map[uint64]string
	sortedPartitionKeys []uint64
	partitionHashes     map[uint64]int

	// Cache-related fields
	cachedMembers []string
	membersDirty  bool
}

// New creates and returns a new Consistent object.
// New creates and returns a new Consistent object with an initial list of members.
func New(members []string, config Config) (*Consistent, error) {
	// Set defaults
	if config.PartitionCount == 0 {
		config.PartitionCount = DefaultPartitionCount
	}
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = DefaultReplicationFactor
	}
	if config.Load == 0 {
		config.Load = DefaultLoad
	}

	// Validate configuration
	if err := validateConfig(len(members), config); err != nil {
		return nil, err
	}

	c := &Consistent{
		config:              config,
		members:             make(map[string]struct{}),
		partitionCount:      uint64(config.PartitionCount),
		ring:                make(map[uint64]string),
		partitions:          make(map[int]string),
		loads:               make(map[string]float64),
		membersDirty:        true,
		sortedPartitionKeys: make([]uint64, 0, config.PartitionCount),
		partitionHashes:     make(map[uint64]int, config.PartitionCount),
	}

	c.hasher = config.Hasher
	for _, member := range members {
		c.initMember(member)
	}
	// Precompute partition hashes and sort them for efficient lookups.
	bs := make([]byte, 8)
	for partID := 0; partID < int(c.partitionCount); partID++ {
		binary.LittleEndian.PutUint64(bs, uint64(partID))
		partKey := c.hasher.Sum64(bs)
		c.sortedPartitionKeys = append(c.sortedPartitionKeys, partKey)
		c.partitionHashes[partKey] = partID
	}
	sort.Slice(c.sortedPartitionKeys, func(i, j int) bool {
		return c.sortedPartitionKeys[i] < c.sortedPartitionKeys[j]
	})

	if len(members) > 0 {
		if err := c.distributePartitions(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// NewConsistent creates and returns a new empty Consistent object.
func NewConsistent(config Config) (*Consistent, error) {
	return New([]string{}, config)
}

// validateConfig validates the configuration parameters.
func validateConfig(memberCount int, config Config) error {
	if config.Hasher == nil {
		return errors.New("hasher cannot be nil")
	}
	if memberCount == 0 {
		return nil // Empty ring is valid
	}

	// Check if the configuration can support the required partitions
	avgLoad := float64(config.PartitionCount) / float64(memberCount) * config.Load
	maxLoad := math.Ceil(avgLoad)

	// Rough estimation: if average load per member exceeds virtual nodes per member significantly,
	// it might be difficult to distribute partitions
	if maxLoad > float64(config.ReplicationFactor)*2 {
		return fmt.Errorf("configuration may cause distribution issues: partitionCount=%d, memberCount=%d, load=%.2f results in avgLoad=%.2f per member",
			config.PartitionCount, memberCount, config.Load, maxLoad)
	}

	return nil
}

// initMember adds a member to the hash ring during initialization.
func (c *Consistent) initMember(member string) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := buildVirtualNodeKey(member, i)
		h := c.hasher.Sum64(key)
		c.ring[h] = member
		c.sortedSet = append(c.sortedSet, h)
	}
	// Sort the hash values in ascending order.
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})
	// Storing members in this map helps find backup members for a partition.
	c.members[member] = struct{}{}
	// Mark the member cache as dirty.
	c.membersDirty = true
}

// distributePartitions distributes the partitions.
func (c *Consistent) distributePartitions() error {
	loads := make(map[string]float64)
	partitions := make(map[int]string)

	bs := make([]byte, 8)
	for partID := uint64(0); partID < c.partitionCount; partID++ {
		binary.LittleEndian.PutUint64(bs, partID)
		key := c.hasher.Sum64(bs)
		idx := sort.Search(len(c.sortedSet), func(i int) bool {
			return c.sortedSet[i] >= key
		})
		if idx >= len(c.sortedSet) {
			idx = 0
		}
		if err := c.distributeWithLoad(int(partID), idx, partitions, loads); err != nil {
			return err
		}
	}
	c.partitions = partitions
	c.loads = loads
	return nil
}

// distributeWithLoad distributes partitions based on load.
func (c *Consistent) distributeWithLoad(partID, idx int, partitions map[int]string, loads map[string]float64) error {
	avgLoad := c.averageLoad()
	var count int
	for {
		count++
		if count >= len(c.sortedSet) {
			// You need to reduce the partition count, increase the member count, or increase the load factor.
			return fmt.Errorf("%w: partition %d cannot be assigned after %d attempts (avgLoad=%.2f, members=%d, virtualNodes=%d)",
				ErrInsufficientSpace, partID, count, avgLoad, len(c.members), len(c.sortedSet))
		}
		i := c.sortedSet[idx]
		member := c.ring[i]
		load := loads[member]
		if load+1 <= avgLoad {
			partitions[partID] = member
			loads[member]++
			return nil
		}
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
	}
}

// Add adds a new member to the consistent hash ring
func (c *Consistent) Add(ctx context.Context, member string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// First, check if the member already exists (only needs a read lock).
	c.mu.RLock()
	if _, ok := c.members[member]; ok {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	// Acquire the write lock to modify the ring structure.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check if the member already exists after acquiring the lock.
	if _, ok := c.members[member]; ok {
		return nil
	}

	// Add the member to the ring and partitions incrementally.
	c.members[member] = struct{}{}
	c.addToRing(member)
	c.remapPartitionsForNewMember(member)
	c.membersDirty = true

	return nil
}

// remapPartitionsForNewMember incrementally updates the partition map when a new member is added.
// This is a true incremental update that respects the load factor.
func (c *Consistent) remapPartitionsForNewMember(member string) {
	c.loads[member] = 0
	avgLoad := c.averageLoad()

	for i := 0; i < c.config.ReplicationFactor; i++ {
		vnodeKey := buildVirtualNodeKey(member, i)
		h := c.hasher.Sum64(vnodeKey)

		// Find the position of the new virtual node in the sorted set.
		idx := sort.Search(len(c.sortedSet), func(j int) bool {
			return c.sortedSet[j] >= h
		})
		if idx >= len(c.sortedSet) {
			idx = 0
		}

		// Find the predecessor virtual node to define the range of partitions to check.
		prevIdx := idx - 1
		if prevIdx < 0 {
			prevIdx = len(c.sortedSet) - 1
		}
		prevHash := c.sortedSet[prevIdx]

		// Find the starting index of partitions that fall in the range (prevHash, h].
		partIdx := sort.Search(len(c.sortedPartitionKeys), func(j int) bool {
			return c.sortedPartitionKeys[j] > prevHash
		})

		// Iterate over the partitions in the affected range.
		for {
			if partIdx >= len(c.sortedPartitionKeys) {
				partIdx = 0 // Wrap around the sorted partition keys.
			}
			partKey := c.sortedPartitionKeys[partIdx]

			// Stop if iterated past the new virtual node's hash, completing the range check.
			isPast := false
			if prevHash < h { // Normal case, no wrap-around for the vnode ring.
				if partKey > h || partKey <= prevHash {
					isPast = true
				}
			} else { // Wrap-around case for the vnode ring.
				if partKey > h && partKey <= prevHash {
					isPast = true
				}
			}
			if isPast {
				break
			}

			// This partition is a candidate to be moved to the new member.
			if c.loads[member]+1 <= avgLoad {
				partID := c.partitionHashes[partKey]
				oldOwner := c.partitions[partID]
				// Only steal the partition if it was previously owned by someone else.
				if oldOwner != member {
					c.loads[oldOwner]--
					c.partitions[partID] = member
					c.loads[member]++
				}
			} else {
				// The new member is overloaded, cannot take any more partitions for this vnode.
				break
			}
			partIdx++
		}
	}
}

// Remove removes a member from the consistent hash ring.
func (c *Consistent) Remove(ctx context.Context, member string) error {
	return c.RemoveByName(ctx, member)
}

// RemoveByName removes a member from the consistent hash ring by name.
func (c *Consistent) RemoveByName(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// First, check if the member exists
	c.mu.RLock()
	if _, ok := c.members[name]; !ok {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	// Acquire the write lock to quickly update data structures.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check if the member exists.
	if _, ok := c.members[name]; !ok {
		return nil
	}

	// First, find all partitions owned by the member being removed.
	partitionsToRemap := []int{}
	for partID, owner := range c.partitions {
		if owner == name {
			partitionsToRemap = append(partitionsToRemap, partID)
		}
	}

	delete(c.loads, name)
	delete(c.members, name)
	c.removeFromRing(name)

	// Remap only the affected partitions with load balancing.
	avgLoad := c.averageLoad()
	bs := make([]byte, 8)
	for _, partID := range partitionsToRemap {
		binary.LittleEndian.PutUint64(bs, uint64(partID))
		key := c.hasher.Sum64(bs)

		// Find the theoretical owner's position on the ring.
		idx := sort.Search(len(c.sortedSet), func(i int) bool {
			return c.sortedSet[i] >= key
		})
		if idx >= len(c.sortedSet) {
			idx = 0
		}

		// Find a new owner that is not overloaded.
		// Start searching from the theoretical owner clockwise.
		for i := 0; i < len(c.sortedSet); i++ {
			searchIdx := (idx + i) % len(c.sortedSet)
			newOwner := c.ring[c.sortedSet[searchIdx]]

			if c.loads[newOwner]+1 <= avgLoad {
				c.partitions[partID] = newOwner
				c.loads[newOwner]++
				break // Found a new owner, move to the next partition.
			}
		}
	}

	c.membersDirty = true
	return nil
}

// LocateKey finds the owner for a given key.
func (c *Consistent) LocateKey(ctx context.Context, key []byte) string {
	select {
	case <-ctx.Done():
		return ""
	default:
	}
	partID := c.FindPartitionID(key)
	return c.GetPartitionOwner(ctx, partID)
}

// LocateReplicas returns the N members closest to the key in the hash ring.
func (c *Consistent) LocateReplicas(ctx context.Context, key []byte, count int) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	partID := c.FindPartitionID(key)
	return c.getClosestN(ctx, partID, count)
}

// GetClosestN is an alias for LocateReplicas for backward compatibility.
func (c *Consistent) GetClosestN(ctx context.Context, key []byte, count int) ([]string, error) {
	return c.LocateReplicas(ctx, key, count)
}

// LocateReplicasForPartition returns the N closest members for a given partition.
func (c *Consistent) LocateReplicasForPartition(ctx context.Context, partID, count int) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return c.getClosestN(ctx, partID, count)
}

// GetClosestNForPartition is an alias for LocateReplicasForPartition for backward compatibility.
func (c *Consistent) GetClosestNForPartition(ctx context.Context, partID, count int) ([]string, error) {
	return c.LocateReplicasForPartition(ctx, partID, count)
}

// getClosestN gets the N closest members.
func (c *Consistent) getClosestN(ctx context.Context, partID, count int) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if count > len(c.members) {
		return nil, ErrInsufficientMemberCount
	}

	if len(c.sortedSet) == 0 {
		return nil, ErrInsufficientMemberCount
	}

	// Hash the partition ID to find its position on the ring.
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(partID))
	partKey := c.hasher.Sum64(bs)

	// Use binary search to find the starting position in the sorted ring
	startIdx := sort.Search(len(c.sortedSet), func(i int) bool {
		return c.sortedSet[i] >= partKey
	})

	// If didn't find an exact match or went past the end, wrap around
	if startIdx >= len(c.sortedSet) {
		startIdx = 0
	}

	// Collect unique members by traversing the ring clockwise
	res := make([]string, 0, count)
	seen := make(map[string]struct{})
	idx := startIdx

	for len(res) < count && len(seen) < len(c.members) {
		hash := c.sortedSet[idx]
		member := c.ring[hash]

		// Add member if haven't seen it before
		if _, exists := seen[member]; !exists {
			res = append(res, member)
			seen[member] = struct{}{}
		}

		// Move to next virtual node (with wraparound)
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
	}

	return res, nil
}

// FindPartitionID returns the partition ID for a given key.
func (c *Consistent) FindPartitionID(key []byte) int {
	hkey := c.hasher.Sum64(key)
	return int(hkey % c.partitionCount)
}

// GetPartitionOwner returns the owner of a given partition.
func (c *Consistent) GetPartitionOwner(ctx context.Context, partID int) string {
	select {
	case <-ctx.Done():
		return ""
	default:
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.getPartitionOwner(ctx, partID)
}

// getPartitionOwner returns the owner of a given partition (not thread-safe).
func (c *Consistent) getPartitionOwner(ctx context.Context, partID int) string {
	return c.partitions[partID] // Returns empty string if not found
}

// GetMembers returns a thread-safe copy of the members. It returns an empty Member slice if there are no members.
func (c *Consistent) GetMembers(ctx context.Context) []string {
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	// First, try to check the cache with a read lock.
	c.mu.RLock()
	if !c.membersDirty && c.cachedMembers != nil {
		// Return a copy of the cached slice.
		res := make([]string, len(c.cachedMembers))
		copy(res, c.cachedMembers)
		c.mu.RUnlock()
		return res
	}
	c.mu.RUnlock()

	// Acquire the write lock to update the cache.
	c.mu.Lock()
	defer c.mu.Unlock()

	// After acquiring the write lock, check again if cache was updated.
	if !c.membersDirty && c.cachedMembers != nil {
		res := make([]string, len(c.cachedMembers))
		copy(res, c.cachedMembers)
		return res
	}

	// Create a thread-safe copy of the member list.
	members := make([]string, 0, len(c.members))
	for member := range c.members {
		members = append(members, member)
	}

	// Update the cache.
	c.cachedMembers = make([]string, 0, len(members))
	copy(c.cachedMembers, members)
	c.membersDirty = false

	return members
}

// LoadDistribution exposes the load distribution of members.
func (c *Consistent) LoadDistribution(ctx context.Context) map[string]float64 {
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy.
	res := make(map[string]float64)
	for member, load := range c.loads {
		res[member] = load
	}
	return res
}

// AverageLoad exposes the current average load.
func (c *Consistent) AverageLoad(ctx context.Context) float64 {
	select {
	case <-ctx.Done():
		return 0
	default:
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.averageLoad()
}

// averageLoad calculates the average load (internal method).
func (c *Consistent) averageLoad() float64 {
	if len(c.members) == 0 {
		return 0
	}

	avgLoad := float64(c.partitionCount/uint64(len(c.members))) * c.config.Load
	return math.Ceil(avgLoad)
}

// addToRing only adds a member to the hash ring (without redistributing partitions).
func (c *Consistent) addToRing(member string) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := buildVirtualNodeKey(member, i)
		h := c.hasher.Sum64(key)
		c.ring[h] = member
		c.sortedSet = append(c.sortedSet, h)
	}
	// Sort the hash values in ascending order.
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})
}

// removeFromRing only removes a member from the hash ring (without redistributing partitions).
func (c *Consistent) removeFromRing(name string) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := buildVirtualNodeKey(name, i)
		h := c.hasher.Sum64(key)
		delete(c.ring, h)
		c.delSlice(h)
	}
}

// buildVirtualNodeKey efficiently builds virtual node key, avoiding fmt.Sprintf performance overhead
func buildVirtualNodeKey(memberStr string, index int) []byte {
	indexStr := strconv.Itoa(index)
	key := make([]byte, 0, len(memberStr)+len(indexStr))
	key = append(key, memberStr...)
	key = append(key, indexStr...)
	return key
}

// delSlice removes a value from the slice (optimized with binary search).
func (c *Consistent) delSlice(val uint64) {
	// Use binary search to locate the element's position.
	idx := sort.Search(len(c.sortedSet), func(i int) bool {
		return c.sortedSet[i] >= val
	})

	// Check if the exact value was found.
	if idx < len(c.sortedSet) && c.sortedSet[idx] == val {
		// Remove the found element.
		c.sortedSet = append(c.sortedSet[:idx], c.sortedSet[idx+1:]...)
	}
}
