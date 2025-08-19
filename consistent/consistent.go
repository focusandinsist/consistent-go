package consistent

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

const (
	// DefaultPartitionCount is the default number of partitions.
	DefaultPartitionCount int = 271
	// DefaultReplicationFactor is the default replication factor.
	DefaultReplicationFactor int = 20
	// DefaultLoad is the default load factor.
	DefaultLoad float64 = 1.25
)

// ErrInsufficientMemberCount is the error returned when the number of members is insufficient.
var ErrInsufficientMemberCount = errors.New("insufficient number of members")

// Hasher generates a 64-bit unsigned hash for a given byte slice.
// A Hasher should minimize collisions (generating the same hash for different byte slices).
// Performance is also important, so fast functions are preferred.
type Hasher interface {
	Sum64([]byte) uint64
}

// Member represents a member in the consistent hash ring.
type Member interface {
	String() string
}

// Config represents the configuration that controls the consistent hashing package.
type Config struct {
	// Hasher is responsible for generating a 64-bit unsigned hash for a given byte slice.
	Hasher Hasher

	// Keys are distributed among partitions. A prime number is good for distributing keys uniformly.
	// If you have too many keys, choose a large PartitionCount.
	PartitionCount int

	// Members are replicated on the consistent hash ring. This number represents
	// how many times a member is replicated on the ring.
	ReplicationFactor int

	// Load is used to calculate the average load.
	Load float64
}

// Consistent holds information about the members of the consistent hash ring.
type Consistent struct {
	mu sync.RWMutex

	config         Config
	hasher         Hasher
	sortedSet      []uint64
	partitionCount uint64
	loads          map[string]float64
	members        map[string]Member
	partitions     map[int]Member
	ring           map[uint64]Member

	// Cache-related fields
	cachedMembers []Member
	membersDirty  bool
}

// New creates and returns a new Consistent object.
func New(members []Member, config Config) *Consistent {
	if config.Hasher == nil {
		panic("Hasher cannot be nil")
	}
	if config.PartitionCount == 0 {
		config.PartitionCount = DefaultPartitionCount
	}
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = DefaultReplicationFactor
	}
	if config.Load == 0 {
		config.Load = DefaultLoad
	}

	c := &Consistent{
		config:         config,
		members:        make(map[string]Member),
		partitionCount: uint64(config.PartitionCount),
		ring:           make(map[uint64]Member),
		membersDirty:   true,
	}

	c.hasher = config.Hasher
	for _, member := range members {
		c.add(member)
	}
	if members != nil {
		c.distributePartitions()
	}
	return c
}

// GetMembers returns a thread-safe copy of the members. It returns an empty Member slice if there are no members.
func (c *Consistent) GetMembers() []Member {
	// First, try to check the cache with a read lock.
	c.mu.RLock()
	if !c.membersDirty && c.cachedMembers != nil {
		// Cache is valid, safely return a copy under the read lock.
		result := make([]Member, len(c.cachedMembers))
		copy(result, c.cachedMembers)
		c.mu.RUnlock()
		return result
	}
	c.mu.RUnlock() // Release the read lock, prepare to acquire the write lock.

	// Acquire the write lock to update the cache.
	c.mu.Lock()
	defer c.mu.Unlock()

	// After acquiring the write lock, it's possible another goroutine has already updated the cache, so we need to check again.
	if !c.membersDirty && c.cachedMembers != nil {
		result := make([]Member, len(c.cachedMembers))
		copy(result, c.cachedMembers)
		return result
	}

	// Create a thread-safe copy of the member list.
	members := make([]Member, 0, len(c.members))
	for _, member := range c.members {
		members = append(members, member)
	}

	// Update the cache (safe under the write lock).
	c.cachedMembers = make([]Member, len(members))
	copy(c.cachedMembers, members)
	c.membersDirty = false

	return members
}

// AverageLoad exposes the current average load.
func (c *Consistent) AverageLoad() float64 {
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

// distributeWithLoad distributes partitions based on load.
func (c *Consistent) distributeWithLoad(partID, idx int, partitions map[int]Member, loads map[string]float64) {
	avgLoad := c.averageLoad()
	var count int
	for {
		count++
		if count >= len(c.sortedSet) {
			// The user needs to reduce the partition count, increase the member count, or increase the load factor.
			panic("not enough space to distribute partitions")
		}
		i := c.sortedSet[idx]
		member := c.ring[i]
		load := loads[member.String()]
		if load+1 <= avgLoad {
			partitions[partID] = member
			loads[member.String()]++
			return
		}
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
	}
}

// distributePartitions distributes the partitions.
func (c *Consistent) distributePartitions() {
	loads := make(map[string]float64)
	partitions := make(map[int]Member)

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
		c.distributeWithLoad(int(partID), idx, partitions, loads)
	}
	c.partitions = partitions
	c.loads = loads
}

// add adds a member to the hash ring (internal method).
func (c *Consistent) add(member Member) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", member.String(), i))
		h := c.hasher.Sum64(key)
		c.ring[h] = member
		c.sortedSet = append(c.sortedSet, h)
	}
	// Sort the hash values in ascending order.
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})
	// Storing members in this map helps find backup members for a partition.
	c.members[member.String()] = member
	// Mark the member cache as dirty.
	c.membersDirty = true
}

// Add adds a new member to the consistent hash ring (optimized version).
func (c *Consistent) Add(member Member) {
	// First, check if the member already exists (only needs a read lock).
	c.mu.RLock()
	if _, ok := c.members[member.String()]; ok {
		c.mu.RUnlock()
		return
	}
	c.mu.RUnlock()

	// Calculate the new partition distribution in temporary variables (no lock needed).
	newPartitions, newLoads := c.calculatePartitionsWithNewMember(member)

	// Acquire the write lock to quickly update data structures.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check if the member already exists.
	if _, ok := c.members[member.String()]; ok {
		return
	}

	// Add the member to the ring.
	c.addToRing(member)

	// Quickly update partition and load information.
	c.partitions = newPartitions
	c.loads = newLoads
	c.members[member.String()] = member
	c.membersDirty = true
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

// Remove removes a member from the consistent hash ring (optimized version).
func (c *Consistent) Remove(name string) {
	// First, check if the member exists (only needs a read lock).
	c.mu.RLock()
	if _, ok := c.members[name]; !ok {
		c.mu.RUnlock()
		return
	}
	c.mu.RUnlock()

	// Calculate the partition distribution after removing the member in temporary variables (no lock needed).
	newPartitions, newLoads := c.calculatePartitionsWithoutMember(name)

	// Acquire the write lock to quickly update data structures.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check if the member exists.
	if _, ok := c.members[name]; !ok {
		return
	}

	// Remove the member from the ring.
	c.removeFromRing(name)

	// Quickly update partition and load information.
	delete(c.members, name)
	c.membersDirty = true

	if len(c.members) == 0 {
		// The consistent hash ring is now empty, reset the partition table.
		c.partitions = make(map[int]Member)
		c.loads = make(map[string]float64)
		return
	}

	c.partitions = newPartitions
	c.loads = newLoads
}

// LoadDistribution exposes the load distribution of members.
func (c *Consistent) LoadDistribution() map[string]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy.
	res := make(map[string]float64)
	for member, load := range c.loads {
		res[member] = load
	}
	return res
}

// FindPartitionID returns the partition ID for a given key.
func (c *Consistent) FindPartitionID(key []byte) int {
	hkey := c.hasher.Sum64(key)
	return int(hkey % c.partitionCount)
}

// GetPartitionOwner returns the owner of a given partition.
func (c *Consistent) GetPartitionOwner(partID int) Member {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.getPartitionOwner(partID)
}

// getPartitionOwner returns the owner of a given partition (not thread-safe).
func (c *Consistent) getPartitionOwner(partID int) Member {
	member, ok := c.partitions[partID]
	if !ok {
		return nil
	}
	// Return the member directly.
	return member
}

// LocateKey finds the owner for a given key.
func (c *Consistent) LocateKey(key []byte) Member {
	partID := c.FindPartitionID(key)
	return c.GetPartitionOwner(partID)
}

// getClosestN gets the N closest members (internal method).
func (c *Consistent) getClosestN(partID, count int) ([]Member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var res []Member
	if count > len(c.members) {
		return res, ErrInsufficientMemberCount
	}

	var ownerKey uint64
	owner := c.getPartitionOwner(partID)
	// Hash and sort all names.
	var keys []uint64
	kmems := make(map[uint64]Member)
	for name, member := range c.members {
		key := c.hasher.Sum64([]byte(name))
		if name == owner.String() {
			ownerKey = key
		}
		keys = append(keys, key)
		kmems[key] = member
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Find the owner of the key.
	idx := 0
	for idx < len(keys) {
		if keys[idx] == ownerKey {
			key := keys[idx]
			res = append(res, kmems[key])
			break
		}
		idx++
	}

	// Find the closest (replica owner) members.
	for len(res) < count {
		idx++
		if idx >= len(keys) {
			idx = 0
		}
		key := keys[idx]
		res = append(res, kmems[key])
	}
	return res, nil
}

// GetClosestN returns the N members closest to the key in the hash ring.
// This can be useful for finding replica members.
func (c *Consistent) GetClosestN(key []byte, count int) ([]Member, error) {
	partID := c.FindPartitionID(key)
	return c.getClosestN(partID, count)
}

// GetClosestNForPartition returns the N closest members for a given partition.
// This can be useful for finding replica members.
func (c *Consistent) GetClosestNForPartition(partID, count int) ([]Member, error) {
	return c.getClosestN(partID, count)
}

// addToRing only adds a member to the hash ring (without redistributing partitions).
func (c *Consistent) addToRing(member Member) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", member.String(), i))
		h := c.hasher.Sum64(key)
		c.ring[h] = member
		c.sortedSet = append(c.sortedSet, h)
	}
	// Sort the hash values in ascending order.
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})
}

// calculatePartitionsWithNewMember calculates the partition distribution after adding a new member.
func (c *Consistent) calculatePartitionsWithNewMember(newMember Member) (map[int]Member, map[string]float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a temporary ring and sorted set.
	tempRing := make(map[uint64]Member)
	tempSortedSet := make([]uint64, len(c.sortedSet))
	copy(tempSortedSet, c.sortedSet)

	// Copy existing members to the temporary ring.
	for k, v := range c.ring {
		tempRing[k] = v
	}

	// Add the new member to the temporary ring.
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", newMember.String(), i))
		h := c.hasher.Sum64(key)
		tempRing[h] = newMember
		tempSortedSet = append(tempSortedSet, h)
	}

	// Sort the temporary set.
	sort.Slice(tempSortedSet, func(i int, j int) bool {
		return tempSortedSet[i] < tempSortedSet[j]
	})

	// Calculate the new partition distribution, passing the correct number of members.
	newMemberCount := len(c.members) + 1
	return c.calculatePartitionsWithRingAndMemberCount(tempRing, tempSortedSet, newMemberCount)
}

// removeFromRing only removes a member from the hash ring (without redistributing partitions).
func (c *Consistent) removeFromRing(name string) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", name, i))
		h := c.hasher.Sum64(key)
		delete(c.ring, h)
		c.delSlice(h)
	}
}

// calculatePartitionsWithoutMember calculates the partition distribution after removing a member (optimized version).
func (c *Consistent) calculatePartitionsWithoutMember(memberName string) (map[int]Member, map[string]float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Pre-calculate all hash values of the member to be deleted and store them in a map for quick lookup.
	hashesToDelete := make(map[uint64]struct{})
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", memberName, i))
		h := c.hasher.Sum64(key)
		hashesToDelete[h] = struct{}{}
	}

	// Create a temporary ring and sorted set.
	tempRing := make(map[uint64]Member)
	var tempSortedSet []uint64

	// Iterate through c.ring only once, using the map to quickly check if deletion is needed.
	for k, v := range c.ring {
		if _, shouldDelete := hashesToDelete[k]; !shouldDelete {
			// This node needs to be kept.
			tempRing[k] = v
			tempSortedSet = append(tempSortedSet, k)
		}
	}

	// Sort the temporary set.
	sort.Slice(tempSortedSet, func(i int, j int) bool {
		return tempSortedSet[i] < tempSortedSet[j]
	})

	// Calculate the new partition distribution.
	return c.calculatePartitionsWithRingAndMemberCount(tempRing, tempSortedSet, len(c.members)-1)
}

// calculatePartitionsWithRingAndMemberCount calculates the partition distribution using the given ring and member count.
func (c *Consistent) calculatePartitionsWithRingAndMemberCount(ring map[uint64]Member, sortedSet []uint64, memberCount int) (map[int]Member, map[string]float64) {
	loads := make(map[string]float64)
	partitions := make(map[int]Member)

	if memberCount == 0 {
		return partitions, loads
	}

	// Calculate the average load.
	avgLoad := float64(c.partitionCount/uint64(memberCount)) * c.config.Load
	avgLoad = math.Ceil(avgLoad)

	bs := make([]byte, 8)
	for partID := uint64(0); partID < c.partitionCount; partID++ {
		binary.LittleEndian.PutUint64(bs, partID)
		key := c.hasher.Sum64(bs)
		idx := sort.Search(len(sortedSet), func(i int) bool {
			return sortedSet[i] >= key
		})
		if idx >= len(sortedSet) {
			idx = 0
		}

		// Allocate the partition, considering load balancing.
		var count int
		for {
			count++
			if count >= len(sortedSet) {
				panic("not enough space to distribute partitions")
			}
			i := sortedSet[idx]
			member := ring[i]
			load := loads[member.String()]
			if load+1 <= avgLoad {
				partitions[int(partID)] = member
				loads[member.String()]++
				break
			}
			idx++
			if idx >= len(sortedSet) {
				idx = 0
			}
		}
	}

	return partitions, loads
}
