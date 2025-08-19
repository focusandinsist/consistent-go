package consistent

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
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

// ErrInsufficientSpace is the error returned when there's not enough space to distribute partitions.
var ErrInsufficientSpace = errors.New("not enough space to distribute partitions")

// buildVirtualNodeKey efficiently builds virtual node key, avoiding fmt.Sprintf performance overhead
func buildVirtualNodeKey(memberStr string, index int) []byte {
	// Estimate capacity: member string length + number length (max 10 digits)
	indexStr := strconv.Itoa(index)
	key := make([]byte, 0, len(memberStr)+len(indexStr))
	key = append(key, memberStr...)
	key = append(key, indexStr...)
	return key
}

// Hasher generates a 64-bit unsigned hash for a given byte slice.
// A Hasher should minimize collisions (generating the same hash for different byte slices).
// Performance is also important, so fast functions are preferred.
type Hasher interface {
	Sum64([]byte) uint64
}

// Member represents a member in the consistent hash ring.
type Member interface {
	String() string
	Clone() Member
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

// New creates and returns a new Consistent object.
func New(members []Member, config Config) (*Consistent, error) {
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
		if err := c.distributePartitions(); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// GetMembers returns a thread-safe copy of the members. It returns an empty Member slice if there are no members.
func (c *Consistent) GetMembers() []Member {
	// First, try to check the cache with a read lock.
	c.mu.RLock()
	if !c.membersDirty && c.cachedMembers != nil {
		// Cache is valid, safely return cloned copies under the read lock.
		res := make([]Member, len(c.cachedMembers))
		for i, member := range c.cachedMembers {
			res[i] = member.Clone()
		}
		c.mu.RUnlock()
		return res
	}
	c.mu.RUnlock()

	// Acquire the write lock to update the cache.
	c.mu.Lock()
	defer c.mu.Unlock()

	// After acquiring the write lock, check again if cache was updated.
	if !c.membersDirty && c.cachedMembers != nil {
		res := make([]Member, len(c.cachedMembers))
		for i, member := range c.cachedMembers {
			res[i] = member.Clone()
		}
		return res
	}

	// Create a thread-safe copy of the member list using Clone().
	members := make([]Member, 0, len(c.members))
	for _, member := range c.members {
		members = append(members, member.Clone())
	}

	// Update the cache with original members (not cloned).
	c.cachedMembers = make([]Member, 0, len(c.members))
	for _, member := range c.members {
		c.cachedMembers = append(c.cachedMembers, member)
	}
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
func (c *Consistent) distributeWithLoad(partID, idx int, partitions map[int]Member, loads map[string]float64) error {
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
		load := loads[member.String()]
		if load+1 <= avgLoad {
			partitions[partID] = member
			loads[member.String()]++
			return nil
		}
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
	}
}

// distributePartitions distributes the partitions.
func (c *Consistent) distributePartitions() error {
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
		if err := c.distributeWithLoad(int(partID), idx, partitions, loads); err != nil {
			return err
		}
	}
	c.partitions = partitions
	c.loads = loads
	return nil
}

// add adds a member to the hash ring (internal method).
func (c *Consistent) add(member Member) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := buildVirtualNodeKey(member.String(), i)
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

// Add adds a new member to the consistent hash ring
func (c *Consistent) Add(member Member) error {
	// First, check if the member already exists (only needs a read lock).
	c.mu.RLock()
	if _, ok := c.members[member.String()]; ok {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	// Calculate the new partition distribution in temporary variables (no lock needed).
	newPartitions, newLoads, err := c.calculatePartitionsWithNewMember(member)
	if err != nil {
		// If pre-calculation fails, do not proceed with adding
		return fmt.Errorf("failed to add member %s: %w", member.String(), err)
	}

	// Acquire the write lock to quickly update data structures.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check if the member already exists.
	if _, ok := c.members[member.String()]; ok {
		return nil
	}

	// Add the member to the ring.
	c.addToRing(member)

	// Quickly update partition and load information.
	c.partitions = newPartitions
	c.loads = newLoads
	c.members[member.String()] = member
	c.membersDirty = true

	return nil
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

// Remove removes a member from the consistent hash ring.
func (c *Consistent) Remove(member Member) error {
	name := member.String()

	// First, check if the member exists
	c.mu.RLock()
	if _, ok := c.members[name]; !ok {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	// Calculate the partition distribution after removing the member in temporary variables (no lock needed).
	newPartitions, newLoads, err := c.calculatePartitionsWithoutMember(name)
	if err != nil {
		return fmt.Errorf("failed to remove member %s: %w", name, err)
	}

	// Acquire the write lock to quickly update data structures.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check if the member exists.
	if _, ok := c.members[name]; !ok {
		return nil
	}

	// Remove the member from the ring.
	c.removeFromRing(name)

	// Quickly update partition and load information.
	delete(c.members, name)
	c.membersDirty = true

	if len(c.members) == 0 {
		c.partitions = make(map[int]Member)
		c.loads = make(map[string]float64)
		return nil
	}

	c.partitions = newPartitions
	c.loads = newLoads
	return nil
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
	return member
}

// LocateKey finds the owner for a given key.
func (c *Consistent) LocateKey(key []byte) Member {
	partID := c.FindPartitionID(key)
	return c.GetPartitionOwner(partID)
}

// getClosestN gets the N closest members.
func (c *Consistent) getClosestN(partID, count int) ([]Member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if count > len(c.members) {
		return nil, ErrInsufficientMemberCount
	}

	if len(c.sortedSet) == 0 {
		return nil, ErrInsufficientMemberCount
	}

	// Get the partition owner to determine the starting key
	owner := c.getPartitionOwner(partID)
	if owner == nil {
		return nil, ErrInsufficientMemberCount
	}

	// Hash the owner's name to find the starting position
	ownerKey := c.hasher.Sum64([]byte(owner.String()))

	// Use binary search to find the starting position in the sorted ring
	startIdx := sort.Search(len(c.sortedSet), func(i int) bool {
		return c.sortedSet[i] >= ownerKey
	})

	// If didn't find an exact match or went past the end, wrap around
	if startIdx >= len(c.sortedSet) {
		startIdx = 0
	}

	// Collect unique members by traversing the ring clockwise
	res := make([]Member, 0, count)
	seen := make(map[string]struct{})
	idx := startIdx

	for len(res) < count && len(seen) < len(c.members) {
		hash := c.sortedSet[idx]
		member := c.ring[hash]
		memberKey := member.String()

		// Add member if haven't seen it before
		if _, exists := seen[memberKey]; !exists {
			res = append(res, member)
			seen[memberKey] = struct{}{}
		}

		// Move to next virtual node (with wraparound)
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
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
		key := buildVirtualNodeKey(member.String(), i)
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
func (c *Consistent) calculatePartitionsWithNewMember(newMember Member) (map[int]Member, map[string]float64, error) {
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
		key := buildVirtualNodeKey(newMember.String(), i)
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
	partitions, loads, err := c.calculatePartitionsWithRingAndMemberCount(tempRing, tempSortedSet, newMemberCount)
	if err != nil {
		return nil, nil, err
	}
	return partitions, loads, nil
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

// calculatePartitionsWithoutMember calculates the partition distribution after removing a member.
func (c *Consistent) calculatePartitionsWithoutMember(memberName string) (map[int]Member, map[string]float64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Pre-calculate all hash values of the member to be deleted and store them in a map for quick lookup.
	hashesToDelete := make(map[uint64]struct{})
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := buildVirtualNodeKey(memberName, i)
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
	partitions, loads, err := c.calculatePartitionsWithRingAndMemberCount(tempRing, tempSortedSet, len(c.members)-1)
	if err != nil {
		return nil, nil, err
	}
	return partitions, loads, nil
}

// calculatePartitionsWithRingAndMemberCount calculates the partition distribution using the given ring and member count.
func (c *Consistent) calculatePartitionsWithRingAndMemberCount(ring map[uint64]Member, sortedSet []uint64, memberCount int) (map[int]Member, map[string]float64, error) {
	loads := make(map[string]float64)
	partitions := make(map[int]Member)

	if memberCount == 0 {
		return partitions, loads, nil
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
				err := fmt.Errorf("%w: failed to assign partition %d (avgLoad=%.2f, members=%d, virtualNodes=%d)",
					ErrInsufficientSpace, partID, avgLoad, memberCount, len(sortedSet))
				return nil, nil, err
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

	return partitions, loads, nil
}
