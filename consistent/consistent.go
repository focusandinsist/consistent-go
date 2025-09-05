package consistent

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

const (
	// DefaultPartitionCount is the default number of partitions in the consistent hash ring.
	// Using a prime number is recommended to help distribute keys more uniformly across partitions
	// due to the nature of the modulo operation in hashing. This value can be increased for
	// systems with a very large number of keys to improve key distribution, or decreased
	// for smaller setups to save memory.
	// Note: Changing this on a live system will cause a massive reshuffling of keys.
	DefaultPartitionCount = 271

	// DefaultReplicationFactor is the default number of virtual nodes created for each member.
	// A higher number leads to a more uniform distribution of partitions to members, which is
	// especially useful when the number of members is small. However, it increases memory usage
	// and slows down Add/Remove operations. For a large number of members, this can be decreased.
	DefaultReplicationFactor = 20

	// DefaultLoad is the default load factor for balancing partitions.
	// It determines the maximum load a member can have, calculated as (total partitions / member count) * Load.
	// A value of 1.25 allows a member's load to be up to 25% higher than the average, providing
	// flexibility for the distribution algorithm to place all partitions successfully.
	// Increasing this value may be necessary for small clusters to avoid placement errors(ErrInsufficientSpace).
	DefaultLoad = 1.25
)

var (
	ErrInsufficientMemberCount = errors.New("insufficient number of members")
	ErrInsufficientSpace       = errors.New("not enough space to distribute partitions")
	ErrEmptyMemberName         = errors.New("member name cannot be empty")
)

// Hasher generates a 64-bit unsigned hash for a given byte slice.
// A Hasher should minimize collisions, which occur when different byte slices generate the same hash.
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
// All public methods of this struct are thread-safe.
type Consistent struct {
	mu sync.RWMutex

	// config .
	config Config
	// hasher is the specific hash function implementation used for all hashing operations.
	hasher Hasher

	// sortedSet represents the logical hash ring. It's a sorted slice of all virtual node hashes.
	sortedSet []uint64
	// ring maps a virtual node hash from sortedSet back to its owner member's name.
	ring map[uint64]string
	// members is a set of all unique member names, used for fast O(1) existence checks.
	members map[string]struct{}

	// partitions is the final mapping of a partition ID to its current owner member.
	partitions map[int]string
	// loads tracks the current load (number of assigned partitions) for each member.
	loads map[string]float64

	// partitionCount is a cached copy of config.PartitionCount as a uint64.
	partitionCount uint64
	// partitionHashes maps a pre-computed partition key hash back to its original partition ID (0 to PartitionCount-1).
	partitionHashes map[uint64]int
	// sortedPartitionKeys is a pre-computed, sorted slice of all partition key hashes.
	// This is an optimization that enables efficient incremental rebalancing when adding a new member.
	sortedPartitionKeys []uint64
	// cachedMembers is a cached slice of member names for the GetMembers method
	// to avoid regenerating the list on every call.
	cachedMembers []string
	// membersDirty is a flag indicating that cachedMembers is out of date and needs to be rebuilt.
	membersDirty bool
}

// New creates and returns a new empty Consistent object, ready for dynamic member additions.
func New(config Config) (*Consistent, error) {
	return NewWithMembers([]string{}, config)
}

// NewWithMembers creates and returns a new Consistent object, pre-populated with an initial list of members.
func NewWithMembers(members []string, config Config) (*Consistent, error) {
	// Check config
	if config.PartitionCount < 0 {
		return nil, errors.New("PartitionCount cannot be negative")
	}
	if config.ReplicationFactor < 0 {
		return nil, errors.New("ReplicationFactor cannot be negative")
	}
	if config.Load < 0 {
		return nil, errors.New("load must be positive")
	}
	// Set defaults
	if config.Hasher == nil {
		config.Hasher = NewDefaultHasher()
	}
	if config.PartitionCount == 0 {
		config.PartitionCount = DefaultPartitionCount
	}
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = DefaultReplicationFactor
	}
	if config.Load == 0.0 {
		config.Load = DefaultLoad
	}

	// Validate configuration
	if err := validateConfig(len(members), config); err != nil {
		return nil, err
	}

	c := &Consistent{
		config:              config,
		members:             make(map[string]struct{}, len(members)),
		partitionCount:      uint64(config.PartitionCount),
		ring:                make(map[uint64]string),
		partitions:          make(map[int]string),
		loads:               make(map[string]float64),
		membersDirty:        true,
		sortedPartitionKeys: make([]uint64, 0, config.PartitionCount),
		partitionHashes:     make(map[uint64]int, config.PartitionCount),
	}

	c.hasher = config.Hasher

	// Add all virtual nodes of members, then sort the entire ring after all nodes have been added.
	for _, member := range members {
		c.members[member] = struct{}{}
		c.addVirtualNodes(member)
	}
	if len(members) > 0 {
		sort.Slice(c.sortedSet, func(i, j int) bool {
			return c.sortedSet[i] < c.sortedSet[j]
		})
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

// validateConfig validates the configuration parameters.
func validateConfig(memberCount int, config Config) error {
	if memberCount == 0 {
		return nil // Empty ring is valid
	}

	// Check if the configuration can support the required partitions
	avgLoad := float64(config.PartitionCount) / float64(memberCount) * config.Load
	maxLoad := math.Ceil(avgLoad)

	// Sanity check to prevent configurations that are highly likely to fail
	// during partition distribution. This heuristic ensures the number of virtual nodes
	// is not disproportionately small compared to the expected partition load.
	if maxLoad > float64(config.ReplicationFactor)*2 {
		return fmt.Errorf(
			"bad configuration: the calculated maxLoad (%g) per member is too high for the given ReplicationFactor (%d). "+
				"This configuration is unlikely to succeed. "+
				"Please increase ReplicationFactor or decrease PartitionCount/Load",
			maxLoad, config.ReplicationFactor,
		)
	}

	return nil
}

// Add adds a new member to the consistent hash ring.
func (c *Consistent) Add(ctx context.Context, member string) error {
	if member == "" {
		return ErrEmptyMemberName
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// Check if the member already exists.
	c.mu.RLock()
	if _, ok := c.members[member]; ok {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	// Acquire the write lock to update the ring.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check if the member already exists.
	if _, ok := c.members[member]; ok {
		return nil
	}

	// Add the member and its virtual nodes, then sort and rebalance.
	isFirstMember := len(c.members) == 0
	c.members[member] = struct{}{}
	c.addVirtualNodes(member)
	sort.Slice(c.sortedSet, func(i, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})

	if isFirstMember {
		if err := c.distributePartitions(); err != nil {
			// Revert if distribution fails.
			delete(c.members, member)
			c.removeVirtualNodes(member)
			return fmt.Errorf("failed to distribute partitions for the first member: %w", err)
		}
	} else {
		c.remapPartitionsForNewMember(member)
	}

	c.membersDirty = true

	return nil
}

// Remove removes a member from the consistent hash ring.
func (c *Consistent) Remove(ctx context.Context, member string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// Check if the member exists.
	c.mu.RLock()
	if _, ok := c.members[member]; !ok {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	// Acquire the write lock to update the ring.
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check if the member still exists.
	if _, ok := c.members[member]; !ok {
		return nil
	}

	// Find all partitions owned by the member being removed.
	partitionsToRemap := []int{}
	for partID, owner := range c.partitions {
		if owner == member {
			partitionsToRemap = append(partitionsToRemap, partID)
		}
	}

	delete(c.loads, member)
	delete(c.members, member)
	c.removeVirtualNodes(member)
	if len(c.members) == 0 {
		c.partitions = make(map[int]string)
		c.membersDirty = true
		return nil
	}

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
func (c *Consistent) LocateKey(ctx context.Context, key []byte) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
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
	return c.getClosestN(partID, count)
}

// GetMembers returns a thread-safe copy of the members. It returns an empty Member slice if there are no members.
func (c *Consistent) GetMembers(ctx context.Context) []string {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	// Check the cache.
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

	// Check again if cache was updated.
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
