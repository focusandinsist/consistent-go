package consistent

import (
	"context"
	"encoding/binary"
	"math"
	"sort"
)

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
	return c.getClosestN(partID, count)
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
	return c.getClosestN(partID, count)
}

// GetClosestNForPartition is an alias for LocateReplicasForPartition for backward compatibility.
func (c *Consistent) GetClosestNForPartition(ctx context.Context, partID, count int) ([]string, error) {
	return c.LocateReplicasForPartition(ctx, partID, count)
}

// getClosestN gets the N closest members.
func (c *Consistent) getClosestN(partID, count int) ([]string, error) {
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

	return c.getPartitionOwner(partID)
}

// getPartitionOwner returns the owner of a given partition (not thread-safe).
func (c *Consistent) getPartitionOwner(partID int) string {
	return c.partitions[partID] // Returns empty string if not found
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

// averageLoad calculates the average load.
func (c *Consistent) averageLoad() float64 {
	if len(c.members) == 0 {
		return 0
	}

	avgLoad := float64(c.partitionCount/uint64(len(c.members))) * c.config.Load
	return math.Ceil(avgLoad)
}
