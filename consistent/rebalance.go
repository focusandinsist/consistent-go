package consistent

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
)

// Add adds a new member to the consistent hash ring
func (c *Consistent) Add(ctx context.Context, member string) error {
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

	// Add the member to the ring and partitions incrementally.
	c.members[member] = struct{}{}
	c.addToRing(member)
	c.remapPartitionsForNewMember(member)
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

	// Double-check if the member already exists.
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
	c.removeFromRing(member)

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
			return fmt.Errorf("%w: partition %d cannot be assigned after %d attempts (avgLoad=%g, members=%d, virtualNodes=%d)",
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

// addToRing only adds a member's virtual nodes to the ring. It does not rebalance partitions.
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

// removeFromRing only removes a member's virtual nodes from the ring. It does not rebalance partitions.
func (c *Consistent) removeFromRing(name string) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := buildVirtualNodeKey(name, i)
		h := c.hasher.Sum64(key)
		delete(c.ring, h)
		c.delSlice(h)
	}
}

// delSlice removes a value from the slice.
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
	// The members map is used for quick, O(1) lookups to check for member existence.
	c.members[member] = struct{}{}

	c.membersDirty = true
}

// buildVirtualNodeKey builds virtual node key
func buildVirtualNodeKey(memberStr string, index int) []byte {
	indexStr := strconv.Itoa(index)
	key := make([]byte, 0, len(memberStr)+len(indexStr))
	key = append(key, memberStr...)
	key = append(key, indexStr...)
	return key
}
