package consistent

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
)

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

// addVirtualNodes adds all virtual nodes for a given member to the ring.
// It does NOT sort the ring, the caller is responsible for sorting.
func (c *Consistent) addVirtualNodes(member string) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := buildVirtualNodeKey(member, i)
		h := c.hasher.Sum64(key)
		c.ring[h] = member
		c.sortedSet = append(c.sortedSet, h)
	}
}

// remapPartitionsForNewMember incrementally reassigns partitions to a newly added member.
// This is a key performance optimization. Instead of re-calculating the entire partition map,
// it only re-evaluates partitions that fall under the new member's influence on the hash ring.
// It also enforces the "bounded load" constraint: a partition will only be moved to the new member
// if the member has not yet reached its maximum load capacity, preventing it from becoming a hot spot.
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

				// Only reassign the partition if the new member is not already the owner.
				if oldOwner != member {
					// Only decrement the old owner's load if it was a real, existing owner.
					if oldOwner != "" {
						c.loads[oldOwner]--
					}
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

// removeVirtualNodes removes all virtual nodes for a given member from the ring.
func (c *Consistent) removeVirtualNodes(member string) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := buildVirtualNodeKey(member, i)
		h := c.hasher.Sum64(key)
		delete(c.ring, h)
		c.removeVirtualNode(h)
	}
}

// removeVirtualNode removes a single virtual node hash from the sortedSet.
func (c *Consistent) removeVirtualNode(vnodeHash uint64) {
	// Use binary search to locate the element's position.
	idx := sort.Search(len(c.sortedSet), func(i int) bool {
		return c.sortedSet[i] >= vnodeHash
	})

	// Check if the exact value was found.
	if idx < len(c.sortedSet) && c.sortedSet[idx] == vnodeHash {
		// Remove the found element.
		c.sortedSet = append(c.sortedSet[:idx], c.sortedSet[idx+1:]...)
	}
}

// buildVirtualNodeKey builds virtual node key
func buildVirtualNodeKey(memberStr string, index int) []byte {
	indexStr := strconv.Itoa(index)
	key := make([]byte, 0, len(memberStr)+len(indexStr))
	key = append(key, memberStr...)
	key = append(key, indexStr...)
	return key
}
