package consistent

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
)

// GetClosestN is an alias for LocateReplicas for backward compatibility.
func (c *Consistent) GetClosestN(ctx context.Context, key []byte, count int) ([]string, error) {
	return c.LocateReplicas(ctx, key, count)
}

// GetClosestNForPartition is an alias for LocateReplicasForPartition for backward compatibility.
func (c *Consistent) GetClosestNForPartition(ctx context.Context, partID, count int) ([]string, error) {
	return c.LocateReplicasForPartition(ctx, partID, count)
}

// LocateReplicasForPartition returns the current partition owner first, followed
// by unique members found clockwise from the partition's position on the ring.
func (c *Consistent) LocateReplicasForPartition(ctx context.Context, partID, count int) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return c.getClosestN(partID, count)
}

// getClosestN gets the primary owner and closest replica candidates.
func (c *Consistent) getClosestN(partID, count int) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if count < 0 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidReplicaCount, count)
	}
	if err := c.validatePartitionID(partID); err != nil {
		return nil, err
	}
	if count > len(c.members) {
		return nil, ErrInsufficientMemberCount
	}

	if len(c.sortedSet) == 0 {
		return nil, ErrInsufficientMemberCount
	}

	res := make([]string, 0, count)
	if count == 0 {
		return res, nil
	}

	primary, err := c.getPartitionOwner(partID)
	if err != nil {
		return nil, err
	}

	// Hash the partition ID to find its position on the ring.
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(partID))
	partKey := c.hasher.Sum64(bs)

	startIdx := c.ringIndex(partKey)

	// The partition map is authoritative after load-aware placement. Return its
	// owner first, then fill the remaining replicas by walking the ring.
	res = append(res, primary)
	seen := map[string]struct{}{primary: {}}
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

func (c *Consistent) validatePartitionID(partID int) error {
	if partID < 0 || uint64(partID) >= c.partitionCount {
		return fmt.Errorf("%w: %d (partition count=%d)", ErrInvalidPartitionID, partID, c.partitionCount)
	}
	return nil
}

// FindPartitionID returns the partition ID for a given key.
func (c *Consistent) FindPartitionID(key []byte) int {
	hkey := c.hasher.Sum64(key)
	return int(hkey % c.partitionCount)
}

// GetPartitionOwner returns the owner of a given partition.
func (c *Consistent) GetPartitionOwner(ctx context.Context, partID int) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.getPartitionOwner(partID)
}

// getPartitionOwner returns the owner of a given partition (not thread-safe).
func (c *Consistent) getPartitionOwner(partID int) (string, error) {
	if err := c.validatePartitionID(partID); err != nil {
		return "", err
	}
	if len(c.members) == 0 {
		return "", ErrInsufficientMemberCount
	}
	owner := c.partitions[partID]
	if owner == "" {
		return "", ErrInsufficientMemberCount
	}
	return owner, nil
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
func (c *Consistent) AverageLoad(ctx context.Context) (float64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.averageLoad(), nil
}

// averageLoad calculates the average load.
func (c *Consistent) averageLoad() float64 {
	if len(c.members) == 0 {
		return 0
	}

	avgLoad := (float64(c.partitionCount) / float64(len(c.members))) * c.config.Load
	return math.Ceil(avgLoad)
}
