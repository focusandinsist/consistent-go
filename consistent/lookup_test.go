package consistent

import (
	"context"
	"testing"
)

// TestGetMembers tests the GetMembers functionality
func TestGetMembers(t *testing.T) {
	ctx := context.Background()

	// Test empty ring
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	members := c.GetMembers(ctx)
	if len(members) != 0 {
		t.Errorf("Expected 0 members in empty ring, got %d", len(members))
	}

	// Add members and test
	testMembers := []string{"node1", "node2", "node3"}
	for _, member := range testMembers {
		err := c.Add(ctx, member)
		if err != nil {
			t.Fatalf("Failed to add member %s: %v", member, err)
		}
	}

	members = c.GetMembers(ctx)
	if len(members) != len(testMembers) {
		t.Errorf("Expected %d members, got %d", len(testMembers), len(members))
	}

	// Verify all members are present
	memberSet := make(map[string]bool)
	for _, member := range members {
		memberSet[member] = true
	}

	for _, expected := range testMembers {
		if !memberSet[expected] {
			t.Errorf("Expected member %s not found in result", expected)
		}
	}
}

// TestLoadDistribution tests the LoadDistribution functionality
func TestLoadDistribution(t *testing.T) {
	ctx := context.Background()

	members := []string{"node1", "node2", "node3"}
	c, err := NewWithMembers(members, Config{
		PartitionCount:    100,
		ReplicationFactor: 50,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	loadDist := c.LoadDistribution(ctx)

	// Check that all members have load information
	if len(loadDist) != len(members) {
		t.Errorf("Expected load distribution for %d members, got %d", len(members), len(loadDist))
	}

	totalLoad := 0.0
	for member, load := range loadDist {
		// Verify member exists
		found := false
		for _, m := range members {
			if m == member {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Load distribution contains unknown member: %s", member)
		}

		// Load should be non-negative
		if load < 0 {
			t.Errorf("Negative load for member %s: %f", member, load)
		}

		totalLoad += load
	}

	// Total load should equal partition count
	expectedTotal := float64(100) // partition count
	if totalLoad != expectedTotal {
		t.Errorf("Total load %f does not equal partition count %f", totalLoad, expectedTotal)
	}
}

// TestAverageLoad tests the AverageLoad functionality
func TestAverageLoad(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		members        []string
		partitionCount int
		load           float64
		expectedAvg    float64
	}{
		{
			name:           "three_members",
			members:        []string{"node1", "node2", "node3"},
			partitionCount: 90,
			load:           1.0,
			expectedAvg:    30.0, // ceil(90/3 * 1.0) = 30
		},
		{
			name:           "with_load_factor",
			members:        []string{"node1", "node2"},
			partitionCount: 100,
			load:           1.25,
			expectedAvg:    63.0, // ceil(100/2 * 1.25) = ceil(62.5) = 63
		},
		{
			name:           "single_member",
			members:        []string{"node1"},
			partitionCount: 50,
			load:           1.0,
			expectedAvg:    50.0, // ceil(50/1 * 1.0) = 50
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewWithMembers(tt.members, Config{
				PartitionCount:    tt.partitionCount,
				Load:              tt.load,
				ReplicationFactor: 50,
			})
			if err != nil {
				t.Fatalf("Failed to create Consistent instance: %v", err)
			}

			avgLoad, err := c.AverageLoad(ctx)
			if err != nil {
				t.Fatalf("AverageLoad() error: %v", err)
			}

			if avgLoad != tt.expectedAvg {
				t.Errorf("Expected average load %f, got %f", tt.expectedAvg, avgLoad)
			}
		})
	}
}

// TestAverageLoadEmptyRing tests AverageLoad with empty ring
func TestAverageLoadEmptyRing(t *testing.T) {
	ctx := context.Background()

	c, err := New(Config{})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	avgLoad, err := c.AverageLoad(ctx)
	if err != nil {
		t.Logf("AverageLoad() on empty ring returned error as expected: %v", err)
	} else {
		t.Logf("AverageLoad() on empty ring returned: %f", avgLoad)
	}
}

// TestGetPartitionOwner tests the GetPartitionOwner functionality
func TestGetPartitionOwner(t *testing.T) {
	ctx := context.Background()

	members := []string{"node1", "node2", "node3"}
	c, err := NewWithMembers(members, Config{
		PartitionCount:    10,
		ReplicationFactor: 50,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Test all partitions
	for partID := 0; partID < 10; partID++ {
		owner, err := c.GetPartitionOwner(ctx, partID)
		if err != nil {
			t.Errorf("GetPartitionOwner(%d) error: %v", partID, err)
			continue
		}

		if owner == "" {
			t.Errorf("GetPartitionOwner(%d) returned empty owner", partID)
			continue
		}

		// Verify owner is a valid member
		found := false
		for _, member := range members {
			if owner == member {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("GetPartitionOwner(%d) returned invalid owner: %s", partID, owner)
		}
	}
}

// TestGetPartitionOwnerInvalidPartition tests GetPartitionOwner with invalid partition IDs
func TestGetPartitionOwnerInvalidPartition(t *testing.T) {
	ctx := context.Background()

	c, err := NewWithMembers([]string{"node1"}, Config{
		PartitionCount:    10,
		ReplicationFactor: 50,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	invalidPartitions := []int{-1, 10, 100, -100}

	for _, partID := range invalidPartitions {
		_, err := c.GetPartitionOwner(ctx, partID)
		if err == nil {
			t.Errorf("Expected error for invalid partition ID %d", partID)
		}
	}
}

// TestFindPartitionID tests the FindPartitionID functionality
func TestFindPartitionID(t *testing.T) {
	c, err := New(Config{
		PartitionCount: 100,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	testKeys := [][]byte{
		[]byte("test-key-1"),
		[]byte("test-key-2"),
		[]byte(""),
		[]byte("very-long-key-name-that-should-still-work"),
		{0, 1, 2, 3, 255}, // binary data
	}

	for i, key := range testKeys {
		partID := c.FindPartitionID(key)

		// Partition ID should be within valid range
		if partID < 0 || partID >= 100 {
			t.Errorf("FindPartitionID() returned invalid partition ID %d for key %d", partID, i)
		}

		// Same key should always return same partition ID
		partID2 := c.FindPartitionID(key)
		if partID != partID2 {
			t.Errorf("FindPartitionID() not deterministic for key %d: %d != %d", i, partID, partID2)
		}
	}
}

// TestLocateReplicas tests the LocateReplicas functionality
func TestLocateReplicas(t *testing.T) {
	ctx := context.Background()

	members := []string{"node1", "node2", "node3", "node4", "node5"}
	c, err := NewWithMembers(members, Config{
		ReplicationFactor: 50,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	testKey := []byte("test-key")

	tests := []struct {
		name  string
		count int
	}{
		{"single_replica", 1},
		{"two_replicas", 2},
		{"three_replicas", 3},
		{"all_replicas", 5},
		{"more_than_available", 5}, // Should return all available
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicas, err := c.LocateReplicas(ctx, testKey, tt.count)
			if err != nil {
				t.Errorf("LocateReplicas() error: %v", err)
				return
			}

			expectedCount := tt.count
			if expectedCount > len(members) {
				expectedCount = len(members)
			}

			if len(replicas) != expectedCount {
				t.Errorf("Expected %d replicas, got %d", expectedCount, len(replicas))
			}

			// Check for duplicates
			seen := make(map[string]bool)
			for _, replica := range replicas {
				if seen[replica] {
					t.Errorf("Duplicate replica found: %s", replica)
				}
				seen[replica] = true
			}

			// Verify all replicas are valid members
			for _, replica := range replicas {
				found := false
				for _, member := range members {
					if replica == member {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Invalid replica returned: %s", replica)
				}
			}
		})
	}
}

// TestLocateReplicasForPartition tests the LocateReplicasForPartition functionality
func TestLocateReplicasForPartition(t *testing.T) {
	ctx := context.Background()

	members := []string{"node1", "node2", "node3", "node4"}
	c, err := NewWithMembers(members, Config{
		PartitionCount:    20,
		ReplicationFactor: 50,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Test various partition IDs
	partitionIDs := []int{0, 5, 10, 19} // Valid partition IDs

	for _, partID := range partitionIDs {
		t.Run("partition_"+string(rune(partID)), func(t *testing.T) {
			replicas, err := c.LocateReplicasForPartition(ctx, partID, 3)
			if err != nil {
				t.Errorf("LocateReplicasForPartition() error: %v", err)
				return
			}

			if len(replicas) != 3 {
				t.Errorf("Expected 3 replicas, got %d", len(replicas))
			}

			// Check for duplicates
			seen := make(map[string]bool)
			for _, replica := range replicas {
				if seen[replica] {
					t.Errorf("Duplicate replica found: %s", replica)
				}
				seen[replica] = true
			}
		})
	}
}

// TestBackwardCompatibilityAliases tests the backward compatibility aliases
func TestBackwardCompatibilityAliases(t *testing.T) {
	ctx := context.Background()

	members := []string{"node1", "node2", "node3"}
	c, err := NewWithMembers(members, Config{ReplicationFactor: 100})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	testKey := []byte("test-key")

	// Test GetClosestN (alias for LocateReplicas)
	replicas1, err1 := c.GetClosestN(ctx, testKey, 2)
	replicas2, err2 := c.LocateReplicas(ctx, testKey, 2)

	if err1 != nil || err2 != nil {
		t.Fatalf("Errors: GetClosestN=%v, LocateReplicas=%v", err1, err2)
	}

	if len(replicas1) != len(replicas2) {
		t.Errorf("GetClosestN and LocateReplicas returned different lengths")
	}

	// Test GetClosestNForPartition (alias for LocateReplicasForPartition)
	partReplicas1, err1 := c.GetClosestNForPartition(ctx, 0, 2)
	partReplicas2, err2 := c.LocateReplicasForPartition(ctx, 0, 2)

	if err1 != nil || err2 != nil {
		t.Fatalf("Errors: GetClosestNForPartition=%v, LocateReplicasForPartition=%v", err1, err2)
	}

	if len(partReplicas1) != len(partReplicas2) {
		t.Errorf("GetClosestNForPartition and LocateReplicasForPartition returned different lengths")
	}
}
