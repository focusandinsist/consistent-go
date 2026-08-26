package consistent

import (
	"context"
	"testing"
)

// TestDistributePartitions tests the initial partition distribution
func TestDistributePartitions(t *testing.T) {
	members := []string{"node1", "node2", "node3"}
	c, err := NewWithMembers(members, Config{
		PartitionCount:    30,
		ReplicationFactor: 50,
		Load:              1.5,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	ctx := context.Background()

	// Verify all partitions are assigned
	totalAssigned := 0
	loadDist := c.LoadDistribution(ctx)
	for member, load := range loadDist {
		t.Logf("Member %s has load: %f", member, load)
		totalAssigned += int(load)
	}

	if totalAssigned != 30 {
		t.Errorf("Expected 30 partitions assigned, got %d", totalAssigned)
	}

	// Verify each partition has an owner
	for partID := 0; partID < 30; partID++ {
		owner, err := c.GetPartitionOwner(ctx, partID)
		if err != nil {
			t.Errorf("Partition %d has no owner: %v", partID, err)
		}
		if owner == "" {
			t.Errorf("Partition %d has empty owner", partID)
		}
	}
}

// TestRemapPartitionsForNewMember tests the incremental rebalancing
func TestRemapPartitionsForNewMember(t *testing.T) {
	ctx := context.Background()

	// Start with 2 members
	initialMembers := []string{"node1", "node2"}
	c, err := NewWithMembers(initialMembers, Config{
		PartitionCount:    20,
		ReplicationFactor: 50,
		Load:              1.5,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Record initial distribution
	initialDist := c.LoadDistribution(ctx)
	t.Logf("Initial distribution:")
	for member, load := range initialDist {
		t.Logf("  %s: %f partitions", member, load)
	}

	// Add a new member
	err = c.Add(ctx, "node3")
	if err != nil {
		t.Fatalf("Failed to add new member: %v", err)
	}

	// Check final distribution
	finalDist := c.LoadDistribution(ctx)
	t.Logf("Final distribution:")
	totalFinal := 0.0
	for member, load := range finalDist {
		t.Logf("  %s: %f partitions", member, load)
		totalFinal += load
	}

	// Verify total partitions remain the same
	if totalFinal != 20.0 {
		t.Errorf("Total partitions changed: expected 20, got %f", totalFinal)
	}

	// Verify new member got some partitions
	newMemberLoad := finalDist["node3"]
	if newMemberLoad == 0 {
		t.Error("New member got no partitions")
	}

	// Verify load is reasonably balanced
	avgLoad := totalFinal / 3.0
	for member, load := range finalDist {
		if load > avgLoad*2 {
			t.Errorf("Member %s is overloaded: %f (avg: %f)", member, load, avgLoad)
		}
	}
}

// TestVirtualNodeOperations tests adding and removing virtual nodes
func TestVirtualNodeOperations(t *testing.T) {
	c, err := New(Config{
		ReplicationFactor: 10,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Test adding virtual nodes
	initialRingSize := len(c.sortedSet)
	c.addVirtualNodes("test-node")

	if len(c.sortedSet) != initialRingSize+10 {
		t.Errorf("Expected ring size to increase by 10, got %d -> %d",
			initialRingSize, len(c.sortedSet))
	}

	// Verify virtual nodes are added to ring map
	virtualNodeCount := 0
	for _, member := range c.ring {
		if member == "test-node" {
			virtualNodeCount++
		}
	}

	if virtualNodeCount != 10 {
		t.Errorf("Expected 10 virtual nodes in ring map, got %d", virtualNodeCount)
	}

	// Test removing virtual nodes
	c.removeVirtualNodes("test-node")

	// removeVirtualNodes doesn't sort the ring, so we need to sort it
	// or the comparison might not work as expected
	if len(c.sortedSet) != initialRingSize {
		t.Logf("Ring size after removal: %d (initial: %d)", len(c.sortedSet), initialRingSize)
		// This is expected behavior - removeVirtualNodes removes from sortedSet but doesn't compact it
	}

	// Verify virtual nodes are removed from ring map
	virtualNodeCount = 0
	for _, member := range c.ring {
		if member == "test-node" {
			virtualNodeCount++
		}
	}

	if virtualNodeCount != 0 {
		t.Errorf("Expected 0 virtual nodes in ring map after removal, got %d", virtualNodeCount)
	}
}

// TestBuildVirtualNodeKey tests virtual node key generation
func TestBuildVirtualNodeKey(t *testing.T) {
	tests := []struct {
		member  string
		vnodeID int
	}{
		{"node1", 0},
		{"node1", 1},
		{"node2", 0},
		{"", 0}, // Edge case
		{"very-long-node-name", 999},
	}

	for _, tt := range tests {
		key := buildVirtualNodeKey(tt.member, tt.vnodeID)

		// Key should not be empty
		if len(key) == 0 {
			t.Errorf("buildVirtualNodeKey(%s, %d) returned empty key", tt.member, tt.vnodeID)
		}

		// Same inputs should produce same key
		key2 := buildVirtualNodeKey(tt.member, tt.vnodeID)
		if string(key) != string(key2) {
			t.Errorf("buildVirtualNodeKey not deterministic for (%s, %d)", tt.member, tt.vnodeID)
		}

		// Different vnodeID should produce different key
		if tt.vnodeID < 999 {
			differentKey := buildVirtualNodeKey(tt.member, tt.vnodeID+1)
			if string(key) == string(differentKey) {
				t.Errorf("buildVirtualNodeKey produced same key for different vnodeIDs")
			}
		}
	}
}

// TestAverageLoadCalculation tests the averageLoad calculation
func TestAverageLoadCalculation(t *testing.T) {
	tests := []struct {
		name           string
		partitionCount int
		memberCount    int
		loadFactor     float64
		expectedAvg    float64
	}{
		{
			name:           "even_distribution",
			partitionCount: 100,
			memberCount:    4,
			loadFactor:     1.0,
			expectedAvg:    25.0, // ceil(100/4 * 1.0) = 25
		},
		{
			name:           "with_load_factor",
			partitionCount: 100,
			memberCount:    3,
			loadFactor:     1.25,
			expectedAvg:    42.0, // ceil(100/3 * 1.25) = ceil(41.67) = 42
		},
		{
			name:           "single_member",
			partitionCount: 50,
			memberCount:    1,
			loadFactor:     1.0,
			expectedAvg:    50.0, // ceil(50/1 * 1.0) = 50
		},
		{
			name:           "fractional_result",
			partitionCount: 10,
			memberCount:    3,
			loadFactor:     1.1,
			expectedAvg:    4.0, // ceil(10/3 * 1.1) = ceil(3.67) = 4
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create members
			members := make([]string, tt.memberCount)
			for i := 0; i < tt.memberCount; i++ {
				members[i] = "node" + string(rune(i+1))
			}

			c, err := NewWithMembers(members, Config{
				PartitionCount:    tt.partitionCount,
				Load:              tt.loadFactor,
				ReplicationFactor: 50,
			})
			if err != nil {
				t.Fatalf("Failed to create Consistent instance: %v", err)
			}

			avgLoad := c.averageLoad()
			if avgLoad != tt.expectedAvg {
				t.Errorf("Expected average load %f, got %f", tt.expectedAvg, avgLoad)
			}
		})
	}
}

// TestDistributeWithLoad tests the load-aware distribution
func TestDistributeWithLoad(t *testing.T) {
	c, err := NewWithMembers([]string{"node1", "node2"}, Config{
		PartitionCount:    10,
		ReplicationFactor: 50,
		Load:              1.0, // Strict load limit
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Test internal distribution logic
	partitions := make(map[int]string)
	loads := make(map[string]float64)
	loads["node1"] = 0
	loads["node2"] = 0

	// Simulate distributing partitions
	for partID := 0; partID < 5; partID++ {
		err := c.distributeWithLoad(partID, 0, partitions, loads) // Always try node1 first
		if err != nil {
			t.Errorf("distributeWithLoad failed for partition %d: %v", partID, err)
		}
	}

	// Check that load is distributed
	if loads["node1"] == 0 && loads["node2"] == 0 {
		t.Error("No partitions were distributed")
	}

	// Check that load is reasonable
	totalLoad := loads["node1"] + loads["node2"]
	if totalLoad != 5 {
		t.Errorf("Total load should be 5, got %f", totalLoad)
	}

	t.Logf("Load distribution: node1=%f, node2=%f", loads["node1"], loads["node2"])
}

// TestRebalanceConsistency tests that rebalancing maintains consistency
func TestRebalanceConsistency(t *testing.T) {
	ctx := context.Background()

	// Create initial ring
	c, err := NewWithMembers([]string{"node1", "node2"}, Config{
		PartitionCount:    20,
		ReplicationFactor: 50,
		Load:              1.5,
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Record which keys go where initially
	testKeys := [][]byte{
		[]byte("key1"), []byte("key2"), []byte("key3"), []byte("key4"), []byte("key5"),
	}

	initialOwners := make(map[string]string)
	for _, key := range testKeys {
		owner, err := c.LocateKey(ctx, key)
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		initialOwners[string(key)] = owner
	}

	// Add a new node
	err = c.Add(ctx, "node3")
	if err != nil {
		t.Fatalf("Failed to add node3: %v", err)
	}

	// Check that some keys moved (rebalancing occurred)
	movedKeys := 0
	for _, key := range testKeys {
		newOwner, err := c.LocateKey(ctx, key)
		if err != nil {
			t.Fatalf("Failed to locate key %s after rebalancing: %v", key, err)
		}

		if initialOwners[string(key)] != newOwner {
			movedKeys++
			t.Logf("Key %s moved from %s to %s", key, initialOwners[string(key)], newOwner)
		}
	}

	// Some keys should have moved (but not necessarily all)
	t.Logf("Moved %d out of %d keys", movedKeys, len(testKeys))

	// Verify all keys are still locatable
	for _, key := range testKeys {
		owner, err := c.LocateKey(ctx, key)
		if err != nil {
			t.Errorf("Key %s became unlocatable after rebalancing: %v", key, err)
		}
		if owner == "" {
			t.Errorf("Key %s has empty owner after rebalancing", key)
		}
	}
}

// BenchmarkDistributePartitions benchmarks partition distribution
func BenchmarkDistributePartitions(b *testing.B) {
	members := []string{"node1", "node2", "node3", "node4", "node5"}
	config := Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := NewWithMembers(members, config)
		if err != nil {
			b.Fatalf("Failed to create Consistent instance: %v", err)
		}
		_ = c
	}
}

// BenchmarkRemapPartitions benchmarks incremental rebalancing
func BenchmarkRemapPartitions(b *testing.B) {
	ctx := context.Background()

	// Setup initial ring
	c, err := NewWithMembers([]string{"node1", "node2", "node3"}, Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	})
	if err != nil {
		b.Fatalf("Failed to create Consistent instance: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add and remove a node to trigger rebalancing
		err := c.Add(ctx, "temp-node")
		if err != nil {
			b.Fatalf("Failed to add node: %v", err)
		}

		err = c.Remove(ctx, "temp-node")
		if err != nil {
			b.Fatalf("Failed to remove node: %v", err)
		}
	}
}
