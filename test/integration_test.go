package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/focusandinsist/consistent-go/consistent"
)

// TestFullWorkflow tests a complete workflow of operations
func TestFullWorkflow(t *testing.T) {
	config := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            consistent.NewDefaultHasher(),
	}

	ctx := context.Background()

	// Start with empty cluster
	c, err := consistent.New(config)
	if err != nil {
		t.Fatalf("Failed to create consistent hash: %v", err)
	}

	// Generate test keys
	testKeys := make([]string, 100)
	for i := 0; i < 100; i++ {
		testKeys[i] = fmt.Sprintf("key-%03d", i)
	}

	// Phase 1: Add initial nodes
	t.Log("Phase 1: Adding initial nodes")
	initialNodes := []string{"node-A", "node-B", "node-C"}
	for _, node := range initialNodes {
		err := c.Add(ctx, node)
		if err != nil {
			t.Fatalf("Failed to add node %s: %v", node, err)
		}
	}

	// Verify initial distribution
	dist1 := getIntegrationKeyDistribution(ctx, c, testKeys)
	t.Log("Initial distribution:")
	for node, count := range dist1 {
		t.Logf("  %s: %d keys", node, count)
	}

	// Phase 2: Add more nodes and verify rebalancing
	t.Log("Phase 2: Adding more nodes")
	additionalNodes := []string{"node-D", "node-E"}
	for _, node := range additionalNodes {
		err := c.Add(ctx, node)
		if err != nil {
			t.Fatalf("Failed to add node %s: %v", node, err)
		}

		// Verify node got some keys (after fix)
		dist := getIntegrationKeyDistribution(ctx, c, testKeys)
		if dist[node] == 0 {
			t.Errorf("Node %s has 0 keys after addition", node)
		}
	}

	// Verify final distribution after all additions
	dist2 := getIntegrationKeyDistribution(ctx, c, testKeys)
	t.Log("Distribution after adding all nodes:")
	for node, count := range dist2 {
		t.Logf("  %s: %d keys", node, count)
	}

	// Phase 3: Remove some nodes and verify rebalancing
	t.Log("Phase 3: Removing nodes")
	nodesToRemove := []string{"node-B", "node-D"}
	for _, node := range nodesToRemove {
		err := c.Remove(ctx, node)
		if err != nil {
			t.Fatalf("Failed to remove node %s: %v", node, err)
		}

		// Verify node is gone
		members := c.GetMembers(ctx)
		for _, member := range members {
			if member == node {
				t.Errorf("Node %s still present after removal", node)
			}
		}
	}

	// Verify final distribution after removals
	dist3 := getIntegrationKeyDistribution(ctx, c, testKeys)
	t.Log("Distribution after removing nodes:")
	for node, count := range dist3 {
		t.Logf("  %s: %d keys", node, count)
	}

	// Phase 4: Verify consistency throughout
	t.Log("Phase 4: Verifying consistency")
	for _, key := range testKeys[:10] { // Test subset for performance
		owner, err := c.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Error locating key %s: %v", key, err)
		}

		// Verify owner is still a valid member
		members := c.GetMembers(ctx)
		found := false
		for _, member := range members {
			if member == owner {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Key %s is owned by non-existent node %s", key, owner)
		}
	}

	// Verify all keys are still accessible
	totalKeys := 0
	for _, count := range dist3 {
		totalKeys += count
	}
	if totalKeys != len(testKeys) {
		t.Errorf("Lost keys during operations: expected %d, got %d", len(testKeys), totalKeys)
	}
}

// TestScenarioClusterExpansion tests gradual cluster expansion
func TestScenarioClusterExpansion(t *testing.T) {
	config := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            consistent.NewDefaultHasher(),
	}

	ctx := context.Background()

	// Start with 2 nodes
	c, err := consistent.NewWithMembers([]string{"node-1", "node-2"}, config)
	if err != nil {
		t.Fatalf("Failed to create initial cluster: %v", err)
	}

	testKeys := generateIntegrationTestKeys(50)

	// Track key movements during expansion
	keyLocations := make(map[string][]string) // key -> [locations over time]

	// Record initial locations
	for _, key := range testKeys {
		owner, err := c.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Error locating key %s: %v", key, err)
		}
		keyLocations[key] = []string{owner}
	}

	// Gradually expand cluster
	for i := 3; i <= 8; i++ {
		newNode := fmt.Sprintf("node-%d", i)
		t.Logf("Adding %s (cluster size: %d)", newNode, i)

		err := c.Add(ctx, newNode)
		if err != nil {
			t.Fatalf("Failed to add %s: %v", newNode, err)
		}

		// Record new locations
		movedKeys := 0
		for _, key := range testKeys {
			owner, err := c.LocateKey(ctx, []byte(key))
			if err != nil {
				t.Fatalf("Error locating key %s: %v", key, err)
			}

			lastOwner := keyLocations[key][len(keyLocations[key])-1]
			keyLocations[key] = append(keyLocations[key], owner)

			if owner != lastOwner {
				movedKeys++
			}
		}

		movePercentage := float64(movedKeys) / float64(len(testKeys)) * 100
		t.Logf("  Keys moved: %d/%d (%.1f%%)", movedKeys, len(testKeys), movePercentage)

		// Verify new node got some keys
		dist := getIntegrationKeyDistribution(ctx, c, testKeys)
		if dist[newNode] == 0 {
			t.Errorf("New node %s has 0 keys", newNode)
		}
	}

	// Analyze key movement patterns
	t.Log("Key movement analysis:")
	for _, key := range testKeys[:5] { // Analyze first 5 keys
		locations := keyLocations[key]
		t.Logf("  %s: %v", key, locations)

		// Verify key didn't move unnecessarily
		moves := 0
		for j := 1; j < len(locations); j++ {
			if locations[j] != locations[j-1] {
				moves++
			}
		}

		// A key should not move too frequently
		if moves > 3 {
			t.Logf("    Warning: Key %s moved %d times (may be excessive)", key, moves)
		}
	}
}

// TestScenarioFailureRecovery tests node failure and recovery
func TestScenarioFailureRecovery(t *testing.T) {
	config := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            consistent.NewDefaultHasher(),
	}

	ctx := context.Background()

	// Start with 5 nodes
	initialNodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	c, err := consistent.NewWithMembers(initialNodes, config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	testKeys := generateIntegrationTestKeys(100)

	// Record initial state
	initialDist := getIntegrationKeyDistribution(ctx, c, testKeys)
	t.Log("Initial distribution:")
	for node, count := range initialDist {
		t.Logf("  %s: %d keys", node, count)
	}

	// Simulate node failures
	failedNodes := []string{"node-2", "node-4"}
	for _, node := range failedNodes {
		t.Logf("Simulating failure of %s", node)

		err := c.Remove(ctx, node)
		if err != nil {
			t.Fatalf("Failed to remove failed node %s: %v", node, err)
		}

		// Verify all keys are still accessible
		for _, key := range testKeys {
			owner, err := c.LocateKey(ctx, []byte(key))
			if err != nil {
				t.Fatalf("Key %s became inaccessible after %s failure: %v", key, node, err)
			}
			if owner == node {
				t.Errorf("Key %s still assigned to failed node %s", key, node)
			}
		}
	}

	// Check distribution after failures
	afterFailureDist := getIntegrationKeyDistribution(ctx, c, testKeys)
	t.Log("Distribution after failures:")
	for node, count := range afterFailureDist {
		t.Logf("  %s: %d keys", node, count)
	}

	// Simulate recovery by adding replacement nodes
	replacementNodes := []string{"node-6", "node-7"}
	for _, node := range replacementNodes {
		t.Logf("Adding replacement node %s", node)

		err := c.Add(ctx, node)
		if err != nil {
			t.Fatalf("Failed to add replacement node %s: %v", node, err)
		}

		// Verify new node gets some load
		dist := getIntegrationKeyDistribution(ctx, c, testKeys)
		if dist[node] == 0 {
			t.Errorf("Replacement node %s has 0 keys", node)
		}
	}

	// Final distribution
	finalDist := getIntegrationKeyDistribution(ctx, c, testKeys)
	t.Log("Final distribution after recovery:")
	for node, count := range finalDist {
		t.Logf("  %s: %d keys", node, count)
	}

	// Verify cluster health
	members := c.GetMembers(ctx)
	if len(members) != 5 {
		t.Errorf("Expected 5 members after recovery, got %d", len(members))
	}

	// Verify load balance
	loads := c.LoadDistribution(ctx)
	t.Log("Load distribution (partitions):")
	for node, load := range loads {
		t.Logf("  %s: %.0f partitions", node, load)
	}
}

// TestScenarioReplicaPlacement tests replica placement across different scenarios
func TestScenarioReplicaPlacement(t *testing.T) {
	config := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            consistent.NewDefaultHasher(),
	}

	ctx := context.Background()

	// Test with different cluster sizes
	clusterSizes := []int{3, 5, 7, 10}

	for _, size := range clusterSizes {
		t.Run(fmt.Sprintf("ClusterSize%d", size), func(t *testing.T) {
			// Create cluster
			members := make([]string, size)
			for i := 0; i < size; i++ {
				members[i] = fmt.Sprintf("node-%d", i+1)
			}

			c, err := consistent.NewWithMembers(members, config)
			if err != nil {
				t.Fatalf("Failed to create cluster of size %d: %v", size, err)
			}

			testKey := "replica-test-key"

			// Test different replica counts
			for replicaCount := 1; replicaCount <= min(size, 5); replicaCount++ {
				replicas, err := c.LocateReplicas(ctx, []byte(testKey), replicaCount)
				if err != nil {
					t.Fatalf("Error locating %d replicas in cluster of size %d: %v",
						replicaCount, size, err)
				}

				if len(replicas) != replicaCount {
					t.Errorf("Expected %d replicas, got %d", replicaCount, len(replicas))
				}

				// Verify no duplicates
				seen := make(map[string]bool)
				for _, replica := range replicas {
					if seen[replica] {
						t.Errorf("Duplicate replica %s for %d replicas in cluster of size %d",
							replica, replicaCount, size)
					}
					seen[replica] = true
				}

				// Verify all replicas are valid members
				for _, replica := range replicas {
					found := false
					for _, member := range members {
						if member == replica {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("Invalid replica %s in cluster of size %d", replica, size)
					}
				}
			}
		})
	}
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Helper function to get key distribution for integration tests
func getIntegrationKeyDistribution(ctx context.Context, c *consistent.Consistent, keys []string) map[string]int {
	dist := make(map[string]int)

	// Initialize all members with 0
	members := c.GetMembers(ctx)
	for _, member := range members {
		dist[member] = 0
	}

	// Count actual distribution
	for _, key := range keys {
		owner, err := c.LocateKey(ctx, []byte(key))
		if err != nil {
			continue
		}
		dist[owner]++
	}

	return dist
}

// Helper function to generate test keys for integration tests
func generateIntegrationTestKeys(count int) []string {
	keys := make([]string, count)
	prefixes := []string{"user", "order", "session", "product", "cache"}

	for i := 0; i < count; i++ {
		prefix := prefixes[i%len(prefixes)]
		keys[i] = fmt.Sprintf("%s-%03d", prefix, i)
	}
	return keys
}

// TestConcurrentOperations tests concurrent lookups and member additions.
func TestConcurrentOperations(t *testing.T) {
	ctx := context.Background()

	c, err := consistent.NewWithMembers([]string{"node-1", "node-2", "node-3"}, consistent.Config{
		PartitionCount:    50,
		ReplicationFactor: 100,
		Load:              1.5,
	})
	if err != nil {
		t.Fatalf("Failed to create consistent hash: %v", err)
	}

	const numGoroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("worker-%d-key-%d", workerID, j)
				if _, err := c.LocateKey(ctx, []byte(key)); err != nil {
					errors <- fmt.Errorf("worker %d failed to locate key %s: %v", workerID, key, err)
					return
				}
			}
		}(i)
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		if err := c.Add(ctx, "dynamic-node-1"); err != nil {
			errors <- fmt.Errorf("failed to add dynamic-node-1: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		if err := c.Add(ctx, "dynamic-node-2"); err != nil {
			errors <- fmt.Errorf("failed to add dynamic-node-2: %v", err)
		}
	}()

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	if members := c.GetMembers(ctx); len(members) != 5 {
		t.Errorf("Expected 5 members after concurrent operations, got %d", len(members))
	}
}
