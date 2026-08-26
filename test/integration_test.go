package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/focusandinsist/consistent-go/consistent"
)

// TestFullWorkflow tests a complete workflow from cluster creation to key operations
func TestFullWorkflow(t *testing.T) {
	ctx := context.Background()

	// Step 1: Create initial cluster
	initialMembers := []string{"server-1", "server-2", "server-3"}
	config := consistent.Config{
		PartitionCount:    100,
		ReplicationFactor: 200,
		Load:              1.25,
	}

	c, err := consistent.NewWithMembers(initialMembers, config)
	if err != nil {
		t.Fatalf("Failed to create initial cluster: %v", err)
	}

	// Step 2: Test key distribution
	testKeys := generateTestKeys(1000)
	keyDistribution := make(map[string]int)

	for _, key := range testKeys {
		member, err := c.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s: %v", key, err)
		}
		keyDistribution[member]++
	}

	t.Logf("Initial distribution across %d members:", len(initialMembers))
	for member, count := range keyDistribution {
		percentage := float64(count) / float64(len(testKeys)) * 100
		t.Logf("  %s: %d keys (%.1f%%)", member, count, percentage)
	}

	// Step 3: Add new members (scale up)
	newMembers := []string{"server-4", "server-5"}
	for _, member := range newMembers {
		err := c.Add(ctx, member)
		if err != nil {
			t.Fatalf("Failed to add member %s: %v", member, err)
		}
	}

	// Step 4: Check redistribution after scaling up
	newKeyDistribution := make(map[string]int)

	for _, key := range testKeys {
		newMember, err := c.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s after scaling: %v", key, err)
		}
		newKeyDistribution[newMember]++
	}

	t.Logf("Distribution after adding %d members:", len(newMembers))
	for member, count := range newKeyDistribution {
		percentage := float64(count) / float64(len(testKeys)) * 100
		t.Logf("  %s: %d keys (%.1f%%)", member, count, percentage)
	}

	// Verify that new members got some keys
	for _, newMember := range newMembers {
		if newKeyDistribution[newMember] == 0 {
			t.Errorf("New member %s received no keys", newMember)
		}
	}

	// Verify distribution is reasonably balanced (no member should have >50% of keys)
	maxPercentage := 0.0
	for _, count := range newKeyDistribution {
		percentage := float64(count) / float64(len(testKeys)) * 100
		if percentage > maxPercentage {
			maxPercentage = percentage
		}
	}
	if maxPercentage > 50.0 {
		t.Errorf("Load distribution too unbalanced: max member has %.1f%% of keys", maxPercentage)
	}

	// Step 5: Test replica placement
	replicaCount := 3
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("replica-test-key-%d", i)
		replicas, err := c.LocateReplicas(ctx, []byte(key), replicaCount)
		if err != nil {
			t.Fatalf("Failed to locate replicas for key %s: %v", key, err)
		}

		if len(replicas) != replicaCount {
			t.Errorf("Expected %d replicas for key %s, got %d", replicaCount, key, len(replicas))
		}

		// Verify replicas are unique
		uniqueReplicas := make(map[string]bool)
		for _, replica := range replicas {
			if uniqueReplicas[replica] {
				t.Errorf("Duplicate replica %s for key %s", replica, key)
			}
			uniqueReplicas[replica] = true
		}
	}

	// Step 6: Test member removal (scale down)
	memberToRemove := "server-2"
	err = c.Remove(ctx, memberToRemove)
	if err != nil {
		t.Fatalf("Failed to remove member %s: %v", memberToRemove, err)
	}

	// Step 7: Verify all keys are still accessible after removal
	finalKeyDistribution := make(map[string]int)
	for _, key := range testKeys {
		member, err := c.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate key %s after member removal: %v", key, err)
		}
		if member == memberToRemove {
			t.Errorf("Key %s still assigned to removed member %s", key, memberToRemove)
		}
		finalKeyDistribution[member]++
	}

	t.Logf("Final distribution after removing %s:", memberToRemove)
	for member, count := range finalKeyDistribution {
		percentage := float64(count) / float64(len(testKeys)) * 100
		t.Logf("  %s: %d keys (%.1f%%)", member, count, percentage)
	}

	// Step 8: Verify cluster state
	members := c.GetMembers(ctx)
	expectedMembers := []string{"server-1", "server-3", "server-4", "server-5"}
	if len(members) != len(expectedMembers) {
		t.Errorf("Expected %d members, got %d", len(expectedMembers), len(members))
	}

	memberSet := make(map[string]bool)
	for _, member := range members {
		memberSet[member] = true
	}
	for _, expected := range expectedMembers {
		if !memberSet[expected] {
			t.Errorf("Expected member %s not found in cluster", expected)
		}
	}
}

// TestConcurrentOperations tests concurrent access to the consistent hash
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

	// Concurrent key lookups
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("worker-%d-key-%d", workerID, j)
				_, err := c.LocateKey(ctx, []byte(key))
				if err != nil {
					errors <- fmt.Errorf("worker %d failed to locate key %s: %v", workerID, key, err)
					return
				}
			}
		}(i)
	}

	// Concurrent member operations
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		err := c.Add(ctx, "dynamic-node-1")
		if err != nil {
			errors <- fmt.Errorf("failed to add dynamic-node-1: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		err := c.Add(ctx, "dynamic-node-2")
		if err != nil {
			errors <- fmt.Errorf("failed to add dynamic-node-2: %v", err)
		}
	}()

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Verify final state
	members := c.GetMembers(ctx)
	if len(members) != 5 {
		t.Errorf("Expected 5 members after concurrent operations, got %d", len(members))
	}
}

// Helper functions
func generateTestKeys(count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("test-key-%d", i)
	}
	return keys
}
