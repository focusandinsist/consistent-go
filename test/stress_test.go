package test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/focusandinsist/consistent-go/consistent"
)

// TestHighLoadStress tests the system under high load conditions
func TestHighLoadStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	ctx := context.Background()

	// Create a large cluster
	members := make([]string, 20)
	for i := 0; i < 20; i++ {
		members[i] = fmt.Sprintf("node-%02d", i+1)
	}

	config := consistent.Config{
		PartitionCount:    1000,
		ReplicationFactor: 500,
		Load:              1.5,
	}

	ring, err := consistent.NewWithMembers(members, config)
	if err != nil {
		t.Fatalf("Failed to create large cluster: %v", err)
	}

	const numKeys = 100000
	const numGoroutines = 50
	const keysPerGoroutine = numKeys / numGoroutines

	t.Logf("Starting stress test with %d keys across %d goroutines", numKeys, numGoroutines)

	start := time.Now()
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)
	results := make(chan map[string]int, numGoroutines)

	// Concurrent key lookups
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			localDistribution := make(map[string]int)
			startKey := workerID * keysPerGoroutine

			for j := 0; j < keysPerGoroutine; j++ {
				key := fmt.Sprintf("stress-key-%d", startKey+j)
				member, err := ring.LocateKey(ctx, []byte(key))
				if err != nil {
					errors <- fmt.Errorf("worker %d failed to locate key %s: %v", workerID, key, err)
					return
				}
				localDistribution[member]++
			}

			results <- localDistribution
		}(i)
	}

	wg.Wait()
	close(errors)
	close(results)

	duration := time.Since(start)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Error(err)
		errorCount++
	}

	if errorCount > 0 {
		t.Fatalf("Stress test failed with %d errors", errorCount)
	}

	// Aggregate results
	totalDistribution := make(map[string]int)
	for localDist := range results {
		for member, count := range localDist {
			totalDistribution[member] += count
		}
	}

	// Calculate performance metrics
	opsPerSecond := float64(numKeys) / duration.Seconds()
	t.Logf("Stress test completed in %v", duration)
	t.Logf("Performance: %.0f operations/second", opsPerSecond)

	// Verify distribution quality
	expectedPerMember := numKeys / len(members)
	maxDeviation := float64(expectedPerMember) * 0.3 // Allow 30% deviation

	t.Log("Load distribution across members:")
	for member, count := range totalDistribution {
		percentage := float64(count) / float64(numKeys) * 100
		deviation := float64(count - expectedPerMember)
		t.Logf("  %s: %d keys (%.1f%%, deviation: %+.0f)", member, count, percentage, deviation)

		if deviation > maxDeviation || deviation < -maxDeviation {
			t.Errorf("Member %s has excessive load deviation: %+.0f (max allowed: ±%.0f)",
				member, deviation, maxDeviation)
		}
	}

	// Performance threshold check
	minOpsPerSecond := 10000.0 // Minimum expected performance
	if opsPerSecond < minOpsPerSecond {
		t.Errorf("Performance below threshold: %.0f ops/sec (minimum: %.0f)", opsPerSecond, minOpsPerSecond)
	}
}

// TestConcurrentModifications tests concurrent cluster modifications
func TestConcurrentModifications(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent modification test in short mode")
	}

	ctx := context.Background()

	initialMembers := []string{"base-1", "base-2", "base-3"}
	config := consistent.Config{
		PartitionCount:    200,
		ReplicationFactor: 300,
		Load:              2.0,
	}

	ring, err := consistent.NewWithMembers(initialMembers, config)
	if err != nil {
		t.Fatalf("Failed to create initial cluster: %v", err)
	}

	const testDuration = 10 * time.Second
	const numReaders = 20
	const numWriters = 5

	t.Logf("Running concurrent modification test for %v", testDuration)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})
	errors := make(chan error, numReaders+numWriters)

	// Start reader goroutines
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			readCount := 0
			for {
				select {
				case <-stopChan:
					t.Logf("Reader %d completed %d reads", readerID, readCount)
					return
				default:
					key := fmt.Sprintf("reader-%d-key-%d", readerID, readCount)
					_, err := ring.LocateKey(ctx, []byte(key))
					if err != nil {
						errors <- fmt.Errorf("reader %d failed to locate key: %v", readerID, err)
						return
					}
					readCount++

					// Small delay to prevent CPU spinning
					time.Sleep(time.Microsecond * 100)
				}
			}
		}(i)
	}

	// Start writer goroutines
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			addCount := 0
			removeCount := 0

			for {
				select {
				case <-stopChan:
					t.Logf("Writer %d completed %d adds, %d removes", writerID, addCount, removeCount)
					return
				default:
					// Randomly add or remove members
					if rand.Float32() < 0.5 {
						// Add member
						memberName := fmt.Sprintf("dynamic-%d-%d", writerID, addCount)
						err := ring.Add(ctx, memberName)
						if err != nil {
							// Ignore duplicate member errors (check error message)
							if !isAlreadyExistsError(err) {
								errors <- fmt.Errorf("writer %d failed to add member %s: %v", writerID, memberName, err)
								return
							}
						} else {
							addCount++
						}
					} else {
						// Try to remove a dynamic member
						members := ring.GetMembers(ctx)
						var dynamicMembers []string
						for _, member := range members {
							if len(member) > 8 && member[:8] == "dynamic-" {
								dynamicMembers = append(dynamicMembers, member)
							}
						}

						if len(dynamicMembers) > 0 {
							memberToRemove := dynamicMembers[rand.Intn(len(dynamicMembers))]
							err := ring.Remove(ctx, memberToRemove)
							if err != nil {
								// Ignore member not found errors (check error message)
								if !isNotFoundError(err) {
									errors <- fmt.Errorf("writer %d failed to remove member %s: %v", writerID, memberToRemove, err)
									return
								}
							} else {
								removeCount++
							}
						}
					}

					// Delay between modifications
					time.Sleep(time.Millisecond * 50)
				}
			}
		}(i)
	}

	// Run test for specified duration
	time.Sleep(testDuration)
	close(stopChan)
	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Error(err)
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Concurrent modification test failed with %d errors", errorCount)
	}

	// Verify final cluster state
	finalMembers := ring.GetMembers(ctx)
	t.Logf("Final cluster has %d members", len(finalMembers))

	// Ensure it still have the base members
	baseMembers := map[string]bool{"base-1": true, "base-2": true, "base-3": true}
	for _, member := range finalMembers {
		if baseMembers[member] {
			delete(baseMembers, member)
		}
	}

	if len(baseMembers) > 0 {
		t.Errorf("Missing base members: %v", baseMembers)
	}
}

// TestLongRunningStability tests system stability over extended periods
func TestLongRunningStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running stability test in short mode")
	}

	ctx := context.Background()

	members := []string{"stable-1", "stable-2", "stable-3", "stable-4", "stable-5"}
	config := consistent.Config{
		PartitionCount:    500,
		ReplicationFactor: 400,
		Load:              1.5,
	}

	ring, err := consistent.NewWithMembers(members, config)
	if err != nil {
		t.Fatalf("Failed to create stable cluster: %v", err)
	}

	const testDuration = 30 * time.Second // Reduced for CI/testing
	const operationInterval = time.Millisecond * 10

	t.Logf("Running stability test for %v", testDuration)

	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Continuous key operations
	wg.Add(1)
	go func() {
		defer wg.Done()

		operationCount := 0
		for {
			select {
			case <-stopChan:
				t.Logf("Completed %d stability operations", operationCount)
				return
			default:
				key := fmt.Sprintf("stability-key-%d", operationCount)
				_, err := ring.LocateKey(ctx, []byte(key))
				if err != nil {
					t.Errorf("Stability operation %d failed: %v", operationCount, err)
					return
				}
				operationCount++
				time.Sleep(operationInterval)
			}
		}
	}()

	// Periodic cluster modifications
	wg.Add(1)
	go func() {
		defer wg.Done()

		modificationCount := 0
		ticker := time.NewTicker(time.Second * 2)
		defer ticker.Stop()

		for {
			select {
			case <-stopChan:
				t.Logf("Completed %d stability modifications", modificationCount)
				return
			case <-ticker.C:
				// Add and then remove a temporary member
				tempMember := fmt.Sprintf("temp-member-%d", modificationCount)

				err := ring.Add(ctx, tempMember)
				if err != nil {
					t.Errorf("Failed to add temporary member %s: %v", tempMember, err)
					return
				}

				time.Sleep(time.Millisecond * 500)

				err = ring.Remove(ctx, tempMember)
				if err != nil {
					t.Errorf("Failed to remove temporary member %s: %v", tempMember, err)
					return
				}

				modificationCount++
			}
		}
	}()

	// Run for specified duration
	time.Sleep(testDuration)
	close(stopChan)
	wg.Wait()

	// Verify final state
	finalMembers := ring.GetMembers(ctx)
	if len(finalMembers) != len(members) {
		t.Errorf("Expected %d members after stability test, got %d", len(members), len(finalMembers))
	}

	// Verify all original members are still present
	memberSet := make(map[string]bool)
	for _, member := range finalMembers {
		memberSet[member] = true
	}

	for _, originalMember := range members {
		if !memberSet[originalMember] {
			t.Errorf("Original member %s missing after stability test", originalMember)
		}
	}

	t.Log("Long-running stability test completed successfully")
}

// Helper functions for error checking
func isAlreadyExistsError(err error) bool {
	return strings.Contains(err.Error(), "already exists") ||
		strings.Contains(err.Error(), "duplicate")
}

func isNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "not found") ||
		strings.Contains(err.Error(), "does not exist")
}
