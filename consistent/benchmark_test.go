package consistent

import (
	"context"
	"strconv"
	"testing"
)

// BenchmarkNew benchmarks creating new Consistent instances
func BenchmarkNew(b *testing.B) {
	config := Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := New(config)
		if err != nil {
			b.Fatalf("Failed to create Consistent instance: %v", err)
		}
		_ = c
	}
}

// BenchmarkNewWithMembers benchmarks creating instances with initial members
func BenchmarkNewWithMembers(b *testing.B) {
	members := []string{"node1", "node2", "node3", "node4", "node5"}
	config := Config{
		PartitionCount:    271,
		ReplicationFactor: 100,
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

// BenchmarkAdd benchmarks adding members to the ring
func BenchmarkAdd(b *testing.B) {
	ctx := context.Background()
	c, err := New(Config{})
	if err != nil {
		b.Fatalf("Failed to create Consistent instance: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memberName := "node" + strconv.Itoa(i)
		err := c.Add(ctx, memberName)
		if err != nil {
			b.Fatalf("Failed to add member: %v", err)
		}
	}
}

// BenchmarkRemove benchmarks removing members from the ring
func BenchmarkRemove(b *testing.B) {
	ctx := context.Background()

	// Pre-populate with members
	members := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		members[i] = "node" + strconv.Itoa(i)
	}

	c, err := NewWithMembers(members, Config{
		ReplicationFactor: 200,
		Load:              2.0, // Higher load to avoid capacity issues during removal
	})
	if err != nil {
		b.Fatalf("Failed to create Consistent instance: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memberName := "node" + strconv.Itoa(i)
		err := c.Remove(ctx, memberName)
		if err != nil {
			b.Fatalf("Failed to remove member: %v", err)
		}
	}
}

// BenchmarkLocateKey benchmarks key location
func BenchmarkLocateKey(b *testing.B) {
	ctx := context.Background()
	members := []string{"node1", "node2", "node3", "node4", "node5"}
	c, err := NewWithMembers(members, Config{ReplicationFactor: 100})
	if err != nil {
		b.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Pre-generate keys
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte("key" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.LocateKey(ctx, keys[i])
		if err != nil {
			b.Fatalf("Failed to locate key: %v", err)
		}
	}
}

// BenchmarkLocateReplicas benchmarks replica location
func BenchmarkLocateReplicas(b *testing.B) {
	ctx := context.Background()
	members := []string{"node1", "node2", "node3", "node4", "node5"}
	c, err := NewWithMembers(members, Config{ReplicationFactor: 100})
	if err != nil {
		b.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Pre-generate keys
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte("key" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := c.LocateReplicas(ctx, keys[i], 3)
		if err != nil {
			b.Fatalf("Failed to locate replicas: %v", err)
		}
	}
}

// BenchmarkGetMembers benchmarks getting member list
func BenchmarkGetMembers(b *testing.B) {
	ctx := context.Background()
	members := []string{"node1", "node2", "node3", "node4", "node5"}
	c, err := NewWithMembers(members, Config{ReplicationFactor: 100})
	if err != nil {
		b.Fatalf("Failed to create Consistent instance: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.GetMembers(ctx)
	}
}

// BenchmarkLoadDistribution benchmarks getting load distribution
func BenchmarkLoadDistribution(b *testing.B) {
	ctx := context.Background()
	members := []string{"node1", "node2", "node3", "node4", "node5"}
	c, err := NewWithMembers(members, Config{ReplicationFactor: 100})
	if err != nil {
		b.Fatalf("Failed to create Consistent instance: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.LoadDistribution(ctx)
	}
}

// BenchmarkFindPartitionID benchmarks partition ID calculation
func BenchmarkFindPartitionID(b *testing.B) {
	c, err := New(Config{})
	if err != nil {
		b.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Pre-generate keys
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte("key" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.FindPartitionID(keys[i])
	}
}

// BenchmarkConcurrentLocateKey benchmarks concurrent key location
func BenchmarkConcurrentLocateKey(b *testing.B) {
	ctx := context.Background()
	members := []string{"node1", "node2", "node3", "node4", "node5"}
	c, err := NewWithMembers(members, Config{ReplicationFactor: 100})
	if err != nil {
		b.Fatalf("Failed to create Consistent instance: %v", err)
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte("key" + strconv.Itoa(i))
			_, err := c.LocateKey(ctx, key)
			if err != nil {
				b.Fatalf("Failed to locate key: %v", err)
			}
			i++
		}
	})
}

// BenchmarkScaleOperations benchmarks scaling operations
func BenchmarkScaleOperations(b *testing.B) {
	ctx := context.Background()

	// Different cluster sizes to benchmark
	clusterSizes := []int{3, 5, 10, 20, 50}

	for _, size := range clusterSizes {
		b.Run("cluster_size_"+strconv.Itoa(size), func(b *testing.B) {
			// Create initial cluster
			members := make([]string, size)
			for i := 0; i < size; i++ {
				members[i] = "node" + strconv.Itoa(i)
			}

			c, err := NewWithMembers(members, Config{
				ReplicationFactor: 200,
				Load:              2.0, // Higher load to avoid capacity issues
			})
			if err != nil {
				b.Fatalf("Failed to create Consistent instance: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Add a node
				newNode := "temp" + strconv.Itoa(i)
				err := c.Add(ctx, newNode)
				if err != nil {
					b.Fatalf("Failed to add node: %v", err)
				}

				// Remove the node
				err = c.Remove(ctx, newNode)
				if err != nil {
					b.Fatalf("Failed to remove node: %v", err)
				}
			}
		})
	}
}

// BenchmarkDifferentConfigurations benchmarks different configuration parameters
func BenchmarkDifferentConfigurations(b *testing.B) {
	ctx := context.Background()
	members := []string{"node1", "node2", "node3"}

	configs := []struct {
		name              string
		partitionCount    int
		replicationFactor int
	}{
		{"small", 50, 100},
		{"medium", 271, 100},
		{"large", 1000, 200},
	}

	for _, config := range configs {
		b.Run(config.name, func(b *testing.B) {
			c, err := NewWithMembers(members, Config{
				PartitionCount:    config.partitionCount,
				ReplicationFactor: config.replicationFactor,
			})
			if err != nil {
				b.Fatalf("Failed to create Consistent instance: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte("key" + strconv.Itoa(i))
				_, err := c.LocateKey(ctx, key)
				if err != nil {
					b.Fatalf("Failed to locate key: %v", err)
				}
			}
		})
	}
}

// BenchmarkMemoryUsage benchmarks memory usage patterns
func BenchmarkMemoryUsage(b *testing.B) {
	memberCounts := []int{10, 50, 100, 500}

	for _, count := range memberCounts {
		b.Run("members_"+strconv.Itoa(count), func(b *testing.B) {
			members := make([]string, count)
			for i := 0; i < count; i++ {
				members[i] = "node" + strconv.Itoa(i)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c, err := NewWithMembers(members, Config{ReplicationFactor: 200})
				if err != nil {
					b.Fatalf("Failed to create Consistent instance: %v", err)
				}
				_ = c
			}
		})
	}
}
