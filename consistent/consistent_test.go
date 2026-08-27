package consistent

import (
	"context"
	"errors"
	"testing"
)

// TestNew tests the creation of a new Consistent instance
func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid_default_config",
			config: Config{
				Hasher: NewDefaultHasher(),
			},
			wantErr: false,
		},
		{
			name:    "empty_config_uses_defaults",
			config:  Config{},
			wantErr: false,
		},
		{
			name: "negative_partition_count",
			config: Config{
				PartitionCount: -1,
			},
			wantErr: true,
		},
		{
			name: "negative_replication_factor",
			config: Config{
				ReplicationFactor: -1,
			},
			wantErr: true,
		},
		{
			name: "negative_load",
			config: Config{
				Load: -1.0,
			},
			wantErr: true,
		},
		{
			name: "custom_valid_config",
			config: Config{
				PartitionCount:    100,
				ReplicationFactor: 50,
				Load:              1.5,
				Hasher:            NewXXHasher(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && c == nil {
				t.Error("New() returned nil Consistent instance")
			}
		})
	}
}

// TestNewWithMembers tests creation with initial members
func TestNewWithMembers(t *testing.T) {
	tests := []struct {
		name    string
		members []string
		config  Config
		wantErr bool
	}{
		{
			name:    "empty_members",
			members: []string{},
			config:  Config{},
			wantErr: false,
		},
		{
			name:    "single_member",
			members: []string{"node1"},
			config:  Config{PartitionCount: 50, ReplicationFactor: 100},
			wantErr: false,
		},
		{
			name:    "multiple_members",
			members: []string{"node1", "node2", "node3"},
			config:  Config{ReplicationFactor: 100},
			wantErr: false,
		},
		{
			name:    "duplicate_members",
			members: []string{"node1", "node1", "node2"},
			config:  Config{ReplicationFactor: 100},
			wantErr: false, // Should handle duplicates gracefully
		},
		{
			name:    "empty_member_name",
			members: []string{"node1", "", "node2"},
			config:  Config{ReplicationFactor: 100},
			wantErr: false, // Empty names should be filtered out during Add
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewWithMembers(tt.members, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewWithMembers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && c == nil {
				t.Error("NewWithMembers() returned nil Consistent instance")
			}
		})
	}
}

// TestAdd tests adding members to the ring
func TestAdd(t *testing.T) {
	ctx := context.Background()
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	tests := []struct {
		name    string
		member  string
		wantErr bool
	}{
		{
			name:    "valid_member",
			member:  "node1",
			wantErr: false,
		},
		{
			name:    "empty_member_name",
			member:  "",
			wantErr: true,
		},
		{
			name:    "duplicate_member",
			member:  "node1", // Adding same member again
			wantErr: false,   // Should be idempotent
		},
		{
			name:    "another_valid_member",
			member:  "node2",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.Add(ctx, tt.member)
			if (err != nil) != tt.wantErr {
				t.Errorf("Add() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// Verify members were added
	members := c.GetMembers(ctx)
	expectedMembers := []string{"node1", "node2"}
	if len(members) != len(expectedMembers) {
		t.Errorf("Expected %d members, got %d", len(expectedMembers), len(members))
	}
}

// TestRemove tests removing members from the ring
func TestRemove(t *testing.T) {
	ctx := context.Background()

	// Create instance with initial members
	initialMembers := []string{"node1", "node2", "node3"}
	c, err := NewWithMembers(initialMembers, Config{
		PartitionCount:    50,
		ReplicationFactor: 100,
		Load:              2.0, // Higher load to avoid capacity issues
	})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	tests := []struct {
		name    string
		member  string
		wantErr bool
	}{
		{
			name:    "remove_existing_member",
			member:  "node3",
			wantErr: false,
		},
		{
			name:    "remove_non_existent_member",
			member:  "nonexistent",
			wantErr: false, // Remove non-existent member should be idempotent
		},
		{
			name:    "remove_already_removed_member",
			member:  "node3",
			wantErr: false, // Remove already removed member should be idempotent
		},
		{
			name:    "remove_empty_name",
			member:  "",
			wantErr: false, // Remove empty name should be idempotent
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.Remove(ctx, tt.member)
			if (err != nil) != tt.wantErr {
				t.Errorf("Remove() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// Verify member was removed
	members := c.GetMembers(ctx)
	expectedMembers := []string{"node1", "node2"}
	if len(members) != len(expectedMembers) {
		t.Errorf("Expected %d members after removal, got %d", len(expectedMembers), len(members))
	}
}

func TestRemove_FailureLeavesStateUnchanged(t *testing.T) {
	ctx := context.Background()
	c, err := NewWithMembers([]string{"node1", "node2"}, Config{
		PartitionCount:    10,
		ReplicationFactor: 3,
		Load:              0.9,
	})
	if err != nil {
		t.Fatalf("NewWithMembers() error = %v", err)
	}

	wantMembers := c.GetMembers(ctx)
	wantLoads := c.LoadDistribution(ctx)
	wantOwners := make(map[int]string, 10)
	for partID := 0; partID < 10; partID++ {
		owner, err := c.GetPartitionOwner(ctx, partID)
		if err != nil {
			t.Fatalf("GetPartitionOwner(%d) error = %v", partID, err)
		}
		wantOwners[partID] = owner
	}
	wantReplicas, err := c.LocateReplicas(ctx, []byte("state-check"), 2)
	if err != nil {
		t.Fatalf("LocateReplicas() error = %v", err)
	}

	if err := c.Remove(ctx, "node1"); err == nil {
		t.Fatal("Remove() error = nil, want insufficient-space error")
	}

	gotMembers := c.GetMembers(ctx)
	if len(gotMembers) != len(wantMembers) {
		t.Fatalf("GetMembers() returned %v after failed Remove(), want %v", gotMembers, wantMembers)
	}
	for _, member := range wantMembers {
		found := false
		for _, got := range gotMembers {
			if got == member {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("GetMembers() missing %q after failed Remove(): %v", member, gotMembers)
		}
	}

	gotLoads := c.LoadDistribution(ctx)
	for member, want := range wantLoads {
		if got := gotLoads[member]; got != want {
			t.Errorf("LoadDistribution()[%q] = %v after failed Remove(), want %v", member, got, want)
		}
	}

	for partID, want := range wantOwners {
		got, err := c.GetPartitionOwner(ctx, partID)
		if err != nil {
			t.Errorf("GetPartitionOwner(%d) after failed Remove() error = %v", partID, err)
			continue
		}
		if got != want {
			t.Errorf("GetPartitionOwner(%d) = %q after failed Remove(), want %q", partID, got, want)
		}
	}

	gotReplicas, err := c.LocateReplicas(ctx, []byte("state-check"), 2)
	if err != nil {
		t.Fatalf("LocateReplicas() after failed Remove() error = %v", err)
	}
	if len(gotReplicas) != len(wantReplicas) {
		t.Fatalf("LocateReplicas() after failed Remove() = %v, want %v", gotReplicas, wantReplicas)
	}
	for i := range wantReplicas {
		if gotReplicas[i] != wantReplicas[i] {
			t.Errorf("LocateReplicas() after failed Remove() = %v, want %v", gotReplicas, wantReplicas)
			break
		}
	}
}

// TestLocateKey tests key location functionality
func TestLocateKey(t *testing.T) {
	ctx := context.Background()

	members := []string{"node1", "node2", "node3"}
	c, err := NewWithMembers(members, Config{ReplicationFactor: 100})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	testKeys := [][]byte{
		[]byte("test-key-1"),
		[]byte("test-key-2"),
		[]byte("test-key-3"),
		[]byte(""),
		[]byte("very-long-key-name-that-should-still-work-correctly"),
	}

	for i, key := range testKeys {
		t.Run(string(key), func(t *testing.T) {
			owner, err := c.LocateKey(ctx, key)
			if err != nil {
				t.Errorf("LocateKey() error = %v for key %d", err, i)
				return
			}
			if owner == "" {
				t.Errorf("LocateKey() returned empty owner for key %d", i)
			}

			// Verify owner is a valid member
			validOwner := false
			for _, member := range members {
				if owner == member {
					validOwner = true
					break
				}
			}
			if !validOwner {
				t.Errorf("LocateKey() returned invalid owner %s for key %d", owner, i)
			}
		})
	}
}

// TestContextCancellation tests context cancellation handling
func TestContextCancellation(t *testing.T) {
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Test Add with cancelled context
	err = c.Add(ctx, "node1")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Add() with cancelled context should return context.Canceled, got %v", err)
	}

	// Test Remove with cancelled context
	err = c.Remove(ctx, "node1")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Remove() with cancelled context should return context.Canceled, got %v", err)
	}

	// Test LocateKey with cancelled context
	_, err = c.LocateKey(ctx, []byte("test"))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("LocateKey() with cancelled context should return context.Canceled, got %v", err)
	}
}

// TestConcurrentAccess tests thread safety
func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("Failed to create Consistent instance: %v", err)
	}

	// Add initial member
	err = c.Add(ctx, "node1")
	if err != nil {
		t.Fatalf("Failed to add initial member: %v", err)
	}

	// Test concurrent reads and writes
	done := make(chan bool, 3)

	// Concurrent adds
	go func() {
		for i := 0; i < 10; i++ {
			c.Add(ctx, "node"+string(rune(i+2)))
		}
		done <- true
	}()

	// Concurrent lookups
	go func() {
		for i := 0; i < 100; i++ {
			c.LocateKey(ctx, []byte("test-key-"+string(rune(i))))
		}
		done <- true
	}()

	// Concurrent member queries
	go func() {
		for i := 0; i < 50; i++ {
			c.GetMembers(ctx)
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	for i := 0; i < 3; i++ {
		<-done
	}

	// Verify final state is consistent
	members := c.GetMembers(ctx)
	if len(members) == 0 {
		t.Error("No members found after concurrent operations")
	}
}
