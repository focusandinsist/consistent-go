package test

import (
	"context"
	"testing"

	"github.com/focusandinsist/consistent-go/consistent"
)

// TestDatabaseShardingScenario simulates a database sharding use case
func TestDatabaseShardingScenario(t *testing.T) {
	ctx := context.Background()

	// Simulate database shards
	shards := []string{
		"db-shard-us-east-1",
		"db-shard-us-west-1",
		"db-shard-eu-west-1",
	}

	config := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 300,
		Load:              1.25,
	}

	ring, err := consistent.NewWithMembers(shards, config)
	if err != nil {
		t.Fatalf("Failed to create sharding ring: %v", err)
	}

	// Simulate user IDs that need to be sharded
	userIDs := []string{
		"user_12345", "user_67890", "user_11111", "user_22222", "user_33333",
		"user_44444", "user_55555", "user_66666", "user_77777", "user_88888",
		"user_99999", "user_00000", "user_12121", "user_34343", "user_56565",
	}

	// Track which shard each user is assigned to
	userShardMapping := make(map[string]string)
	shardUserCount := make(map[string]int)

	t.Log("Initial user-to-shard mapping:")
	for _, userID := range userIDs {
		shard, err := ring.LocateKey(ctx, []byte(userID))
		if err != nil {
			t.Fatalf("Failed to locate shard for user %s: %v", userID, err)
		}
		userShardMapping[userID] = shard
		shardUserCount[shard]++
		t.Logf("  %s -> %s", userID, shard)
	}

	t.Log("\nInitial shard distribution:")
	for shard, count := range shardUserCount {
		percentage := float64(count) / float64(len(userIDs)) * 100
		t.Logf("  %s: %d users (%.1f%%)", shard, count, percentage)
	}

	// Simulate adding a new shard (scaling up)
	newShard := "db-shard-ap-southeast-1"
	t.Logf("\nAdding new shard: %s", newShard)

	err = ring.Add(ctx, newShard)
	if err != nil {
		t.Fatalf("Failed to add new shard %s: %v", newShard, err)
	}

	// Check how users are redistributed
	newUserShardMapping := make(map[string]string)
	newShardUserCount := make(map[string]int)
	migratedUsers := []string{}

	t.Log("\nUser mapping after adding new shard:")
	for _, userID := range userIDs {
		shard, err := ring.LocateKey(ctx, []byte(userID))
		if err != nil {
			t.Fatalf("Failed to locate shard for user %s after scaling: %v", userID, err)
		}
		newUserShardMapping[userID] = shard
		newShardUserCount[shard]++

		if userShardMapping[userID] != shard {
			migratedUsers = append(migratedUsers, userID)
			t.Logf("  %s -> %s (MIGRATED from %s)", userID, shard, userShardMapping[userID])
		} else {
			t.Logf("  %s -> %s", userID, shard)
		}
	}

	t.Log("\nNew shard distribution:")
	for shard, count := range newShardUserCount {
		percentage := float64(count) / float64(len(userIDs)) * 100
		t.Logf("  %s: %d users (%.1f%%)", shard, count, percentage)
	}

	migrationPercentage := float64(len(migratedUsers)) / float64(len(userIDs)) * 100
	t.Logf("\nMigration summary: %d users migrated (%.1f%%)", len(migratedUsers), migrationPercentage)

	// Verify migration is reasonable (should be around 25% when adding 1 shard to 3)
	if migrationPercentage < 15 || migrationPercentage > 35 {
		t.Errorf("Unexpected migration percentage: %.1f%% (expected 15-35%%)", migrationPercentage)
	}

	// Verify new shard got some users
	if newShardUserCount[newShard] == 0 {
		t.Error("New shard received no users")
	}

	// Test replica placement for high availability
	t.Log("\nTesting replica placement for high availability:")
	for _, userID := range userIDs[:5] { // Test first 5 users
		replicas, err := ring.LocateReplicas(ctx, []byte(userID), 2)
		if err != nil {
			t.Fatalf("Failed to locate replicas for user %s: %v", userID, err)
		}

		if len(replicas) != 2 {
			t.Errorf("Expected 2 replicas for user %s, got %d", userID, len(replicas))
		}

		if replicas[0] == replicas[1] {
			t.Errorf("Duplicate replicas for user %s: %v", userID, replicas)
		}

		t.Logf("  %s: primary=%s, backup=%s", userID, replicas[0], replicas[1])
	}
}

// TestCacheClusterScenario simulates a distributed cache scenario
func TestCacheClusterScenario(t *testing.T) {
	ctx := context.Background()

	// Simulate cache nodes
	cacheNodes := []string{
		"cache-01.internal",
		"cache-02.internal",
		"cache-03.internal",
	}

	config := consistent.Config{
		PartitionCount:    100,
		ReplicationFactor: 150,
		Load:              1.5,
	}

	ring, err := consistent.NewWithMembers(cacheNodes, config)
	if err != nil {
		t.Fatalf("Failed to create cache ring: %v", err)
	}

	// Simulate cache keys
	cacheKeys := []string{
		"session:user:12345",
		"product:details:67890",
		"cart:items:user:11111",
		"user:profile:22222",
		"search:results:electronics",
		"category:products:books",
		"trending:items:today",
		"recommendations:user:33333",
		"inventory:product:44444",
		"pricing:product:55555",
	}

	// Test initial distribution
	keyNodeMapping := make(map[string]string)
	nodeKeyCount := make(map[string]int)

	t.Log("Cache key distribution:")
	for _, key := range cacheKeys {
		node, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate cache node for key %s: %v", key, err)
		}
		keyNodeMapping[key] = node
		nodeKeyCount[node]++
		t.Logf("  %s -> %s", key, node)
	}

	t.Log("\nNode load distribution:")
	for node, count := range nodeKeyCount {
		percentage := float64(count) / float64(len(cacheKeys)) * 100
		t.Logf("  %s: %d keys (%.1f%%)", node, count, percentage)
	}

	// Simulate node failure and recovery
	failedNode := "cache-02.internal"
	t.Logf("\nSimulating failure of node: %s", failedNode)

	err = ring.Remove(ctx, failedNode)
	if err != nil {
		t.Fatalf("Failed to remove failed node %s: %v", failedNode, err)
	}

	// Check redistribution after failure
	failoverMapping := make(map[string]string)
	failoverNodeCount := make(map[string]int)
	affectedKeys := []string{}

	t.Log("\nKey distribution after node failure:")
	for _, key := range cacheKeys {
		node, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate cache node for key %s after failure: %v", key, err)
		}
		failoverMapping[key] = node
		failoverNodeCount[node]++

		if keyNodeMapping[key] != node {
			affectedKeys = append(affectedKeys, key)
			t.Logf("  %s -> %s (MOVED from %s)", key, node, keyNodeMapping[key])
		} else {
			t.Logf("  %s -> %s", key, node)
		}
	}

	t.Log("\nNode load after failure:")
	for node, count := range failoverNodeCount {
		percentage := float64(count) / float64(len(cacheKeys)) * 100
		t.Logf("  %s: %d keys (%.1f%%)", node, count, percentage)
	}

	affectedPercentage := float64(len(affectedKeys)) / float64(len(cacheKeys)) * 100
	t.Logf("\nCache invalidation: %d keys affected (%.1f%%)", len(affectedKeys), affectedPercentage)

	// Verify no keys are assigned to failed node
	for key, node := range failoverMapping {
		if node == failedNode {
			t.Errorf("Key %s still assigned to failed node %s", key, failedNode)
		}
	}

	// Simulate adding replacement node
	replacementNode := "cache-04.internal"
	t.Logf("\nAdding replacement node: %s", replacementNode)

	err = ring.Add(ctx, replacementNode)
	if err != nil {
		t.Fatalf("Failed to add replacement node %s: %v", replacementNode, err)
	}

	// Check final distribution
	finalNodeCount := make(map[string]int)
	t.Log("\nFinal key distribution:")
	for _, key := range cacheKeys {
		node, err := ring.LocateKey(ctx, []byte(key))
		if err != nil {
			t.Fatalf("Failed to locate cache node for key %s after recovery: %v", key, err)
		}
		finalNodeCount[node]++
		t.Logf("  %s -> %s", key, node)
	}

	t.Log("\nFinal node load distribution:")
	for node, count := range finalNodeCount {
		percentage := float64(count) / float64(len(cacheKeys)) * 100
		t.Logf("  %s: %d keys (%.1f%%)", node, count, percentage)
	}

	// Verify replacement node got some keys
	if finalNodeCount[replacementNode] == 0 {
		t.Error("Replacement node received no keys")
	}
}

// TestLoadBalancerScenario simulates a load balancer scenario
func TestLoadBalancerScenario(t *testing.T) {
	ctx := context.Background()

	// Simulate backend servers
	servers := []string{
		"api-server-1.prod",
		"api-server-2.prod",
		"api-server-3.prod",
		"api-server-4.prod",
	}

	config := consistent.Config{
		PartitionCount:    200,
		ReplicationFactor: 250,
		Load:              1.2,
	}

	lb, err := consistent.NewWithMembers(servers, config)
	if err != nil {
		t.Fatalf("Failed to create load balancer ring: %v", err)
	}

	// Simulate client requests with session affinity
	clientSessions := []string{
		"session_abc123", "session_def456", "session_ghi789",
		"session_jkl012", "session_mno345", "session_pqr678",
		"session_stu901", "session_vwx234", "session_yza567",
		"session_bcd890", "session_efg123", "session_hij456",
	}

	sessionServerMapping := make(map[string]string)
	serverSessionCount := make(map[string]int)

	t.Log("Session-to-server mapping (session affinity):")
	for _, session := range clientSessions {
		server, err := lb.LocateKey(ctx, []byte(session))
		if err != nil {
			t.Fatalf("Failed to locate server for session %s: %v", session, err)
		}
		sessionServerMapping[session] = server
		serverSessionCount[server]++
		t.Logf("  %s -> %s", session, server)
	}

	t.Log("\nServer load distribution:")
	for server, count := range serverSessionCount {
		percentage := float64(count) / float64(len(clientSessions)) * 100
		t.Logf("  %s: %d sessions (%.1f%%)", server, count, percentage)
	}

	// Simulate server maintenance (graceful removal)
	maintenanceServer := "api-server-2.prod"
	t.Logf("\nTaking server %s offline for maintenance", maintenanceServer)

	err = lb.Remove(ctx, maintenanceServer)
	if err != nil {
		t.Fatalf("Failed to remove server %s for maintenance: %v", maintenanceServer, err)
	}

	// Check session redistribution
	redistributedSessions := []string{}
	newServerSessionCount := make(map[string]int)

	t.Log("\nSession redistribution after server maintenance:")
	for _, session := range clientSessions {
		server, err := lb.LocateKey(ctx, []byte(session))
		if err != nil {
			t.Fatalf("Failed to locate server for session %s after maintenance: %v", session, err)
		}
		newServerSessionCount[server]++

		if sessionServerMapping[session] != server {
			redistributedSessions = append(redistributedSessions, session)
			t.Logf("  %s -> %s (MOVED from %s)", session, server, sessionServerMapping[session])
		} else {
			t.Logf("  %s -> %s", session, server)
		}
	}

	redistributionPercentage := float64(len(redistributedSessions)) / float64(len(clientSessions)) * 100
	t.Logf("\nSession redistribution: %d sessions moved (%.1f%%)", len(redistributedSessions), redistributionPercentage)

	// Verify maintenance server has no sessions
	if newServerSessionCount[maintenanceServer] > 0 {
		t.Errorf("Maintenance server %s still has %d sessions", maintenanceServer, newServerSessionCount[maintenanceServer])
	}

	// Test backup server selection for high availability
	t.Log("\nTesting backup server selection:")
	for _, session := range clientSessions[:5] {
		backups, err := lb.LocateReplicas(ctx, []byte(session), 2)
		if err != nil {
			t.Fatalf("Failed to locate backup servers for session %s: %v", session, err)
		}

		if len(backups) != 2 {
			t.Errorf("Expected 2 servers for session %s, got %d", session, len(backups))
		}

		t.Logf("  %s: primary=%s, backup=%s", session, backups[0], backups[1])
	}
}
