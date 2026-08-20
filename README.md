<div align="center">

# consistent-go

**Bounded-load consistent hashing for Go, built around stable partitions and incremental rebalancing.**

[![Go Reference](https://pkg.go.dev/badge/github.com/focusandinsist/consistent-go/consistent.svg)](https://pkg.go.dev/github.com/focusandinsist/consistent-go/consistent)
[![Go Version](https://img.shields.io/badge/Go-1.21%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License](https://img.shields.io/github/license/focusandinsist/consistent-go)](LICENSE)
[![GitHub Stars](https://img.shields.io/github/stars/focusandinsist/consistent-go?style=flat&logo=github)](https://github.com/focusandinsist/consistent-go/stargazers)

[English](README.md) | [简体中文](README_zh-CN.md)

</div>

`consistent-go` is a small, concurrent-safe routing library for sharded databases, distributed caches, sticky load balancing, and stable task assignment. It combines a virtual-node hash ring with a fixed partition table and a configurable load bound, so topology changes remain observable and migration-friendly.

## Why consistent-go?

- **Stable partitions**: keys map to a finite partition space before they map to members. You can inspect, count, and migrate partitions instead of discovering moved keys one by one.
- **Bounded load**: partition placement respects a configurable per-member ceiling, reducing hotspots caused by probabilistic ring placement.
- **Incremental rebalancing**: adding a member examines only the ranges influenced by its virtual nodes; removing one remaps only its owned partitions.
- **Fast lookup path**: key lookup is a 64-bit hash, a modulo operation, and a partition-table read. It does not walk the ring.
- **Concurrent-safe API**: public operations are protected for concurrent access and accept `context.Context` where cancellation matters.
- **Practical hashing defaults**: xxHash64 is the default, with MurmurHash3 and a small custom `Hasher` interface available.

## How it works

```text
                         virtual nodes
members ────────────────────────► hash ring
                                      │
                                      │ assigns ownership
                                      ▼
key ── xxHash64 ──► partition ID ──► partition table ──► member
     hash % count      [0, N)          bounded load
```

This extra partition layer is the important difference from a basic consistent-hash ring: topology changes produce a finite, inspectable set of partition moves that an application can turn into a migration plan.

## Quick start

Install the library:

```bash
go get github.com/focusandinsist/consistent-go/consistent
```

Create a ring and route a key:

```go
package main

import (
	"context"
	"fmt"
	"log"

	consistent "github.com/focusandinsist/consistent-go/consistent"
)

func main() {
	ctx := context.Background()

	ring, err := consistent.NewWithMembers(
		[]string{"cache-a", "cache-b", "cache-c"},
		consistent.Config{
			PartitionCount:    271,
			ReplicationFactor: 128,
			Load:              1.25,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	owner, err := ring.LocateKey(ctx, []byte("user:42"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("owner:", owner)

	if err := ring.Add(ctx, "cache-d"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("partition load:", ring.LoadDistribution(ctx))
}
```

Need to build the ring dynamically? Start empty and add members as your discovery layer reports them:

```go
ring, err := consistent.New(consistent.Config{})
if err != nil {
	log.Fatal(err)
}

_ = ring.Add(ctx, "node-a")
_ = ring.Add(ctx, "node-b")
_ = ring.Remove(ctx, "node-a")
```

## Core API

| Operation | Purpose |
| --- | --- |
| `New`, `NewWithMembers` | Create an empty or pre-populated ring |
| `Add`, `Remove` | Change membership and incrementally rebalance |
| `LocateKey` | Resolve a key to its current member |
| `FindPartitionID` | Resolve a key to a stable partition ID |
| `GetPartitionOwner` | Inspect the owner of a partition |
| `LocateReplicas` | Return unique neighboring member candidates |
| `LoadDistribution` | Inspect assigned partition counts by member |
| `AverageLoad` | Read the active per-member partition ceiling |

All zero-valued configuration fields use defaults:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `PartitionCount` | `271` | Number of stable partitions |
| `ReplicationFactor` | `20` | Virtual nodes per member |
| `Load` | `1.25` | Allowed load relative to the average |
| `Hasher` | xxHash64 | Hash implementation used by the ring |

Changing `PartitionCount` or `Hasher` changes key placement at scale. Treat either change as a data migration, not a routine live configuration update.

## Where it fits

| Use case | What the library provides |
| --- | --- |
| Database sharding | Stable partition IDs and enumerable ownership |
| Distributed caching | Deterministic key routing and limited remapping |
| Sticky load balancing | Stable backend selection across membership changes |
| Task assignment | Predictable ownership for tenants, jobs, or queues |
| Replica planning | Ordered, unique neighboring member candidates |

## How is it different?

| Approach | Stable partitions | Load bound | Arbitrary removal | Replica candidates | Incremental partition moves |
| --- | :---: | :---: | :---: | :---: | :---: |
| Basic virtual-node rings | No | No | Usually | Sometimes | Not enumerable |
| Jump consistent hash | No | No | No | No | Not enumerable |
| `consistent-go` | **Yes** | **Yes** | **Yes** | **Yes** | **Yes** |

The project deliberately focuses on local routing. It does **not** provide service discovery, failure detection, consensus, network replication, or data transfer. Those responsibilities belong to the system embedding the library.

## Testing

The repository uses separate modules for the library and its black-box scenario suite:

```bash
cd consistent
go test ./...

cd ../test
go test -short ./...
```

The scenario suite covers database sharding, cache failover, sticky load balancing, concurrent membership changes, and long-running stability. See the [testing guide](docs/testing-guide.md) for the full layout.

## Documentation

- [Architecture, trade-offs, and comparison](docs/project-overview-20260814.md)
- [Hash function selection](docs/hash-function-guide.md)
- [Testing guide](docs/testing-guide.md)

## Operational notes

- Every process must use the same members, member names, hasher, partition count, replication factor, and load value.
- Membership events must be distributed consistently by an external control plane; this package does not run consensus.
- `LoadDistribution` measures partitions, not bytes, requests, CPU, or the number of keys stored by each member.
- Use a high-quality, deterministic 64-bit hash. CRC-style checksums are not suitable for this ring's regular partition inputs.

## Contributing

Issues and pull requests are welcome. A useful contribution includes a focused test, a clear behavioral explanation, and benchmark evidence when it changes a hot path.

If this project saves you from rebuilding the same routing layer, consider [starring the repository](https://github.com/focusandinsist/consistent-go). It helps other Go developers find it.

## License

[MIT](LICENSE) © focusandinsist
