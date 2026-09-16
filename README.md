# Plexus: Next-Gen Distributed State Machine & Raft Graph Framework

[![Go Test](https://img.shields.io/badge/go-1.22%2B-blue)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Plexus** is a modern Go framework for building high-performance, single-process distributed applications—inspired by HashiCorp Vault, Consul, and embedded Raft architectures.

State machines, Raft consensus, peer discovery, cluster membership, and inter-node communication are all embedded directly into your application process. No external coordination services (ZooKeeper, etcd, etc.) required.

---

## Key Highlights

1. **Effortless State Machines (`Store`)**: Define distributed deterministic state machines with minimal boilerplate, generic type-safe command routing (`plexus.Handle[Req, Resp]`), pre/post hooks, and snapshot streaming.
2. **"Broken CAP" Sequential Local-Read Consistency**: When a mutation is proposed (`Apply`), Plexus waits until the entry is committed by quorum **and** applied to the *current node's* state machine. Subsequent reads from local memory (`ShouldReadLocally()`) are strictly consistent without needing Raft read-index network round-trips.
3. **High-Performance Direct-I/O Log Store (`SegmentLogStore`)**: Append-only 4 KiB block-aligned segment files (`000000000000001.seg`), in-memory sparse index for $O(1)$ random lookups, CRC32 corruption detection, and cross-platform buffered fallback.
4. **The Raft Graph (Command Topology)**: A breakthrough concept bringing streaming pipeline topologies (like Kafka Streams) directly to Raft state machines. Upstream state mutations deterministically emit downstream commands.
5. **Exactly-Once Semantics (EOS) via Deduplication**: Every downstream command in a Raft Graph is tagged with an immutable, deterministic idempotency key:
   $$\text{IdempotencyKey} = \text{fmt.Sprintf}("\%s:\%d:\%d:\%d", \text{UpstreamID}, \text{Term}, \text{Index}, \text{SeqID})$$
   Downstream state machines deduplicate via an embedded dedup engine (`dedup.Store`), guaranteeing that retries, network partitions, and leader re-elections never cause duplicate execution.

---

## Architecture Overview

```
+-----------------------------------------------------------------------------------+
|                                 Plexus Process                                    |
|                                                                                   |
|  +--------------------+       +---------------------+                             |
|  |     Store A        |       |      Store B        |  (Developer State Machines) |
|  | (e.g. KV / Dict)   |       | (e.g. Auth / Graph) |                             |
|  +---------+----------+       +----------+----------+                             |
|            |                             |                                        |
|            +--------------+--------------+                                        |
|                           v                                                       |
|           +-------------------------------+                                       |
|           |     Multi-Store Raft FSM      |                                       |
|           +---------------+---------------+                                       |
|                           |                                                       |
|           +---------------+---------------+                                       |
|           |         Raft Machine          |  (Quorum + Local Apply Sync)          |
|           +---------------+---------------+                                       |
|                           |                                                       |
|       +-------------------+-------------------+                                   |
|       v                                       v                                   |
| +-----------+                           +-----------+                             |
| | Log Store | (Direct I/O Segments)     | Transport | (Embedded TLS/Net RPC)      |
| +-----------+                           +-----------+                             |
+-----------------------------------------------------------------------------------+
```

---

## Quickstart: Embedded Distributed KV Store

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/avivklas/plexus"
	"github.com/avivklas/plexus/pkg/stores/kv"
)

func main() {
	ctx := context.Background()

	// 1. Configure single-process cluster coordinator
	cfg := plexus.DefaultClusterConfig("node-1", "127.0.0.1:9090", "/var/data/plexus")
	cfg.Bootstrap = true // Set true for initial cluster seed

	cluster, err := plexus.NewCluster(cfg)
	if err != nil {
		log.Fatalf("failed to initialize cluster: %v", err)
	}

	// 2. Instantiate and register a Store (effortless UseState API)
	kvStore := kv.New()
	mutator := cluster.UseState(kvStore)
	kvStore.AttachMutator(mutator)

	// 3. Start cluster & embedded consensus
	if err := cluster.Start(ctx); err != nil {
		log.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	// 4. Mutate with Quorum + Local Apply consistency
	if err := kvStore.Set(ctx, "cluster-status", "operational"); err != nil {
		log.Fatalf("mutation failed: %v", err)
	}

	// 5. Read immediately from local memory with guaranteed consistency!
	val, ok := kvStore.Get("cluster-status")
	fmt.Printf("Read locally: %s (found: %v)\n", val, ok)
}
```

---

## The Raft Graph & Exactly-Once Semantics

Connect multiple Raft consensus machines into an upstream $\to$ downstream command topology with end-to-end exactly-once guarantees:

```go
// 1. Create a Raft Graph
g := plexus.NewGraph("ecommerce-pipeline")
defer g.Close()

// 2. Add machines with embedded deduplication
g.AddMachine(ordersMachine, nil)
g.AddMachine(billingMachine, plexus.NewMemoryDedup())
g.EnableDedupOnStore(billingMachine, billingStore)

// 3. Wire the pipeline
g.Pipe(ordersMachine, billingMachine, func(ctx context.Context, cmd *plexus.Command, res any) ([]*plexus.Command, error) {
	var order struct {
		ID     string `json:"id"`
		Amount int    `json:"amount"`
	}
	_ = cmd.Decode(&order)

	// Generate downstream billing invoice command
	downstreamCmd, _ := plexus.NewCommand("billing.invoice", struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
	}{
		OrderID: order.ID,
		Amount:  order.Amount,
	})

	return []*plexus.Command{downstreamCmd}, nil
}, "order.create")
```

Whenever `ordersMachine` commits an `order.create` mutation:
1. The upstream leader automatically triggers the transformer.
2. The command is assigned an idempotency key: `orders-cluster:<term>:<index>:<seq>`.
3. The downstream machine checks its embedded `dedup.Store`.
4. If a network retry or leader failover resends the command, downstream recognizes the key and skips re-execution.

---

## Testing & Verification

Run the full suite with race detector:

```bash
go test -v -race ./...
```
