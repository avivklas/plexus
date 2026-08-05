# Benchmark: Does the Plexus Log Store Design Actually Win?

This report tests the performance claims in [`specs/raft_log_store.md`](../specs/raft_log_store.md)
against implemented alternatives. It was written to be able to falsify those
claims, and one of them largely did not survive.

**Short version.** Three of the four claims hold with large, unambiguous effect
sizes. The fourth — that `O_DIRECT` beats buffered I/O — is real but small
(about 8% median latency, no throughput difference at all), and it comes with a
severe read-path caveat that the naive implementation gets catastrophically
wrong. The headline win of this design is *append-only segments over a B-tree*,
not direct I/O.

---

## 1. What Is Being Compared

Four claims, stated separately because their evidence differs enormously.

| # | Claim | Verdict | Effect size |
| --- | --- | --- | --- |
| A | Append-only segments beat a B-tree (BoltDB) for a Raft log | **Confirmed** | ~3x throughput, 5.5x less write amplification, 6,200x faster truncation |
| B | Group commit amortizes durability cost | **Confirmed** | 23.5x throughput, 14x less write amplification |
| C | Logical tail truncation beats physically zeroing the region | **Confirmed** | 106x faster, 28.2 MB vs 0 bytes written |
| D | `O_DIRECT` beats buffered I/O + `fdatasync` | **Weak / partly refuted** | ~8% p50 latency; no significant throughput or device-byte difference |

## 2. Arms

| Arm | Mechanism | Represents |
| --- | --- | --- |
| `direct` | Append-only segments, `O_DIRECT\|O_DSYNC`, 4 KiB aligned user-space paging, tail-block protocol, 1 MiB read-ahead | The spec |
| `buffered` | Identical segments and framing, ordinary buffered writes + `fdatasync` | etcd, `hashicorp/raft-wal` |
| `bolt` | `go.etcd.io/bbolt` B-tree, one key per log index, one transaction per batch | `hashicorp/raft-boltdb` |
| `direct-zero` | Like `direct`, but truncation physically overwrites the dropped region with zeros | The strategy §6.1 rejects |
| `direct-nora` | Like `direct`, read-ahead disabled | Naive `O_DIRECT` |

### Fairness controls

These matter more than the numbers, because a storage benchmark is trivially
riggable by weakening one arm's durability promise.

1. **Identical durability contract.** Every arm's `Append` is durable on return.
   `direct` uses `O_DIRECT|O_DSYNC`; `buffered` issues `fdatasync`; `bolt` runs
   with `NoSync=false` so bbolt fsyncs on commit. No arm returns early.
2. **Shared code above the paging layer.** `direct` and `buffered` share the
   entry codec, in-memory index, segment rotation, recovery scan and truncation
   logic verbatim. They differ only in the `pager` implementation, so any delta
   between them is attributable to the paging mechanism and nothing else.
3. **Identical framing in all three arms.** `bolt` stores the same 26-byte
   header + CRC32 encoding as its value, so the comparison is not distorted by
   a different serialization format.
4. **Kernel-accounted I/O.** Bytes and syscalls come from `/proc/self/io`, not
   from each arm's own counters, so an arm cannot flatter itself.
5. **Correctness verified on every run.** After each measured run, entries are
   read back and compared byte for byte against what was written. All 365
   recorded runs passed. An arm cannot win by not storing the data.
6. **Cold page cache.** `/proc/sys/vm/drop_caches` is written before each
   measured phase (the container runs `--privileged`).
7. **Pre-allocation everywhere.** Both segment arms `fallocate` their segments,
   so neither is charged for filesystem metadata growth the other avoids.

## 3. Environment and Its Limits

| | |
| --- | --- |
| Host | Apple M5 Pro, 24 GB, Apple Fabric NVMe |
| Guest | Colima (Apple Virtualization.framework), Linux 6.8.0 arm64, 4 vCPU, 7.9 GB |
| Filesystem | overlay2 on ext4, 512-byte logical block, 4 KiB I/O granularity |
| Go | 1.24.13 linux/arm64 |
| Repetitions | 7 for the wide sweep, 15 paired for the A/B; medians reported |

**The environment was validated before any measurement was taken**, because a
virtualized disk that silently discards flushes would make every durability
number fiction:

- `O_DIRECT` is genuinely honored: an unaligned write fails with `EINVAL`, so
  the kernel is enforcing the flag rather than ignoring it.
- `fsync` costs 377µs at p50 — not a no-op, so flushes are reaching the host.

**Limits to keep in mind.** Absolute latencies are inflated by virtualization;
bare-metal NVMe would be roughly 20-80µs for a flush rather than ~350µs. That
compresses the *relative* advantage of anything that saves CPU or syscalls,
and inflates the advantage of anything that saves a device round trip. The VM
is also noisy: run-to-run spread on the wide sweep reached 250%, which is why
claim D is settled with a paired experiment instead. Structural results (bytes
written, syscall counts, truncation cost) are environment-independent and
should reproduce anywhere. Latency results are indicative.

## 4. Claim A — Append-only segments vs B-tree

256-byte entries. `wamp` is write amplification: device bytes divided by useful
framed bytes.

| batch | arm | p50 µs | entries/s | bytes/entry | wamp |
| --- | --- | --- | --- | --- | --- |
| 1 | bolt | 228.9 | 3,346 | 24,144.6 | 85.6 |
| 1 | buffered | 80.3 | 9,599 | 4,377.3 | 15.5 |
| 1 | **direct** | **79.1** | **9,882** | **4,377.3** | **15.5** |
| 8 | bolt | 908.8 | 8,755 | 3,736.3 | 13.3 |
| 8 | **direct** | **227.6** | **26,185** | **792.3** | **2.8** |
| 128 | bolt | 1,022.9 | 117,523 | 882.4 | 3.1 |
| 128 | **direct** | **367.6** | **232,041** | **312.0** | **1.1** |

At batch=8 the segment design is **3.0x the throughput** and writes **4.7x
fewer bytes per entry**. The gap is widest exactly where a Raft leader lives:
small entries, modest batches. Even at batch=128, where bbolt amortizes its
page writes best, the segment arms still write 2.8x fewer bytes.

The write-amplification result is the durable one. A B-tree must write whole
dirty pages, so a 282-byte framed entry drags 4 KiB pages along with it; at
batch=1 bbolt writes 24 KB of device traffic per 282-byte entry. That is a
property of the data structure, not of this hardware.

## 5. Claim B — Group commit

`direct` arm, 256-byte entries:

| batch | p50 µs | entries/s | bytes/entry | wamp |
| --- | --- | --- | --- | --- |
| 1 | 79.1 | 9,882 | 4,377.3 | 15.52 |
| 8 | 227.6 | 26,185 | 792.3 | 2.81 |
| 32 | 366.8 | 60,215 | 408.1 | 1.45 |
| 128 | 367.6 | 232,041 | 312.0 | 1.11 |

**23.5x throughput and 14x less write amplification** from batch 1 to 128, at
the cost of 4.6x commit latency. Note that latency barely moves from batch 32
to 128 (366.8 → 367.6µs) while throughput nearly quadruples: past batch=32 the
commit is dominated by the device round trip, so additional batching is very
nearly free. That is the strongest single argument in the whole report for
making the group-commit window configurable, and it supports the spec's open
question about adaptive sizing.

## 6. Claim C — Logical vs physical truncation

Dropping 100,000 of 200,000 entries:

| arm | duration | device bytes written |
| --- | --- | --- |
| `buffered` (logical) | 0.9 µs | 0 |
| `direct` (logical) | 112.2 µs | 0 |
| `direct-zero` (physical) | 11,878.7 µs | 28,205,056 |
| `bolt` | 692,639.7 µs | 159,744 |

Logical truncation is **106x faster than zeroing** and writes nothing at all,
confirming §6.1. The 28.2 MB that `direct-zero` writes is pure waste: every one
of those bytes is overwritten by the next valid append.

`direct` costs 112µs against `buffered`'s 0.9µs because `O_DIRECT` must reload
the block containing the new tail before it can rewrite it (§6.1.5), where
buffered I/O lets the kernel handle the partial page. Both are noise next to
bolt.

**bolt is 6,200x slower than `direct`** at 693 ms, and this is the most
operationally alarming number in the report — a follower that gets truncated
stalls for two thirds of a second. Note that bolt writes only 160 KB doing it:
the cost is not I/O but the 100,000 individual B-tree deletes and the resulting
free-space bookkeeping. This is precisely the problem `hashicorp/raft-wal` was
built to escape.

## 7. Claim D — `O_DIRECT` vs buffered I/O

This is the claim the report was least able to support, and the wide sweep's
noise could not resolve it. It was therefore re-run as a **paired A/B**: both
arms run back to back within each repetition and their order alternates, so
slow VM drift affects both members of a pair equally. 15 pairs per cell, with a
bootstrap 95% confidence interval on the median paired ratio.

### Commit latency, p50 (lower is better)

| entry | batch | direct | buffered | ratio | 95% CI | direct wins | verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 256 | 1 | 74.8 | 75.1 | 1.019 | [0.978, 1.070] | 10/15 | no significant difference |
| 256 | 8 | 209.5 | 228.9 | 1.088 | [1.077, 1.099] | 13/15 | direct faster |
| 256 | 128 | 330.4 | 370.2 | 1.088 | [1.044, 1.135] | 13/15 | direct faster |
| 4096 | 8 | 335.2 | 362.9 | 1.080 | [1.028, 1.102] | 12/15 | direct faster |

### Throughput (higher is better)

| entry | batch | direct | buffered | ratio | 95% CI | verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 256 | 1 | 10,488 | 10,463 | 0.996 | [0.894, 1.028] | no significant difference |
| 256 | 8 | 29,310 | 28,005 | 0.981 | [0.882, 1.008] | no significant difference |
| 256 | 128 | 247,844 | 231,164 | 0.956 | [0.876, 1.025] | no significant difference |
| 4096 | 8 | 15,579 | 15,271 | 0.981 | [0.925, 1.092] | no significant difference |

### What the data actually says

**`O_DIRECT` is about 8% faster at median commit latency once batching is on,
and that is the whole win.** The confidence intervals exclude 1.0 for three of
four cells, so the effect is real rather than noise, but it is small.

Throughput shows **no significant difference in any cell** — every interval
straddles 1.0. p99 is a wash except at batch=128.

Two measurements explain the 8% and are more informative than the timing:

**Device bytes are exactly identical.**

| entry | batch | direct B/entry | buffered B/entry | ratio |
| --- | --- | --- | --- | --- |
| 256 | 1 | 4,377.3 | 4,377.3 | 1.000 |
| 256 | 8 | 792.3 | 792.3 | 1.000 |
| 256 | 128 | 312.0 | 312.0 | 1.000 |
| 4096 | 8 | 4,632.1 | 4,632.1 | 1.000 |

This retires a worry the spec raised about itself. The tail-block protocol pads
partial blocks with zeros, and that padding looks expensive in user-space
accounting — 46.8% of bytes staged at batch=1, falling to 4.8% at batch=128:

| entry | batch | app bytes | padding | padding % |
| --- | --- | --- | --- | --- |
| 256 | 1 | 13,131,776 | 6,146,792 | 46.8% |
| 256 | 8 | 12,677,120 | 4,082,304 | 32.2% |
| 256 | 32 | 13,058,048 | 2,019,584 | 15.5% |
| 256 | 128 | 19,968,000 | 962,048 | 4.8% |
| 4096 | 8 | 37,056,512 | 2,033,472 | 5.5% |
| 32768 | 8 | 133,230,592 | 1,025,240 | 0.8% |

But that padding is **not a cost relative to buffered I/O**, because the kernel
writes whole 4 KiB pages anyway. The ratio of 1.000 across every cell shows the
buffered arm pays exactly the same device traffic; it simply does not see it in
user space. Block granularity is a property of the device, not of `O_DIRECT`.
So `padding_bytes_written_total` (spec R4.4.6) is a useful signal for tuning the
commit window, but it should not be read as waste that buffered I/O avoids.

**`O_DIRECT` halves the syscalls per commit.**

| arm | syscalls per commit |
| --- | --- |
| `direct` | 1 write, 0 sync |
| `buffered` | 1 write, 1 sync |

`O_DSYNC` folds durability into the write, so the commit path makes one syscall
instead of two. That is the mechanism behind the ~8% median latency edge, and
it is a CPU-side saving. It would likely be a *larger* relative win on
bare-metal NVMe, where the device round trip is 20-80µs rather than ~350µs and
syscall overhead is a bigger share of the total.

### The read path is where `O_DIRECT` can lose badly

50,000 cold-cache point reads:

| arm | p50 µs | entries/s | device bytes read |
| --- | --- | --- | --- |
| `direct` (1 MiB read-ahead) | 0.08 | 4,232,789 | 14,155,776 |
| `buffered` | 0.33 | 1,999,675 | 14,102,528 |
| `bolt` | 0.50 | 156,337 | 34,148,352 |
| `direct-nora` (naive) | 31.21 | 30,455 | 218,779,648 |

Naive `O_DIRECT` is **390x slower** than the read-ahead version and reads
**15.5x more from the device**, because every point read pulls a full 4 KiB
block for a 282-byte entry and nothing is cached. This is a genuine trap: an
implementation that follows the spec's write path but skips §4.5.5 will be
slower than BoltDB at reads by a factor of 5.

With read-ahead, `direct` is the fastest arm — 4.1x lower p50 and 2.1x the
throughput of buffered — because a hit in the user-space window costs no
syscall at all where the buffered arm still issues a `pread` for every entry.
But note what that means: **you have re-implemented the page
cache in user space.** The memory saved by not double-buffering is spent again
on the read-ahead window. The honest framing is that `O_DIRECT` moves control
of caching into the application rather than eliminating it.

### Recovery is a real cost of the design

Rebuilding state after restart, 200,000 entries:

| arm | duration | device bytes read |
| --- | --- | --- |
| `bolt` | 1.8 ms | 143,360 |
| `buffered` | 53.4 ms | 56,496,128 |
| `direct` | 57.7 ms | 56,721,408 |

`bolt` is **32x faster** because a B-tree *is* a persistent index: opening the
database costs two page reads. The segment design has no on-disk index by
choice (spec R5.4) and must scan 56 MB to rebuild one. 58 ms for 200k entries
extrapolates to roughly 2.9 seconds for 10 million — recoverable, but no longer
negligible, and it argues for the index checkpoint the spec left as open
question #4.

## 8. What Changed in the Spec

Three changes are warranted by this data:

1. **§4.5.5 read-ahead is upgraded from SHOULD to MUST.** A 390x penalty is not
   an optimization, it is a correctness-of-design issue.
2. **§4.4.6 padding metric is reframed.** It is a tuning signal for the commit
   window, not a measure of waste relative to buffered I/O.
3. **Open question #4 (index checkpointing) gains a concrete threshold** from
   the recovery measurement.

The `O_DIRECT` decision itself stands, but for honestly stated reasons: bounded
and application-controlled memory, one syscall per commit instead of two, and
no dependence on kernel writeback timing — *not* a large throughput win, which
the data does not support.

## 9. Reproducing

```bash
cd bench
docker build -t plexus-bench .

# Wide sweep: all arms, all workloads, 7 repetitions.
mkdir -p results
docker run --rm --privileged -v "$PWD/results":/out \
  -e OUT=/out/results.jsonl -e REPS=7 plexus-bench
python3 analyze.py results/results.jsonl

# Paired A/B for the O_DIRECT vs buffered question, 15 pairs.
docker run --rm --privileged -v "$PWD/results":/out \
  -e OUT=/out/ab.jsonl -e REPS=15 \
  --entrypoint /usr/local/bin/ab.sh plexus-bench
python3 ab.py results/ab.jsonl

# Environment validation (O_DIRECT honored? fsync real?).
docker run --rm --privileged --entrypoint /bin/sh plexus-bench \
  -c 'cd /src && go run _probe/probe.go /data'
```

`--privileged` is required only so the harness can drop the page cache between
phases. Without it the read and recovery numbers will flatter the buffered arm.

Raw results for the run described here are in `results/results.jsonl`
(245 rows) and `results/ab.jsonl` (120 rows). Every row has `"verified": true`.
