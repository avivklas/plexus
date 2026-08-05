# Specification: Direct-I/O Raft Log Store

Status: Draft
Language: Go
Target platform: Linux (x86_64 / arm64), NVMe SSD
Scope: The persistent log storage engine backing a Raft consensus implementation.

This document is the authoritative design specification for the Plexus Raft log store. It
is written to be consumed both by humans and by autonomous coding agents: rules are stated
as normative requirements (MUST / SHOULD / MAY), and every subsystem ends with
Behavior-Driven Development scenarios that define acceptance.

---

## 1. Problem Statement and Goals

### 1.1 Problem

A Raft log store has an access pattern that general-purpose storage engines serve badly:

- Writes are **strictly append-only** at a monotonically increasing index.
- Writes are **latency-critical** — a leader cannot acknowledge a client until the entry is
  durable on a quorum, so every append sits on the critical path of the whole cluster.
- Reads are overwhelmingly **sequential** (follower catch-up, restart recovery), with
  occasional random point reads (a lagging follower asking for one specific index).
- Deletes happen at **two extremes only**: the head (compaction after a snapshot) and the
  tail (truncation of uncommitted entries when a new leader overwrites them).

Page-oriented, in-place-update storage (B-trees, slotted pages, BoltDB) is a poor fit. It
rewrites whole disk blocks to record a small change, pays free-space-tracking costs that make
appends slower *after* a large truncation, and buys random-access capabilities the workload
never uses.

### 1.2 Goals

1. **G1 — Sequential-only physical write pattern.** Every steady-state write is an append at
   an increasing file offset.
2. **G2 — Minimal write amplification.** Bytes written to the device per entry ≈ payload +
   fixed header + block padding. No read-modify-write of unrelated data.
3. **G3 — Predictable, bounded commit latency.** Group commit amortizes durability cost
   across concurrent proposals without letting any single proposal wait unboundedly.
4. **G4 — Deterministic durability.** When an append returns, the data is on stable media —
   not in the OS page cache, and not in a volatile device cache.
5. **G5 — Crash safety with detection, not hope.** Any torn, partial, or stale record is
   detectable at recovery time and never returned to the Raft layer as valid.
6. **G6 — O(1) index → offset resolution** for random reads, with memory proportional to the
   number of live entries, not their size.
7. **G7 — Truncation is cheap.** Both head compaction and tail truncation are metadata
   operations; neither writes proportional-to-data bytes.

### 1.3 Non-Goals

- Cross-platform parity. Direct I/O is a Linux-first design; macOS and Windows are supported
  only as a degraded buffered-I/O fallback for development (see §9).
- Serving as a general key-value store. The only random-access key is the Raft log index.
- Multi-writer concurrency. Exactly one process, and exactly one writer goroutine, owns a
  log directory at a time.
- Replication, elections, or snapshot content. Those live in the Raft layer above; this
  store only persists what it is handed.

---

## 2. Chosen Mechanism (Decision Record)

**Decision:** Use **append-only sequential segment files with user-space 4 KiB block paging
over `O_DIRECT`**, plus an in-memory sparse index mapping log index → physical location.

The append-only-segments half of this decision is strongly supported by measurement: ~3x the
throughput of a B-tree, 5.5x less write amplification, and truncation 6,200x faster. The
`O_DIRECT` half is a much closer call. Both are quantified in
[bench/RESULTS.md](../bench/RESULTS.md).

**Rejected alternatives:**

| Alternative | Why rejected |
| --- | --- |
| B-tree / BoltDB (as in classic `hashicorp/raft-boltdb`) | Random writes, high write amplification, free-space fragmentation causes appends to degrade after large truncations. |
| `mmap` over the log file | Write-back timing is controlled by the kernel, not the application; `msync` granularity and page-fault stalls make commit latency unpredictable; SIGBUS on I/O error is hard to handle safely. |
| Buffered I/O + `fsync` | Correct and used by mature engines (etcd, `hashicorp/raft-wal`). Chosen against for bounded application-controlled memory, one syscall per commit instead of two, and independence from kernel writeback timing — **not** for raw speed. Measured, `O_DIRECT` is only ~8% faster at median commit latency and shows no significant throughput advantage at all. This is the weakest decision in the document; see [bench/RESULTS.md](../bench/RESULTS.md) §7. |
| Single unbounded log file | Head compaction after a snapshot would require rewriting or hole-punching; segment `unlink` is strictly simpler. |

**Consequence of choosing `O_DIRECT`:** the kernel stops doing alignment and buffering on our
behalf. The engine MUST therefore implement its own paging layer. All three of the following
must be 4096-byte aligned on every I/O call: the file offset, the transfer length, and the
memory address of the buffer. Violating any one of them fails the syscall with `EINVAL`.

---

## 3. On-Disk Layout

### 3.1 Directory Structure

```
<data-dir>/
  000000000000001.seg      # segment files, named by their base (first) log index
  000000000000542.seg
  000000000001337.seg      # active (tail) segment
  meta                     # Raft StableStore: current term, voted-for, etc.
```

**Requirements**

- R3.1.1 Segment file names MUST be the segment's base log index, zero-padded to 15 decimal
  digits, with the `.seg` suffix. Lexicographic filename order therefore equals log order.
- R3.1.2 Exactly one segment is *active* (open for append) at any time: the one with the
  highest base index.
- R3.1.3 Segment target size MUST be configurable, default **64 MiB**, and MUST be a multiple
  of the block size.
- R3.1.4 A segment MUST be created with `fallocate(2)` at full target size before first use.
  This gives contiguous physical extents, removes filesystem metadata updates from the append
  path, and makes `O_DIRECT` write latency predictable.
- R3.1.5 A new segment MUST be pre-allocated in the background *before* the active one fills,
  so that segment rotation never blocks a commit on `fallocate`.
- R3.1.6 An entry MUST NOT span two segment files. If the encoded entry does not fit in the
  remaining space of the active segment, the segment is sealed and the entry begins the next
  one.

### 3.2 Segment Header

The first block (bytes `[0, 4096)`) of every segment is a header block, so all entry data
starts at a 4 KiB boundary.

| Offset | Size | Field | Description |
| --- | --- | --- | --- |
| 0 | 4 | Magic | `0x504C5853` (`"PLXS"`) |
| 4 | 2 | Version | Format version, currently `1` |
| 6 | 2 | BlockSize | Block size in bytes, as a power-of-two exponent (`12` = 4096) |
| 8 | 8 | BaseIndex | Log index of the first entry in this segment |
| 16 | 8 | CreatedAtUnixNano | Wall-clock creation time, diagnostic only |
| 24 | 4 | CRC32 | IEEE CRC over bytes `[0, 24)` |
| 28 | 4068 | Reserved | MUST be zero |

- R3.2.1 The header block MUST be written and made durable before any entry is appended to
  the segment.
- R3.2.2 On open, a segment whose header magic, version, or CRC does not validate MUST be
  treated as unreadable and MUST cause startup to fail loudly rather than be silently skipped.

### 3.3 Entry Binary Layout

Every entry carries a fixed 26-byte header followed by its opaque payload.

```
+---------------+---------------+---------------+---------------+---------------+
| Magic  (2 B)  | Term   (8 B)  | Index  (8 B)  | Size   (4 B)  | CRC32  (4 B)  |
+---------------+---------------+---------------+---------------+---------------+
|<------------------------ Fixed Header (26 bytes) ---------------------------->|
+-------------------------------------------------------------------------------+
| Payload (Size bytes)                                                           |
+-------------------------------------------------------------------------------+
```

| Field | Size | Encoding | Purpose |
| --- | --- | --- | --- |
| Magic | 2 | big-endian `0x4A46` (`"JF"`) | Cheap check that an offset is a plausible entry boundary during recovery. |
| Term | 8 | big-endian uint64 | Raft term. Persisted so the index can be rebuilt without consulting any sidecar. |
| Index | 8 | big-endian uint64 | Raft log index. Also lets recovery detect stale post-truncation data (see §7.3). |
| Size | 4 | big-endian uint32 | Payload length in bytes. |
| CRC32 | 4 | big-endian uint32, IEEE polynomial | Checksum over header bytes `[0, 22)` concatenated with the payload. |

**Requirements**

- R3.3.1 All multi-byte integers MUST be big-endian, so a hex dump of a segment is readable
  in log order.
- R3.3.2 The CRC MUST cover the magic, term, index, and size fields as well as the payload.
  A checksum that covers only the payload cannot detect a corrupted length field, which is
  the field that determines how far the recovery scanner advances.
- R3.3.3 `Size` MUST be validated against the remaining bytes in the segment *before* it is
  used to slice a buffer. A corrupt length must produce `ErrCorruptEntry`, never a panic or
  an out-of-range read.
- R3.3.4 The maximum payload size MUST be configurable, default 8 MiB, and MUST be enforced
  on both encode and decode.
- R3.3.5 An entry MAY straddle one or more 4 KiB block boundaries. Blocks are a unit of
  I/O, not a unit of framing.

**Design note (deliberate, revisit before v1 freeze):** 26 bytes is not a power of two, so
entry headers land at arbitrary alignments within a block. This costs nothing on x86 and
little on modern arm64, but it does prevent zero-copy casting of the header to a struct. A
32-byte header (26 + 6 reserved) would buy that at ~0.02% space overhead for 32 KiB entries
and ~6% for 100-byte entries. Deferred pending real payload-size measurements.

---

## 4. The Paging Layer

This is the core of the design and the part `O_DIRECT` forces us to own.

### 4.1 Alignment Rules

- R4.1.1 `BlockSize` MUST be 4096 bytes. It MUST be validated at startup against the device's
  logical block size (`BLKSSZGET` / `sysfs`); a larger device block size MUST raise a
  configuration error rather than be silently ignored.
- R4.1.2 Every `pwrite`/`pread` issued against a segment MUST use a file offset that is a
  multiple of `BlockSize`.
- R4.1.3 Every transfer length MUST be a multiple of `BlockSize`.
- R4.1.4 Every buffer passed to a direct read or write MUST begin at a memory address that is
  a multiple of `BlockSize`. Go's allocator provides no such guarantee, so buffers MUST come
  from the aligned allocator (§4.2).

### 4.2 Aligned Memory Allocation

Two acceptable implementations:

**(a) Over-allocate and slice** — simple, no syscall, but relies on the Go heap not moving
allocated objects. This holds for every released Go runtime to date but is not a language
guarantee.

```go
package logstore

import "unsafe"

const BlockSize = 4096

// AlignedBuffer returns a slice of exactly size bytes whose first element is
// located at an address that is a multiple of BlockSize.
func AlignedBuffer(size int) []byte {
	buf := make([]byte, size+BlockSize)
	addr := uintptr(unsafe.Pointer(&buf[0]))
	offset := int((BlockSize - (addr % BlockSize)) % BlockSize)
	return buf[offset : offset+size : offset+size]
}
```

**(b) Anonymous `mmap`** — `unix.Mmap` with `MAP_ANONYMOUS|MAP_PRIVATE` returns page-aligned
memory by definition, is immune to any future moving collector, and can be locked into RAM
with `mlock`. It costs a syscall per allocation and must be explicitly unmapped.

- R4.2.1 The implementation MUST use (b) `mmap` for the long-lived write buffer and the
  read-block pool, because these are allocated once and reused for the process lifetime.
- R4.2.2 (a) MAY be used in tests and on the fallback buffered path.
- R4.2.3 Aligned buffers MUST be pooled and reused. Allocating per-operation defeats the
  latency goal.

### 4.3 The Write Path (Group Commit)

```
   Proposals (concurrent goroutines)
        |  Append(entries)
        v
+---------------------------------------------------------------+
| User-space aligned staging buffer (e.g. 256 KiB, mmap'd)      |
| [ Entry 1 (200 B) ][ Entry 2 (3000 B) ][ Entry 3 (900 B) ]... |
+---------------------------------------------------------------+
        |
        |  flush trigger: (a) >= 1 full block staged
        |                 (b) group-commit timer elapsed
        |                 (c) caller demands immediate durability
        v
+---------------------------------------------------------------+
| pwrite(fd, buf, n*4096, blockAlignedOffset)   [O_DIRECT]      |
|  -> bypasses page cache, straight to the device               |
+---------------------------------------------------------------+
        |
        v
   fdatasync (only if O_DSYNC not set; see R4.4.2)
        |
        v
   Wake every waiter whose entry is covered by the flushed range
```

**Requirements**

- R4.3.1 A single writer goroutine MUST own the staging buffer. Callers submit entries over a
  channel and block on a per-batch completion signal. This removes all locking from the
  serialization hot path.
- R4.3.2 Entries MUST be serialized into the staging buffer in strictly increasing index
  order.
- R4.3.3 A flush MUST be triggered by whichever comes first: at least one full block of
  staged data, an elapsed group-commit interval (default **1 ms**, configurable, `0` disables
  batching), or an explicit durability demand from the Raft layer.
- R4.3.4 A flush MUST write only whole blocks. Data occupying the final partial block is
  handled by the tail-block protocol (§4.4).
- R4.3.5 `Append` MUST NOT return to the caller until the bytes covering its entries have been
  flushed *and* made durable. Raft's safety argument depends on this; a "fast" append that
  returns early is a correctness bug, not an optimization.
- R4.3.6 If the staging buffer fills before a flush completes, submitting goroutines MUST
  block (backpressure). Growing the buffer without bound MUST NOT happen.
- R4.3.7 A failed write MUST poison the store: all subsequent operations return the original
  error until the store is closed and reopened. Continuing after a partial write risks
  silently creating a hole in the log.

### 4.4 The Tail-Block Protocol (Partial Flushes)

The hard case: Raft demands durability when only 1.5 KiB of a 4 KiB block is populated.

- R4.4.1 On a partial flush, the engine MUST copy the live bytes into an aligned scratch
  block, zero-fill the remainder, and write the full 4 KiB.
- R4.4.2 The engine MUST guarantee the write reached stable media. Either open the segment
  with `O_DSYNC` (durability per write, no extra syscall) or issue `fdatasync` after the
  write. `O_DSYNC` is RECOMMENDED for the append path. `O_DIRECT` alone is **not** a
  durability guarantee — it bypasses the page cache but says nothing about the device's
  volatile write cache.
- R4.4.3 The staged bytes MUST remain live in the staging buffer after a partial flush. The
  buffer's write offset does not advance past the tail block boundary.
- R4.4.4 The next flush MUST rewrite the same tail block at the same offset, overwriting the
  zero padding with real entry data. Rewriting the same block repeatedly is expected and
  correct.
- R4.4.5 Zero padding MUST be distinguishable from an entry: the entry magic is non-zero, so
  a reader that encounters a zero magic knows it has reached the end of live data within the
  block.
- R4.4.6 The engine SHOULD track and expose a `padding_bytes_written` metric. A high ratio of
  padding to payload indicates the group-commit window is too short for the workload. It is a
  tuning signal only, and MUST NOT be read as overhead that buffered I/O would avoid:
  measured device traffic is byte-for-byte identical between this design and a buffered
  `fdatasync` implementation, because the kernel writes whole pages either way.

**Risk — torn tail-block rewrite.** Rewriting a block that already contains committed entries
means a crash mid-write can damage previously durable data. Mitigations, in order of
preference: (i) rely on the device's atomic sector write guarantee, which holds for the
4 KiB logical sector on essentially all NVMe hardware; (ii) detect the damage at recovery via
per-entry CRC and truncate to the last valid entry; (iii) for deployments that cannot accept
(i), a future format version MAY add a per-block sequence number so that a torn block is
always identifiable. The v1 engine relies on (i) + (ii) and MUST document this in the README.

### 4.5 The Read Path

- R4.5.1 To read entry `i`, the engine MUST consult the in-memory index (§5) for its
  `{segment, offset, length}`.
- R4.5.2 The engine MUST expand `[offset, offset+length)` down and up to block boundaries,
  read those whole blocks into a pooled aligned buffer, then slice out the exact entry bytes.
- R4.5.3 The CRC MUST be verified on every read. A mismatch MUST return `ErrCorruptEntry` and
  MUST NOT return partial data to the caller.
- R4.5.4 An entry still resident in the staging buffer and not yet flushed MUST be served from
  memory, not from disk. The index must know about unflushed entries.
- R4.5.5 Sequential reads (follower catch-up) MUST use a read-ahead window larger than one
  block (default 1 MiB, configurable), because `O_DIRECT` forgoes kernel read-ahead entirely.
  This is not an optimization: measured without it, point reads are 390x slower and pull
  15.5x more bytes off the device, making the store 5x slower at reads than the BoltDB
  design this spec replaces. See [bench/RESULTS.md](../bench/RESULTS.md) §7.
- R4.5.6 Reads MUST be safe to issue concurrently from many goroutines, and MUST NOT block the
  writer.

---

## 5. In-Memory Index

Raft log indices are dense and strictly monotonic, which makes a general-purpose map
unnecessary.

- R5.1 The index MUST be a ring buffer of fixed-size location records, indexed by
  `logIndex - baseIndex`, giving O(1) lookup with no hashing and no search.
- R5.2 Each record MUST hold: segment identifier (uint32), byte offset within segment
  (uint32), entry length including header (uint32), and term (uint64) — 20 bytes per entry,
  or ~20 MB per million live entries.
- R5.3 The index MUST track `firstIndex` (after compaction) and `lastIndex` (after truncation
  or append) as explicit fields. These, not the ring contents, are the authority on what the
  log contains.
- R5.4 The index MUST be rebuildable purely by scanning segment files. It MUST NOT be
  persisted as a separate on-disk artifact in v1 — a second durable structure is a second
  thing that can be inconsistent after a crash.
- R5.5 If startup scan time becomes a problem, a *checkpoint* of the index MAY be added later
  as a pure cache: written asynchronously, validated by CRC, and discarded without ceremony
  if it does not match the segments. The scan path must remain the source of truth.
- R5.6 Index mutation MUST be confined to the writer goroutine. Readers access it through an
  atomically published immutable snapshot or an RWMutex; the choice is an implementation
  detail but reads MUST NOT be able to observe a torn record.

---

## 6. Truncation and Compaction

### 6.1 Tail Truncation (`DeleteRange` toward the tail)

Occurs when a new leader overwrites a follower's uncommitted entries.

**Decision:** Truncation is **logical**. The engine MUST NOT overwrite the truncated disk
region with zeros or empty segments.

**Rationale.** Physically zeroing truncated regions was considered and rejected:

- *Write amplification.* Truncating 100 MB of uncommitted entries would write 100 MB of
  zeros — pure waste of NVMe endurance for data that is about to be overwritten anyway.
- *Double write penalty.* Every byte is paid for twice: once on append, once to erase.
- *It buys nothing.* Raft truncation is always at the tail, and the correct leader's next
  append naturally overwrites the stale bytes at the same offsets. The zeros would be
  transient.

**Requirements**

- R6.1.1 `TruncateAfter(index)` MUST update the in-memory index to drop entries above `index`,
  and MUST reset the segment write offset to the byte position where entry `index+1` began.
- R6.1.2 If the truncation point falls in an earlier segment than the active one, every
  segment entirely above the truncation point MUST be unlinked, and the segment containing
  the truncation point MUST be reopened as active.
- R6.1.3 Truncation MUST be an O(number of dropped segments) operation. It MUST NOT be
  proportional to the number of dropped bytes.
- R6.1.4 Truncation MUST be durable in effect even though it writes no data: see the crash
  recovery guard in §7.3, which is what makes logical truncation safe.
- R6.1.5 After truncation, the block containing the new tail MUST be re-read into the staging
  buffer so that subsequent appends rewrite it correctly rather than clobbering the still-live
  entries that precede the truncation point within that block.

### 6.2 Head Compaction (post-snapshot)

- R6.2.1 `Compact(index)` MUST unlink every sealed segment whose highest index is `< index`.
- R6.2.2 A segment MUST NOT be unlinked while any read is in flight against it. Segments MUST
  be reference-counted, with unlink deferred until the count drops to zero.
- R6.2.3 Compaction MUST advance `firstIndex` and release the corresponding index-ring slots.
- R6.2.4 A partially covered segment MUST NOT be rewritten to reclaim its prefix. Waiting for
  the whole segment to fall below the snapshot index is correct and free.

---

## 7. Crash Recovery

### 7.1 Startup Scan

- R7.1.1 On open, the engine MUST enumerate `*.seg` files, sort them by base index, and
  validate every segment header.
- R7.1.2 It MUST detect gaps in base indices between consecutive segments and fail startup
  with a diagnostic. A hole in the log is unrecoverable and MUST NOT be papered over.
- R7.1.3 Only the final segment needs a full entry scan in the common case; earlier segments
  were sealed and their entry counts recorded in the sealed-segment footer. The engine MUST
  nonetheless support a `--verify-all` mode that CRC-checks every entry in every segment.

### 7.2 Scanning a Segment

Starting at the first byte after the header block, repeatedly:

1. Read the 26-byte entry header.
2. If `Magic != 0x4A46`, stop. This is either zero padding or garbage; the live log ends here.
3. If `Size` exceeds the configured maximum or the remaining segment bytes, stop and record a
   corruption marker.
4. Recompute the CRC over header `[0,22)` + payload. On mismatch, stop.
5. Apply the recovery guard (§7.3).
6. Record `{index → segment, offset, length, term}` in the in-memory index and advance.

- R7.2.1 The scan MUST stop at the *first* failure and treat everything after it as absent.
  It MUST NOT attempt to skip forward and resynchronize; a valid-looking entry after a bad
  one is more likely to be stale pre-truncation data than a recoverable entry.
- R7.2.2 The recovered write offset MUST be the byte position immediately after the last valid
  entry.
- R7.2.3 The engine MUST log, at warn level, the segment, offset, and reason whenever a scan
  stops for anything other than clean end-of-data.

### 7.3 The Recovery Guard (what makes logical truncation safe)

Because truncated bytes are left on disk, a crash between truncation and the next append
leaves stale-but-CRC-valid entries beyond the true tail. The guard catches them.

- R7.3.1 The scanner MUST verify that each entry's `Index` field equals the expected next
  index (`previous + 1`). A mismatch means the scanner has walked off the end of the live log
  into stale data and MUST stop immediately.
- R7.3.2 The scanner MUST verify that each entry's `Term` is monotonically non-decreasing. A
  term that goes backwards is stale data from a superseded leader and MUST stop the scan.
- R7.3.3 These two checks are load-bearing, not defensive extras. They MUST be exercised by
  a dedicated test that writes entries, truncates logically, kills the process without any
  further append, reopens, and asserts the recovered `lastIndex` equals the truncation point.

---

## 8. Public API

The store MUST satisfy the `hashicorp/raft` storage interfaces so it can be dropped into an
existing Raft implementation.

```go
package plexus

// LogStore is the durable Raft log.
type LogStore interface {
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(index uint64, out *Log) error
	StoreLog(log *Log) error
	StoreLogs(logs []*Log) error   // batched; the group-commit entry point
	DeleteRange(min, max uint64) error
}

// MonotonicLogStore tells the Raft layer that indices are always consecutive,
// which lets it skip defensive gap handling.
type MonotonicLogStore interface {
	IsMonotonic() bool
}

// StableStore persists small pieces of Raft metadata (current term, vote).
type StableStore interface {
	Set(key, val []byte) error
	Get(key []byte) ([]byte, error)
	SetUint64(key []byte, val uint64) error
	GetUint64(key []byte) (uint64, error)
}
```

- R8.1 `IsMonotonic()` MUST return `true`.
- R8.2 `StoreLogs` MUST be atomic in effect: either all entries in the batch are durable and
  visible, or none are.
- R8.3 `DeleteRange(min, max)` MUST support exactly two shapes and reject everything else with
  `ErrInvalidRange`: a head range (`min == FirstIndex()`, head compaction) and a tail range
  (`max == LastIndex()`, tail truncation). A range in the middle of the log has no meaning in
  Raft and MUST NOT be silently accepted.
- R8.4 `GetLog` MUST return `ErrLogNotFound` for an index below `FirstIndex()` or above
  `LastIndex()`.
- R8.5 The `StableStore` MUST be a separate small file written with buffered I/O and `fsync`.
  It is tiny and rarely written; forcing it through the direct-I/O paging layer adds
  complexity for no benefit.
- R8.6 All exported errors MUST be package-level sentinel values comparable with
  `errors.Is`: `ErrCorruptEntry`, `ErrLogNotFound`, `ErrInvalidRange`, `ErrStoreClosed`,
  `ErrStorePoisoned`, `ErrEntryTooLarge`.

### 8.1 Reference Codec

```go
package plexus

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	HeaderSize = 26
	MagicNum   = 0x4A46 // "JF"
)

var ErrCorruptEntry = errors.New("plexus: log entry corruption detected")

type LogEntry struct {
	Term    uint64
	Index   uint64
	Payload []byte
}

// EncodeEntry serializes entry into dest, which must be a region of an aligned
// staging buffer with at least HeaderSize+len(entry.Payload) bytes available.
// It returns the number of bytes written.
func EncodeEntry(entry LogEntry, dest []byte) int {
	payloadLen := uint32(len(entry.Payload))

	binary.BigEndian.PutUint16(dest[0:2], MagicNum)
	binary.BigEndian.PutUint64(dest[2:10], entry.Term)
	binary.BigEndian.PutUint64(dest[10:18], entry.Index)
	binary.BigEndian.PutUint32(dest[18:22], payloadLen)

	copy(dest[HeaderSize:HeaderSize+payloadLen], entry.Payload)

	// CRC covers the header fields as well as the payload, so a corrupted
	// length field cannot survive validation and mislead the recovery scanner.
	sum := crc32.ChecksumIEEE(dest[0 : HeaderSize-4])
	sum = crc32.Update(sum, crc32.IEEETable, entry.Payload)
	binary.BigEndian.PutUint32(dest[22:26], sum)

	return HeaderSize + int(payloadLen)
}

// DecodeEntry validates and extracts one entry from the head of src, returning
// the entry and its total encoded size. maxPayload bounds the length field so a
// corrupt header cannot drive an out-of-range slice.
func DecodeEntry(src []byte, maxPayload int) (LogEntry, int, error) {
	if len(src) < HeaderSize {
		return LogEntry{}, 0, ErrCorruptEntry
	}
	if binary.BigEndian.Uint16(src[0:2]) != MagicNum {
		return LogEntry{}, 0, ErrCorruptEntry
	}

	term := binary.BigEndian.Uint64(src[2:10])
	index := binary.BigEndian.Uint64(src[10:18])
	payloadLen := int(binary.BigEndian.Uint32(src[18:22]))
	storedCRC := binary.BigEndian.Uint32(src[22:26])

	total := HeaderSize + payloadLen
	if payloadLen > maxPayload || total > len(src) {
		return LogEntry{}, 0, ErrCorruptEntry
	}

	sum := crc32.ChecksumIEEE(src[0 : HeaderSize-4])
	sum = crc32.Update(sum, crc32.IEEETable, src[HeaderSize:total])
	if sum != storedCRC {
		return LogEntry{}, 0, ErrCorruptEntry
	}

	payload := make([]byte, payloadLen)
	copy(payload, src[HeaderSize:total])
	return LogEntry{Term: term, Index: index, Payload: payload}, total, nil
}
```

### 8.2 Opening a Direct-I/O File

```go
package plexus

import (
	"os"

	"golang.org/x/sys/unix"
)

// openDirect opens a segment bypassing the OS page cache. O_DSYNC is included
// because O_DIRECT alone says nothing about the device's volatile write cache:
// it removes the kernel buffer, not the drive's.
func openDirect(path string) (*os.File, error) {
	flags := os.O_CREATE | os.O_RDWR | unix.O_DIRECT | unix.O_DSYNC
	return os.OpenFile(path, flags, 0o644)
}
```

---

## 9. Configuration

| Option | Default | Constraint |
| --- | --- | --- |
| `DataDir` | — | Required. Must be on a filesystem supporting `O_DIRECT` (ext4, XFS). |
| `BlockSize` | 4096 | Must equal the device logical block size. |
| `SegmentSize` | 64 MiB | Multiple of `BlockSize`; 16 MiB – 1 GiB. |
| `StagingBufferSize` | 256 KiB | Multiple of `BlockSize`; ≥ `MaxPayloadSize + HeaderSize`. |
| `GroupCommitInterval` | 1 ms | `0` disables batching (flush per append). |
| `MaxPayloadSize` | 8 MiB | Enforced on encode and decode. |
| `SequentialReadUnit` | 1 MiB | Multiple of `BlockSize`. |
| `DirectIO` | `true` | `false` selects the buffered fallback. |
| `PreallocAhead` | 1 segment | Segments pre-created in the background. |

- R9.1 On platforms without `O_DIRECT` (macOS, Windows), the engine MUST fall back to buffered
  I/O with `fsync`/`F_FULLFSYNC`, MUST keep the identical on-disk format, and MUST log a
  prominent warning at startup that this configuration is for development only.
- R9.2 The engine MUST refuse to start if `O_DIRECT` is requested but the underlying
  filesystem rejects it, rather than silently degrading.

## 10. Observability

The engine MUST expose at minimum:

- `append_latency_seconds` (histogram, from submit to durability acknowledgement)
- `flush_latency_seconds` (histogram, the `pwrite` + sync itself)
- `group_commit_batch_entries` and `group_commit_batch_bytes` (histograms)
- `padding_bytes_written_total` (counter — the direct cost of partial flushes)
- `bytes_written_total`, `bytes_read_total`
- `segments_active`, `segment_rotations_total`, `segments_unlinked_total`
- `truncations_total{kind="head"|"tail"}`
- `corrupt_entries_detected_total` (counter — any non-zero value warrants investigation)
- `recovery_duration_seconds` and `recovery_entries_scanned` (gauges, set at startup)

---

## 11. Acceptance Criteria (BDD)

### Feature: Aligned Direct I/O

**Scenario: Every direct write satisfies all three alignment constraints**
- **Given** a segment opened with `O_DIRECT`
- **When** the engine flushes any amount of staged data
- **Then** the file offset is a multiple of 4096
- **And** the transfer length is a multiple of 4096
- **And** the buffer's base address is a multiple of 4096
- **And** the write does not fail with `EINVAL`.

**Scenario: Unaligned buffer is rejected before reaching the kernel**
- **Given** a caller constructs a write buffer from a plain `make([]byte, ...)`
- **When** it is passed to the direct write path
- **Then** the engine returns a programming error before issuing the syscall.

### Feature: Group Commit

**Scenario: Concurrent proposals share one flush**
- **Given** a group-commit interval of 1 ms
- **And** 100 goroutines each appending one 200-byte entry within the same interval
- **When** the batch flushes
- **Then** exactly one `pwrite` covers all 100 entries
- **And** all 100 callers are woken after that write is durable
- **And** no caller returns before its bytes are on stable media.

**Scenario: A slow trickle still commits promptly**
- **Given** a single append of 100 bytes arriving with no other traffic
- **When** the group-commit interval elapses
- **Then** the entry is flushed within one interval plus one device write latency
- **And** the caller does not wait for the block to fill.

### Feature: Tail-Block Protocol

**Scenario: Partial block is padded and later overwritten**
- **Given** 1500 bytes of staged entries and an immediate durability demand
- **When** the engine flushes
- **Then** a full 4096-byte block is written with 2596 zero bytes of padding
- **And** the staged bytes remain in the staging buffer
- **And** `padding_bytes_written_total` increases by 2596.

**Scenario: Next append reuses the padded block**
- **Given** the previous scenario has completed
- **When** a further 1000-byte entry is appended and flushed
- **Then** the same 4096-byte offset is rewritten
- **And** a subsequent read of both entries returns both payloads intact.

### Feature: Corruption Detection

**Scenario: A flipped payload bit is caught on read**
- **Given** a durably written entry
- **When** a single bit in its payload is flipped directly in the segment file
- **And** the entry is read back
- **Then** `GetLog` returns an error matching `ErrCorruptEntry`
- **And** no partial payload is handed to the caller.

**Scenario: A corrupted length field cannot mislead recovery**
- **Given** a durably written entry
- **When** its `Size` field is overwritten with `0xFFFFFFFF`
- **And** the store is reopened
- **Then** the recovery scan stops at that entry
- **And** the store opens with `LastIndex` equal to the last preceding valid entry
- **And** no panic or out-of-range read occurs.

**Scenario: A torn final write is discarded**
- **Given** the process is killed after a partial block reaches the device
- **When** the store is reopened
- **Then** the incomplete trailing entry fails its CRC check
- **And** the log is recovered up to the last complete, valid entry.

### Feature: Logical Tail Truncation

**Scenario: Truncation writes no data bytes**
- **Given** a log with 100,000 entries totalling 100 MB
- **When** `DeleteRange` truncates back to index 50,000
- **Then** `bytes_written_total` does not increase
- **And** the operation completes in under one millisecond
- **And** `LastIndex()` returns 50,000.

**Scenario: The next append overwrites stale bytes in place**
- **Given** the log was truncated to index 50,000
- **When** a new entry for index 50,001 is appended
- **Then** it is written at the exact byte offset the old index 50,001 occupied
- **And** reading index 50,001 returns the new payload.

**Scenario: Crash after truncation with no subsequent append**
- **Given** the log was truncated to index 50,000
- **And** stale valid-CRC entries for 50,001..100,000 remain on disk
- **When** the process is killed and the store reopened
- **Then** the recovery guard detects the term or index discontinuity at 50,001
- **And** `LastIndex()` returns 50,000
- **And** `GetLog(50001)` returns `ErrLogNotFound`.

**Scenario: Truncation into the middle of a block preserves earlier entries**
- **Given** entries 900, 901, and 902 all reside in the same 4 KiB block
- **When** the log is truncated to index 900
- **And** a new entry 901 is appended and flushed
- **Then** entry 900 is still readable with its original payload
- **And** entry 901 returns the new payload.

### Feature: Segment Lifecycle

**Scenario: Rotation never blocks a commit**
- **Given** `PreallocAhead` is 1 and the active segment is nearly full
- **When** an append crosses the segment boundary
- **Then** the pre-allocated next segment is adopted without an inline `fallocate`
- **And** the append's latency stays within the normal distribution.

**Scenario: An entry never spans two segments**
- **Given** the active segment has 1 KiB of space remaining
- **When** a 4 KiB entry is appended
- **Then** the active segment is sealed with that space unused
- **And** the entry is written entirely within the next segment.

**Scenario: Compaction unlinks fully covered segments only**
- **Given** a snapshot at index 150,000 and segments based at 1, 60,000, and 140,000
- **When** `Compact(150000)` runs
- **Then** the segments based at 1 and 60,000 are unlinked
- **And** the segment based at 140,000 is retained untouched
- **And** `FirstIndex()` returns 140,000.

**Scenario: A segment with an in-flight read is not unlinked**
- **Given** a follower is streaming entries from a segment eligible for compaction
- **When** `Compact` runs
- **Then** the unlink is deferred until the read completes
- **And** the reader observes no error.

### Feature: Recovery

**Scenario: Clean restart rebuilds the index from segments alone**
- **Given** a store containing 1,000,000 entries across 20 segments
- **When** the store is closed cleanly and reopened
- **Then** the in-memory index is reconstructed by scanning segments
- **And** `FirstIndex()` and `LastIndex()` match the pre-restart values
- **And** every entry reads back byte-identical.

**Scenario: A gap between segments fails startup loudly**
- **Given** a middle segment file is manually deleted
- **When** the store is opened
- **Then** startup fails with an error naming the missing index range
- **And** the store does not open in a partially usable state.

### Feature: Durability Contract

**Scenario: Append does not return before data is on stable media**
- **Given** a store configured with `O_DIRECT | O_DSYNC`
- **When** `StoreLogs` returns successfully
- **And** power is cut immediately afterwards
- **Then** on reboot every entry in that batch is present and passes CRC.

**Scenario: A write error poisons the store**
- **Given** the underlying device returns `EIO` on a flush
- **When** the error is observed
- **Then** the failing append returns the error
- **And** every subsequent operation returns `ErrStorePoisoned`
- **And** the store must be closed and reopened to recover.

---

## 12. Open Questions

1. **Header width.** Keep 26 bytes, or pad to 32 for cast-friendly alignment? Decide with
   measured payload-size distribution from the target workload (§3.3 design note).
2. **Per-block sequence numbers.** Worth adding to make torn tail-block rewrites detectable
   without relying on device sector atomicity (§4.4 risk)? Adds 8 bytes per block, ~0.2%.
3. **`io_uring` for the flush path.** Would remove a syscall per commit and allow pipelining
   flushes. Real benefit is unclear when `O_DSYNC` already serializes on the device; measure
   before adopting.
4. **Index checkpointing.** Measured, the startup scan costs 58 ms per 200,000 entries
   (~2.9 s extrapolated to 10 million), against 1.8 ms for BoltDB, which needs no scan
   because a B-tree is already a persistent index. This is the largest measured regression
   the design accepts. At what live-entry count does that justify the optional cache in
   R5.5 — and should compaction be tuned to cap the scan instead?
5. **Group-commit interval tuning.** Should the interval adapt to observed arrival rate rather
   than sitting at a static 1 ms? Measurement makes this the highest-leverage knob in the
   design: batching gives 23.5x throughput and 14x less write amplification, and beyond
   batch=32 commit latency is essentially flat (366.8 µs → 367.6 µs from 32 to 128) while
   throughput still quadruples. Larger batches are nearly free once the device round trip
   dominates, which a static interval cannot exploit.
6. **Multiple devices.** Is striping segments across devices in scope, or does the operator
   handle that with RAID/LVM below us?

---

## 13. References

Consensus and Raft storage:
- [hashicorp/raft-wal](https://github.com/hashicorp/raft-wal) — experimental Raft WAL storage;
  documents the truncation and free-space problems that motivated moving off BoltDB.
- [`wal` package docs](https://pkg.go.dev/github.com/hashicorp/raft-wal) — `IsMonotonic` and
  the `MonotonicLogStore` contract.
- [Designing an Efficient Replicated Log Store with Consensus Protocol](https://www.usenix.org/conference/hotcloud19/presentation/kim) (USENIX HotCloud '19) — the granularity gap between a WAL record and a consensus entry.
- [Diving into etcd/raft](https://medium.com/@danielchia/diving-into-etcd-raft-e1e0dfa0e8d4) — how a production Raft implementation structures its storage.
- [Raftly: Building a Production-Grade Raft Implementation from Scratch](https://anirudhology.github.io/) — layered architecture with clean interfaces between storage and consensus.
- [Raft logs — purpose, retention and rotation](https://support.neo4j.com/) (Neo4j) — operational view of segment rotation and pruning.

Write-ahead logging and log-structured storage:
- [The Write-Ahead Log: A Foundation for Reliability](https://www.architecture-weekly.com/) — why sequential appends beat scattered random writes.
- Kleppmann, *Designing Data-Intensive Applications*, Ch. 3 "Storage and Retrieval" — B-tree
  fixed-size pages versus log-structured segments.
- [What Is a Log-Structured Merge Tree?](https://aerospike.com/blog/what-is-a-log-structured-merge-tree/) (Aerospike) — append-only sequence components.
- [Durability is a promise you can't hand-wave: Write-Ahead Log](https://ahmadalhour.com/) — entry header fields and the role of the payload size field.

Direct I/O and alignment:
- [Direct IO — facebook/rocksdb wiki](https://github.com/facebook/rocksdb/wiki/Direct-IO) — offset, length, and buffer address must all be aligned.
- [Direct I/O — Red Hat Enterprise Linux GFS documentation](https://docs.redhat.com/) — the `O_DIRECT` I/O constraints.
- [Write error: Invalid argument, when file is opened with O_DIRECT](https://stackoverflow.com/questions/34204504/) — the canonical `EINVAL` alignment failure.
- [Traditional IO vs mmap vs Direct IO: How Disk Access Really Works](https://dev.to/) — comparison of the three access mechanisms.
- [Storage Wars: Object vs Block vs File Systems Under the Hood](https://lowlevellore.com/) — `O_DIRECT` and double-buffering elimination.
- [Inside PostgreSQL's 8KB Page](https://boringsql.com/) — 4 KB physical sectors and filesystem block sizes.

Integrity:
- [CRC-32 — Just Solve the File Format Problem](http://fileformats.archiveteam.org/wiki/CRC-32) — the IEEE CRC-32 variant.
- [Full Disk Encryption on Linux with LUKS](https://mattiazignale.com/) — sizing a binary header so it always lands within one atomically written sector.
