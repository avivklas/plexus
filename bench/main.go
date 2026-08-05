//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"
)

type Result struct {
	Store     string `json:"store"`
	Workload  string `json:"workload"`
	EntrySize int    `json:"entry_size"`
	Batch     int    `json:"batch"`
	N         int    `json:"n"`

	DurationSec   float64 `json:"duration_sec"`
	EntriesPerSec float64 `json:"entries_per_sec"`

	P50us  float64 `json:"p50_us"`
	P99us  float64 `json:"p99_us"`
	P999us float64 `json:"p999_us"`
	Maxus  float64 `json:"max_us"`

	// Kernel-accounted, uniform across arms.
	KernelWriteBytes int64   `json:"kernel_write_bytes"`
	KernelWriteSys   int64   `json:"kernel_write_syscalls"`
	KernelReadBytes  int64   `json:"kernel_read_bytes"`
	BytesPerEntry    float64 `json:"bytes_per_entry"`
	WriteAmp         float64 `json:"write_amplification"`

	// Application-accounted, segment arms only.
	AppDeviceBytes int64 `json:"app_device_bytes"`
	AppPadding     int64 `json:"app_padding_bytes"`
	AppWrites      int64 `json:"app_writes"`
	AppSyncs       int64 `json:"app_syncs"`

	Verified bool   `json:"verified"`
	Note     string `json:"note,omitempty"`
	Tag      string `json:"tag,omitempty"`
	Rep      int    `json:"rep,omitempty"`
}

func main() {
	var (
		store     = flag.String("store", "direct", "direct|buffered|bolt|direct-zero|buffered-zero")
		workload  = flag.String("workload", "append", "append|truncate|recover|read")
		entrySize = flag.Int("entry", 256, "payload bytes per entry")
		batch     = flag.Int("batch", 1, "entries per durable commit")
		n         = flag.Int("n", 5000, "total entries")
		dir       = flag.String("dir", "/data/bench", "working directory")
		segSize   = flag.Int64("segsize", 64<<20, "segment size bytes")
		warmup    = flag.Int("warmup", 200, "warmup entries, excluded from stats")
		verify    = flag.Bool("verify", true, "read back and check every entry")
		readUnit  = flag.Int("readahead", 1<<20, "O_DIRECT read-ahead window bytes; 0 disables")
		tag       = flag.String("tag", "", "free-form label carried into the result")
		rep       = flag.Int("rep", 0, "repetition number, for paired analysis")
	)
	flag.Parse()
	directReadUnit = *readUnit

	os.RemoveAll(*dir)
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fail(err)
	}
	defer os.RemoveAll(*dir)

	res := Result{Store: *store, Workload: *workload, EntrySize: *entrySize, Batch: *batch, N: *n,
		Tag: *tag, Rep: *rep}
	if *store == "direct" && *readUnit == 0 {
		res.Store = "direct-nora"
	}

	// Size staging so one batch fits, keeping the group-commit sweep about
	// commit frequency rather than about buffer capacity.
	staging := 256 << 10
	if need := (*batch)*(*entrySize+HeaderSize) + BlockSize; need > staging {
		staging = min(need, 16<<20)
	}

	open := func() (Store, error) { return openStore(*store, *dir, *segSize, staging) }

	switch *workload {
	case "append":
		runAppend(&res, open, *entrySize, *batch, *n, *warmup, *verify)
	case "truncate":
		runTruncate(&res, open, *entrySize, *n)
	case "recover":
		runRecover(&res, open, *entrySize, *n)
	case "read":
		runRead(&res, open, *entrySize, *n)
	default:
		fail(fmt.Errorf("unknown workload %q", *workload))
	}

	out, _ := json.Marshal(res)
	fmt.Println(string(out))
}

func openStore(kind, dir string, segSize int64, staging int) (Store, error) {
	switch kind {
	case "direct":
		return newSegLog(dir, kind, segSize, staging, directFactory, false)
	case "direct-zero":
		return newSegLog(dir, kind, segSize, staging, directFactory, true)
	case "buffered":
		return newSegLog(dir, kind, segSize, staging, bufferedFactory, false)
	case "buffered-zero":
		return newSegLog(dir, kind, segSize, staging, bufferedFactory, true)
	case "bolt":
		return newBoltStore(dir + "/raft.db")
	}
	return nil, fmt.Errorf("unknown store %q", kind)
}

func directFactory(path string, size int64, staging int) (pager, error) {
	return newDirectPager(path, size, staging)
}

func bufferedFactory(path string, size int64, staging int) (pager, error) {
	return newBufferedPager(path, size, staging)
}

// fill builds one batch of entries ending at index `next+count-1`.
func fill(next uint64, count, size int) []Entry {
	out := make([]Entry, count)
	for i := range out {
		idx := next + uint64(i)
		out[i] = Entry{Term: 1, Index: idx, Payload: payloadFor(idx, size)}
	}
	return out
}

func runAppend(res *Result, open func() (Store, error), size, batch, n, warmup int, verify bool) {
	s, err := open()
	if err != nil {
		fail(err)
	}
	next := s.LastIndex() + 1

	for done := 0; done < warmup; done += batch {
		if err := s.Append(fill(next, batch, size)); err != nil {
			fail(err)
		}
		next += uint64(batch)
	}

	dropCaches()
	io0 := readProcIO()
	st0 := s.Stats()
	lat := make([]time.Duration, 0, n/batch+1)

	t0 := time.Now()
	for done := 0; done < n; done += batch {
		b := fill(next, batch, size)
		ts := time.Now()
		if err := s.Append(b); err != nil {
			fail(err)
		}
		lat = append(lat, time.Since(ts))
		next += uint64(batch)
	}
	elapsed := time.Since(t0)

	io := readProcIO().since(io0)
	st := s.Stats()

	res.DurationSec = elapsed.Seconds()
	res.EntriesPerSec = float64(n) / elapsed.Seconds()
	fillLatency(res, lat)
	fillIO(res, io, n, size)
	res.AppDeviceBytes = st.DeviceBytes - st0.DeviceBytes
	res.AppPadding = st.PaddingBytes - st0.PaddingBytes
	res.AppWrites = st.DeviceWrites - st0.DeviceWrites
	res.AppSyncs = st.SyncCalls - st0.SyncCalls

	if verify {
		res.Verified = verifyAll(s, size)
	}
	if err := s.Close(); err != nil {
		fail(err)
	}
}

func runTruncate(res *Result, open func() (Store, error), size, n int) {
	s, err := open()
	if err != nil {
		fail(err)
	}
	next := s.LastIndex() + 1
	const buildBatch = 256
	for done := 0; done < n; done += buildBatch {
		c := min(buildBatch, n-done)
		if err := s.Append(fill(next, c, size)); err != nil {
			fail(err)
		}
		next += uint64(c)
	}

	target := s.FirstIndex() + uint64(n/2) - 1
	dropCaches()
	io0 := readProcIO()
	st0 := s.Stats()

	t0 := time.Now()
	if err := s.TruncateTail(target); err != nil {
		fail(err)
	}
	elapsed := time.Since(t0)

	io := readProcIO().since(io0)
	st := s.Stats()

	res.DurationSec = elapsed.Seconds()
	fillLatency(res, []time.Duration{elapsed})
	fillIO(res, io, n/2, size)
	res.AppDeviceBytes = st.DeviceBytes - st0.DeviceBytes
	res.AppWrites = st.DeviceWrites - st0.DeviceWrites
	res.Note = fmt.Sprintf("dropped %d of %d entries", n/2, n)

	if s.LastIndex() != target {
		fail(fmt.Errorf("truncate: last=%d want %d", s.LastIndex(), target))
	}
	// A truncation that loses surviving entries is not a faster truncation.
	res.Verified = verifyAll(s, size)
	if err := s.Close(); err != nil {
		fail(err)
	}
}

func runRecover(res *Result, open func() (Store, error), size, n int) {
	s, err := open()
	if err != nil {
		fail(err)
	}
	next := s.LastIndex() + 1
	const buildBatch = 256
	for done := 0; done < n; done += buildBatch {
		c := min(buildBatch, n-done)
		if err := s.Append(fill(next, c, size)); err != nil {
			fail(err)
		}
		next += uint64(c)
	}
	want := s.LastIndex()
	if err := s.Close(); err != nil {
		fail(err)
	}

	dropCaches()
	io0 := readProcIO()
	t0 := time.Now()
	s2, err := open()
	if err != nil {
		fail(err)
	}
	elapsed := time.Since(t0)
	io := readProcIO().since(io0)

	if s2.LastIndex() != want {
		fail(fmt.Errorf("recover: last=%d want %d", s2.LastIndex(), want))
	}
	res.DurationSec = elapsed.Seconds()
	res.EntriesPerSec = float64(n) / elapsed.Seconds()
	fillLatency(res, []time.Duration{elapsed})
	fillIO(res, io, n, size)
	res.Verified = verifyAll(s2, size)
	s2.Close()
}

func runRead(res *Result, open func() (Store, error), size, n int) {
	s, err := open()
	if err != nil {
		fail(err)
	}
	next := s.LastIndex() + 1
	const buildBatch = 256
	for done := 0; done < n; done += buildBatch {
		c := min(buildBatch, n-done)
		if err := s.Append(fill(next, c, size)); err != nil {
			fail(err)
		}
		next += uint64(c)
	}

	dropCaches()
	io0 := readProcIO()
	lat := make([]time.Duration, 0, n)
	t0 := time.Now()
	for i := s.FirstIndex(); i <= s.LastIndex(); i++ {
		ts := time.Now()
		if _, err := s.Get(i); err != nil {
			fail(err)
		}
		lat = append(lat, time.Since(ts))
	}
	elapsed := time.Since(t0)
	io := readProcIO().since(io0)

	res.DurationSec = elapsed.Seconds()
	res.EntriesPerSec = float64(n) / elapsed.Seconds()
	fillLatency(res, lat)
	fillIO(res, io, n, size)
	res.Verified = true
	s.Close()
}

// verifyAll re-reads live entries and compares them byte for byte with what was
// written. Without this an arm could "win" by not storing the data. Large logs
// are sampled, always including both boundaries, to keep verification from
// dominating the measured run.
func verifyAll(s Store, size int) bool {
	first, last := s.FirstIndex(), s.LastIndex()
	if last < first {
		return true
	}
	total := last - first + 1
	step := uint64(1)
	if total > 20000 {
		step = total / 5000
	}

	check := func(i uint64) bool {
		e, err := s.Get(i)
		if err != nil {
			fmt.Fprintf(os.Stderr, "verify: index %d: %v\n", i, err)
			return false
		}
		if e.Index != i || !bytes.Equal(e.Payload, payloadFor(i, size)) {
			fmt.Fprintf(os.Stderr, "verify: index %d payload mismatch\n", i)
			return false
		}
		return true
	}

	for i := first; i <= last; i += step {
		if !check(i) {
			return false
		}
	}
	return check(first) && check(last)
}

func fillLatency(res *Result, lat []time.Duration) {
	if len(lat) == 0 {
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pick := func(p float64) float64 {
		i := int(float64(len(lat)-1) * p)
		return float64(lat[i].Nanoseconds()) / 1000
	}
	res.P50us = pick(0.50)
	res.P99us = pick(0.99)
	res.P999us = pick(0.999)
	res.Maxus = float64(lat[len(lat)-1].Nanoseconds()) / 1000
}

func fillIO(res *Result, io procIO, entries, size int) {
	res.KernelWriteBytes = io.EffectiveWrite()
	res.KernelWriteSys = io.SysWrite
	res.KernelReadBytes = io.ReadBytes
	if entries > 0 {
		res.BytesPerEntry = float64(io.EffectiveWrite()) / float64(entries)
		useful := float64(entries) * float64(size+HeaderSize)
		if useful > 0 {
			res.WriteAmp = float64(io.EffectiveWrite()) / useful
		}
	}
}

// dropCaches clears the page cache so a buffered arm cannot be credited for
// reads that never touch the device. Requires --privileged; ignored otherwise.
func dropCaches() {
	if f, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0); err == nil {
		f.WriteString("3\n")
		f.Close()
	}
	time.Sleep(50 * time.Millisecond)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "FATAL:", err)
	os.Exit(1)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
