// Probe: is this environment capable of producing trustworthy durability numbers?
// Checks (1) O_DIRECT support, (2) whether fsync actually costs anything.
package main

import (
	"fmt"
	"os"
	"sort"
	"syscall"
	"time"
	"unsafe"
)

const blockSize = 4096

func aligned(size int) []byte {
	buf := make([]byte, size+blockSize)
	addr := uintptr(unsafe.Pointer(&buf[0]))
	off := int((blockSize - (addr % blockSize)) % blockSize)
	return buf[off : off+size]
}

func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	i := int(float64(len(d)-1) * p)
	return d[i]
}

func report(label string, d []time.Duration) {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	fmt.Printf("  %-28s p50=%-12v p99=%-12v max=%v\n",
		label, percentile(d, 0.50), percentile(d, 0.99), d[len(d)-1])
}

func main() {
	dir := os.Args[1]
	n := 300

	// (1) Can we even open with O_DIRECT here?
	dpath := dir + "/direct.dat"
	fd, err := syscall.Open(dpath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_DIRECT, 0o644)
	if err != nil {
		fmt.Printf("O_DIRECT open: UNSUPPORTED (%v)\n", err)
	} else {
		buf := aligned(blockSize)
		for i := range buf {
			buf[i] = byte(i)
		}
		if _, err := syscall.Pwrite(fd, buf, 0); err != nil {
			fmt.Printf("O_DIRECT write: FAILED (%v)\n", err)
		} else {
			fmt.Printf("O_DIRECT: SUPPORTED (4KiB aligned pwrite succeeded)\n")
		}
		// Confirm the kernel really enforces alignment; if an unaligned write
		// succeeds, O_DIRECT is being silently ignored by this filesystem.
		un := make([]byte, blockSize)
		if _, err := syscall.Pwrite(fd, un[1:], 0); err == nil {
			fmt.Printf("O_DIRECT: WARNING unaligned write succeeded -> flag ignored\n")
		} else {
			fmt.Printf("O_DIRECT: alignment enforced by kernel (unaligned -> %v)\n", err)
		}
		syscall.Close(fd)
		os.Remove(dpath)
	}

	// (2) Does fsync cost anything? A no-op fsync means the host is lying to the
	// guest and every durability measurement below would be fiction.
	f, err := os.OpenFile(dir+"/sync.dat", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		panic(err)
	}
	small := make([]byte, 256)
	var syncs, writes []time.Duration
	for i := 0; i < n; i++ {
		t0 := time.Now()
		f.WriteAt(small, int64(i)*4096)
		writes = append(writes, time.Since(t0))
		t1 := time.Now()
		f.Sync()
		syncs = append(syncs, time.Since(t1))
	}
	fmt.Println("buffered 256B write + fsync:")
	report("write (no sync)", writes)
	report("fsync", syncs)
	f.Close()
	os.Remove(dir + "/sync.dat")

	// (3) O_DIRECT|O_DSYNC 4KiB write latency: the spec's actual append path.
	df, err := os.OpenFile(dir+"/dsync.dat",
		os.O_CREATE|os.O_RDWR|syscall.O_DIRECT|syscall.O_DSYNC, 0o644)
	if err != nil {
		fmt.Printf("O_DIRECT|O_DSYNC open failed: %v\n", err)
		return
	}
	buf := aligned(blockSize)
	var directs []time.Duration
	for i := 0; i < n; i++ {
		t0 := time.Now()
		if _, err := df.WriteAt(buf, int64(i)*blockSize); err != nil {
			fmt.Printf("direct write failed: %v\n", err)
			return
		}
		directs = append(directs, time.Since(t0))
	}
	fmt.Println("O_DIRECT|O_DSYNC 4KiB write:")
	report("write+durable", directs)
	df.Close()
	os.Remove(dir + "/dsync.dat")
}
