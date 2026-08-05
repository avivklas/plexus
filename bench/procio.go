//go:build linux

package main

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// procIO reads /proc/self/io. This is the fair cross-arm metric: the kernel,
// not the application, accounts these numbers, so an arm cannot flatter itself
// through its own instrumentation. WriteBytes counts bytes the process caused
// to be sent to the storage layer, which captures page-cache writeback that an
// application-level counter would miss entirely.
type procIO struct {
	ReadBytes  int64
	WriteBytes int64
	Cancelled  int64
	SysRead    int64
	SysWrite   int64
}

func readProcIO() procIO {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return procIO{}
	}
	defer f.Close()

	var p procIO
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		name, val, ok := strings.Cut(sc.Text(), ": ")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		if err != nil {
			continue
		}
		switch name {
		case "read_bytes":
			p.ReadBytes = n
		case "write_bytes":
			p.WriteBytes = n
		case "cancelled_write_bytes":
			p.Cancelled = n
		case "syscr":
			p.SysRead = n
		case "syscw":
			p.SysWrite = n
		}
	}
	return p
}

func (p procIO) since(start procIO) procIO {
	return procIO{
		ReadBytes:  p.ReadBytes - start.ReadBytes,
		WriteBytes: p.WriteBytes - start.WriteBytes,
		Cancelled:  p.Cancelled - start.Cancelled,
		SysRead:    p.SysRead - start.SysRead,
		SysWrite:   p.SysWrite - start.SysWrite,
	}
}

// EffectiveWrite discounts writeback that was cancelled by truncation or
// deletion, which is what makes it meaningful for the compaction workload.
func (p procIO) EffectiveWrite() int64 { return p.WriteBytes - p.Cancelled }
