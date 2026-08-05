#!/usr/bin/env python3
"""Aggregate benchmark JSONL into medians across repetitions."""
import json
import statistics
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "results/results.jsonl"
rows = [json.loads(l) for l in open(path) if l.strip()]

unverified = [r for r in rows if not r["verified"]]
print(f"rows={len(rows)}  unverified={len(unverified)}")
if unverified:
    for r in unverified:
        print("  UNVERIFIED:", r["store"], r["workload"], r["entry_size"], r["batch"])

groups = defaultdict(list)
for r in rows:
    groups[(r["workload"], r["entry_size"], r["batch"], r["store"])].append(r)

NUM = ["duration_sec", "entries_per_sec", "p50_us", "p99_us", "p999_us", "max_us",
       "kernel_write_bytes", "kernel_write_syscalls", "kernel_read_bytes",
       "bytes_per_entry", "write_amplification", "app_padding_bytes",
       "app_device_bytes", "app_writes", "app_syncs"]

agg = {}
for k, rs in groups.items():
    m = {"n": rs[0]["n"], "reps": len(rs)}
    for f in NUM:
        m[f] = statistics.median(r[f] for r in rs)
    # Spread across reps, to show whether differences exceed run-to-run noise.
    p50s = [r["p50_us"] for r in rs]
    m["p50_spread_pct"] = (max(p50s) - min(p50s)) / max(min(p50s), 1e-9) * 100
    agg[k] = m


def table(title, keyfilter, cols, fmts, sortkey=None):
    print(f"\n### {title}")
    keys = [k for k in agg if keyfilter(k)]
    keys.sort(key=sortkey or (lambda k: (k[1], k[2], k[3])))
    hdr = ["store", "entry", "batch"] + [c[0] for c in cols]
    print(" | ".join(hdr))
    print(" | ".join("---" for _ in hdr))
    for k in keys:
        m = agg[k]
        cells = [k[3], str(k[1]), str(k[2])]
        for (_, f), fmt in zip(cols, fmts):
            cells.append(fmt(m[f]))
        print(" | ".join(cells))


f1 = lambda v: f"{v:,.1f}"
f0 = lambda v: f"{v:,.0f}"
f2 = lambda v: f"{v:,.2f}"

lat = [("p50_us", "p50_us"), ("p99_us", "p99_us"), ("p999_us", "p999_us"),
       ("entries/s", "entries_per_sec"), ("B/entry", "bytes_per_entry"),
       ("wamp", "write_amplification"), ("spread%", "p50_spread_pct")]
latf = [f1, f1, f1, f0, f1, f2, f1]

table("Append: group-commit sweep (256B entries)",
      lambda k: k[0] == "append" and k[1] == 256, lat, latf,
      sortkey=lambda k: (k[2], k[3]))

table("Append: entry-size sweep (batch=8)",
      lambda k: k[0] == "append" and k[2] == 8 and k[1] != 256, lat, latf,
      sortkey=lambda k: (k[1], k[3]))

tr = [("dur_us", "p50_us"), ("kwrite_B", "kernel_write_bytes"),
      ("appwrite_B", "app_device_bytes"), ("spread%", "p50_spread_pct")]
table("Tail truncation (100k of 200k entries dropped)",
      lambda k: k[0] == "truncate", tr, [f1, f0, f0, f1],
      sortkey=lambda k: k[3])

rc = [("dur_ms", "duration_sec"), ("entries/s", "entries_per_sec"),
      ("kread_B", "kernel_read_bytes"), ("spread%", "p50_spread_pct")]
table("Restart recovery (200k entries)",
      lambda k: k[0] == "recover", rc,
      [lambda v: f"{v*1000:,.1f}", f0, f0, f1], sortkey=lambda k: k[3])

rd = [("p50_us", "p50_us"), ("p99_us", "p99_us"), ("entries/s", "entries_per_sec"),
      ("kread_B", "kernel_read_bytes"), ("spread%", "p50_spread_pct")]
table("Point reads, cold cache (50k entries)",
      lambda k: k[0] == "read", rd, [f2, f2, f0, f0, f1], sortkey=lambda k: k[3])

print("\n### Padding overhead of O_DIRECT (append cells)")
print("store | entry | batch | app_bytes | padding | padding%")
print(" | ".join(["---"] * 6))
for k in sorted([k for k in agg if k[0] == "append" and k[3].startswith("direct")],
                key=lambda k: (k[1], k[2])):
    m = agg[k]
    tot, pad = m["app_device_bytes"], m["app_padding_bytes"]
    pct = pad / tot * 100 if tot else 0
    print(f"{k[3]} | {k[1]} | {k[2]} | {tot:,.0f} | {pad:,.0f} | {pct:.1f}%")
