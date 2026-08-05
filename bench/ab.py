#!/usr/bin/env python3
"""Paired analysis of the O_DIRECT vs buffered A/B.

Pairs the two arms by repetition, so slow drift in the VM affects both members
of a pair equally. Reports the median paired ratio with a bootstrap confidence
interval and a sign test, rather than comparing two independent medians.
"""
import json
import random
import statistics
import sys
from collections import defaultdict

random.seed(20260805)

path = sys.argv[1] if len(sys.argv) > 1 else "results/ab.jsonl"
rows = [json.loads(l) for l in open(path) if l.strip()]

bad = [r for r in rows if not r["verified"]]
print(f"rows={len(rows)}  unverified={len(bad)}\n")

# cell -> rep -> store -> row
cells = defaultdict(lambda: defaultdict(dict))
for r in rows:
    cells[(r["entry_size"], r["batch"])][r["rep"]][r["store"]] = r


def bootstrap_ci(vals, iters=20000, alpha=0.05):
    n = len(vals)
    meds = []
    for _ in range(iters):
        meds.append(statistics.median(random.choice(vals) for _ in range(n)))
    meds.sort()
    return meds[int(iters * alpha / 2)], meds[int(iters * (1 - alpha / 2))]


def analyse(metric, higher_is_better, label):
    print(f"### {label}")
    print("entry | batch | pairs | direct med | buffered med | "
          "ratio (buf/dir) | 95% CI | direct wins | verdict")
    print(" | ".join(["---"] * 9))
    for cell in sorted(cells):
        reps = cells[cell]
        pairs = [(v["direct"][metric], v["buffered"][metric])
                 for v in reps.values() if "direct" in v and "buffered" in v]
        if not pairs:
            continue
        d = [p[0] for p in pairs]
        b = [p[1] for p in pairs]
        ratios = [bb / dd for dd, bb in pairs if dd > 0]
        lo, hi = bootstrap_ci(ratios)
        med = statistics.median(ratios)

        if higher_is_better:
            wins = sum(1 for dd, bb in pairs if dd > bb)
        else:
            wins = sum(1 for dd, bb in pairs if dd < bb)

        # A ratio interval straddling 1.0 means the data cannot separate the arms.
        if lo <= 1.0 <= hi:
            verdict = "no significant difference"
        elif (med > 1.0) == (not higher_is_better):
            verdict = "direct faster"
        else:
            verdict = "buffered faster"

        print(f"{cell[0]} | {cell[1]} | {len(pairs)} | {statistics.median(d):,.1f} | "
              f"{statistics.median(b):,.1f} | {med:.3f} | "
              f"[{lo:.3f}, {hi:.3f}] | {wins}/{len(pairs)} | {verdict}")
    print()


analyse("p50_us", False, "Commit latency p50 (lower is better)")
analyse("p99_us", False, "Commit latency p99 (lower is better)")
analyse("entries_per_sec", True, "Throughput (higher is better)")

print("### Device bytes written (kernel-accounted, identical inputs)")
print("entry | batch | direct B/entry | buffered B/entry | ratio")
print(" | ".join(["---"] * 5))
for cell in sorted(cells):
    reps = cells[cell]
    d = [v["direct"]["bytes_per_entry"] for v in reps.values() if "direct" in v]
    b = [v["buffered"]["bytes_per_entry"] for v in reps.values() if "buffered" in v]
    if not d or not b:
        continue
    dm, bm = statistics.median(d), statistics.median(b)
    print(f"{cell[0]} | {cell[1]} | {dm:,.1f} | {bm:,.1f} | {bm/dm:.3f}")

print("\n### Write syscalls per commit")
print("entry | batch | direct | buffered")
print(" | ".join(["---"] * 4))
for cell in sorted(cells):
    reps = cells[cell]
    for v in reps.values():
        if "direct" in v and "buffered" in v:
            dv, bv = v["direct"], v["buffered"]
            commits = dv["n"] / dv["batch"]
            print(f"{cell[0]} | {cell[1]} | "
                  f"{dv['app_writes']/commits:.2f} write + {dv['app_syncs']/commits:.2f} sync | "
                  f"{bv['app_writes']/commits:.2f} write + {bv['app_syncs']/commits:.2f} sync")
            break
