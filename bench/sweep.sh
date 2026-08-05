#!/bin/sh
# Runs the full benchmark matrix, one process per cell so that /proc/self/io
# accounting starts clean. Results are emitted as JSON lines on stdout.
set -u

OUT=${OUT:-/out/results.jsonl}
mkdir -p "$(dirname "$OUT")"
: > "$OUT"

run() {
	echo "RUN $*" >&2
	if bench "$@" >> "$OUT" 2>/tmp/err; then
		:
	else
		echo "FAILED: $* :: $(tail -2 /tmp/err)" >&2
	fi
}

SEG=${SEG:-67108864}
REPS=${REPS:-3}

rep=1
while [ "$rep" -le "$REPS" ]; do
echo "=== repetition $rep/$REPS ===" >&2

# W1/W2 -- group commit sweep. Commit counts are held roughly constant across
# batch sizes so each cell takes comparable wall time.
for store in direct buffered bolt; do
	run -store=$store -workload=append -entry=256 -batch=1   -n=3000  -segsize=$SEG
	run -store=$store -workload=append -entry=256 -batch=8   -n=16000 -segsize=$SEG
	run -store=$store -workload=append -entry=256 -batch=32  -n=32000 -segsize=$SEG
	run -store=$store -workload=append -entry=256 -batch=128 -n=64000 -segsize=$SEG
done

# W3 -- entry size sweep at a fixed, realistic batch size.
for store in direct buffered bolt; do
	run -store=$store -workload=append -entry=128   -batch=8 -n=16000 -segsize=$SEG
	run -store=$store -workload=append -entry=1024  -batch=8 -n=16000 -segsize=$SEG
	run -store=$store -workload=append -entry=4096  -batch=8 -n=8000  -segsize=$SEG
	run -store=$store -workload=append -entry=32768 -batch=8 -n=4000  -segsize=$SEG
done

# W4 -- tail truncation. direct-zero is the physical-overwrite strategy the
# spec rejects; it is included to price that rejection.
for store in direct buffered bolt direct-zero; do
	run -store=$store -workload=truncate -entry=256 -n=200000 -segsize=$SEG
done

# W5 -- restart recovery.
for store in direct buffered bolt; do
	run -store=$store -workload=recover -entry=256 -n=200000 -segsize=$SEG
done

# W6 -- point reads (follower catch-up), cold cache. The direct arm is measured
# both with the spec's read-ahead window and without it, because O_DIRECT gives
# up kernel read-ahead and has to rebuild it in user space.
for store in direct buffered bolt; do
	run -store=$store -workload=read -entry=256 -n=50000 -segsize=$SEG
done
run -store=direct -workload=read -entry=256 -n=50000 -segsize=$SEG -readahead=0

rep=$((rep + 1))
done

echo "DONE -> $OUT" >&2
wc -l "$OUT" >&2
