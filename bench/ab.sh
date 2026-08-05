#!/bin/sh
# Paired A/B for the one genuinely contested claim: does O_DIRECT beat buffered
# I/O + fdatasync on the append path?
#
# The two arms run back to back within each repetition, and their order flips on
# alternate repetitions. Pairing cancels the slow VM drift that made the wide
# sweep's numbers unreliable; alternating cancels any first-mover advantage from
# cache or scheduler state.
set -u

OUT=${OUT:-/out/ab.jsonl}
REPS=${REPS:-15}
mkdir -p "$(dirname "$OUT")"
: > "$OUT"

cell() { # rep, order, flags...
	rep=$1; shift
	order=$1; shift
	for s in $order; do
		bench -store=$s -rep=$rep -tag=ab "$@" >> "$OUT" 2>/dev/null \
			|| echo "FAILED rep=$rep $s $*" >&2
	done
}

rep=1
while [ "$rep" -le "$REPS" ]; do
	if [ $((rep % 2)) -eq 0 ]; then
		order="direct buffered"
	else
		order="buffered direct"
	fi
	echo "rep $rep/$REPS ($order)" >&2
	cell "$rep" "$order" -workload=append -entry=256   -batch=1   -n=3000
	cell "$rep" "$order" -workload=append -entry=256   -batch=8   -n=16000
	cell "$rep" "$order" -workload=append -entry=256   -batch=128 -n=64000
	cell "$rep" "$order" -workload=append -entry=4096  -batch=8   -n=8000
	rep=$((rep + 1))
done

echo "DONE -> $OUT" >&2
wc -l "$OUT" >&2
