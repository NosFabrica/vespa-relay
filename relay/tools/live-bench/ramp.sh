#!/usr/bin/env bash
# How many live feeds the relay holds before delivery degrades: one relay, feeds added step by step (STEPS), each
# step publishing for HOLD seconds, each scored against the target. A step passes when every socket opened, none
# dropped, at least MIN_GOT% of the frames owed arrived, and delivery p99 stayed under P99_MS. The capacity is the
# last step that passed. The first WARMUP steps (1) are reported but not scored: they carry the JIT's warm-up, and
# read worse than the step after them. Every live-feeds.mjs setting applies (RATE, PER_SOCKET, AUTHORS, FOLLOWS, OPEN_RATE...).
#
#   STEPS=10000,20000,40000 RATE=1 PER_SOCKET=4 ramp.sh NATIVE_WEBSOCKET=true
#   AUTHORS=1000 FOLLOWS=20 RATE=10 PER_SOCKET=25 STEPS=25000,50000,100000 ramp.sh NATIVE_WEBSOCKET=true
source "$(dirname "$0")/lib.sh"
P99_MS=${P99_MS:-500}
MIN_GOT=${MIN_GOT:-99.9}
WARMUP=${WARMUP:-1}
ensure_vespa || exit 1
relay_start "$@" || exit 1
log="$BENCH_OUT/ramp-load.txt"
STEPS=${STEPS:-10000,20000,40000} HOLD=${HOLD:-30} RELAY_PID=$RELAY_PID feeds > "$log" 2>&1
relay_stop
grep -v "publishing for" "$log"
echo "--- target: every socket open, none dropped, got% >= $MIN_GOT, p99 < ${P99_MS} ms"
# Step lines: feeds sockets fail eose_p50 eose_p99 | owed got% p50 p90 p99 max ms | dropped cpu rss pressure gate
awk -v p99max="$P99_MS" -v gotmin="$MIN_GOT" -v warmup="$WARMUP" '
  /^[0-9]/ {
    if (++step <= warmup) { printf "  %8d feeds: warm-up, not scored (p99 %s ms)\n", $1, $11; next }
    why = ""
    if ($3 > 0) why = why " " $3 " sockets failed to open"
    if ($8 + 0 < gotmin) why = why " got% " $8
    if ($11 == "-" || $11 + 0 >= p99max) why = why " p99 " $11 " ms"
    if ($14 > 0) why = why " " $14 " dropped"
    if (why == "") { if (!failed) pass = $1; printf "  %8d feeds: pass  (p99 %s ms, got%% %s, relay %s%% cpu)\n", $1, $11, $8, $15 }
    else { printf "  %8d feeds: FAIL (%s )\n", $1, why; if (!failed) failed = $1 }
  }
  END { printf "capacity: %s feeds%s\n", pass ? pass : "none of the steps", failed ? " (first failure at " failed ")" : " (no step failed; raise STEPS)" }
' "$log"
