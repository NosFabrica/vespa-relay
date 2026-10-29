#!/usr/bin/env bash
# What one step of live fan-out costs the relay: boots it with the VAR=value arguments, opens FEEDS feeds, warms
# them with one publishing step, then measures the second: the relay's user and kernel CPU over 30 s of it, and
# the step's delivery (median, p99, share delivered).
#
#   cpu.sh [VAR=value ...]          e.g.  cpu.sh NATIVE_WEBSOCKET=true "JAVA_TOOL_OPTIONS=-XX:+UseParallelGC"
#
# FEEDS (12000), RATE events/s (5), PER_SOCKET (1) and LENS (include:spam) shape the load (live-feeds.mjs).
# One line out: "user U cores, kernel K cores | p50 P p99 Q got% G".
source "$(dirname "$0")/lib.sh"
FEEDS=${FEEDS:-12000}
RATE=${RATE:-5}
ensure_vespa || exit 1
relay_start "$@" || exit 1
log="$BENCH_OUT/cpu-load.txt"
STEPS=$FEEDS,$FEEDS HOLD=60 RATE=$RATE CHILDREN=${CHILDREN:-4} RELAY_PID=$RELAY_PID feeds > "$log" 2>&1 &
load=$!
# The second "publishing" line is the measured step; 10 s in, the step is past its start.
for _ in $(seq 1 300); do [ "$(grep -c 'publishing for' "$log")" -ge 2 ] && break; sleep 2; done
sleep 10
read -r user kernel < <(relay_cpu 30)
wait $load
relay_stop
delivery=$(grep -E "^[0-9]" "$log" | tail -1 | awk '{print "p50", $9, "p99", $11, "got%", $8}')
echo "user $user cores, kernel $kernel cores | $delivery"
