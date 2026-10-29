#!/usr/bin/env bash
# Where the relay's CPU goes under live fan-out: cpu.sh's load, with 45 s of JFR recorded inside the measured step,
# summarised by jfr-summary.py into areas of the fan-out path. JFR samples Java only: the kernel's share of the same
# step is in cpu.sh's "kernel" figure.
#
#   profile.sh [VAR=value ...]      leaves $BENCH_OUT/live.jfr for `jfr view` or JMC
source "$(dirname "$0")/lib.sh"
FEEDS=${FEEDS:-12000}
ensure_vespa || exit 1
relay_start "$@" || exit 1
log="$BENCH_OUT/profile-load.txt"
rm -f "$BENCH_OUT/live.jfr"
STEPS=$FEEDS,$FEEDS HOLD=60 RATE=${RATE:-5} CHILDREN=${CHILDREN:-4} RELAY_PID=$RELAY_PID feeds > "$log" 2>&1 &
load=$!
for _ in $(seq 1 300); do [ "$(grep -c 'publishing for' "$log")" -ge 2 ] && break; sleep 2; done
sleep 5
jcmd "$RELAY_PID" JFR.start name=live settings=profile duration=45s filename="$BENCH_OUT/live.jfr" > /dev/null 2>&1
wait $load
for _ in $(seq 1 30); do [ -s "$BENCH_OUT/live.jfr" ] && break; sleep 2; done
relay_stop
grep -E "^[0-9]" "$log" | tail -1
jfr print --json --events jdk.ExecutionSample "$BENCH_OUT/live.jfr" > "$BENCH_OUT/live-samples.json"
python3 "$(dirname "$0")/jfr-summary.py" "$BENCH_OUT/live-samples.json"
