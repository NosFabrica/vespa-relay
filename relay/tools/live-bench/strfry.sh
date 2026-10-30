#!/usr/bin/env bash
# The same measured step as cpu.sh, against strfry (ghcr.io/hoytech/strfry) on 127.0.0.1:7790, for a reference
# point from a C++ relay. Its own config with compression off (the relay sends none, so per-frame deflate would
# not be a like-for-like cost), no nofile raise, and host networking (no docker-proxy in the path); feeds carry
# no search field, since strfry has no NIP-50.
#
#   strfry.sh                        FEEDS (12000), RATE (5)
source "$(dirname "$0")/lib.sh"
FEEDS=${FEEDS:-12000}
IMAGE=ghcr.io/hoytech/strfry:latest
docker info > /dev/null 2>&1 || ensure_vespa || exit 1
dir="$BENCH_OUT/strfry"
rm -rf "$dir" && mkdir -p "$dir/db" && chmod 777 "$dir/db"
docker run --rm --entrypoint cat "$IMAGE" /app/strfry.conf |
  sed -e 's/port = 7777/port = 7790/' -e 's/nofiles = [0-9]*/nofiles = 0/' |
  awk '/compression \{/ {c = 1} c && /enabled = true/ {sub(/true/, "false"); c = 0} {print}' > "$dir/strfry.conf"
docker rm -f live-bench-strfry > /dev/null 2>&1
docker run -d --name live-bench-strfry --network host -v "$dir/strfry.conf:/app/strfry.conf:ro" -v "$dir/db:/app/strfry-db" "$IMAGE" relay > /dev/null
sleep 3
RELAY_PID=$(pgrep -x strfry | head -1)
[ -n "$RELAY_PID" ] || { echo "live-bench: strfry did not start: $(docker logs live-bench-strfry 2>&1 | tail -2)" >&2; exit 1; }
log="$BENCH_OUT/strfry-load.txt"
LENS=none STEPS=$FEEDS,$FEEDS HOLD=60 RATE=${RATE:-5} CHILDREN=${CHILDREN:-4} RELAY_PID=$RELAY_PID \
  node "$REPO/relay/tools/live-feeds.mjs" ws://127.0.0.1:7790 > "$log" 2>&1 &
load=$!
# Before it, the publisher signs the step's events, minutes of it at high RATE: give up only after an hour, and
# never read a sample outside the step.
for _ in $(seq 1 1800); do [ "$(grep -c 'publishing for' "$log")" -ge 2 ] && break; sleep 2; done
[ "$(grep -c 'publishing for' "$log")" -ge 2 ] || { echo "live-bench: the measured step never started; see $log" >&2; kill_tree $load; docker rm -f live-bench-strfry > /dev/null 2>&1; exit 1; }
sleep 10
read -r user kernel < <(relay_cpu 30)
rss=$(awk '/VmRSS/ {print int($2 / 1024)}' "/proc/$RELAY_PID/status")
wait $load
docker rm -f live-bench-strfry > /dev/null 2>&1
delivery=$(grep -E "^[0-9]" "$log" | tail -1 | awk '{print "p50", $9, "p99", $11, "got%", $8}')
echo "user $user cores, kernel $kernel cores, rss $rss MB | $delivery"
