#!/usr/bin/env bash
# What the relay does with a client that stops reading: one that subscribes and goes quiet (expect the silence
# reap near 60 s), then the same under a flood of large events (expect it gone within 60 s whichever rule fires
# first), each timed from the relay's own LOG_CONNECTIONS lines, under paranoid Netty leak detection.
#
#   stalled.sh [VAR=value ...]       e.g.  stalled.sh NATIVE_WEBSOCKET=true
source "$(dirname "$0")/lib.sh"
ensure_vespa || exit 1
relay_start LOG_CONNECTIONS=true "JAVA_TOOL_OPTIONS=-Xms1g -Xmx2g -Dio.netty.leakDetection.level=paranoid" "$@" || exit 1
stamped="$BENCH_OUT/stalled-relay.log"
tail -n 0 -F "$BENCH_OUT/relay.log" 2>/dev/null | while IFS= read -r line; do printf '%s %s\n' "$(date +%s)" "$line"; done > "$stamped" &
stamper=$!
sleep 1
for mode in "idle 0" "flood 400"; do
  set -- $mode
  start=$(date +%s)
  echo "== $1"
  timeout 160 node "$(dirname "$0")/stalled-client.mjs" "$RELAY_WS" "$2" 60000 10 | sed 's/^/   client: /'
  awk -v s="$start" '$1 >= s && /relay connections/ {printf "   relay +%ds: %s\n", $1 - s, substr($0, index($0, $2))}' "$stamped"
  sleep 5
done
kill $stamper 2>/dev/null
echo "leak reports: $(grep -c 'LEAK:' "$BENCH_OUT/relay.log"), connections closed after a failure: $(grep -c 'websocket: closing' "$BENCH_OUT/relay.log")"
relay_stop
