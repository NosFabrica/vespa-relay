#!/usr/bin/env bash
# Whether the relay still serves after running out of file descriptors: its open-files limit lowered to what it holds
# plus HEADROOM (500), a flood of FLOOD (1,500) websockets opened past it and then closed, and the relay asked
# again over HTTP and websocket. Before loadTimeZoneRules, the first failed accept was logged through a formatter
# that loads the JDK's time-zone file, which could not be opened, and the error that left killed the acceptor:
# the relay answered nothing until restarted.
#
#   fd-exhaustion.sh [VAR=value ...]       e.g.  fd-exhaustion.sh NATIVE_WEBSOCKET=true
source "$(dirname "$0")/lib.sh"
HEADROOM=${HEADROOM:-500}
FLOOD=${FLOOD:-1500}
ensure_vespa || exit 1
relay_start "JAVA_TOOL_OPTIONS=-Xms1g -Xmx2g" "$@" || exit 1
held=$(ls "/proc/$RELAY_PID/fd" | wc -l)
prlimit --pid "$RELAY_PID" --nofile=$((held + HEADROOM)) || exit 1
echo "relay holds $held files; limit now $((held + HEADROOM))"
timeout 60 node -e '
const n = Number(process.argv[1]); const all = []; let open = 0, failed = 0;
for (let i = 0; i < n; i++) { const ws = new WebSocket("ws://127.0.0.1:7777"); ws.onopen = () => open++; ws.onerror = () => failed++; all.push(ws); }
setTimeout(() => { console.log(`flood: ${open} of ${n} opened, ${failed} failed`); for (const ws of all) ws.close(); setTimeout(() => process.exit(0), 3000); }, 20000);' "$FLOOD"
echo "after the flood: HTTP $(curl -s -o /dev/null -w '%{http_code}' -m 10 http://localhost:7777/)"
timeout 15 node -e '
const ws = new WebSocket("ws://127.0.0.1:7777");
ws.onopen = () => { console.log("after the flood: a websocket opens"); process.exit(0); };
ws.onerror = () => { console.log("after the flood: a websocket FAILS to open"); process.exit(1); };' ||
  echo "after the flood: a websocket never opened"
echo "accepts refused for want of a file: $(grep -c 'Too many open files' "$BENCH_OUT/relay.log"), uncaught on a relay thread: $(grep -c '^Exception in thread' "$BENCH_OUT/relay.log")"
grep '^Exception in thread' "$BENCH_OUT/relay.log" | head -2
relay_stop
