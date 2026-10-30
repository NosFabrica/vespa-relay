# Sourced by every live-bench script: where things are, and how a relay under test is started, found and stopped.
#
# BENCH_OUT     where logs, histograms and recordings go                  /tmp/vespa-relay-live-bench
# RELAY_HOME    the relay distribution to run (an installDist output)     relay/build/install/vespa-relay
# VESPA_URL / VESPA_CONFIG_URL   the store the relay serves             localhost:8080 / :19071
#
# A relay is started with its environment as VAR=value arguments, which win over the defaults here, so a JVM
# flag or a JDK is just `JAVA_TOOL_OPTIONS=...` or `JAVA_HOME=...` among them.

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
BENCH_OUT=${BENCH_OUT:-/tmp/vespa-relay-live-bench}
# Resolved, since the start script puts the resolved path on the JVM's command line, where relay_pid looks for it.
RELAY_HOME=$(realpath -m "${RELAY_HOME:-$REPO/relay/build/install/vespa-relay}")
VESPA_URL=${VESPA_URL:-http://localhost:8080}
VESPA_CONFIG_URL=${VESPA_CONFIG_URL:-http://localhost:19071}
RELAY_WS=ws://127.0.0.1:7777
mkdir -p "$BENCH_OUT/state"

# Starts dockerd and the compose Vespa when either is down, and waits for the config server: the sandbox's
# daemon has been seen to exit between runs, and a run against a missing store measures nothing.
ensure_vespa() {
  if ! docker info > /dev/null 2>&1; then
    (DOCKER_MIN_API_VERSION=1.24 dockerd > "$BENCH_OUT/dockerd.log" 2>&1 &)
    for _ in $(seq 1 30); do docker info > /dev/null 2>&1 && break; sleep 2; done
  fi
  (cd "$REPO" && docker compose start vespa > /dev/null 2>&1)
  for _ in $(seq 1 120); do
    curl -sS "$VESPA_CONFIG_URL/state/v1/health" 2>/dev/null | grep -q '"code" : "up"' && return 0
    sleep 5
  done
  echo "live-bench: Vespa's config server did not come up" >&2
  return 1
}

# The pid of the relay JVM started from $RELAY_HOME, matched on the JVM's own command line: a shell whose command
# names the same path must never be mistaken for it.
relay_pid() {
  for p in $(pgrep -x java); do
    tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null | grep -q "$RELAY_HOME/lib/" && { echo "$p"; return; }
  done
}

# Stops the relay this bench started, and only that one.
relay_stop() {
  local p
  p=$(relay_pid)
  [ -n "$p" ] && kill "$p" && sleep 3
  return 0
}

# relay_start [VAR=value ...]: boots $RELAY_HOME with those over the defaults, waits for NIP-11, sets RELAY_PID.
relay_start() {
  relay_stop
  if curl -sf -o /dev/null -H "Accept: application/nostr+json" http://localhost:7777/; then
    echo "live-bench: something else already serves :7777; stop it first" >&2
    return 1
  fi
  env RELAY_URL=ws://localhost:7777 RELAY_PORT=7777 VESPA_URL="$VESPA_URL" VESPA_CONFIG_URL="$VESPA_CONFIG_URL" \
    RELAY_STATE_FILE="$BENCH_OUT/state/relay-state.json" STATS_FILE="$BENCH_OUT/state/stats.json" \
    JAVA_TOOL_OPTIONS="-Xms2g -Xmx4g" "$@" \
    nohup "$RELAY_HOME/bin/vespa-relay" > "$BENCH_OUT/relay.log" 2>&1 &
  for _ in $(seq 1 150); do
    curl -sf -o /dev/null -H "Accept: application/nostr+json" http://localhost:7777/ && break
    sleep 2
  done
  RELAY_PID=$(relay_pid)
  [ -n "$RELAY_PID" ] || { echo "live-bench: the relay did not start; see $BENCH_OUT/relay.log" >&2; return 1; }
}

# The relay's user and kernel CPU over the next $1 seconds, as "user kernel" in cores.
relay_cpu() {
  local u0 s0 u1 s1
  read -r u0 s0 < <(awk '{print $14, $15}' "/proc/$RELAY_PID/stat")
  sleep "$1"
  read -r u1 s1 < <(awk '{print $14, $15}' "/proc/$RELAY_PID/stat")
  awk -v u=$((u1 - u0)) -v s=$((s1 - s0)) -v t="$1" 'BEGIN {printf "%.2f %.2f\n", u / 100 / t, s / 100 / t}'
}

feeds() { node "$REPO/relay/tools/live-feeds.mjs" "$RELAY_WS" "$@"; }

# Stops process $1 and everything it started: a backgrounded `feeds` is a subshell, and node forks its own children.
kill_tree() {
  local c
  for c in $(pgrep -P "$1"); do kill_tree "$c"; done
  kill "$1" 2> /dev/null
  return 0
}
