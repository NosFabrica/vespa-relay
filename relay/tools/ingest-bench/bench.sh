#!/usr/bin/env bash
# One arm of the ingest benchmark (docs/ingest-benchmark.md): an empty Vespa, a slice of a real
# corpus written through one ingest path, one row appended to $OUT/results.tsv.
#
#   bench.sh NAME [VAR=value ...]
#
# MODE=relay (the default) boots this repo's relay with the VAR=value pairs as its environment and
# publishes the slice to it with `amy publish --file`: the wire, the policies, the writer, the store.
# MODE=sync runs the same slice through the mirror's IngestPipeline (CorpusIngestBench), with the
# pairs as that probe's BENCH_* settings: the writer and the store, no socket (the path the relay's
# own lanes take; the sync no longer writes this way).
# MODE=forward is how the sync writes: the relay booted as in MODE=relay with RELAY_NSEC=$NSEC, and
# the same CorpusIngestBench pipeline forwarding every batch to it (RelayWriter), so the relay's
# mirror lane writes the store. The pairs reach both processes; each ignores the other's.
#
# Knobs: CORPUS (one event per line, .gz read gzipped), TAKE, SKIP, INFLIGHT, PUBLISHERS (amy
# processes splitting the slice, one socket each: what several clients publishing at once look
# like, and the only way past one socket's send queue), KEEP_STORE=1 (keep
# the store the previous arm filled: a resend pass), CPUS (what the measured processes may run on),
# AMY, AMY_JAVA_OPTS, OUT, VESPA_MEM_LIMIT, GRADLE_ARGS, JAVA_OPTS (the relay's). Needs docker, taskset, python3, and `amy` (amethyst's cli).
set -euo pipefail
NAME=$1
shift
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
OUT=${OUT:-$REPO/build/ingest-bench}
CORPUS=${CORPUS:?CORPUS names the event file}
TAKE=${TAKE:-100000}
SKIP=${SKIP:-0}
INFLIGHT=${INFLIGHT:-2048}
PUBLISHERS=${PUBLISHERS:-1}
CPUS=${CPUS:-0-$(($(nproc) - 1))}
AMY=${AMY:-amy}
RELAY_BIN=${RELAY_BIN:-$REPO/relay/build/install/vespa-relay/bin/vespa-relay}
RUN=$OUT/runs/$(date +%Y%m%d-%H%M%S)-$NAME
mkdir -p "$RUN"
# Every step below needs Docker, and most of them hide its errors: say so once, up front.
if ! docker info >/dev/null 2>&1; then
  echo "Docker is not reachable; the arm needs it for Vespa." >&2
  exit 1
fi
# The relay this harness started, by pid, never by pattern: on a host running the compose
# relay, a `pgrep RelayMainKt` would find that container's JVM too.
PIDFILE=$OUT/relay.pid
stop_relay() {
  local pid
  pid=$(cat "$PIDFILE" 2>/dev/null) || return 0
  if grep -qa RelayMainKt /proc/"$pid"/cmdline 2>/dev/null; then
    kill "$pid"
    while kill -0 "$pid" 2>/dev/null; do sleep 1; done
  fi
  rm -f "$PIDFILE"
}
stop_relay
# Whatever ends the arm, its relay goes with it and Vespa gets every core back.
trap 'stop_relay; docker update --cpuset-cpus "0-$(($(nproc) - 1))" vespa >/dev/null 2>&1 || true' EXIT

if [ "${KEEP_STORE:-0}" != 1 ]; then
  # A fresh store deletes the compose project's Vespa volume: never next to a live relay.
  if (cd "$REPO" && docker compose ps --status running --services 2>/dev/null | grep -qx relay); then
    echo "the compose relay is running: this would wipe the store it serves. Stop it, or KEEP_STORE=1." >&2
    exit 1
  fi
  # The volume the container actually mounts, not a name guessed from the directory
  # (COMPOSE_PROJECT_NAME, or a directory compose normalizes, would miss it silently).
  VOLUME=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/opt/vespa/var"}}{{.Name}}{{end}}{{end}}' vespa 2>/dev/null || true)
  (cd "$REPO" && docker compose rm -sf vespa >/dev/null 2>&1)
  [ -n "$VOLUME" ] && docker volume rm "$VOLUME" >/dev/null
  (cd "$REPO" && VESPA_MEM_LIMIT=${VESPA_MEM_LIMIT:-8g} docker compose up -d vespa >/dev/null 2>&1)
  until curl -s http://localhost:19071/state/v1/health | grep -q '"code" : "up"'; do sleep 2; done
fi
docker update --cpuset-cpus "$CPUS" vespa >/dev/null
VESPA_PID=$(docker inspect -f '{{.State.Pid}}' vespa)
# The container is its cgroup. Under v1 (or a hybrid host) the counter is cpuacct's, found by
# that controller's own line; under v2, the unified tree's, by the `0::` line. Neither found is an
# error: a fallback to the root counter would read the whole host's CPU as Vespa's.
CG_V1=$(sed -n 's/^[0-9]*:[^:]*cpuacct[^:]*://p' /proc/"$VESPA_PID"/cgroup)
CG_V2=$(sed -n 's/^0:://p' /proc/"$VESPA_PID"/cgroup)
VESPA_CPU_FILE=
for dir in /sys/fs/cgroup/cpuacct /sys/fs/cgroup/cpu,cpuacct; do
  if [ -n "$CG_V1" ] && [ -f "$dir$CG_V1/cpuacct.usage" ]; then VESPA_CPU_FILE=$dir$CG_V1/cpuacct.usage; fi
done
if [ -n "$VESPA_CPU_FILE" ]; then
  vespa_cpu_us() { echo $(($(cat "$VESPA_CPU_FILE") / 1000)); }
elif [ -n "$CG_V2" ] && [ -f /sys/fs/cgroup"$CG_V2"/cpu.stat ]; then
  vespa_cpu_us() { awk '/usage_usec/{print $2}' /sys/fs/cgroup"$CG_V2"/cpu.stat; }
else
  echo "Vespa's cgroup CPU counter was not found (neither cpuacct.usage nor cpu.stat)." >&2
  exit 1
fi

if [ "${MODE:-relay}" = sync ]; then
  # Includes the schema deploy the bench's open() does: small next to a slice.
  V0=$(vespa_cpu_us)
  (cd "$REPO" && taskset -c "$CPUS" env BENCH_VESPA_URL=http://localhost:8080 BENCH_CORPUS="$CORPUS" BENCH_TAKE="$TAKE" BENCH_SKIP="$SKIP" "$@" \
    ./gradlew ${GRADLE_ARGS:-} :peers:test --tests '*CorpusIngestBench*' --no-daemon --rerun -PtestHeap=6g -i --console=plain) >"$RUN/gradle.log" 2>&1 || true
  RCPU=-
else
  # The relay deploys the schema on boot, so nothing answers on :8080 before it starts.
  IDENTITY=()
  [ "${MODE:-relay}" = forward ] && IDENTITY=(RELAY_NSEC="${NSEC:?MODE=forward needs NSEC, the relay key the bench signs in with}")
  taskset -c "$CPUS" env VESPA_URL=http://localhost:8080 RELAY_URL=ws://localhost:7777 RELAY_PORT=7777 STORE_METRICS_LOG_SECONDS=15 \
    JAVA_OPTS="${JAVA_OPTS:--Xmx4g}" "${IDENTITY[@]}" "$@" "$RELAY_BIN" >"$RUN/relay.log" 2>&1 &
  RELAY_JOB=$!
  # taskset, env and the launcher script each exec the next, so the job's pid becomes the JVM's.
  echo "$RELAY_JOB" >"$PIDFILE"
  until grep -q "listening on" "$RUN/relay.log" 2>/dev/null; do
    # The job, not the java process: the launcher script runs a while before it execs java.
    if ! kill -0 "$RELAY_JOB" 2>/dev/null; then
      echo "the relay did not boot:" >&2
      tail "$RUN/relay.log" >&2
      exit 1
    fi
    sleep 1
  done
  # The content node has to answer, not only the container; a 200 with no coverage is a node still loading.
  until curl -s "http://localhost:8080/search/?yql=select%20*%20from%20event%20where%20true&hits=0" | grep -q '"coverage"'; do sleep 2; done
  RELAY_PID=$RELAY_JOB
  proc_cpu_ticks() { awk '{print $14+$15}' /proc/"$1"/stat; }
  R0=$(proc_cpu_ticks "$RELAY_PID")
  V0=$(vespa_cpu_us)
  if [ "${MODE:-relay}" = forward ]; then
    (cd "$REPO" && taskset -c "$CPUS" env BENCH_VESPA_URL=http://localhost:8080 BENCH_CORPUS="$CORPUS" BENCH_TAKE="$TAKE" BENCH_SKIP="$SKIP" \
      BENCH_RELAY_URL=ws://localhost:7777 BENCH_NSEC="$NSEC" "$@" \
      ./gradlew ${GRADLE_ARGS:-} :peers:test --tests '*CorpusIngestBench*' --no-daemon --rerun -PtestHeap=6g -i --console=plain) >"$RUN/gradle.log" 2>&1 || true
  else
    # Contiguous parts of the slice, so the set of events is the same at any PUBLISHERS.
    PART=$(((TAKE + PUBLISHERS - 1) / PUBLISHERS))
    PUBLISHER_PIDS=()
    for p in $(seq 0 $((PUBLISHERS - 1))); do
      # The publisher's JVM options are its own: a JAVA_OPTS meant for the relay must not reach amy's launcher.
      taskset -c "$CPUS" env JAVA_OPTS="${AMY_JAVA_OPTS:-}" "$AMY" publish --file "$CORPUS" --relay ws://localhost:7777 --skip $((SKIP + p * PART)) --take "$PART" \
        --in-flight "$INFLIGHT" --progress 10 --timeout 60 --no-verify --json >"$RUN/amy-$p.json" 2>"$RUN/amy-$p.progress" &
      PUBLISHER_PIDS+=($!)
    done
    # A publisher exits 1 on a stall; its summary is still written, and the row reports it.
    wait "${PUBLISHER_PIDS[@]}" || true
  fi
  RCPU=$((($(proc_cpu_ticks "$RELAY_PID") - R0) / $(getconf CLK_TCK)))
  # Both CPU windows end at the last answer.
  V1=$(vespa_cpu_us)
  # The store-metrics line is cumulative; the last one is the arm's total.
  sleep 16
fi
VCPU=$(((${V1:-$(vespa_cpu_us)} - V0) / 1000000))

python3 -I - "$RUN" "$NAME" "$*" "$RCPU" "$VCPU" "$OUT/results.tsv" "${MODE:-relay}" <<'PY'
import json, os, re, sys
run, name, env, rcpu, vcpu, path, mode = sys.argv[1:8]
hdr = ["arm", "env", "sent", "ev_s", "mb_s", "accepted", "duplicate", "refused", "unanswered",
       "ok_p50_ms", "ok_p99_ms", "secs", "relay_cpu_s", "vespa_cpu_s", "relay_us_ev", "vespa_us_ev"]
per = lambda cpu, n: "-" if cpu == "-" else round(int(cpu) * 1e6 / max(1, int(n)))
rows = []
if mode in ("sync", "forward"):
    pat = r"CORPUS-BENCH (pass \d+) w=\S+ probes=\S+ n=(\d+) ([\d.]+)s (\d+) ev/s ([\d.]+) MB/s accepted=(\d+) rejected=(\d+)"
    for line in open(f"{run}/gradle.log"):
        m = re.search(pat, line)
        if m:
            p, n, secs, evs, mbs, acc, rej = m.groups()
            # The pipeline's own counters do not split duplicates from other refusals.
            # Forwarding, the relay's CPU is the whole arm's (one pass per run keeps it honest).
            rows.append([f"{name}/{p.replace(' ', '')}", env or "-", n, evs, mbs, acc, "-", rej, "-", "-", "-", secs, rcpu, vcpu, per(rcpu, n), per(vcpu, n)])
else:
    import glob
    parts = [json.load(open(f)) for f in sorted(glob.glob(f"{run}/amy-*.json"))]
    total = lambda k: sum(r[k] for r in parts)
    # The arm lasts as long as its slowest publisher; percentiles do not add, so the worst is reported.
    span = max(max(r["answering_ms"] for r in parts) / 1000, 0.001)
    answered = total("accepted") + total("duplicate") + total("refused") + total("unanswered")
    sent = total("sent")
    rows.append([name + (f" x{len(parts)}" if len(parts) > 1 else ""), env or "-", sent, round(answered / span), round(total("bytes") / 1e6 / span, 2),
                 total("accepted"), total("duplicate"), total("refused"), total("unanswered"),
                 max(r["ok_ms"]["p50"] for r in parts), max(r["ok_ms"]["p99"] for r in parts), round(span, 1),
                 rcpu, vcpu, per(rcpu, sent), per(vcpu, sent)])
new = not os.path.exists(path) or os.path.getsize(path) == 0
with open(path, "a") as f:
    if new:
        f.write("\t".join(hdr) + "\n")
    for row in rows:
        f.write("\t".join(map(str, row)) + "\n")
print("\t".join(hdr))
for row in rows:
    print("\t".join(map(str, row)))
PY
case "${MODE:-relay}" in
  sync) grep "CORPUS-BENCH" "$RUN/gradle.log" | cut -c1-400 ;;
  forward) grep "CORPUS-BENCH" "$RUN/gradle.log" | cut -c1-400; grep "store-metrics" "$RUN/relay.log" | tail -1 | cut -c1-600 ;;
  *) grep "store-metrics" "$RUN/relay.log" | tail -1 | cut -c1-600 ;;
esac
