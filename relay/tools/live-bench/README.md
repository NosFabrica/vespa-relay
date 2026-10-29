# live-bench

Measures the relay's live fan-out: what an event costs to deliver to every open feed, in CPU, latency and memory.
The readings so far, and what each change moved, are in [docs/live-feeds.md](../../../docs/live-feeds.md).
`live-feeds.mjs` (one directory up) is the load. These scripts boot the relay, aim the load at it and read the
relay's side.

| script | what it answers | out |
|---|---|---|
| `cpu.sh [VAR=value ...]` | CPU and delivery for one step at FEEDS × RATE | `user U cores, kernel K cores \| p50 P p99 Q got% G` |
| `compare.sh FILE [ROUNDS]` | the same, for several variants, alternated round by round | one line per variant per round |
| `memory.sh [VAR=value ...]` | live heap and kernel memory per idle feed, and the classes behind them | bytes per feed, top classes |
| `churn.sh [VAR=value ...]` | whether closed connections leave objects behind | live counts per round |
| `stalled.sh [VAR=value ...]` | how a client that stops reading is cut off, with paranoid leak detection | relay connect/disconnect times |
| `profile.sh [VAR=value ...]` | where the Java CPU goes in that step (JFR) | shares by area of the fan-out path |
| `strfry.sh` | the same step against strfry, as a reference | as `cpu.sh`, plus RSS |

`VAR=value` arguments are the relay's environment over the defaults in `lib.sh`. So a switch, a JVM flag or a
JDK is one argument: `NATIVE_WEBSOCKET=true`, `"JAVA_TOOL_OPTIONS=-Xms4g -Xmx4g -XX:+UseParallelGC"`,
`JAVA_HOME=/opt/jdk-25`. `RELAY_HOME` picks the build (default `relay/build/install/vespa-relay`, from
`./gradlew :relay:installDist`). `BENCH_OUT` is where logs, histograms and recordings go.

```bash
./gradlew :relay:installDist
VESPA_MEM_LIMIT=6g docker compose up -d vespa
relay/tools/live-bench/cpu.sh NATIVE_WEBSOCKET=true
cat > /tmp/variants <<'EOF'
ktor     |
native   | NATIVE_WEBSOCKET=true
parallel | NATIVE_WEBSOCKET=true | JAVA_TOOL_OPTIONS=-Xms4g -Xmx4g -XX:+UseParallelGC
EOF
relay/tools/live-bench/compare.sh /tmp/variants 2
```

## Reading the numbers

- **Alternate, and repeat.** On a shared machine run-to-run drift is as large as many of the effects worth
  measuring. `compare.sh` runs every variant once per round, so drift falls on all of them. A difference that
  doesn't hold across two rounds is not a result.
- **User and kernel CPU are separate figures.** JFR (`profile.sh`) sees only the Java side. On loopback the
  kernel figure also carries the receiving end's TCP work, so it overstates what a real network costs the
  relay. Compare it across variants, not as an absolute.
- **Per frame** means cores ÷ (FEEDS × RATE). At 12,000 feeds and 5 events/s that is 60,000 frames a second.
- **A run that delivered 0%** started against a Vespa that was still coming up. Discard it rather than average it.

## Traps

- The sandbox this was built in caps a process at 20,000 open files, so one relay holds about 19,000 sockets.
  Past that, use `PER_SOCKET` (feeds per socket, up to `MAX_SUBSCRIPTIONS`).
- The Node load generator takes over a core of its own at 16,000 sockets. Beyond about 20,000 feeds the
  machine, not the relay, is the bound: spread the load (`URLS`, other machines).
- Gradle downloads in parallel and Maven Central rate-limits that with 429s. `--max-workers=1` gets past it.
- Docker's default seccomp profile blocks io_uring, which matters only to transports that use it.
