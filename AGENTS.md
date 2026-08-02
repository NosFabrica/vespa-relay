# Working on vespa-relay

A Nostr relay with trust-ranked NIP-50 search. Quartz's protocol engine
(`RelayServerBase`) over a [vespa-eventstore](https://github.com/vitorpamplona/vespa-eventstore)
store, plus a router that mirrors events from upstream relays.

Single Gradle module, `:relay`, JVM only (toolchain 21). `RelayMain` is the
entrypoint.

## Commands

```bash
./gradlew build                    # compile + test + spotless check
./gradlew :relay:test              # tests only
./gradlew :relay:test --tests "*SyncCursors*"
./gradlew spotlessApply            # fix formatting — do this before committing
./gradlew :relay:run               # run locally (needs a Vespa at VESPA_URL)

docker compose up -d --build relay # the usual dev loop
docker compose logs relay --since 5m
```

Git hooks are installed by the build: **pre-commit runs `spotlessCheck`,
pre-push runs the tests**. A commit will be rejected for formatting alone, so
run `spotlessApply` first.

## Layout

```
relay/src/main/kotlin/com/vitorpamplona/quartz/eventstore/relay/
  RelayMain.kt        entrypoint; reads env, deploys the schema, wires everything
  RelayApp.kt         Ktor server + routes
  NostrRelayServer.kt the IEventStore-backed relay backend; installs StoreQueryContext
  MirrorRouter.kt     the router — the biggest and most-changed file (see below)
  RouterConfig.kt     HOCON `streams { }` parsing (strfry-shaped)
  SyncCursors.kt      resume state for paged relays ("bands")
  StreamPhase.kt      per-stream progress reporting + PagingProgress
  RelayHealth.kt      per-authority strikes and eviction
  RelayDiscovery.kt   pulling relay urls out of stored events
  RelayIdentity.kt    RELAY_NSEC — NIP-11 self, NIP-42, NIP-66 monitor
  ParseAudit.kt       what quartz could not parse, grouped to a JSON report
```

`README.md` documents every environment variable and the router config format.
It is the reference; this file is the orientation.

## The router, in one pass

`MirrorRouter` mirrors upstream events into the store. Two kinds of stream:

- **static** — relays listed in `urls` in `router.conf`
- **dynamic** — relays discovered from stored events via `relaySource` (NIP-65
  outbox lists, NIP-85 provider lists, relay hints)

Each stream declares **how** it asks for what it is missing, via `sync`:

| mode | when | why |
|---|---|---|
| `negentropy` | the same event lives on many relays (kinds 0/3/10002) | reconcile id sets, transfer only the difference |
| `fetch` | each relay holds its own events (NIP-85 kind 30382) | comparing huge disjoint sets costs more than the fetch; the cursor answers "what is new" |
| `auto` | unknown | decide by size — reconcile only when both sides hold more than `ROUTER_NEG_MIN_EVENTS` |

**This is a property of the data, not of the relay, and it cannot be measured
from counts.** The `assertions` and `dataViaOutbox` streams both put millions of
events on each side; only one of them overlaps. Declare it.

**Cursor bands** (`SyncCursors`) record the `created_at` span already walked for
a `(relay, filter)` pair, so a re-run asks only outside it. Keyed by the *whole
filter* deliberately: edit a stream's filter and its cursor is invalidated, which
is the intended way to force a re-walk. A paged fetch records `complete = false`;
only a finished negentropy reconcile records `complete = true`.

**Known open bug:** a band holds one span for every kind in the filter, so a
long-lived kind (0) vouches for a short-lived one (30382) and `legs()` skips the
interior. The fix is per-kind spans *inside* the filter-keyed band — not
per-kind keys, which would break the invalidation property above.

## Instrumentation — use it before theorising

Most of this exists because something was diagnosed wrongly from inference.
Reach for it first.

- **health line** (once a minute) — heap, ingest queue depth vs capacity, ev/s,
  relays transferring, connected, fatal count, events lost to store errors. A
  full queue and an empty queue are opposite diagnoses that look identical
  everywhere else.
- **`ROUTER_WIRE_LOG`** — `sent` logs every REQ/CLOSE; `full` adds every message
  received. Empty still logs `NOTICE`, `CLOSED` and failed sends, which are the
  relay explaining itself. It lowers quartz's log floor itself, because
  `QUARTZ_LOG_LEVEL=WARN` would otherwise silently discard its own output.
- **`ROUTER_STREAMS`** — run one stream alone, so a measurement isn't three
  streams competing for one socket budget, heap and ingest queue.
- **`ingest stages`** — per-stage timing (`dedup`, `write`, `proj.fetch`,
  `proj.write`, `versions`). This is what identified a projection read-back as
  90% of ingest.
- **paging progress** — percentage and ETA measured on the *time axis*, because
  a paged fetch has no event denominator. Its predecessor computed
  `downloaded/downloaded` and printed `100%, ETA ~0:00` for hours.

## Conventions

**Comments say why, with evidence.** The codebase's KDoc records what actually
happened — measured numbers, the wrong turn that motivated the current shape.
Match that register. `// increment the counter` is noise; "this was
`inboundCapacity = batch * 4` with no ceiling, so batch 20000 sized the queue at
80,000 events and the heap went over" is the house style.

**Tests assert the property, not the implementation.** `NIP-50 extensions
survive the session to the engine query` passed unchanged through a wholesale
replacement of the mechanism it covers. Conversely, a test that asserted
`everReconciled`'s exact behaviour passed while shipping a bug, because it
encoded the implementation's own opinion of itself.

**A configured component must never be silently inert.** Several bugs here were
a switch that was read, accepted, and did nothing. If a flag needs something else
to be true, make it true and say so.

**Don't publish claims you can't support.** Negative NIP-66 records are signed
and public. `Unreachability.proves()` is deliberately conservative: an unknown
failure stays quiet, because silence costs a retry and being wrong costs a false
statement about someone else's server.

## Traps that have cost real time

- **JitPack pins are commit hashes, and Gradle resolves conflicts
  lexicographically.** Pinning quartz to `6d518adddb` while the store carried
  `79f198c729` silently resolved to the latter — `'7' > '6'`. Hence
  `resolutionStrategy { force(libs.quartz) }` in `relay/build.gradle.kts`. Never
  remove it, and check that a pin actually took effect.
- **JitPack's build-status API lies.** It reported a build `ok` whose log ended
  in `exit code 1`. Only the presence of the artifact file proves anything.
- **Two KDoc blocks in a row** fail ktlint (`standard:kdoc`, "dangling toplevel
  KDoc"). Each doc needs its own declaration.
- **`grep` may be aliased to `ugrep`**, which silently returns nothing on some
  large files. Use `/usr/bin/grep` when a search "finds nothing" implausibly.
- **`\n` inside a Kotlin raw string is literal**, which breaks HOCON fixtures in
  tests. Use real line breaks.
- **Verify under load, not while idle.** A schema fix was "confirmed" by counting
  zero rejections during a window with no writes flowing. It was the wrong fix.
- **When editing quartz/amethyst alongside this repo**, that project *is*
  multiplatform: commas in backticked test names break Kotlin/Native, and
  `java.util` APIs (`toSortedMap`) break every non-JVM target. Compile more than
  the JVM target.

## Operations

`docker-compose.yml` runs Vespa and the relay. Vespa holds ~50M events in this
deployment; its transaction-log replay means the relay takes a minute to reach
the router on boot.

The relay **deploys the bundled Vespa schema on every boot** (`AUTO_DEPLOY`,
default true), so the cluster always matches the code talking to it. A no-change
deploy is a cheap no-op. This is not decoration: when the schema drifted, Vespa
answered every write with `Status 400 ... Field 'name_parts' is not defined` and
the router counted, dropped and carried on — 2,336,288 good events lost in one
run, unrecoverable, while every status line read healthy.

Structural rejections are surfaced separately from ordinary ones on the health
line (`N event(s) LOST to store errors`), because a bad signature is the event's
fault and dropping it is correct, whereas a batch failing structurally is ours.
