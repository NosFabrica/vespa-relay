# Working on vespa-relay

A Nostr relay with trust-ranked NIP-50 search. Quartz's protocol engine
(`RelayServerBase`) over a [vespa-eventstore](https://github.com/NosFabrica/vespa-eventstore)
store, plus a router that mirrors events from upstream relays.

Single Gradle module, `:relay`, JVM only (toolchain 21). `RelayMain` is the
entrypoint.

## Commands

```bash
./gradlew build                    # compile + test + spotless check
./gradlew :relay:test              # tests only
./gradlew :relay:test --tests "*SyncBands*"
./gradlew spotlessApply            # fix formatting — do this before committing
./gradlew :relay:run               # run locally (needs a Vespa at VESPA_URL)

node tools/webtest/run.mjs         # web UI module tests (plain node, no deps)
node tools/webtest/navwalk.mjs     # browser walkthrough of URL/backstack/entity
                                   # navigation (needs playwright + chromium)

docker compose up -d --build relay # the usual dev loop
docker compose logs relay --since 5m
```

Git hooks are installed by the build: **pre-commit runs `spotlessCheck`,
pre-push runs the tests**. A commit will be rejected for formatting alone, so
run `spotlessApply` first.

## Layout

```
relay/src/main/kotlin/com/nosfabrica/vespa/relay/
  RelayMain.kt          entrypoint; reads env, deploys the schema, wires everything
  config/
    EnvSettings.kt      NIP-11 limits etc. from env, via `env.intOr(...)` rather
                        than `env["..."]` — grep for both or you will conclude a
                        working setting is dead
    PubKeys.kt          npub-only parsing for every pubkey setting
    RelayIdentity.kt    RELAY_NSEC — NIP-11 self, NIP-42, NIP-66 monitor
  server/               the serving side
    NostrRelayServer.kt the IEventStore-backed relay backend; installs StoreQueryContext
    HttpServer.kt       serveRelay: Ktor server + routes, Nip11Info
    RelayInfo.kt        the NIP-11 document
    RelayWebSocket.kt   the ws route
    Nip86Route.kt       the management API
    BanListFile.kt      NIP-86 ban state that outlives the container
    ServingPressure.kt  EWMA of client read latency; the router yields to it
    ConnectionCountListener.kt  LOG_CONNECTIONS
  router/               the mirror (see below)
    SyncEngine.kt       wiring, live tails, health/stats lines
    IngestPipeline.kt   bounded queue -> verify -> batchInsert, poison isolation
    BisectingInsert.kt  the batch-bisecting write
    StaticBackfill.kt   history catch-up for configured upstreams
    DynamicSync.kt      relaySource streams: discover, fan out, sync each relay
    DeleteMissingSync.kt  the deleteMissing path: reconcile both ways, delete retractions
    UpstreamPush.kt     dir = up: reconcile and publish what the upstream lacks
    SyncBands.kt        covered created_at bands per (relay, filter)
    config/             the declarative side
      RouterConfig.kt     the stream model (streams, directions, sync modes)
      RelaySourceConfig.kt  the relaySource model (sources, selects, bindings)
      RouterConfigLoader.kt HOCON `streams { }` parsing (strfry-shaped)
    discovery/          which relays to dial, and what to believe about them
      RelayDiscovery.kt   pulling relay urls out of stored events
      HostStrikes.kt      per-authority strikes and eviction
      Unreachability.kt   which failures may be published as NIP-66 records
    progress/           observability
      StreamPhases.kt     per-stream progress reporting
      PagingProgress.kt   time-axis progress for paged walks
  maintenance/          background jobs behind the server
    ExpirationSweeper.kt  NIP-40
    ParseAudit.kt       what quartz could not parse, grouped to a JSON report
    SchemaDeploy.kt     the every-boot Vespa schema deploy
    TrustReconcile.kt   the startup trust-projection repair
    FtsReindex.kt       REINDEX_FTS_ON_START
    OrphanScoreSweep.kt SWEEP_ORPHAN_SCORES_ON_START
  util/Format.kt        fmtDuration / fmtDay / fmtCount

relay/src/main/resources/
  index.html            the search UI's markup + styles; its behavior lives in web/
  web/                  the page's native ES modules, served at /web — no build
                        step, zero dependencies. app.js is state + wiring;
                        shared/ is the client + codec + caches; entity.js
                        renders /npub1…//note1…//naddr1… paths; cards/ is the
                        kind registry — one renderer module per family, a
                        generic floor for the rest, and a render test that
                        FAILS if a kind registers without a fixture.
                        index.html's header records the rules and why "one
                        file" ended.
  kind_stats.html       self-contained operator diagnostics — deliberately NOT
  observer_stats.html   on the module graph; each carries its own tiny client
```

`README.md` documents every environment variable and the router config format.
It is the reference; this file is the orientation.

## The router, in one pass

`SyncEngine` syncs upstream events into the store. Operators know this
subsystem as **the router** — `router.conf`, the `router:` log prefix and the
`router` package keep that name. Its env vars are `SYNC_*`; the pre-rename
`ROUTER_*` spellings still work and warn on boot. Two kinds of stream:

- **static** — relays listed in `urls` in `router.conf`
- **dynamic** — relays discovered from stored events via `relaySource` (NIP-65
  outbox lists, NIP-85 provider lists, relay hints)

Each stream declares **how** it asks for what it is missing, via `sync`:

| mode | when | why |
|---|---|---|
| `negentropy` | the same event lives on many relays (kinds 0/3/10002) | reconcile id sets, transfer only the difference |
| `fetch` | the two sides barely overlap, or the store is empty and there is nothing to compare against | comparing disjoint sets costs more than downloading, and builds a huge local id snapshot to do it |
| `auto` | unknown | reconcile once WE hold more than `SYNC_NEG_MIN_EVENTS` for the filter, else page. Only our own count — asking the relay too meant a NIP-45 COUNT per relay per cycle, and COUNT is optional, widely unimplemented, and slow where it exists |

**It is a property of the data AND of how the stream asks — not of the relay,
and not measurable from counts.** NIP-85 assertions were the standing example of
`fetch` here: per-provider, millions each, no overlap. Asked per (relay,
provider) instead of by kind, the same data overlaps almost entirely and
`negentropy` is right. Narrowing the ask inverted the answer, so re-derive it
when a stream's filter changes shape rather than trusting the label.

**Sync bands** (`SyncBands`) record the `created_at` span already walked for
a `(relay, filter)` pair, so a re-run asks only outside it. Keyed by the *whole
filter* deliberately: edit a stream's filter and its band is invalidated, which
is the intended way to force a re-walk. A paged fetch records `complete = false`;
only a finished negentropy reconcile records `complete = true`.

**Known open bug:** a band holds one span for every kind in the filter, so a
long-lived kind (0) vouches for a short-lived one (30382) and `legs()` skips the
interior. The fix is per-kind spans *inside* the filter-keyed band — not
per-kind keys, which would break the invalidation property above. Still
unfixed; it stopped biting only because `assertions` narrowed to a single kind,
so any multi-kind filter can walk into it again.

## Instrumentation — use it before theorising

Most of this exists because something was diagnosed wrongly from inference.
Reach for it first.

- **health line** (once a minute) — heap, ingest queue depth vs capacity, ev/s,
  relays transferring, connected, fatal count, events lost to store errors. A
  full queue and an empty queue are opposite diagnoses that look identical
  everywhere else.
- **`SYNC_WIRE_LOG`** — `sent` logs every REQ/CLOSE; `full` adds every message
  received. Empty still logs `NOTICE`, `CLOSED` and failed sends, which are the
  relay explaining itself. It lowers quartz's log floor itself, because
  `QUARTZ_LOG_LEVEL=WARN` would otherwise silently discard its own output.
- **`SYNC_STREAMS`** — run one stream alone, so a measurement isn't three
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
- **JitPack caches builds per group-spelling.** `com.github.NosFabrica` and
  `com.github.nosfabrica` are separate cache entries for the same repo; one
  can permanently hold a failed infra build while the other serves fine. The
  store coordinate uses lowercase for this reason — check the other spelling
  before concluding a commit "doesn't build".
- **Two KDoc blocks in a row** fail ktlint (`standard:kdoc`, "dangling toplevel
  KDoc"). Each doc needs its own declaration.
- **`grep` may be aliased to `ugrep`**, which silently returns nothing on some
  large files. Use `/usr/bin/grep` when a search "finds nothing" implausibly.
- **`\n` inside a Kotlin raw string is literal**, which breaks HOCON fixtures in
  tests. Use real line breaks.
- **A timeout is an idle window, not a deadline.** quartz's accessory APIs
  reset their clock on every message (`1622bd7109`); code here must match. A
  deadline measured from send counts time spent QUEUED behind other work, so a
  relay steadily answering looks like one that refused. Measured on one relay,
  146 asks: a 15s deadline scored 55 answered and 91 "timed out" with a median
  answer of 75ms — and **zero** of those 91 were ever refused. Size an idle
  window by the slowest SINGLE answer, not by the queue.
- **Verify under load, not while idle.** A schema fix was "confirmed" by counting
  zero rejections during a window with no writes flowing. It was the wrong fix.
- **When editing quartz/amethyst alongside this repo**, that project *is*
  multiplatform: commas in backticked test names break Kotlin/Native, and
  `java.util` APIs (`toSortedMap`) break every non-JVM target. Compile more than
  the JVM target.

## Operations

`docker-compose.yml` runs Vespa and the relay. Vespa holds ~50M events in this
deployment.

**The trust reconcile runs in the background.** It used to be
`runBlocking { reconcileTrustWithRetry(store) }` between opening the store and
starting the listener — no websocket, no router and no NIP-11 until it finished,
measured at 12+ minutes on this corpus with Vespa at 356% CPU. It now runs
behind the server: the relay serves immediately and ranked search returns less
until the reconcile lands. `TRUST_RECONCILE_ON_START=false` skips it entirely.

If a start still looks slow, check that before blaming Vespa's transaction-log
replay — that wrong attribution is already in this file's history.

The relay **deploys the bundled Vespa schema on every boot** (`AUTO_DEPLOY`,
default true), so the cluster always matches the code talking to it. A no-change
deploy is a cheap no-op. This is not decoration: when the schema drifted, Vespa
answered every write with `Status 400 ... Field 'name_parts' is not defined` and
the router counted, dropped and carried on — 2,336,288 good events lost in one
run, unrecoverable, while every status line read healthy.

Structural rejections are surfaced separately from ordinary ones on the health
line (`N event(s) LOST to store errors`), because a bad signature is the event's
fault and dropping it is correct, whereas a batch failing structurally is ours.

**Two levers delete data.** Both default to a dry run that reports and removes
nothing; run that first, and read the number before believing it.

- `SWEEP_ORPHAN_SCORES_ON_START` — kind-30382 cards from providers no stored
  10040 names. On this deployment that was 33.8M of 51.9M, and the dry run
  predicted the figure exactly.
- `deleteMissing` on a stream — records the upstream no longer serves. The
  licence is a COMPLETED reconcile, never a volume: quartz throws rather than
  falling back, so a normal return means every window was compared and an empty
  answer is the relay's answer rather than its silence. There is deliberately no
  size guard, because a mass retraction is exactly the case that matters.

The counterpart to both: a deletion is not a tombstone. A stream that still asks
by kind re-downloads whatever was freed on its next walk, so reclaiming space and
narrowing the ask are one job.
