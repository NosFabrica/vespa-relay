# Working on vespa-relay

A Nostr relay with trust-ranked NIP-50 search. Quartz's protocol engine
(`RelayServerBase`) over a [vespa-eventstore](https://github.com/NosFabrica/vespa-eventstore)
store, plus a router that mirrors events from upstream relays.

Three Gradle modules, JVM only (toolchain 21), two processes over one store:

- **`:relay`** — the serving side. `RelayMain` is its entrypoint.
- **`:sync`** — the router, as its own process so it restarts without the
  relay or Vespa noticing. `SyncMain` is its entrypoint; the package keeps the
  `router` name on purpose (see below).
- **`:common`** — only what both genuinely read: `RelayIdentity`,
  `SchemaDeploy`, `QuartzLogLevel`, `fmtDuration`, and `ServingPressure` —
  whose mean crosses the process boundary over the relay's `GET /pressure`,
  polled by the sync process to yield ingest when client reads slow down.
  Anything one process owns lives in that process's module.

## Commands

```bash
./gradlew build                    # compile + test + spotless check, all modules
./gradlew :relay:test              # serving-side tests
./gradlew :sync:test               # router tests (moved with the module)
./gradlew :sync:test --tests "*SyncBands*"
./gradlew spotlessApply            # fix formatting — do this before committing
./gradlew :relay:run               # the relay, locally (needs a Vespa at VESPA_URL)
./gradlew :sync:run                # the router, locally (adds SYNC_CONFIG_FILE)

node tools/webtest/run.mjs         # web UI module tests (plain node, no deps)

docker compose up -d --build relay # the usual dev loop (serving only)
docker compose --profile sync up -d --build   # …with the mirror
docker compose --profile sync restart sync    # new router.conf, relay untouched
docker compose logs relay --since 5m
docker compose logs sync --since 5m
```

Git hooks are installed by the build: **pre-commit runs `spotlessCheck`,
pre-push runs the tests**. A commit will be rejected for formatting alone, so
run `spotlessApply` first.

## Layout

All three modules share the `com.nosfabrica.vespa.relay` package root — files
moved between modules in the process split without renaming packages, so
history and imports stayed put.

```
common/src/main/kotlin/com/nosfabrica/vespa/relay/
  config/RelayIdentity.kt   RELAY_NSEC — NIP-11 self, NIP-42, NIP-66 monitor;
                            both processes read it (same key on purpose)
  server/ServingPressure.kt EWMA of client read latency. The relay record()s
                            into it and serves it on GET /pressure; the sync
                            process adopt()s the polled mean and yields on it
  maintenance/
    QuartzLogLevel.kt       QUARTZ_LOG_LEVEL, split from ParseAudit — the one
                            piece of it both processes read
    SchemaDeploy.kt         the every-boot Vespa schema deploy (both processes)
  util/Format.kt            fmtDuration — the one formatter both processes print

relay/src/main/kotlin/com/nosfabrica/vespa/relay/
  RelayMain.kt          entrypoint; reads env, deploys the schema, wires the
                        serving side. REFUSES to boot if SYNC_CONFIG* is set —
                        the mirror is :sync's process now
  config/
    EnvSettings.kt      NIP-11 limits etc. from env, via `env.intOr(...)` rather
                        than `env["..."]` — grep for both or you will conclude a
                        working setting is dead
    PubKeys.kt          npub-only parsing for every pubkey setting
  server/               the serving side
    NostrRelayServer.kt the IEventStore-backed relay backend; installs StoreQueryContext
    HttpServer.kt       serveRelay: Ktor server + routes, Nip11Info, /pressure
    RelayInfo.kt        the NIP-11 document
    RelayWebSocket.kt   the ws route
    Nip86Route.kt       the management API
    BanListFile.kt      NIP-86 ban state that outlives the container
    ConnectionCountListener.kt  LOG_CONNECTIONS
  maintenance/          background jobs behind the server
    ExpirationSweeper.kt  NIP-40
    TrustReconcile.kt   the startup trust-projection repair
    FtsReindex.kt       REINDEX_FTS_ON_START
    OrphanScoreSweep.kt SWEEP_ORPHAN_SCORES_ON_START

sync/src/main/kotlin/com/nosfabrica/vespa/relay/
  maintenance/ParseAudit.kt   what quartz could not parse, grouped to a JSON
                              report — lives here because ingest is what feeds it
  util/SyncFormat.kt          fmtDay / fmtCount / nowSeconds, internal again
  router/               the mirror (see below)
    SyncMain.kt           entrypoint; env, store, engine, block
    SyncEngine.kt         wiring, live tails, health/stats lines
    PressurePoller.kt     polls the relay's /pressure into ServingPressure
    IngestPipeline.kt     bounded queue -> verify -> batchInsert, poison isolation
    BisectingInsert.kt    the batch-bisecting write
    StaticBackfill.kt     history catch-up for configured upstreams
    DynamicSync.kt        relaySource streams: discover, fan out, sync each relay
    DeleteMissingSync.kt  the deleteMissing path: reconcile both ways, delete retractions
    UpstreamPush.kt       dir = up: reconcile and publish what the upstream lacks
    SyncBands.kt          covered created_at bands per (relay, filter)
    TorTransport.kt       SYNC_TOR_SOCKS: the second OkHttp client, chosen per
                          url, whose .onion names resolve INSIDE the proxy
    config/               the declarative side
      RouterConfig.kt       the stream model (streams, directions, sync modes)
      RelaySourceConfig.kt  the relaySource model (sources, selects, bindings)
      RouterConfigLoader.kt HOCON `streams { }` parsing (strfry-shaped)
    discovery/            which relays to dial, and what to believe about them
      RelayDiscovery.kt     pulling relay urls out of stored events
      HostStrikes.kt        per-authority strikes and eviction
      Unreachability.kt     which failures may be published as NIP-66 records
    progress/             observability
      StreamPhases.kt       per-stream progress reporting
      PagingProgress.kt     time-axis progress for paged walks

relay/src/main/resources/
  index.html            the search UI's markup + styles; its behavior lives in web/
  web/                  the page's native ES modules, served at /web — no build
                        step, zero dependencies. app.js is state + wiring;
                        shared/ is the client + codec + caches; searchfield.js
                        is the search box itself — a contenteditable, because
                        `from:npub1…`/`to:npub1…` draw as a face and a name and
                        an <input>'s value is characters and nothing else, with
                        shared/query.js the ONE tokenizer both it and the query
                        builder ask — it also lifts `#hashtag` out of the NIP-50
                        search, which app.js turns into the three ways Nostr
                        writes a topic: `#t`, a kind-1111 comment naming it in
                        `i`/`I` (NIP-73), and a `#l` label under `#L: #t`
                        (NIP-32), all ORed in one REQ; entity.js
                        renders /npub1…//note1…//naddr1… paths; cards/ is the
                        kind registry — one renderer module per family, a
                        generic floor for the rest, and a render test that
                        FAILS if a kind registers without a fixture, a badge
                        label or a family tone. The NIP-51 lists and sets are
                        one table in cards/lists.js rather than one renderer
                        each: they differ only in which tags carry their items.
                        index.html's header records the rules and why "one
                        file" ended.
  kind_stats.html       operator diagnostics, each carrying its own tiny relay
  observer_stats.html   client on purpose: they must work when the app does
                        not. kind_stats reads shared/kinds.js for the kinds to
                        count — that list IS "which kinds do we support", and a
                        second copy would go stale in the direction that hides
                        events. The CLIENT is what stays self-contained; a
                        table of integers is not worth duplicating
```

`docs/configuration.md` documents every environment variable and
`docs/router.md` the router config format. They are the reference; this file is
the orientation.

## The router, in one pass

`SyncEngine` syncs upstream events into the store. Operators know this
subsystem as **the router** — `router.conf`, the `router:` log prefix and the
`router` package keep that name. Its env vars are `SYNC_*`; the pre-rename
`ROUTER_*` spellings still work and warn on boot.

It is its own process (`SyncMain`, the compose `sync` service behind
`--profile sync`), so a `router.conf` change is a `restart sync`, never a
relay outage — and the sync bands make the re-run resume rather than
re-download. The relay hard-errors if `SYNC_CONFIG*` is aimed at it: that
setting used to start the mirror in-process, and accepting-but-ignoring it
would be a mirror that quietly stopped mirroring. Two kinds of stream:

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

**`.onion` upstreams go through Tor, chosen per url** (`TorTransport`,
`SYNC_TOR_SOCKS`). quartz's socket builder takes
`(NormalizedRelayUrl) -> OkHttpClient`, so hidden services get the proxied
client and clearnet keeps the direct one — routing a 20k-relay cycle through
Tor to reach the handful on it would trade the fan-out for the exception. The
mechanism is OkHttp's: given a SOCKS proxy it hands the hostname over instead
of calling `Dns.lookup`, so the name resolves inside Tor. Never give that
client a `Dns` — it would move resolution back out here, breaking `.onion` and
leaking every hidden service we sync with to the local resolver. Two rules
follow from the same place: the TCP pre-probe is skipped for onion urls (it is
a DNS lookup, and it would publish `UnknownHostException` as proof of a dead
relay), and nothing negative is ever published about one — reaching a hidden
service depends on our circuit as much as on their server.

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
  `resolutionStrategy { force(libs.quartz) }` in EVERY module's build file —
  each of `:common`, `:relay` and `:sync` resolves quartz independently, and a
  new module must add its own force. Never remove one, and check that a pin
  actually took effect.
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

`docker-compose.yml` runs Vespa, the relay, and (behind `--profile sync`) the
sync process and a client-only Tor — four containers, one store. Vespa holds
~50M events in this deployment. The JVM memory budget is per process: the sync
container carries the large limit because the negentropy id snapshots live
there; check the machine's total against those three limits when the profile is
on (Tor's rounds to nothing).

**Tor is a container, not a layer.** One image carries both JVM processes on
purpose, so installing a daemon in it would put Tor in the serving relay too,
give the container a second thing to supervise, and make `restart sync` — a
`router.conf` edit, meant to be cheap — drop every circuit and re-bootstrap.
Its SOCKS port is published nowhere: an open SOCKS proxy on a public interface
is an open proxy.

**Clients-first crosses the process boundary by HTTP.** The relay measures
client read latency into `ServingPressure` and serves the mean on
`GET /pressure`; the sync process polls it (`SYNC_PRESSURE_URL`) and yields
ingest between batches past the threshold. Three failed polls reset the
throttle — a relay that is down has no clients to protect — and both the feed
being off and the feed being lost are said out loud in the sync log.

**The trust reconcile runs in the background.** It used to be
`runBlocking { reconcileTrustWithRetry(store) }` between opening the store and
starting the listener — no websocket, no router and no NIP-11 until it finished,
measured at 12+ minutes on this corpus with Vespa at 356% CPU. It now runs
behind the server: the relay serves immediately and ranked search returns less
until the reconcile lands. `TRUST_RECONCILE_ON_START=false` skips it entirely.

If a start still looks slow, check that before blaming Vespa's transaction-log
replay — that wrong attribution is already in this file's history.

Both processes **deploy the bundled Vespa schema on every boot** (`AUTO_DEPLOY`,
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
  size guard, because a mass retraction is exactly the case that matters. It is
  scoped by `ownedKinds` (required): the rest of the filter is mirrored from the
  same relay and dropped only when a service's whole owned set is retracted —
  measured, no NIP-85 provider relay serves its own key's kind 0, so judging
  those by absence would delete every healthy provider's profile.

The counterpart to both: a deletion is not a tombstone. A stream that still asks
by kind re-downloads whatever was freed on its next walk, so reclaiming space and
narrowing the ask are one job.
