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
docker compose --profile onion up -d          # …with the relay's own .onion
docker compose --profile sync restart sync    # new router.conf, relay untouched
docker compose logs relay --since 5m
docker compose logs sync --since 5m

# A cloud sandbox has the docker binaries but NO daemon running. Start one
# before anything that needs it, and lower the API floor or testcontainers
# cannot talk to it (see the trap below):
DOCKER_MIN_API_VERSION=1.24 dockerd > /tmp/dockerd.log 2>&1 &
```

Bumping `vespaEventStore` is not done until the store's OWN integration gate has
run against the commit being pinned — the relay's tests use `InMemoryEventIndex`
and cannot see a Vespa-only regression:

```bash
git clone https://github.com/NosFabrica/vespa-eventstore && cd vespa-eventstore
git checkout <the pinned commit>          # test what the relay actually resolves
TESTCONTAINERS_RYUK_DISABLED=true ./gradlew :benchmark:test -Pintegration --no-daemon
```

Six ITs, ~7 min total, each standing up a real Vespa. Fetching that repo works
here; pushing to it does not (the git proxy only holds a credential for repos in
the session's sources).

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
    RelayAddresses.kt   the OTHER addresses this relay answers at — the .onion
                        Tor publishes into RELAY_ONION_HOSTNAME_FILE, read on
                        demand because the address is minted after we boot
  server/               the serving side
    NostrRelayServer.kt the IEventStore-backed relay backend; installs StoreQueryContext
    MultiAddressAuthPolicy.kt  NIP-42 for a relay with two front doors: a Tor
                        client signs the .onion it dialled, and quartz's
                        OptionalAuthPolicy binds exactly one url
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
    IngestPipeline.kt     bounded queue -> dedup -> verify -> batchInsert, poison
                          isolation. Dedup FIRST is the point: a schnorr check is
                          ~48us and a mirror is offered the same event once per
                          relay holding it
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
                        shared/query.js the ONE tokenizer the field asks — and
                        the REQ builder too: buildFilters() turns `#hashtag`
                        into the three ways Nostr writes a topic (`#t`, a
                        kind-1111 comment naming it in `i`/`I` per NIP-73, and a
                        `#l` label per NIP-32), ORed in one REQ, with the page
                        state passed IN so the whole thing is testable —
                        tools/webtest/query.test.mjs asserts the filters, and
                        RelayProtocolTest asserts the relay answers them;
                        shared/parents.js answers "in reply to WHO" — NIP-10's
                        rule for which `e` tag is the parent, plus the by-id
                        lookup for the author when the tag carries no hint;
                        entity.js
                        renders /npub1…//note1…//naddr1… paths, with related.js
                        the second ask a git permalink makes AFTER its card is
                        up — a repository's state, issues, patches and releases
                        (`#a` its address), an issue's or a patch's verdict and
                        thread (`#e` its id). Never awaited by the paint and
                        silent on failure: the page is complete without it. The
                        cards it draws are the SAME cards at preview depth, so
                        they click, walk under j/k and toggle their json for
                        free — app.js delegates all three off #results; feed.js is the
                        latest feed — three cards under the hero for a signed-in
                        reader, a hundred at /?feed=1 — and is an EMPTY SEARCH:
                        buildFilters() with no words and none of the bar's
                        NIP-50 extensions leaves `{kinds, limit}`, a plain
                        NIP-01 read, which is the only shape the store answers
                        newest-first (a stray `sort:` would rank it while the
                        page said "latest"), so the feed hides the Filters
                        disclosure — but NOT the kind chips, which are the
                        `kinds` of that same read and narrow the feed under the
                        hero and at /?feed=1&tab=media alike, replacing the
                        content default rather than intersecting it (four of
                        the seven narrowing chips share no kind with it); what
                        is
                        feed.js's own is the shaping — replies, future dates and
                        duplicates never reach a card; cards/ is the
                        kind registry — one renderer module per family, a
                        generic floor for the rest, and a render test that
                        FAILS if a kind registers without a fixture, a badge
                        label or a family tone. The NIP-51 lists and sets are
                        one table in cards/lists.js rather than one renderer
                        each: they differ only in which tags carry their items.
                        cards/code.js is the one that PARSES: NIP-34 puts `git
                        format-patch` output in a patch's content, so the mail
                        is taken apart (subject, series marker, commit message,
                        diff) rather than dumped — its title used to be git's
                        own `From <sha> Mon Sep 17 00:00:00 2001`. Code clips
                        by LINES there, never by characters: `clip()` trims, and
                        indentation is the code.
                        index.html's header records the rules and why "one
                        file" ended.
                        readiness.js is the panel under the box that answers
                        "can this relay rank for ME yet" — the store treats the
                        reader's lens as a FILTER, so a signed-in reader whose
                        trust chain has not been mirrored here gets an EMPTY
                        search rather than a degraded one, and the page said
                        nothing about it. It walks the chain the router walks
                        (your 10002 → your 10040 → your provider's 30382s → a
                        ranked read that comes back), reports the FIRST broken
                        link and leaves the rest `waiting`, and shows here/there
                        as a percentage — observer_stats.html's arithmetic for
                        one reader's own row — only where a denominator honestly
                        exists. An import within a rounded 90% of done counts as
                        done and says NOTHING: the tail is the accounts the
                        provider scored lowest, and "importing — 99%" nags
                        somebody whose search is already complete. A `ready`
                        verdict is then kept in a week-long cookie, so the seven
                        round trips are not re-paid by a reader with nothing
                        left to learn. shared/readiness.js is the decision, pure
                        and tested; readiness.js is the asks and the words. Its one
                        action is for the failure that cannot fix itself: with
                        no 10002 stored NOTHING discovers you, ever, so the
                        panel takes a relay url, reads your three lists off it
                        and republishes them here VERBATIM for the next cycle to
                        find. Signing in enrols you in nothing — the server's
                        `onObserver` hook is wired to nothing, whatever older
                        comments claimed.
  observer_stats.html   an operator diagnostic carrying its own tiny relay
                        client on purpose: it must work when the app does not,
                        and asking the way a client would means it also TESTS
                        what it asks
  stats.html            the corpus dashboard, and the ONE page here with no
                        relay client: it charts GET /stats.json and nothing
                        else. Neither reason observer_stats carries one applies
                        — no aggregation on it is a protocol feature to test,
                        and thirty days of distinct-pubkey counts is not a
                        question to ask over a websocket. THE JSON IS THE
                        ARTIFACT; the page is one reader of it (see
                        maintenance/StatsRollup).
                        Its Kinds table REPLACED kind_stats.html, whose url now
                        301s here. That page asked one NIP-45 COUNT per kind it
                        already knew to name — shared/kinds.js plus whatever an
                        operator typed in — so a kind nobody had registered was
                        invisible on the only page that would have revealed it.
                        A grouping histogram enumerates instead, which is why
                        the table is EVERY kind and not a top-N. What went with
                        that page: it exercised NIP-45 over the relay's own
                        websocket. Nothing here speaks the protocol; if that
                        check is wanted back it wants to be a test, not a page
```

The statistics rollup lives in `relay/maintenance/` beside the other background
jobs, and talks to Vespa directly rather than through the store:

```
relay/src/main/kotlin/com/nosfabrica/vespa/relay/
  maintenance/
    StatsYql.kt      the grouping pipelines and the readers for what comes
                     back — pure, so both halves are tested against captured
                     engine output rather than against an assumed shape
    StatsVespa.kt    POST to /search/, refuse a degraded answer (the same
                     coverage question the store's SearchCoverage asks, and
                     for the same reason: `full` answers it in neither
                     direction)
    StatsRollup.kt   the document, section by section, each failing on its own
  server/
    StatsSnapshot.kt what GET /stats.json serves — held in memory with an
                     ETag, written through to STATS_FILE so a deploy does not
                     blank the page for the minutes a first rollup takes
```

Four of the pipelines are `EventYql`'s own shapes, reused verbatim because this
deployment has already run them; the rest extend them along `created_at`. It
does NOT build on `EventYql` itself — `grouping()` is private, its pipelines are
a fixed set, and it ships in vespa-eventstore, so every new chart would cost a
store release plus a JitPack pin bump. The duplication is the WHERE clause and
nothing else.

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

**Sync bands** record the `created_at` span already walked for a
`(stream, filter, relay)` triple, so a re-run asks only outside it. Keyed by the
*whole filter* deliberately: edit a stream's filter and its band is invalidated,
which is the intended way to force a re-walk. Keyed by the *stream* too: two
streams asking one relay the same filter walk it at their own moments and to
their own depths, and neither may resume from the other's claim. A paged fetch
records `complete = false`; only a finished negentropy reconcile records
`complete = true`.

**The arithmetic is quartz's** — `SyncCoverage`, in
`nip01Core.relay.client.accessories`, beside `fetchAllPages` and
`negentropySyncOrFetch`. `SyncBands` here is only the file: `SYNC_STATE_FILE`,
the flush thread, the env names. It used to own a copy of the whole algorithm,
and that fork silently missed two upstream fixes — a relay needing NOTHING was
widening the shared snapshot to the full filter, and a `complete` band was not
re-opening its older leg when the caller's floor dropped below it. **Fix band
behaviour upstream, not here**, or the next quartz bump reverts it. The
on-disk shape is this repo's and is pinned by a test: quartz keys a band
`"<relay> <filter>"` in memory, and `SyncBands` takes that apart to write
`{stream: {filter: {relay: {min, max, complete, fullAt, spans}}}}`, splitting at
the FIRST space and rejoining with one. The stream level is this repo's too —
quartz knows nothing about streams, so `SyncBands` holds one `SyncCoverage` per
stream name.

**Both state files are now READ by the relay**, off the `/var/lib/vespa-relay`
mount both containers share, and charted as the *Sync coverage* card on
`/stats.html` — see `SyncCoverageReport`. The router is still the only writer.
Three traps if you touch either format: both files nest **stream → filter →
relay**, and the sweep file's filter also strips `since`/`until`/`limit` (time is
what a sweep varies) while the band file's keeps them, so joining the two still
means reducing both to a common shape; a band's `min`/`max` are the outer edges
across every kind — the card draws the per-kind *intersection* on top, which is
the multi-kind bug below made visible rather than charted as coverage; and **one
stream is many filters**. A `relaySource` whose select binds `authors` narrows
the filter per discovered relay (`DiscoveredRelay.narrowed`), and `authorsPerLeg`
chops that again, so a stream configured as one filter reaches the file as
thousands of them, all under its one name. The report groups by that name,
publishes the members every leg *agrees on* (never `authors`/`ids`/tag values —
they are named in `narrowedBy` instead), and merges the legs that land on one
relay (edges union, `complete` ANDs).

**A file written before the format nested still loads.** Its flat keys name no
stream, so both writers hold them aside and hand each to the first stream that
asks for that (relay, filter) — the stream that wrote it — and write back
whatever is still unclaimed, flat, so a restart does not lose it. Grep
`MIGRATION SHIM`: three blocks in `SyncBands`, three in `SweepState`, two in
`SyncCoverageReport`, all deletable together once every deployment has booted
once on a build that writes the nested shape.

**Windowed reconciliation** (`NegentropyPager`) is the layer *above* a single
reconcile call, and the division of labour with quartz is the thing to
understand before touching it. It moved once already: quartz used to take a
materialised id list and know nothing about our side, so this class did the
pre-split, sniffed the peer's cap off the wire, and re-tried windows around a
dense second. All three now live upstream (`NegentropyLocalIndex`,
`targetWindow`, `NegentropySyncResult.peerCap`, `onUnreconcilableWindow` — see
amethyst#3871), and this class shrank to what survives a call:

- **the cursor** (`SweepState`), written per finished window under
  `{stream: {filter: {relay: {downTo, upTo, at}}}}`, because quartz forgets
  everything between calls and a band is only recorded per leg;
- **the per-peer window size**, learned from `peerCap` across syncs and
  persisted, so a restart does not re-walk the ladder;
- **the order windows are walked in** — strictly newest-first, which is what
  keeps the finished region contiguous, which is the only reason the cursor can
  be one timestamp instead of a set of intervals. Every push preserves it; check
  that before adding one.

Everything inside a call is quartz's: splitting a window it cannot reconcile,
bounding what it reads from the index (`PrimedIndex` hands it the count this
layer already took, so the same window is not counted twice), and draining a
second no window size will fit through the hook this class passes it. Engaged
automatically by `StaticBackfill` once our own count passes
`SYNC_NEG_PAGE_TARGET`; the dynamic fan-out deliberately still shares one
snapshot across its 16k relays, where per-peer windowing would multiply the
store work by the fan-out.

**Known open bug (now upstream's):** a band holds one span for every kind in
the filter, so a long-lived kind (0) vouches for a short-lived one (30382) and
`legs()` skips the interior. The fix is per-kind spans *inside* the
filter-keyed band — not per-kind keys, which would break the invalidation
property above. Still unfixed; it stopped biting only because `assertions`
narrowed to a single kind, so any multi-kind filter can walk into it again.

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
  `proj.write`, `versions`, plus the router's own `verify` and `dedup.pre`).
  This is what identified a projection read-back as 90% of ingest. Signature
  verification was NOT on this line for as long as it existed, so "is verify
  the limit?" was a question no instrument here could answer; anything else
  added to the ingest path belongs on it for the same reason.
- **paging progress** — percentage and ETA measured on the *time axis*, because
  a paged fetch has no event denominator. Its predecessor computed
  `downloaded/downloaded` and printed `100%, ETA ~0:00` for hours.
- **`IngestCostBench`** — what one arriving event costs ingest, split by the
  verdict it ends on, end to end through the real pipeline against a real
  Vespa. Skipped unless `BENCH_VESPA_URL` names a live engine:
  `BENCH_VESPA_URL=http://localhost:8080 BENCH_N=20000 ./gradlew :sync:test
  --tests '*IngestCostBench*' --rerun-tasks -i` (Gradle treats env vars as
  invisible, so without `--rerun-tasks` a second run is silently UP-TO-DATE and
  prints the FIRST run's numbers).

  Measured on a 4-core box sharing its cores with the engine, 72k-doc corpus,
  20k-event batches — the ratios are what travel, not the absolute times:

  | arriving event | µs/event | where it goes |
  |---|---:|---|
  | fresh (written) | 603 | `write` 77%, `verify` 13% |
  | duplicate, no probe | 56 / 49 | `verify` 67% of the work |
  | duplicate, probe on | 21 / 16 | `dedup.pre` only — verify gone |
  | stale replaceable | 158 | `versions` 38%, `verify` 32% |
  | newer replaceable (written) | 1191 | `write` 66% — read-then-supersede |

  Two things to read off it. **Verification is not a rounding error** — 13% of
  a fresh batch and two thirds of a duplicate one — and it costs ~70-95µs in
  situ against ~48µs isolated, because the router competes with Vespa for the
  same cores. And **the id probe cannot see a stale replaceable version**: a
  newer generation of a kind 0/3/10002/30382 has a different id, so it is
  verified in full and only then rejected as `replaced`. Pricing a supersession
  pre-filter for it: the batched version read costs 29µs/event, so it pays for
  itself once **~20%** of replaceable arrivals are stale and is only worth the
  build past ~60-70%. Which of those an operator is in is readable off
  `rejectionBreakdown()` on the stats line and nowhere else.

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
- **An integration run that skips still prints `BUILD SUCCESSFUL`.** Three
  things stack up to make "I ran the ITs" a false claim, and none of them
  announces itself:
  - A cloud sandbox ships `docker`/`dockerd` but runs no daemon. `docker info`
    fails with "is the daemon running?" — start `dockerd` yourself.
  - Docker 29 refuses API versions below 1.40; testcontainers 1.20.4 (what the
    store's ITs use) speaks 1.32 and gets `400 client version 1.32 is too old`.
    Every IT then hits its own `assumeTrue(dockerAvailable())` and aborts.
    Start the daemon as `DOCKER_MIN_API_VERSION=1.24 dockerd`.
  - The Gradle daemon inherits the environment it STARTED with, so env vars set
    on the `./gradlew` command line never reach the forked test JVM if a daemon
    from before is reused. Use `--no-daemon` when the env is the point.

  A skip is invisible in console output. Read `skipped="…"` in
  `build/test-results/test/*.xml`, or `--rerun-tasks` and check the wall time:
  six real Vespa ITs take ~7 minutes, not 7 seconds. Testcontainers logs the
  actual reason through SLF4J, and with no binding on the test classpath that
  goes to a NOP logger — put `logback-classic` on a probe classpath and call
  `DockerClientFactory.instance().isDockerAvailable()` directly to see it.
- **JitPack caches builds per group-spelling.** `com.github.NosFabrica` and
  `com.github.nosfabrica` are separate cache entries for the same repo; one
  can permanently hold a failed infra build while the other serves fine. The
  store coordinate uses lowercase for this reason — check the other spelling
  before concluding a commit "doesn't build".
- **Two KDoc blocks in a row** fail ktlint (`standard:kdoc`, "dangling toplevel
  KDoc"). Each doc needs its own declaration.
- **Vespa's `time.date()` does not zero-pad.** Verified on 8.733: two documents
  nine months apart group as `"2025-1-5"` and `"2025-10-9"`. Unpadded values
  misorder as text wherever the digit count differs in the same position —
  November before February, the 15th before the 5th — but NOT everywhere, since
  `'-'` sorts below `'0'`, so `2025-1-5` beside `2025-10-9` comes out right. A
  chart can look correct for one window and interleave in the next, with every
  bar the right height and only the axis wrong. `StatsYql.isoDay` is the one way
  in; a test asserts the misordering on values chosen to actually exhibit it,
  because the pair the fixture happens to contain does not.
- **A nested grouping and a flat one answer in the SAME shape.** Vespa collapses
  an inner group list's aggregate onto the outer group, so
  `all(group(kind) each(output(count())))` and
  `all(group(kind) each(all(group(pubkey) output(count()))))` both come back as
  one leaf per kind carrying one `count()` — 79 and 35 for the same kind over
  the same corpus, from structurally identical responses. Nothing distinguishes
  them and neither errors: the only thing keeping events out of a column
  labelled users is calling the right builder.
- **Grouping expressions take ARITHMETIC**, which is usually the better bucket.
  `group(created_at / 604800)` renders as `div(created_at, 604800)` and yields a
  plain integer, so it inherits none of `time.date`'s padding problem and sorts
  correctly before anything formats it. Two uses worth knowing: weeks need
  `(created_at + 259200) / 604800`, because epoch second 0 is a THURSDAY and the
  un-shifted division buckets Thursday-to-Wednesday under a chart every reader
  will assume starts on Monday; and months, which cannot be an even division of
  seconds, fold into one sortable integer as
  `time.year(created_at) * 12 + time.monthofyear(created_at)` rather than a
  two-level `time.year`/`time.monthofyear` nest the readers would have to descend.
- **A coarser time bucket is not the finer one re-added.** Events sum across
  buckets; DISTINCT AUTHORS DO NOT — someone posting every day is one author in
  the week and seven in the sum of that week's days. Every granularity has to be
  asked of the engine at that granularity, which is why `activitySection`
  issues three pairs of queries instead of summing one.
- **A bare aggregate with no grouping level is not a query Vespa can answer.**
  `all(output(count()))` works, so the shape looks proven — but swap the
  aggregator and `all(output(max(created_at)))` fails with HTTP 500 and
  `Cannot invoke SingleResultNode.max(…) because "this.max" is null`, which
  reads like an engine bug and sends you looking in the wrong place. Anything
  other than `count()` needs a `group(...) each(output(...))` around it. The
  corpus-wide newest event is therefore DERIVED from the per-kind spans rather
  than asked for.
- **Freshness has to be bounded to the present.** `created_at` is author-signed,
  so an unbounded `max(created_at)` reports the corpus's most optimistically
  dated spam — a relay whose mirror died an hour ago reads as fresh "in 74
  years". Every `lastSeen` here carries `created_at <= now`, and the future-dated
  events are counted separately in `corpus.futureDated`, where they are the
  finding rather than the noise.
- **A nested grouping is ONE query where the obvious shape is N.**
  `all(group(kind) each(all(group(time.date(created_at)) each(output(count())))))`
  returns kind → day → count in a single response — the per-kind daily series
  was a query per kind before that. Note the inner `each()`: without one the
  inner list COLLAPSES onto the outer group (that is what `distinctAuthorsBy`
  relies on), so the same shape minus three characters silently turns a whole
  series into one distinct-day count.
- **Playwright's Chromium has no HTTP cache**, so a browser test can never
  demonstrate a 304: repeat fetches carry no `If-None-Match` and every response
  is a 200, including for `max-age=60` assets. Check conditional requests with
  `curl -H "If-None-Match: …"` against the running relay instead.
- **The bundled query profile is what makes any of this work.**
  `grouping.globalMaxGroups: -1` in `search/query-profiles/default.xml` is why a
  `max()`-less pipeline is legal at all, and the per-request
  `grouping.defaultMaxGroups`/`defaultMaxHits` are why one returns more than TEN
  groups. Omit those two parameters and a kind histogram comes back as a
  plausible-looking top-ten with no error anywhere.
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

`docker-compose.yml` runs Vespa, the relay, (behind `--profile sync`) the sync
process and a client-only Tor, and (behind `--profile onion`) a second Tor that
is the relay's own hidden service — one store throughout. Vespa holds
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

**Two Tors, because they are two jobs.** `tor` (profile `sync`) is the client
proxy the router dials `.onion` *upstreams* through. `tor-onion` (profile
`onion`) is the relay's own front door: one hidden service forwarding to the
relay's port, so any Tor client can reach it without the clearnet name, a
certificate or a public IP. Same image, different torrc, and wanted
independently — a serving-only box wants a front door and no mirror. Three
things about it are not obvious and each cost a config that would not start:

- **tor refuses a hostname in `HiddenServicePort`** ("Unparseable address in
  hidden service port configuration"), so the entrypoint resolves the compose
  service name itself and appends the line to a generated torrc. Docker hands a
  re-created container a new IP and nothing in Tor re-resolves it, so the same
  script watches the name and restarts the container when it moves — the key is
  on a volume, so the address survives and only the descriptor blinks.
- **the key IS the address.** `tor_onion` holds it, mounted into that container
  and nowhere else; the relay gets a second volume carrying the hostname alone,
  read-only. `docker compose down -v` is what changes the address.
- **the torrc is generated, so tuning is a mounted file** —
  `tor/onion.extra.conf.example`, appended last. Single-hop mode (half the
  latency, and a one-way door on that key), PoW defenses, introduction points:
  each is a decision, so each is off and documented rather than chosen here.
- **the relay has to know its own `.onion`**, because NIP-42 auth events are
  signed against the address the client dialled. Without it reads and publishes
  still work and every AUTH fails, which costs Tor clients their ranking lens
  and looks like nothing at all — the reason the address is handed over in a
  file rather than an env var someone pastes in later.
- **the clearnet endpoint advertises it** with `Onion-Location` on EVERY
  response, the websocket 101 included — a Nostr client may make no other
  request, so a header on `GET /` alone would never reach the clients this is
  for. Tor Browser shows the ".onion available" button; Amethyst caches the
  mapping and rewrites the host when Tor is on. It rewrites the TRANSPORT, not
  the relay's identity, so such a client signs its AUTH with the clearnet url
  on a connection that arrived through the onion: accepting any address we
  answer at, rather than binding the policy to the address dialled, is what
  makes that work.

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

**Real retractions are the third path, and the only one that leaves a
tombstone.** `contentViaOutbox` mirrors kinds 5 and 62 with the content, and the
store enforces them at insert — so the erase survives the next walk, which is
exactly what the two levers above cannot promise. The catch is that the store's
fast path caches "authors known to have a stored kind 5/62" **per instance**,
which is exact for one writer and wrong for this deployment's two: the relay's
copy never hears about what the router mirrored, and the router's never hears
about what a client published here. Both entrypoints therefore open the store
with `writers = STORE_WRITERS` (`SHARED_STRICT`), so NIP-09/NIP-62 are checked
against the store rather than a half-informed cache. **The topology is an
argument, not an env var** — it is a fact about this deployment that the library
cannot detect, and `:common` holds the one copy of the reasoning. `SHARED` only
bounds the window (its rebuild is a corpus-wide visit, hours here), and
`SINGLE_WRITER` is simply false for us.

The open cost is the guard read on the pure-record bulk path: it asks for every
tombstone by the batch's authors with no narrowing and no limit, so it scales
with one author's whole deletion history rather than with the batch. The mixed
path already narrows the same probe to the batch's own ids — until the record
path does too, a prolific deleter is the thing to watch on the `guards` stage.
