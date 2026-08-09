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

# Dials the five real indexer relays and reports how each ENDS an empty page
# (DRAINED / IDLE / CLOSED / …). Off by default, asserts nothing, and is the
# only thing here that can tell our reading of a relay apart from the relay.
./gradlew :sync:test --tests '*RealRelayDrainProbe*' -DrealRelayProbe=true -i
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
    IngestPipeline.kt     bounded queue -> dedup -> supersede -> verify ->
                          batchInsert, poison isolation. Dropping FIRST is the
                          point: a schnorr check is ~48us isolated and ~70-95us
                          in situ, and a mirror is offered the same event once
                          per relay holding it — and older VERSIONS of it from
                          the relays that never got the newest
    ProbeGate.kt          whether either drop-probe still earns its round trip,
                          learned from what it drops. Replaces telling ingest
                          which phase a stream is in
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
      RelayAliases.kt       which discovered urls are ONE relay (see below)
      AliasProbe.kt         the fingerprint: a relay's newest events, as ids
      AliasFolding.kt       apply() reads verdicts; measure() earns them
      AliasMonitor.kt       the schedule measure() runs on, off the sync cycle
      RelayAliasRecord.kt   the verdict as a signed NIP-66 30166 `same-as` tag
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

**Fixed, and worth knowing how.** A band used to hold one span for the whole
filter, so a long-lived kind (0) vouched for a short-lived one (30382) and
`legs()` skipped the interior. Upstream took per-kind spans *inside* the
filter-keyed band (amethyst#3862, picked up in `94e3136`) — not per-kind keys,
which would break the invalidation property above. Every stream in
`router.conf.example` is multi-kind, so nothing here is shielded by luck: the
`indexers` stream alone is `[0, 10002]` on the paged path, which is exactly the
combination quartz refuses to record a band for unless the caller threads
`observedByKind`.

**What a per-kind span means — and does not.** It is the envelope of the events
actually RECEIVED for that kind, not the window that was walked. One REQ carries
every kind in the filter, so a kind whose floor sits higher than the band's outer
`min` may just have started existing later, not "we stopped walking there" — the
two are indistinguishable from the band. The card draws that intersection on top
of the outer edges and labels it *evidence*, deliberately — see `stats.html`.

**Do not assume the leg below a floor is empty. It was measured, and it is not.**
`RealRelayDrainProbe` asked the five `indexers` relays for kind 10002 below the
exact floor each one's band carried, twice:

| relay | ending | below the floor |
|---|---|---|
| purplepag.es | `IDLE` both runs | 13 events, oldest `created_at` **0** |
| user.kindpag.es | `DRAINED` | 1 — the boundary second, re-read by design |
| directory.yabu.me | `DRAINED` (1,225,329 events; 120s only reaches 259,616) | a real backlog |
| profiles.nostr1.com | `DRAINED` | 1 |
| indexer.coracle.social | `DRAINED` (hung >12min once) | 1 |

The theory that cost a day here — relay lists postdate NIP-65, so those legs can
never return anything — is false: directory.yabu.me had 1.2M events below its
floor. It survived because nothing had dialled a relay to check it. Reach for the
probe before theorising about a floor.

A stuck floor means walks are not FINISHING, and the two ways differ. yabu.me was
real backlog and drains once allowed to run. purplepag.es never EOSEs — it serves
13 events, one stamped `created_at = 0` that `isPlausible` rightly refuses, then
goes quiet — so it can never earn a drain and its leg stays open by design. That
is the conservative branch working, not a bug to route around. `fetchAllPages`
returns a `PagedFetchResult` now
— `downloaded` plus an `end` naming every way a walk can stop (`DRAINED`,
`LIMIT_REACHED`, `IDLE`, `CLOSED`, `CANNOT_CONNECT`, `UNPAGEABLE`) — and only
`DRAINED`, an EOSE on an empty page, separates an exhausted corpus from a relay
that capped us or went quiet — which is the distinction a floor needs, since a
finished walk can now say so instead of the floor only ever creeping.
Completeness moved from the band onto the span, so a drained leg closes its own
kinds and no others. Route every
paged call site through `drainSettlesThePast`: a drain on the NEWER leg only
means "nothing below the ceiling we already had", and recording it as history
would make the band skip the past it never walked.

**One relay behind many urls.** Most relay software serves its websocket on
every path, so `wss://nos.lol`, `wss://nos.lol/alpha` and
`wss://nos.lol:443/beacon-glyph` are one server, and a relay list can mint them
without limit. From a production `stats.json`: **7,333 discovered urls, 81% of
them one host wearing a fabricated path** — `nostr.oxtr.dev` 55 times,
`nostr.mom` 50, `nos.lol` 40. Weighted by how many lists name each relay, the
top 50 were dialled **10.7x** over. That is where a 94%-duplicate download rate
comes from: the same relay answering the same filter, once per url.

**What the fold actually recovers, fingerprinted end to end against all 513
multi-url hosts (6,730 urls, 15.5 GB, ~1h):**

| | |
|---|---|
| urls in the file | 7,333 |
| …on hosts wearing one url (nothing to fold onto) | 603 |
| …folded away | **5,514** |
| **dials** | **1,819 — 4.03x, 75.2% removed** |

Not the 6.57x an earlier revision of this file claimed. That number was
`GROUP BY hostname` on the stats file — it assumed every url folds, and 672 of
them cannot: their host never answers a probe, or the path is a genuinely
different endpoint. **Do not restate the grouping as a result.** Anything here
with a figure attached was measured; if you change the fold, re-measure rather
than re-deriving.

`HostStrikes` cannot see it — it evicts an authority that goes SILENT, and
every one of these answers perfectly. The duplicate only exists in what comes
back, so `AliasProbe` reads each url's newest 500 events and `RelayAliases`
folds two urls together when the smaller window is ≥50% contained in the larger.

**The probe pages; a one-shot REQ would measure the relay's limit, not the
relay.** Every relay caps a REQ somewhere and almost none say where — across 60
live hosts, `max_limit` is 500 on half of those advertising one and 100, 1024,
2100, 10000 or nothing on the rest, and one advertises 0. A single ask returns
`min(what we wanted, whatever this host allows)`, so the "same" fingerprint is a
different depth at every host and too shallow to mean anything at the strict
ones; worse, a relay that *enforces* its cap refuses outright rather than
truncating (`CLOSED blocked: limit too high`), which arrives as silence and drops
that host out of the fold entirely. So the walk asks a page, takes the oldest
`created_at` back as the next `until`, and repeats — each relay's cap becomes the
page size, the depth stays ours. **The depth matches the page size on purpose**:
a relay that serves a full page answers in ONE round trip, and a relay that caps
below it gets paged up to the same depth as everyone else. 1,000 was measured
and bought nothing — over 112 fold decisions it agreed with 500 on 108, the four
differences all being the one relay that cannot reproduce its own answers, at
3.4s and 1,464 KB per walk against 1.4s and 562 KB. Depth was never what made
the fingerprint stable; the anchor was. Below 500 saves nothing either, since
the page is asked whole regardless. Three consequences that are easy to undo by
accident: `until` is inclusive so every page re-reads its boundary (never trim
the last ask to the exact remainder — it can then never reach the target), the
result is trimmed to the newest N *by timestamp* so two urls paging at different
sizes are still compared at equal depth, and `DEFAULT_MAX_PAGES` has to clear
`target / FALLBACK_PROBE_PAGE` or hosts that cap low get a shallower
fingerprint than everyone else — the exact thing paging is for.

**And the walk is ANCHORED a minute back, because "the newest N" is a moving
window.** Every url in a group starts from one shared `until`, taken before any
of them is dialled and held `ANCHOR_LAG_SECONDS` behind the clock. Two separate
failures, both real:

- *Shared*, or the window slides between dials. Measured live, `wss://nos.lol`
  against `wss://nos.lol/cipher-zulu` scored **0.41** unanchored — same server,
  missed — while the low-traffic `nostr.oxtr.dev` pair scored 0.95–0.98 in the
  same run. A thousand events span minutes on a firehose and the probes are
  minutes apart behind a 16-permit gate. Anchoring took that pair to **0.99**.
  Deeper paging makes it *worse*, not better: a longer walk is a longer drift,
  which is why the anchor had to arrive with the paging rather than after it.
- *Behind the clock*, or the newest second of the window is whatever each relay
  had finished indexing at that instant. An event is not visible to a REQ the
  moment its `created_at` passes — it still has to arrive, verify and index — so
  a walk that runs immediately and one that runs two minutes later disagree
  about the top of the window even from the same anchor. A minute back the
  window is settled, and it absorbs publishers whose clocks run slightly fast
  into the bargain. A minute-old identity is the same identity.

**The leader is probed alone, first, and decides the filter for its group.** A
bare `{limit, until}` is refused outright by a large minority — 88 of 513 hosts
answered `CLOSED blocked: can't handle empty filters` — and because it is the
LEADER going silent, the whole group was unfoldable: 1,572 urls. Falling back to
`{"kinds":[1]}` recovered **963 of them (14.3 points, 3.09x → 5.53x on the
multi-url set)**, 81 of the 86 recovered leaders needing it. Two urls
fingerprinted through different filters are not comparable, so whatever the
leader had to be asked is what its group is asked — which is also why the
leader cannot be probed concurrently with its members. It doubles as the cheap
exit: no usable leader print means `learn` can return nothing, so the members
are never dialled at all.

What the sweep says about the thresholds, over 4,551 folds: containment min
0.500, p1 0.855, p10 0.987, median 1.000. Overwhelmingly bimodal — but there is
a real tail of relays whose answers are not stable ACROSS CONNECTIONS
(multiplexers, sharded backends) scoring 0.5–0.8 where a stable relay scores
1.000; `www.nostr.ltd` lands on exactly 0.500 with a full 1,000-id window on
both sides. So `minOverlap` is load-bearing for roughly 20–40 urls, not
decorative. Below it sits `espelho.girino.org`, which is not measurable at all:
the same url walked twice from the same anchor self-scores **0.435** (nos.lol
scores 0.998, nostr.oxtr.dev 1.000). Its 17 urls can never fold, and the fold
reports that identically to "this is a different relay" — safe, but only one of
those is a correct conclusion. 1 host in 513; left uncoded deliberately.

**A replaceable event has one address and more than one writer, so writing it
is always an EDIT.** NIP-66's relay record is addressed by `d` = the relay url,
and quartz's monitor updates it passively every time a connection is opened —
so the fold and the monitor aim at the same slot. A writer that rebuilds the
record from its own tags deletes everyone else's, and nothing about the result
looks wrong: still signed, still a valid NIP-66 record, just saying less than it
did. Measured in `RelayAliasRecordTest`, `[d, n, rtt-open]` became
`[d, same-as]`. `RelayAliasRecord.edit` is the shape to copy — read what is
there, keep every tag this writer does not own, and stamp
`max(now, existing + 1)`, because a store enforcing replaceable semantics
REJECTS an edit that is not strictly newer and two writers inside one second are
ordinary. An edit lost that way reports success having done nothing.

Both quartz writers on that address — `RelayReachabilityStore` and
`RelayProber.toDiscoveryEventTemplate` — used to rebuild too, so their next
flush dropped our verdict tag. Fixed upstream in amethyst #3882 and #3883 and
taken here with the `4f41f16db5` pin; the local repair pass that used to restore
a clobbered verdict on the next fold is gone with it. `RelayAliasRecordTest`
holds the merge from both directions, so a pin that regressed it would fail the
build rather than quietly lose verdicts again.

Verified against Vespa rather than only `InMemoryEventIndex`, which is the run
that matters here — replaceable-event ordering is the store's behaviour, not the
index's. One 225-url NIP-65 list, real urls from a real polluted event, folded to
97 relays; the fold signed 128 verdicts at ~19:19:45 and quartz's monitor flushed
over the same addresses at 19:23:54, i.e. it wrote LAST — the direction that used
to erase us. 83 records came back carrying both the verdict tag and the monitor's
`n` / `rtt-open` / `rtt-read`, 0 records had a duplicated tag (replace, not
append), and 178 `d` addresses served 178 records with none served twice. The
5-minute `RelayMonitor.DEFAULT_FLUSH_INTERVAL_MS` is why a check run a minute
after the fold sees 128 verdict-only records and concludes nothing merged; wait
out a flush before reading anything into it.

**No false positives in 4,551 folds.** Every path that looks like a real
endpoint — `/relay`, `/invoices`, `/outbox`, `/inbox`, `/all`, `/v1`, `/v2`,
`/nostr` — folded at 0.997–1.000, i.e. served an identical event set, so
dialling both really was redundant. The fold decides per host from evidence
rather than by path name: `relay.nosotros.app/inbox` folded at 1.000 while
`haven.girino.org/inbox` scored 0.003 and was kept, and `nostr.ac` — 20 paths
each serving different content — kept all 20.

Guards worth knowing before you touch the thresholds:
a url with **no** fingerprint is never folded (silence is not evidence — a
relay that is merely down has to come back), a window under 20 ids decides
nothing **in either direction**, and the survivor is the pathless/`wss`/portless url so bands, cursors
and everyone else's relay lists keep pointing at the same string. What each
folded url was *paired with* moves onto the survivor — drop
`wss://nos.lol/alpha` without carrying its bound authors and the stream stops
asking for those authors entirely.

The verdict is a signed **NIP-66 kind 30166** carrying one tag in two forms
(`RelayAliasRecord`) — the same monitor that already signs "I could not reach
this relay" saying the other thing a dial can prove:

```json
["same-as", "wss://nos.lol/",    "500 newest events, 498 shared with wss://nos.lol/"]
["same-as", "wss://nostr.ac/v1", "500 newest events, best 2 shared of 19 peer(s) on this host"]
```

Pointing elsewhere is a fold. Pointing at the record's OWN url is the cleared
verdict — measured, and equivalent to nothing but itself. The two are told apart
after normalisation, never by string, or `wss://nos.lol` vs `wss://nos.lol/`
reads as a fold onto itself and `adopt` silently drops it.

**`same-as`, not `redirect`.** A redirect is a directed edge carrying authority:
the server told you to go elsewhere and the url you asked for is not the
endpoint. Both halves are false — the relay said nothing, we measured it, and
the alias serves fine. A fingerprint establishes an EQUIVALENCE, which is
symmetric: a consumer running union-find over these tags gets the right
partition without inheriting our opinion about which member to dial. That
opinion is `PREFERENCE` and it stays ours.

It is addressable on `d`, so a re-probe replaces rather than appends — including
replacing a fold with a cleared verdict when a host splits one endpoint into two
real relays. It is served (an operator can ask this relay why a url stopped being
synced and get a signed answer), and it is read back on the next boot within a
30-day TTL.

**The cleared form is what stops the re-probing.** `unresolved` returns a group
while ANY member lacks a verdict, so without persisting "this url is its own
relay" every boot re-fingerprints all the non-duplicates — 59 of them in the live
run against a store already holding 128 folds. The group's LEADER is cleared too
when nothing folded onto it: it is the one member never compared against
anything (everything is compared against IT), so otherwise it would be the only
url in a fully decided group still carrying no verdict and the group would come
back forever.

**What the cleared form does not claim.** Every url is compared to its group's
leader, not to each other, so it says "not the leader" rather than "not any of
them". Two paths on a host that duplicate EACH OTHER but not the leader are both
recorded distinct and both keep getting dialled. That is leader-based grouping,
present within a single pass as much as across boots — persisting the verdict
neither causes it nor widens it.

**REFUSING TO FOLD IS NOT PROOF OF DISTINCTNESS, and conflating them published
lies.** `sameRelay` declines below `minSample`; for a long time the url then
fell through to the else branch and was recorded as its own relay instead.
`learn` also guarded a null fingerprint but not an EMPTY one, so a relay that
answered with nothing took the same path. Both were survivable while the verdict
lived in memory and evaporated on restart. Publishing it for thirty days, about
somebody else's server, is not. Caught in the live store, not in review:

```
CLEARED relay.damus.io/lantern-oscar-dynamo   "0 newest events, best 0 shared of 4 peer(s)"
CLEARED relay.satsdisco.com/anchor-nexus-victor  "9 newest events, best 9 shared of 4 peer(s)"
```

The first rests on zero observations; the second shares 100% of its nine events
with the leader and is very likely the same relay. Both sides must now clear
`minSample` before a url is cleared, and a leader too thin to be a yardstick
clears nobody — members or itself. **The general rule when making an in-memory
heuristic durable: re-audit every path that writes, because a guess that cost
one cycle now costs a TTL and is signed.**

**End to end against live relays, audited build, fresh monitor key so the
verdict state is genuinely cold** (`load` filters on `authors`, so a new key is
a clean slate without touching the corpus):

```
cold   225 url(s), nothing known
       measured 170 fingerprint(s) ? 130 alias(es), folds onto 95 relay(s), 1:12
warm   fan-out on 95 relay(s) in 14s, ZERO dials — apply() straight from the store
       measured  16 fingerprint(s) ?   5 alias(es), folds onto 90 relay(s), 1:04
```

**170 ? 16 fingerprints, a 10.6x drop**, and the warm pass still learned 5 new
folds from relays that had been unreachable on the cold one. Audited every
written record for the defect signatures: zero verdicts on an empty window, zero
under the 20-id floor, zero stale `of N peers` phrasing, zero self-referencing
folds.

**The cleared form is unexercised on this corpus.** 13 cleared verdicts before
the thin-window guard, **0** after — every one of them rested on evidence too
thin to publish, and those urls correctly moved to `unmeasured` instead (56 ?
66). So the measured win above comes from FOLDS plus the `unresolved`/`measured`
fix, not from the cleared form. It is still needed for a host like `nostr.ac`
with 20 genuinely distinct paths; that case just is not in this fixture, so
treat the self-form as correct-but-unproven in the field.

**Verdicts are written as each GROUP finishes, not when the pass does.** A pass
yields to the fan-out, so on a cold store — nothing folded, mirror at its widest
— it can run for a quarter of an hour; measured, 13 minutes with zero verdicts in
the store under the old batch-at-the-end write. Anything ending the process in
that window discarded every fingerprint taken, and a cold store is when that work
is most expensive to redo. One group's verdicts are a complete answer; there is
nothing to wait for.

**The fold is two halves that run at different times, and the split is the
point.** `AliasFolding.apply` READS — one `#d` query per 500 urls, no sockets —
and runs between discovery and fan-out on every cycle, which is the only place
it can run, because a duplicate is a property of a url NEXT TO another one.
`AliasFolding.measure` DIALS, and runs on `AliasMonitor`'s own clock (6h,
2 minutes after boot so a restart's opening burst is past), capped at 2,000
fingerprints per pass, widest group first. A stream `submit`s its candidate set
and gets on with its download; the two halves communicate only through the
store, which is what makes the arrangement survive a restart and lets a second
router signing with the same key share the work.

Inline, `measure` sat between "discovery finished" and the first downloaded byte
on EVERY cycle — 1:19 for a 225-url list in the Docker run, against a production
fan-out two orders of magnitude wider. Measured on the same 225-url list, split:

```
20:58:52  sync starts
20:59:08  fan-out on 97 relay(s)          ? 16s, apply() folded from stored verdicts, zero dials
20:59:19  fetching 30/97, 876 event(s)    ? the mirror is already downloading
21:00:52  monitor's first pass begins     ? 2 min after boot, alongside the fan-out
21:02:20  measured 59 fingerprint(s) ? 11 new alias(es), 225 url(s) now fold
          onto 86 relay(s) (139 known, 46 unmeasured) in 1:28
```

16s to fan-out against ~90s inline, on the identical fold; the 1:28 probe pass
cost the mirror nothing, and 97 became 86 for the next cycle.

**The cost of the split is that folding lags discovery by one pass:** a url seen
for the first time has no verdict when `apply` runs, so that cycle dials it
unfolded. Paying that once per new url is the trade. `AliasFoldingTest` asserts
`apply` opens zero sockets — a regression that moved a probe back onto the read
path would fail nothing else in the repo, it would just make cycles slow again.

The 59 fingerprints in that timeline are what a pass cost when `markDistinct`
was in memory only: a fold persisted, "this url is its own relay" did not, so
every boot re-measured all the non-duplicates. The cleared `same-as` form fixed
that — see the verdict section above for the shape and what it does not claim.

**Measured against live relays**, because the thresholds are only worth what the
wire says. A `{"limit": 500}` probe: **85% of 60 sampled hosts answered**
(median 1.85s, p90 2.4s, ~535KB, 500 events), 13% refuse a bare filter outright
(`blocked: filters must specify at least one kind` — personal *haven* relays,
where a path IS real, so declining to fold them is the right answer), 2% error.
Containment between a host and its fabricated paths came out at **0.99–1.00**
across nos.lol, nostr.mom, nostr.oxtr.dev and relay.primal.net — the fold is not
a close call. `max_limit` is 500 on half the hosts that advertise one and every
relay truncated a 1,000 ask to 500 anyway, so `DEFAULT_PROBE_LIMIT` is 500;
asking for more only risks the outright refusal purplepag.es gives
(`blocked: limit too high`), which is why `AliasProbe` retries once at 100.

An end-to-end run against a seeded store: **22 discovered urls folded onto 10**
in 20s, 12 signed verdict records published, and on the next boot those 12
were adopted from the store — `0 new alias(es) from 10 fingerprint(s), 12
known` — so the probe really is a one-off per url, not a recurring cost. Both
figures were measured while the fold still ran inline; the same work is now a
monitor pass, which is why the 20s is no longer time a fan-out waits.

`maxRelaysPerList` (config, per stream) drops an event naming more relays than a
relay list plausibly holds — measured, 148 pubkeys published a kind 10002 of
100–10,591 entries. **Setting it gives up the tag projection for that source**: a
per-event limit needs the event, and `distinctTagValues` returns values already
flattened across every event it matched. That is not a corner case — the NIP-65
select is exactly the shape the projection claims, so before this was wired the
cap silently did nothing on the one stream it exists for (a live run discovered
222 urls from a seeded 200-entry list with the cap set). It is opt-in for that
reason; unset keeps the projection. Redundant default ports (`:443` on `wss`,
`:80` on `ws`) are folded by string in `RelayDiscovery.normalize` rather than by
probe — that one needs no evidence.

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

  | arriving event | µs/event, probe off → on | where it goes |
  |---|---:|---|
  | fresh (written) | 508 | `write` 73%, `verify` 12% |
  | duplicate | 66 → 17, 35 → 11 | `verify` was 2/3 of it; gone |
  | stale replaceable | 130 → 40, 68 → 34 | `versions`+`verify` were 75%; gone |
  | newer replaceable (written) | 778 | `write` 66%; the probe is ~6% overhead |

  Each pair is interleaved, so a warming engine cannot be read as a difference
  between arms. **Verification is not a rounding error** — 12% of a fresh batch,
  two thirds of a duplicate one — and it costs ~70-95µs in situ against ~48µs
  isolated, because the router competes with Vespa for the same cores.

  The asymmetry is the thing to understand before touching either probe: on a
  stream that mostly REJECTS, both probes are worth 2-4x; on one that mostly
  ACCEPTS they are duplicated work the store will redo, and the version probe
  costs ~6%. Which regime a deployment is in is not knowable here and changes
  over a backfill's life, so `ProbeGate` measures it instead of anyone
  declaring it. The break-evens its thresholds come from: ~35% duplicates for
  the id probe (10-23µs/id against the ~44µs a drop saves), ~20% stale for the
  version probe (21-29µs against ~110).

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
