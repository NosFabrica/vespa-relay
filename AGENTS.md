# Working on vespa-relay

A Nostr relay with trust-ranked NIP-50 search: quartz's protocol engine
(`RelayServerBase`) over a [vespa-eventstore](https://github.com/NosFabrica/vespa-eventstore)
store, plus a router that mirrors events from upstream relays into the same
store. Six Gradle modules, JVM only (toolchain 21), two processes:

- `:relay` — the serving side. `RelayMain` is its entrypoint.
- `:sync` — the mirror, and the process that hosts the monitor beside it, so
  the pair restarts without the relay or Vespa noticing. `SyncMain` builds both
  engines over one `PeerClient`. Operators know the subsystem as the router
  (`sync.conf` + `monitor.conf`, the `router:` log prefix).
- `:monitor` — the measuring plane: the alias fold, the consistency gate and
  the fitness grades, signed onto kind-30166 records the mirror's roster selects
  on. It may not depend on `:sync`, and knows no mirror type: what it takes from
  the mirror arrives through `MonitorEngine`'s constructor as the `monitor { }`
  block, a list of labelled derivations, and a sink for an event a probe saw.
- `:common` — only what the serving relay also reads (`RelayIdentity`,
  `SchemaDeploy`, `QuartzLogLevel`, `fmtDuration`, `ServingPressure`). Never
  quartz's relay client, never Ktor.
- `:peers` — how this deployment talks to other relays, shared by the two
  client-side planes: `PeerClient`, `RelaySockets`, `RelayVerdictRecord` and the
  `Verdict` vocabulary, `RelayDiscovery`, `RouterConfig`, `IngestPipeline`,
  `Processors`. Sits above `:common`.
- `:web` — every `.html`, `.js` and `.css` in the repo and the Ktor scaffolding
  that serves them. Depends on Ktor and kotlinx.serialization, nothing of ours.

The rule between them: engines produce documents, `:web` renders them, and the
seam is `/stats.json` — and `/pulse.json`, whose builder is in `:common` for
the same reason (`NoBrowserFilesInEngineModulesTest`). `:web` also owns the
admin gate on `/pulse.json` (`AdminGate`, `Nip98AdminGate`, `AdminSessions`),
which is why it depends on quartz: verifying an `Authorization` header belongs
beside the routes it protects, and the dependency can only run one way — Ktor
must never reach `:common`.

`pulse.html` is the one page here whose logic and styling are NOT inline
(`web/pulse/page.js`, `web/pulse/pulse.css`). It is the one page serving a
non-public document, so its policy is `default-src 'none'` with no
`unsafe-inline` on script or style — which an inline `<script>` cannot satisfy.
Putting one back would not fail loudly; the page would just stop working, in a
browser only, so `PulseSiteTest` fails the build instead. One `stats.html`
serves the relay, the mirror and the monitor, each panel guarded on the section
it reads, and every reference it makes is document-relative (`./web/…`,
`stats.json`; `paths.test.mjs`) so it mounts behind `/sync/` or `/monitor/`.

## Commands

```bash
./gradlew build                    # compile + test + spotless check, all modules
./gradlew :relay:test              # the serving side
./gradlew :sync:test               # the mirror plane
./gradlew :monitor:test            # the fold, the consistency gate, the grades
./gradlew :peers:test              # the plumbing, the verdict record, ingest
./gradlew :sync:test --tests "*SyncBands*"
./gradlew spotlessApply            # fix formatting; the pre-commit hook runs spotlessCheck
./gradlew :relay:run               # the relay, locally (needs a Vespa at VESPA_URL)
./gradlew :sync:run                # the router, locally (adds SYNC_CONFIG_FILE)
./gradlew :web:jsTest              # the web UI's own tests (plain node, no deps)
node web/src/test/js/run.mjs       # the same suite, run directly

# Browser probes. Chromium only, except the row probe, which needs a Vespa and a corpus (see its header).
node web/src/test/browser/row.probe.mjs http://localhost:7777 <observer-npub> "<a list title>"
node web/src/test/browser/syncstatus.probe.mjs   # the per-relay table off a fixture; SHOT=/tmp/sync.png keeps the picture
node web/src/test/browser/pager.probe.mjs        # the pager against a fake WebSocket

# Live probes: off by default, selected by a -D switch, most assert nothing. `--rerun` is load-bearing:
# a second identical run is up-to-date-checked, skipped, and prints nothing.
./gradlew :sync:test --tests '*RealRelayDrainProbe*' -DrealRelayProbe=true --rerun -i                       # how the five indexer relays end an empty page
./gradlew :sync:test --tests '*EnforceRetractionProbe*' -DenforceProbe=true -DenforceProviderRelay=wss://... --rerun -i   # stages a phantom score for the retraction audit
./gradlew :sync:test --tests '*VisitPoolLiveProbe*' -DvisitPoolProbe=true --rerun -i                        # both planes end to end against real relays
./gradlew :monitor:test --tests '*VerdictPanelSeedProbe*' -DseedVerdicts=true -DseedVerdictsNsec=nsec1... -DseedVerdictsCount=600 --rerun -i   # seeds a monitor corpus for the verdicts panel; -DseedVerdictsUrl=ws://localhost:7777 -DseedVerdictsLegacy=15
./gradlew :sync:test --tests '*Seed10040Probe*' -Dseed10040=true -Dseed10040Url=ws://localhost:7777 --rerun -i   # one signed 10040 into a local relay
./gradlew :monitor:test --tests '*AliasFoldOnionProbe*' -DonionFoldProbe=true -DonionFoldSocks=127.0.0.1:9050 --rerun -i   # can the fold measure a hidden service
./gradlew :monitor:test --tests '*AliasFoldLiveProbe*' -DliveFoldProbe=true --rerun -i                      # a real fold pass; -DliveFoldGroups='wss://relay.example,wss://relay.example/alpha' (`;` between groups)
./gradlew :monitor:test --tests '*ConsistencyLivePassProbe*' -DliveConsistency=true --rerun -i              # a whole stability pass; -DliveConsistencyUrls='wss://a,wss://b'
./gradlew :monitor:test --tests '*RelaySelfConsistencyProbe*' -DselfConsistency=true --rerun -i             # each url walked twice at four anchors; -DselfConsistencyUrls='wss://a,wss://b'
./gradlew :monitor:test --tests '*RelayComplianceProbe*' -DcomplianceProbe=true --rerun -i                  # does a relay serve what it was asked; -DcomplianceUrls='wss://a,wss://b'
./gradlew :sync:test --tests '*RelayPagesLiveProbe*' -DpagesProbe=true -DpagesUrl=wss://nos.lol --rerun -i  # does the abort sampler fire; wants a busy relay
./gradlew :sync:test --tests '*RelayReachLiveProbe*' -DrelayReachProbe=true --rerun -i                      # can we sync each relay; -DreachNsec=nsec1… -DreachUrls='wss://a,wss://b'; -DreachNoAuth=true is the control arm
./gradlew :sync:test --tests '*UnpageableLegLiveProbe*' -DunpageableProbe=true --rerun -i                   # the mirror's catch-up leg through quartz's own pager; -DunpageableLegs='wss://a=<coveredTo>,wss://b=<coveredTo>'
./gradlew :monitor:test --tests '*AuthGatedFetchProbe*' -DauthGatedProbe=true --rerun -i                    # pins that the client has a NIP-42 responder; -DauthGatedUrl='wss://relay.example'
D=$(mktemp -d)                                                                                              # the band file at production scale, charted before and after
./gradlew :sync:test --tests '*SyncBandsProdScaleProbe*'          -DprodScaleProbe=true -DprodScaleDir=$D --rerun -i
./gradlew :sync:test --tests '*SyncCoverageReportProdScaleProbe*' -DprodScaleProbe=true -DprodScaleDir=$D --rerun -i

# Probes that need a Vespa. A sandbox ships docker but no daemon; start one and wait for the config server.
DOCKER_MIN_API_VERSION=1.24 dockerd > /tmp/dockerd.log 2>&1 &
VESPA_MEM_LIMIT=6g docker compose up -d vespa
until curl -sS http://localhost:19071/state/v1/health | grep -q '"code" : "up"'; do sleep 5; done
ABORT_CENSUS_VESPA=http://localhost:8080 ./gradlew :sync:test --tests '*AbortCensusLiveProbe*' --rerun -i   # #187's relays on every stream; -DabortCensusUrls='wss://a,wss://b' -DabortCensusMinutes=15 -DabortCensusNsec=nsec1…
WIDTH_RESCUE_VESPA=http://localhost:8080 ./gradlew :sync:test --tests '*WidthRescueLiveProbe*' --rerun -i   # the whole #185 fix running; ~20 minutes
BENCH_VESPA_URL=http://localhost:8080 ./gradlew :peers:test --tests '*RelayListReadCostBench*' --rerun -i    # what a relay-list read costs; BENCH_LIST_VISIT=0 BENCH_LIST_KIND=10002 BENCH_LIST_TAGS=r BENCH_LIST_INDEX=1
BENCH_VESPA_URL=http://localhost:8080 BENCH_N=20000 ./gradlew :peers:test --tests '*IngestCostBench*' -PtestHeap=6g --rerun-tasks -i   # what one arriving event costs ingest
docker run -d --name vespa -p 8080:8080 -p 19071:19071 vespaengine/vespa                                    # a bare engine is enough for the next one
./gradlew :peers:test --tests '*RelayListLiveProbe*' -DliveListProbe=true --rerun -i                        # real 10040s off a live relay through a real Vespa; -DliveListRelay=wss://… -DliveListVespa=http://… -DliveListKind=10002

docker compose up -d --build relay            # the usual dev loop (serving only)
docker compose --profile sync up -d --build   # with the mirror
docker compose --profile onion up -d          # with the relay's own .onion
docker compose --profile sync restart sync    # new sync.conf / monitor.conf, relay untouched
docker compose logs relay --since 5m
docker compose logs sync --since 5m
```

Bumping `vespaEventStore` is not done until the store's own integration gate
has run against the pinned commit; the relay's tests use `InMemoryEventIndex`
and cannot see a Vespa-only regression:

```bash
git clone https://github.com/NosFabrica/vespa-eventstore && cd vespa-eventstore
git checkout <the pinned commit>
TESTCONTAINERS_RYUK_DISABLED=true ./gradlew :benchmark:test -Pintegration --no-daemon
```

Git hooks are installed by the build: pre-commit runs `spotlessCheck`, pre-push
runs the tests. Run `spotlessApply` before committing.

## Layout

All modules share the `com.nosfabrica.vespa.relay` package root, and every
package below it belongs to exactly one module, so a package name says which
module holds the file. The root package itself holds the two entrypoints and
nothing else. `ModuleBoundariesTest` (in `:common`, beside the other guards that
read the checkout rather than run it) fails the build on both, on a dependency
that points the wrong way along the include order, on one nothing imports, and
on Ktor or the store crossing the `/stats.json` seam. The long form, file by
file with the reasoning, is [docs/layout.md](docs/layout.md).

```
common/…/relay/
  identity/                   RelayIdentity (RELAY_NSEC: NIP-11 self, NIP-42, NIP-66 monitor) and PubKeys,
                              every pubkey setting (npub only, no bare hex) + adminPubkeysFromEnv;
                              both processes read RELAY_NSEC and RELAY_ADMIN_PUBKEYS, so one parser each
  pressure/ServingPressure.kt EWMA of client read latency, served on GET /pressure
  pulse/                      PulseDocument (the store's own counters as GET /pulse.json), PulseSettings
                              (PULSE_* parsing, the fail-closed admin check); here because both processes
                              open a store, and because :web must not depend on one
  store/, util/               SchemaDeploy, StoreTopology; QuartzLogLevel (QUARTZ_LOG_LEVEL), fmtDuration
  (test) arch/                the guards that read the checkout: ModuleBoundariesTest, the browser-file
                              rule, the probe-switch list. `:common:test` declares the tree as an input
peers/…/relay/
  peers/                      PeerClient (websocket client, 1,024-socket dispatcher, Tor, NIP-42), RelaySockets,
                              RelayVerdictRecord + Verdict + RelayFacts (the 30166 contract), RelayDiscovery,
                              RelayUrlCache, TorTransport (SYNC_TOR_SOCKS, SYNC_TOR_CONNECT_TIMEOUT_SECONDS), DialGate
  config/                     RouterConfig, RelaySourceConfig, RouterConfigLoader (HOCON `streams { }`)
  ingest/, progress/          IngestPipeline, BisectingInsert, ProbeGate, ParseAudit, refused/; Processors, InFlight,
                              StoreCalls, StatusVocabulary
monitor/…/relay/monitor/
  MonitorEngine, AliasMonitor, StreamWorld, MonitorStatus   the plane, its clock and fast lane, the candidate set, its /stats.json
  AliasFolding, AliasProbe, RelayAliases                     which urls are one server
  ConsistencyPass, RelayConsistency, FitnessPass, RelayCompliance   the stability gate, and the grade the roster selects on
  ReachabilityProbe, Silence, Unreachability, HostStrikes, RelayDocument   what a quiet socket said, what may be published, NIP-11
sync/…/relay/
  SyncMain.kt                 entrypoint; the root package holds this and nothing else
  sync/                       SyncEngine (wiring, the health and stats lines; starts both planes), VisitPool (the
                              mirror), VisitQueue, VisitAborts, RelayComplaints, RelayPages, FilterWidths,
                              RosterBuilder, RetractionAudit, NegentropyPager (SYNC_NEG_PAGE_TARGET), SweepState,
                              SyncBands (SYNC_STATE_FILE), SyncManifest (SYNC_MANIFEST_FILE), UpstreamPush,
                              PressurePoller (SYNC_PRESSURE_URL), RouterTuning, PoolLimits, heal/, refused/
  status/                     StreamPhases, SyncProgress, SyncStatus (the mirror's /stats.json on SYNC_STATUS_PORT),
                              StatusRollup, SyncCoverageReport, RelayStatusReport, GaugeSeries
relay/…/relay/
  RelayMain.kt                entrypoint; refuses to boot if SYNC_CONFIG* is set
  server/config/              EnvSettings (`env.intOr(...)`, grep for it; ALLOW_PUBKEYS and the rest of the
                              serving policy), RelayAddresses (RELAY_ONION_HOSTNAME_FILE)
  server/                     NostrRelayServer, LensRequiredPolicy (REQUIRE_READ_LENS), MultiAddressAuthPolicy, TrustNotice,
                              SearchGate (SEARCH_CONCURRENCY_PER_CONNECTION), HttpServer, RelayInfo (RELAY_NAME), RelayIcon
                              (RELAY_ICON), RelayWebSocket, Nip86Route, BanListFile, ConnectionCountListener (LOG_CONNECTIONS)
  maintenance/                ExpirationSweeper, TrustReconcile (TRUST_RECONCILE_ON_START), FtsReindex (REINDEX_FTS_ON_START),
                              OrphanScoreSweep (SWEEP_ORPHAN_SCORES_ON_START), RelayProfile, MirrorReport, StatsYql / StatsVespa /
                              StatsRollup (STATS_INTERVAL_SECONDS, STATS_COUNTERS_INTERVAL_SECONDS, SELECTIVE_KINDS)
web/…/relay/web/
  StatusSite, CachedPages, WebAssets, PageIcon, StatsSnapshot   the page routes, cache, /web assets, icon, document (STATS_FILE)
  resources/index.html        the search UI; its behaviour is resources/web/ (app.js, searchfield.js, paging.js, feed.js,
                              entity.js, related.js, readiness.js, cards/, shared/ — shared/asks.js and shared/lens.js first)
  resources/stats.html        the one status page; web/sync/ and web/monitor/ hold each plane's cards
  resources/observer_stats.html   an operator diagnostic with its own relay client
web/src/test/js/              the plain-node suite (run.mjs); web/src/test/browser/ holds the Chromium probes
relay/tools/                  fetch-corpus.mjs and fetch-observer-corpus.mjs pull a corpus off staging
tor/                          both torrcs, publish-onion.sh, onion.extra.conf.example
```

`docs/configuration.md` documents every environment variable (`.env.example`
is the copyable start), `docs/router.md` the two config files' format,
`docs/migrations.md` the schema changes a store bump can need on a cluster that
already holds data, and `docs/search-latency.md` what a search costs.

## How the router works

`SyncEngine` mirrors upstream events into the store. Its env vars are `SYNC_*`
(the `ROUTER_*` spellings still work and warn on boot), and the monitor beside
it reads `MONITOR_CONFIG_FILE`; it is its own process,
so a `sync.conf` / `monitor.conf` change is `restart sync`, never a relay outage. The long form
is [docs/router-internals.md](docs/router-internals.md); the config format is
[docs/router.md](docs/router.md).

- **Streams.** A `streams { }` block names a filter, a direction (`down`
  mirrors in; `up` pushes out through `UpstreamPush`) and where its relays come
  from: declared in `urls`, or discovered through `relaySource` entries
  (`select` scans over stored 10002/10040 lists, or a verdict query
  `filter = { "kinds": [30166], "#l": ["prime"] }`), permitted by `gatedBy`,
  forbidden by `exclude`. A `filter { }` block is a NIP-01 filter (`ids` and
  `authors` are raw hex). No stream declares a transport; the re-check clocks
  are `negentropySyncThePastSeconds` and `refetchThePastSeconds`, and a
  retracting stream sets `deleteMissing` scoped by `ownedKinds`.
- **Bands.** `SyncBands` (`SYNC_STATE_FILE`) records the `created_at` span
  already walked per (stream, filter, relay), per kind, so a re-run asks only
  outside it. The arithmetic is quartz's `SyncCoverage`; fix band behaviour
  upstream. A completed reconcile records `reconciledThrough`; an empty page
  records nothing.
- **The visit pool.** `VisitPool` walks declared and discovered relays the same
  way: a rotating queue of (relay, stream) units worked by a fixed set of
  workers. A visit pages forward from the band's edge, audits the past by
  negentropy when due, drains queued heals, then leaves a live tail. One
  unclean ask ends the visit; `VisitAborts` partitions why, `RelayComplaints`
  keeps the relay's sentence, `FilterWidths` narrows an ask refused for width.
  `.onion` urls go through Tor (`SYNC_TOR_SOCKS`, `SYNC_TOR_MAX_SOCKETS`,
  `SYNC_TOR_ALL`), gated separately (`DialGate`).
- **The monitor.** Its own config file (`MONITOR_CONFIG_FILE`, `monitor.conf`;
  a `monitor { }` block in the sync config still works, and declaring both is
  refused). It names the relay lists it scans and nothing else — no stream lends
  it anything, and nothing in it names a stream — because every derived url
  becomes a signed public claim. `RouterConfig.monitorSources()` is the whole
  of it. A deployment with discovery streams and no monitor declaration is
  refused at boot (`RouterConfigLoader.refuseUndeclaredMonitor`); `sources = []`
  is how you measure nothing on purpose. `unwatched` on the mirror's
  `/stats.json` counts the pairs the two files have drifted apart on.
  `AliasMonitor` runs three passes in order over
  `StreamWorld`'s candidate set: the fold (`same-as`), the stability gate
  (`self-consistent`), then `FitnessPass`, which grades what survives with a
  NIP-32 label `["l","prime","relay.fitness",…]` (or `dead`) beside `pageable`,
  `compliant` and `nip77`. All ride one signed kind-30166 record per url, so
  `RelayVerdictRecord.edit` is a read-modify-write and the passes never overlap.
  A verdict ages on its own stamp (`DEFAULT_TTL_SECONDS`) and on a rules epoch
  (`FOLD_EPOCH`, `CONSISTENCY_EPOCH`, `FITNESS_EPOCH`: bump one in the same
  commit as the rule it versions). No pass may join on its slowest url; every
  unit of work runs under `AliasProbe.deadlineMs`.
- **The roster.** `RosterBuilder` reads the verdicts back (a plain store read;
  the mirror never calls into `:monitor`) and decides which (relay, stream)
  units exist and what each asks. `RelayStatusReport` says where each prime
  relay stands, per stream.

## Instrumentation

Reach for these before forming a theory. The long form, with what each one
measured, is [docs/instrumentation.md](docs/instrumentation.md).

- The health line, once a minute: heap, ingest queue depth against capacity,
  ev/s in and out, relays transferring, fatals, events lost to store errors,
  `wedged` with the oldest batch's age.
- `/stats.json` and `/stats.html` on the relay, the mirror (`SYNC_STATUS_PORT`,
  7778) and the monitor; `/observer_stats.html` and `/pressure` on the relay.
  One relay is looked up with `jq` over the document, not on the page.
- `/pulse.json` and `/pulse.html` (`PULSE_PORT` / `SYNC_PULSE_PORT`, off by
  default) — WHERE THE STORE'S RESOURCES GO, live off the store's own counters:
  engine time by the activity that spent it, calls per document, matched
  against served per rank profile, admission outcomes over their denominator,
  and lock wait split by WHAT EACH WAITER WAS BEHIND. ADMINISTRATORS ONLY —
  NIP-98 against `RELAY_ADMIN_PUBKEYS`, a signature traded for a short session
  cookie, and a port set with no admin keys stops the boot. It is the one
  non-public page here: with `PULSE_CLIENT_DETAIL` it names the observer lenses
  and search terms driving the load and quotes slow queries. Read it with a
  NIP-07 extension in a browser, or sign a kind-27235 token for a script.
- The progress document (`SyncProgress`): each stream's phase and clock,
  `roster`/`tails`, the in-flight legs, every running pass.
- `store` on `/stats.json` and the `store call SLOW` lines: which store calls
  are outstanding, whose, how old (`StoreCalls`). Do not wait for `bottleneck`.
- The abort partition on the visits row (`abortedAuthRequired`, `abortedClosed`,
  `abortedQuiet`, `abortedUnreachable`, `abortedUnpageable`, `abortedGaveUp`,
  `abortedFailed`, `abortedBackpressured`) and the `visit … aborted` lines
  beside it. Read `abortedBackpressured` and `visitsHeldByIngest` first: they
  are about this mirror's ingest queue, not about any relay.
- The `prime relays` table: `syncStatus` is the past, `behind` the present,
  `kindCap` and `negentropy` the terms the relay serves us on, and `unwatched`
  the pairs this mirror syncs that our own monitor grades nothing about — a
  config question (monitor.conf against sync.conf), never a relay one.
- `ingest stages`: per-stage timing (`dedup`, `write`, `proj.fetch`,
  `proj.write`, `versions`, `verify`, `dedup.pre`).
- Log prefixes: `router:`, `store call SLOW`, `visit … aborted`,
  `could not derive <label>` when a discovery source fails. `SYNC_WIRE_LOG`
  (`sent` / `full`; empty still logs `NOTICE` and `CLOSED`), `SYNC_STREAMS` to
  run one stream alone, `SYNC_DIAGNOSE` for one stream, `QUARTZ_LOG_LEVEL`.
- The probes above: `RelayReachLiveProbe` before believing "the router cannot
  read these relays", `AliasFoldLiveProbe` before believing a fold verdict,
  `IngestCostBench` for what an event costs ingest (`SYNC_INGEST_CONCURRENCY`
  and `SYNC_INGEST_BATCH` are the levers; `VESPA_FEED_CONNECTIONS`,
  `VESPA_FEED_STREAMS`, `VESPA_FEED_INFLIGHT_FACTOR` are the store's).

## Conventions

**Comments are short, and the code carries the meaning.** A KDoc is one to
three lines: what the thing is, and the one constraint a reader would otherwise
get wrong. An inline comment marks a non-obvious invariant or ordering, in one
or two lines, and nothing else. No history ("this used to be…"), no measured
numbers, no capitalised emphasis, no ellipsis continuations between blocks. If
a block needs a paragraph to explain, extract a named function or constant
instead. The history and the measurements go to `docs/decisions/`, one short
paragraph per decision, and to commit messages; `git log -L` finds the rest.
`VisitPool.kt` is the reference for the register.

**Tests assert the property, not the implementation.** A test that names the
property survives a wholesale replacement of the mechanism; one that encodes
the implementation's own opinion of itself passes while shipping the bug.

**A period knob says what it repeats and over which ground, and names the
transport only where the transport is the distinction.** `refreshSeconds`,
`sweepSeconds`, `fastLaneSeconds` name a job; `negentropySyncThePastSeconds`
and `refetchThePastSeconds` are one job over two mechanisms, chosen per relay
by the monitor's `nip77` verdict. Renames go through `syncEnv(new, *legacy)`
for env vars and a boot warning for config keys, never silently.

**A configured component must never be silently inert.** If a flag needs
something else to be true, make it true and say so.

**Don't publish claims you can't support.** Negative NIP-66 records are signed
and public; `Unreachability.proves()` stays quiet on an unknown failure, because
silence costs a retry and being wrong costs a false statement about someone
else's server.

## Traps

One line each; the evidence is in [docs/traps.md](docs/traps.md).

- A relay's sentence arrives on a connection listener after the refusal ends the walk; `RelayComplaints.awaitSince` gives it 250ms.
- A fallback that returns a count cannot tell empty from refused: `WindowSync.page` returns `PagedFetchResult`, guarded on `refusedOutright`.
- A fix to the filter has to reach every path that sends it; `FilterWidths` is built once in `SyncEngine`, and `ClientWindowSync` takes it with no default.
- A completeness number needs the denominator the work uses: `RelayStatusReport` joins on the unit's owed asks (`RosterBuilder.UnitAsks.identity`), not on the bands present.
- `SCAN_PAGE` (10,000) must stay under the deployed `maxHits` / `VESPA_UNBOUNDED_HITS`, or every discovery pass fails with "could not derive".
- A malformed NIP-77 frame wedges the relay's event loop; `NEG_MAX_SESSIONS_PER_CONNECTION=0` is the operator switch, and `jcmd <pid> Thread.print` shows it in `MessageConsumer.decodeVarInt`.
- A backticked test name with a non-ASCII character crashes the compiler once its body builds a lambda; keep such names ASCII.
- A JitPack version is a commit hash, and hashes have no order; ancestry is `git merge-base --is-ancestor A B`.
- Gradle resolves hash conflicts lexicographically and silently: `resolutionStrategy { force(libs.quartz) }` in every module, and check `./gradlew :relay:dependencies --configuration runtimeClasspath` for `->` on every bump.
- JitPack's build-status API lies; only the artifact file proves a build.
- JitPack caches builds per group-spelling (`com.github.NosFabrica` vs `com.github.nosfabrica`).
- An integration run that skips still prints `BUILD SUCCESSFUL`: start `DOCKER_MIN_API_VERSION=1.24 dockerd`, and read `skipped="…"` in `build/test-results/test/*.xml`.
- Env vars on the `./gradlew` line never reach a forked test JVM under a reused daemon, and a bench is `UP-TO-DATE` the second time: `--no-daemon`, `--rerun-tasks` (or `--rerun`), and `-PtestHeap` at six figures.
- A deploy activates the package and restarts nothing: read `configChangeActions.restart` in the response and `docker compose restart vespa`; a `restart`-flagged field in `proton.def` is the quiet case.
- A deploy adds a derived column and does not populate it, and neither does `REINDEX_FTS_ON_START`; `POST /reindex` alone sits `pending`. See [docs/migrations.md](docs/migrations.md).
- Two KDoc blocks in a row fail ktlint (`standard:kdoc`); each doc needs its own declaration.
- A `@Test` that returns a value does not run; declare `(): Unit =`, and sweep with `javap -p <class> | grep 'public final' | grep -v void`.
- Vespa's `time.date()` does not zero-pad; `StatsYql.isoDay` is the one way in.
- A nested grouping and a flat one answer in the same shape; only the right builder keeps events out of a users column.
- Grouping expressions take arithmetic: weeks are `(created_at + 259200) / 604800`, months `time.year * 12 + time.monthofyear`.
- A coarser time bucket is not the finer one re-added; distinct authors are asked per granularity.
- A bare aggregate with no grouping level fails for anything but `count()`.
- Freshness is bounded to the present: every `lastSeen` carries `created_at <= now`, future-dated events count in `corpus.futureDated`.
- A nested grouping is one query where the obvious shape is N; without the inner `each()` the series collapses to one count.
- Playwright's Chromium has no HTTP cache, so a 304 is checked with `curl -H "If-None-Match: …"`.
- The bundled query profile (`grouping.globalMaxGroups: -1`, `defaultMaxGroups`/`defaultMaxHits`) is what makes the rollup legal.
- `grep` may be aliased to `ugrep`; use `/usr/bin/grep` when a search finds nothing implausibly.
- `\n` inside a Kotlin raw string is literal, which breaks HOCON fixtures.
- A timeout is an idle window, not a deadline; size it by the slowest single answer.
- An idle window bounds one ask and a visit is a sequence of them; `LEG_QUIET_GIVE_UP_MS` fires on silence between asks.
- Every quartz read accessory waits out `auth-required:` on the AUTH's own verdict; do not pass `pendingOnAuthRequired` by hand.
- A TTL on a tag is not a TTL on the event carrying it unless you own every writer.
- Verify under load, not while idle: zero rejections in a window with no writes proves nothing.
- Quartz/amethyst is multiplatform: no commas in backticked test names, no `java.util` in shared code.

## Operations

`docker-compose.yml` runs Vespa, the relay, (`--profile sync`) the sync process
and a client-only Tor, and (`--profile onion`) a second Tor that is the relay's
own hidden service; one store throughout. Both processes deploy the bundled
Vespa schema on every boot (`AUTO_DEPLOY`). Two levers delete data,
`SWEEP_ORPHAN_SCORES_ON_START` and a stream's `deleteMissing`; both default to
a dry run. `https://search-staging.brainstorm.world/` runs this code against a
real corpus with the router on and answers anonymously: reach for it before
inventing a fixture; read it, never publish to it. An anonymous read there
carries `observer:<64-hex>` or `include:spam` (a `CLOSED … auth-required:` is
that gate); the Trusted Lists live on `wss://tapestry.brainstorm.world/relay`;
rank through `460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c`.
To fill a local store, point a `down` stream at it with
`filter = { "kinds": [1985], "search": "include:spam" }`. The rest — the corpus,
the two Tors and `Onion-Location`, `TRUST_RECONCILE_ON_START`, `STORE_WRITERS`,
`SYNC_PRESSURE_URL` — is [docs/operations.md](docs/operations.md).

The decisions behind the code live in `docs/decisions/`, one file per subsystem.
