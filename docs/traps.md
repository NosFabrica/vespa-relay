# Traps that have cost real time

Moved from AGENTS.md on 2026-09-04, unchanged. This is the long form of the AGENTS.md section "Traps".

## Traps that have cost real time

- **A relay's own words reach you on a DIFFERENT listener from its refusal, and
  quartz runs the refusal's first.** `fetchAllPages` returns when the `CLOSED`
  reaches its SUBSCRIPTION listener; `RelayComplaints` records the sentence on a
  CONNECTION listener, which quartz dispatches afterwards
  (`NostrClient.onIncomingMessage` — the same ordering `RelayAuthenticator`
  documents its own grace for). So reading the sentence straight after a refused
  walk is a race, and it is lost at random.
  **Every unit test passed throughout, because a fake answers instantly.** It
  took `WidthRescueLiveProbe` — the real pool, real relays, a real Vespa — to
  see it: from identical code, `git.cloistr.xyz` won the race three times and
  narrowed 139 → 69 → 34 → 17 until it was served, while `purplerelay.com` lost
  it on the second attempt, stopped the narrowing dead at 69 and aborted a visit
  that was one halving from working. `RelayComplaints.awaitSince` gives the
  sentence a 250ms grace, paid only on a refusal. **If a fact arrives on a
  different callback from the thing that makes you want it, assume you can be
  early.**

- **A fallback that returns a COUNT cannot tell "empty" from "refused", and the
  one that claims coverage must never be built on it.** The negentropy sweep's
  last resort for a window it cannot reconcile is a plain REQ, and
  `WindowSync.page` returned an `Int`. A relay that refused that REQ — a filter
  width it caps, an auth wall, a policy `CLOSED` — delivered zero events, which
  is exactly what an honest empty window delivers. So the sweep advanced its
  cursor over the window, and, worse, reached the end of its stack and reported
  `complete = true` — which is what makes the caller stamp `reconciledThrough`
  on the band, the strongest claim this router makes. The history was recorded
  as verified over ground nothing had ever been served for, and every later
  audit was told there was nothing left to find. *An empty stack means the
  windows were all VISITED; it never meant they were all answered.*
  `page` returns `PagedFetchResult` now, three drain sites and the final return
  are guarded on `refusedOutright`, and `negentropyRefused` counts the windows
  that were not claimed. Both guards are mutation-checked.

  **And the same fix has to reach every path that sends the filter.** #185 came
  back precisely because the catch-up was chunked by `FilterWidths` and the
  AUDIT was not: the sweep's fallback REQs carry the identical 139-kind filter,
  so on every width-capped relay the audit went on being refused exactly as
  before while its catch-up worked. One `FilterWidths` for the process now,
  built in `SyncEngine` and passed to both — and the parameter on
  `ClientWindowSync` has NO DEFAULT, because a `= FilterWidths()` would compile
  at every call site and silently restore the bug. The NEG-OPEN itself is
  deliberately left unchunked: a reconcile compares an ID SET, so splitting its
  filter is N `snapshotIdsForNegentropy` reads — the largest allocation this
  router makes — and it does not need to be, since a width-refused NEG-OPEN
  already falls to the REQ path that is chunked.

- **A "how complete is this" number needs the denominator the WORK uses, not the
  one the state file happens to hold.** `RelayStatusReport`'s first version
  counted the bands a (relay, stream) pair held and called the pair complete
  when all of those were settled. A unit owes one ask PER BOUND AUTHOR, so a
  `contentViaOutbox` unit on a many-provider relay owes dozens — and the moment
  the first of them drained, the row read `complete`. Silently, on exactly the
  relays the mirror cares most about, from a table whose entire job is to say
  whether a relay is synced. The denominator has to be what the unit OWES, and
  it was free the whole time: `RosterBuilder.UnitAsks.identity` is already each
  ask's filter as JSON, computed for the roster's own change detection, and that
  is the very string `SyncBands.snapshot` keys a band under — so `askKeys` joins
  exactly, a band for an ask the roster has since dropped is ignored rather than
  counted, and only `settled == askKeys.size` earns `complete`.

  Two things follow, both worth copying. **Join on the string both sides already
  produce, not on a canonical form you invent**: the first version canonicalised
  the relay url on both sides, which was ~10,000 re-parses per status tick to
  produce the strings it started with, and a canonical form that ever stopped
  matching the file's key would have reported the whole roster as `hasn't
  started` with nothing saying why. **And pin that seam against the real
  classes**: `RelayStatusReportTest` drives a real `SyncBands` and a real quartz
  `Filter` so the two keys are produced by the two things that produce them in
  production. Fixture strings are a report's contract with itself and cannot
  catch a divergence; that test is mutation-checked (strip the url's trailing
  slash on one side and four tests fail).

- **`SCAN_PAGE` MUST STAY UNDER THE DEPLOYED `maxHits`, and multi-node is where
  that bites.** Every relay-list read is `RelayDiscovery.scan`, which pages
  `store.query(filter.copy(limit = SCAN_PAGE))` — 10,000 — until a short page.
  Vespa's own `maxHits` default is 400 and a query asking for more is REJECTED
  (`N hits requested, configured limit: 400`), not trimmed, so the store ships a
  query profile setting it to `Int.MAX_VALUE`. Fine on one content node: the hit
  collector sizes from what MATCHES, not from what was asked. **Multi-node
  dispatch does not have that property** — its merge path allocates by the
  REQUESTED hits, and the store's own profile records `Int.MAX_VALUE` killing a
  2-node cluster's jdisc container with "Requested array size exceeds VM limit"
  (2026-08-17), in a crash loop re-triggered by whichever fetch-all caller
  retried first. The documented survival move is to cap `maxHits` in the
  deployed profile and set `VESPA_UNBOUNDED_HITS` to the same value.
  **Cap it below 10,000 and every discovery pass starts failing**: the engine
  rejects the page, `StreamWorld.derive` catches it and prints "could not derive
  <label>", and the roster is silently whatever the other sources named. The
  read itself is not the exposure — `scan` always sends an explicit limit and
  never the unbounded sentinel — the COUPLING is. Lower `SCAN_PAGE` with the
  ceiling, or leave the ceiling above it.

  What a short page CANNOT be is a quiet truncation, and both routes are closed
  deliberately: an over-limit ask is rejected rather than trimmed (above), and
  the bundled profile turns Vespa's soft timeout OFF — on by default, it returns
  HTTP 200 with a partial result at 500ms — with `VespaEventIndex` checking
  `coverage.full` on every response besides. So `page.size < ask` means the store
  had nothing older, which is exactly what `scan` reads it as.

- **A MALFORMED NIP-77 FRAME WEDGES THE WHOLE RELAY, and it is one frame from
  any client.** `negentropy-jvm`'s `ByteArrayReader.readByte()` returns `-1` at
  end of buffer instead of throwing, so `MessageConsumer.decodeVarInt`'s
  continuation-bit loop — `while (b and 0x80 != 0)` over a byte that is now
  always `0xFF` — never terminates. Reproduced deterministically against the
  library: `61` (the version byte alone) terminates; `6100`, `6101`, `61ff`,
  `610080`, `6100ff` and every other truncated frame spin forever.

  In the server that loop runs on a NETTY I/O THREAD —
  `NegSessionRegistry.open` → `NegentropyServerSession.processMessage`, off the
  event loop's task queue — so one `["NEG-OPEN","x",{…},"6100"]` pegs an event
  loop at 100% and every socket bound to it stops being answered. Measured
  here on a real corpus: one frame, `cpu=1,388,930ms` on
  `eventLoopGroupProxy-3-1` in the thread dump, and a FRESH connection asking a
  perfectly valid REQ got no answer at all. Only a restart clears it. Read
  gating does not help — `include:spam` is enough to reach the parser.

  Two consequences worth knowing before you debug the symptom. **A relay that
  answers `/stats.json` in 24ms while websocket reads hang is this**, not
  Vespa, not load: check `jcmd <pid> Thread.print` for a RUNNABLE thread in
  `MessageConsumer.decodeVarInt`. And **the operator switch is
  `NEG_MAX_SESSIONS_PER_CONNECTION=0`** — measured: quartz refuses the
  NEG-OPEN with `NOTICE too many concurrent NEG requests` BEFORE the payload is
  parsed, and the relay keeps serving. The real fix is one line upstream
  (`readByte` must throw, or `decodeVarInt` must check `available()`), and it
  belongs there rather than in a guard here: any pre-validation short of
  re-implementing the parser misses it — `6100` ends in a byte whose
  continuation bit is CLEAR and still loops.

- **A backticked test name with a NON-ASCII character in it can crash the
  Kotlin compiler, in a phase that reports no source line.** A test named
  `` `a dispatcher hop keeps the registry — ingest's own pool books its calls` ``
  compiled fine until it gained a lambda inside it: the lambda's class FILE is
  named after the method, and writing
  `StoreCallsTest$a dispatcher hop keeps the registry — …$1.class` threw
  `java.nio.file.InvalidPathException: Malformed input or input contains
  unmappable characters` out of `JvmWriteOutputsPhase` — an `Internal compiler
  error. See log for more details` with no file and no line, which reads as a
  broken toolchain rather than as a name. Em dashes in test names are all over
  this repo and all of them are fine; what makes one fatal is a nested class
  being emitted for it, so the same name passes for a year and breaks the day
  somebody adds a `runCatching` to it. Keep test names ASCII where the body
  builds a lambda, an object, or a local class, and read an unexplained
  internal compiler error as a filename before assuming anything worse.

- **A JitPack version is a commit hash. Hashes have no order.** This is the
  root of the trap below, and it is worth stating on its own because the
  arithmetic keeps tempting people into a mental model that does not exist.
  `c43339e92b` is not "greater than" `a5507f9a4d` in any sense that means
  anything: it is not newer, not further along the branch, not a superset. The
  two strings sort a particular way and that is all. A commit from last year
  can sort above one from today, and half the time it does — the first hex
  digit decides, and it is uniformly random.

  Gradle does not know that. To it these are version STRINGS, and when two
  parts of the graph ask for different ones it breaks the tie by picking the
  "higher" — which for hashes means it picks an arbitrary one.

- **Gradle resolves those hash conflicts lexicographically, and silently.**
  Pinning quartz to `6d518adddb` while the store carried `79f198c729` resolved
  to the latter — `'7' > '6'` — and nothing said so. Hence
  `resolutionStrategy { force(libs.quartz) }` in EVERY module's build file —
  each of `:common`, `:relay` and `:sync` resolves quartz independently, and a
  new module must add its own force. Never remove one, and check that a pin
  actually took effect.

  Two habits follow from "hashes have no order", and the second is the one that
  gets skipped:

  - When you note which way a bump sorts, you are recording *whether this
    particular bump would survive a conflict*, not a property of the pin and
    not a direction it will keep. The next bump of the same dependency
    re-rolls the dice. Do not carry "this one sorts the safe way" forward.
  - Sorting the safe way is not evidence the pin took. Ordering only decides
    who wins a conflict; it says nothing about whether one occurred. The proof
    is the absence of an arrow in the resolved graph:
    `./gradlew :relay:dependencies --configuration runtimeClasspath` and check
    for `->` on the line, for every module, on every bump.

  What DOES carry real meaning between two hashes is ancestry, and it has to be
  asked of git rather than of the strings: `git merge-base --is-ancestor A B`.
  Dates are nearly as bad as sorting — a commit authored earlier can land on
  main later.
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
    Start the daemon as `DOCKER_MIN_API_VERSION=1.24 dockerd`. The store hit this
    in its own CI and fixed it from the other end in `00eb3f2`, which is in the
    range pinned as of store `e3be81564d` — it now pins
    `systemProperty("api.version", "1.41")` on the test task, above Engine 29's
    floor and supported back to 20.10 — so from that commit the gate runs whether
    or not the daemon's floor was lowered. Keep starting the daemon this way
    anyway: it costs nothing and it is what makes an OLDER store pin testable.
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
- **A deploy activates the package; it does not restart anything.** A store bump
  can change the bundled `services.xml` without changing a line of Kotlin, and
  `AUTO_DEPLOY` then makes it look applied. For a change Vespa classes as
  restart-required — `<jvm options>` is one, and the store's direct-memory
  ceiling landed that way — `prepareandactivate` answers `activated: true` with a
  `configChangeActions.restart` entry naming `default/container.0`, and then
  leaves the running JVM alone. Measured by deploying two packages in sequence
  onto one Vespa: 90s after activation the container JVM's pid predated it and
  still carried the OLD flags, while `:8080` answered 200 for every poll.
  `deployBundledSchema` reports success either way — `awaitServing` asks whether
  an application is live, not whether the live one is the activated one. Read the
  deploy's response body, not its status code, and apply the change with a
  deliberate `docker compose restart vespa`.

  It is not only the container. Store `e1ecd7f23e` moved `numthreadspersearch`
  from 1 to 4, and that is a `restart`-flagged field in Vespa's own `proton.def`
  (`numthreadspersearch int default=1 restart`) — so the restart entry names the
  CONTENT node and proton keeps the old value. That one is quieter still,
  because skipping it is a NO-OP rather than a stale setting doing the wrong
  thing: the store asks per query with `ranking.matching.numThreadsPerSearch`,
  which may only LOWER the configured ceiling, so the ask is clamped back to
  one, every answer stays correct, and the only symptom is a common-word NIP-50
  search that kept the latency the bump halves. Measured the same way as the
  container case — the store's own package deployed at `2bc79f5f40` then at
  `e1ecd7f23e` onto one Vespa: `activated: true` with
  `restart[0].serviceType = "searchnode"` and the message
  `proton.numthreadspersearch has changed from 1 to 4`, and 95s later
  `vespa-proton-bin` still carried its original start time. And THREE things
  that look like confirmation on that un-restarted node are not: the
  `activated: true`, `vespa-get-config -n vespa.config.search.core.proton`
  answering `numthreadspersearch 4` (it reads what the config server serves),
  and proton's own `/state/v1/config` reporting the new generation (it
  subscribes either way and ignores a restart-flagged field until it starts
  again). The process's start time is the only witness. When a store bump
  touches `services.xml`, look up the field in `proton.def` before assuming the
  deploy was the migration.
- **A deploy adds a derived column; it does not populate it — and
  `REINDEX_FTS_ON_START` does not either.** The sibling of the trap above, one
  store bump later, and the reason to check which KIND of field a store release
  adds. A *fed* field (the near tier) is written by a put, so `FtsReindex` is the
  repair. A *derived* one — Vespa builds it at index time from another field, as
  `search_text_gram` does — is left EMPTY on every already-stored event by the
  deploy that adds it, and the re-feed walk skips the whole corpus: the store's
  drift check compares search columns and near arrays, finds them identical, and
  re-puts nothing, so the run logs "reindex complete" having repaired nothing.
  Nothing errors and whole-word search keeps working, so only the missing feature
  shows it. Vespa names the column in the deploy response, under
  `configChangeActions.reindex` ("Non-document field 'search_text_gram' added;
  this may be populated by reindexing") — a body nothing here reads, so a deploy
  that wants a migration and one that does not look identical from the log. The
  repair is a Vespa reindex or a genuine full re-feed, and **`POST /reindex` on
  its own does nothing**: measured on a real Vespa 8, the job sat `pending` for
  over ten minutes with the column still empty, and a REDEPLOY of the identical
  package is what dispatched it (`pending → successful` in ~60s, the partial-word
  query going 0 → 1 hit on the same corpus). Poll `GET …/reindexing` for
  `state`, and deploy again if it stays `pending`. Full procedure, with the real
  endpoint paths: [docs/migrations.md](migrations.md).
- **Two KDoc blocks in a row** fail ktlint (`standard:kdoc`, "dangling toplevel
  KDoc"). Each doc needs its own declaration.
- **A `@Test` that returns a value does not run, and nothing says so.** JUnit 5
  silently ignores a non-void test method, and Kotlin's expression bodies hand
  it one whenever the last statement HAS a value — `assertNotNull` returns what
  it checked, `zipWithNext` returns the list of its lambda's results. Two tests
  here were dead this way and passed the eye test for months: they were in the
  source, in the class file, and never in the run. Declaring `(): Unit =`
  discards the value and the method comes back void; that annotation is
  load-bearing wherever you find it. To sweep for more:
  `javap -p <class> | grep 'public final' | grep -v void` over
  `*/build/classes/kotlin/test`, or compare the test names in the source
  against `build/test-results/test/TEST-*.xml`, which lists what ACTUALLY ran.
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
- **…but an idle window bounds ONE ask, and a visit is a sequence of them.** A
  stream with author-bound asks (`VisitPool.asksOf` — one per provider) asks a
  relay once per author, in sequence, and nothing bounded the sequence — so a
  relay answering every ask with a full `NEG_IDLE_MS` window costs
  `asks × 30s` of a worker, a socket and a claim, and the url is skipped by
  every pass meanwhile.
  Measured in production: `wss://fiatjaf.com/xenon-lima` held **5h00m** having
  delivered 85 events, quiet for the last 4h56m — 600 empty asks. The fix is
  `LEG_QUIET_GIVE_UP_MS`, and note what it is NOT: it fires on SILENCE, not on
  elapsed time, so it cannot cut a leg that is working (every event resets the
  clock). A wall-clock deadline here was tried and removed for exactly that —
  it truncated four healthy upstreams at its 4h mark. The check lives in
  `VisitPool.visit` now, between asks; it also never fires before the first
  ask, because the quiet clock starts at the CLAIM and a visit that queued
  behind a saturated pool arrives already "quiet".
- **An auth-gated relay used to read as an EMPTY one, on every sync path.** A
  relay gating reads answers the REQ with `CLOSED auth-required:` before sending
  anything, and every fetch accessory took that as terminal — so the fetch
  returned empty while our own `RelayAuthenticator` was still signing the
  challenge on that same socket, and the events landed milliseconds after the
  caller gave up. Upstream measured `fetchAll` returning 0 events in 18ms for a
  relay holding 5, and `fetchAllPages` reporting `End.CLOSED` in 20ms.
  `fetchAllWithHooks` had a `pendingOnAuthRequired` flag that fixed it and was
  plumbed into nothing else, so only `AliasProbe` — which drops to the
  option-rich form for its own reasons — was ever getting it right. Fixed in
  amethyst #3905/#3906, on the `1ff1077d58` pin: every read accessory now does
  it, the value is DERIVED from whether the client has a responder rather than
  passed, and the wait is bounded by the auth outcome (a 1s grace, then settle,
  capped by the caller's `idleTimeoutMs`) so an auth-gated relay costs at most
  what a silent one already cost. Do not pass the flag by hand — it was removed
  from every accessory but `fetchAllWithHooks`, and hardcoding it there is how
  you get it wrong when the client changes.
- **A TTL on a tag is not a TTL on the event carrying it — unless you own every
  writer.** Kind 30166 used to have a writer we did not control: quartz's
  `RelayMonitor`, rewriting the record for every relay the client connected to,
  every 5 minutes, preserving our tags. Ageing a verdict on `event.createdAt`
  therefore measured how recently we TALKED to the relay, and any relay still in
  the fan-out is always minutes old — so its verdict never expired while the ones
  we stopped dialling expired on time, which is the wrong way round. The fix that
  lasted was not a private stamp but removing the writer; with the monitor's own
  passes the only ones writing, the record's clock says what it should. Reach for
  a per-tag stamp when a foreign writer is genuinely unavoidable, and prefer
  removing it when it is not.
- **Verify under load, not while idle.** A schema fix was "confirmed" by counting
  zero rejections during a window with no writes flowing. It was the wrong fix.
- **When editing quartz/amethyst alongside this repo**, that project *is*
  multiplatform: commas in backticked test names break Kotlin/Native, and
  `java.util` APIs (`toSortedMap`) break every non-JVM target. Compile more than
  the JVM target.

