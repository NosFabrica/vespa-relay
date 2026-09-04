# Layout

Moved from AGENTS.md on 2026-09-04, unchanged. This is the long form of the AGENTS.md section "Layout".

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

web/src/main/kotlin/com/nosfabrica/vespa/relay/web/
  StatusSite.kt             installPageDefaults (compression + CORS, on the terms
                            measured in HttpServer), statsDocument, and
                            serveStatusSite — one page + its document + its
                            assets, which is a whole background service's UI
  resources/stats.html      THE page. Panels guarded on the section they read,
                            so the relay's, the mirror's and the monitor's
                            documents each draw their own cards from one file
  resources/web/            every module the pages import: shared/ (the design,
                            the render engine, the protocol clients), sync/ and
                            monitor/ (each plane's cards), cards/ (the search
                            UI's per-kind renderers). shared/asks.js is the
                            one to know before touching a search ask: the
                            relay's cost is the MATCH SET, not the limit
                            (limit 1 and limit 200 took the same 4s on
                            staging), so the type-ahead and the results view
                            ask one question at one width and the second gets
                            the first one's answer; app.js runs one type-ahead
                            at a time and paging.js's first ask already covers
                            the preload. Nine ranked searches per typed word
                            became one — docs/search-latency.md
  CachedPages.kt            CachedPage/IconedPage and the ETag exchange every
                            page and document answers with
  WebAssets.kt              /web/… off the classpath, hashed once, and /favicon.ico
  PageIcon.kt               pageWithIcon — every <link rel="icon"> replaced by one
  StatsSnapshot.kt          the served document: two writers merged, persisted

peers/src/main/kotlin/com/nosfabrica/vespa/relay/
  peers/
    PeerClient.kt         the websocket client, the 1,024-socket dispatcher, Tor
                          and NIP-42 — one of each, for BOTH planes, which is
                          why RelaySockets can refcount across them
    RelaySockets.kt       who is still using this socket: one refcount across
                          every stream and every probe pass
    RelayVerdictRecord.kt the signed kind-30166 records — the monitor writes,
                          the mirror reads. The contract between the planes
    Verdict.kt            …and the label vocabulary they are written in. In
                          :peers because a vocabulary only one side can name is
                          not a contract
    RelayFacts.kt         the NIP-66 fact tags that ride the same record
    RelayDiscovery.kt     which urls a relay list names, and which a `dead`
                          verdict of ours holds out
    TorTransport.kt       the .onion transport, and probeIdleMs
    DialGate.kt           what bounds a probe pass's dials — ONE GATE PER
                          TRANSPORT, since the clearnet dispatcher and the
                          Tor one are separate budgets. Not ingest/ProbeGate
  config/                 RouterConfig + its HOCON loader — one file configures
                          both planes, so it sits under both
  ingest/                 IngestPipeline (on an IngestTuning, not the whole
                          config), ParseAudit, the refusal filters, ProbeGate
  progress/               Processors and InFlight — the report both planes
                          register rows in

monitor/src/main/kotlin/com/nosfabrica/vespa/relay/monitor/
  MonitorStatus.kt        this plane's OWN /stats.json — its four pass rows, the
                          subset of the glossary they need, and the served
                          relay's ws url, which the verdict panel has to be told
                          because this page is not served by the relay
  MonitorEngine.kt        the plane: the three passes, the derivation, the boot
                          retirement of verdicts this router would no longer
                          sign. Its CONSTRUCTOR is the account of what the
                          monitor still takes from the mirror
  AliasFolding.kt         which urls are one server wearing several addresses
  ConsistencyPass.kt      which cannot answer the same question twice
  FitnessPass.kt          …and the grades for what survives, signed
  StreamWorld.kt          the candidate set all three measure over
  AliasMonitor.kt         their clock, and the fast lane — which runs the
                          stability gate then fitness over urls named since its
                          last look, and never the fold (per-host work)

relay/src/main/kotlin/com/nosfabrica/vespa/relay/
  RelayMain.kt          entrypoint; reads env, deploys the schema, wires the
                        serving side. REFUSES to boot if SYNC_CONFIG* is set —
                        the mirror is :sync's process now
  config/
    EnvSettings.kt      NIP-11 limits etc. from env, via `env.intOr(...)` rather
                        than `env["..."]` — grep for both or you will conclude a
                        working setting is dead
    PubKeys.kt          npub-only parsing for every pubkey SETTING — not for
                        NIP-01 filter fields, see the router's hexKeys
    RelayAddresses.kt   the OTHER addresses this relay answers at — the .onion
                        Tor publishes into RELAY_ONION_HOSTNAME_FILE, read on
                        demand because the address is minted after we boot
  server/               the serving side
    NostrRelayServer.kt the IEventStore-backed relay backend; installs StoreQueryContext
    LensRequiredPolicy.kt  THE RELAY'S DEFAULT BEFORE AUTH: a REQ or COUNT from
                        a connection that has not authenticated is answered only
                        if EVERY filter says whose eyes it is read through — a
                        NIP-50 `observer:<64-hex>`, or `include:spam` waiving
                        one. Otherwise `CLOSED … auth-required:`, which is the
                        prefix NIP-42 clients already retry through. It exists
                        because this store has NO house observer, so a lensless
                        read is not "the same answers, unranked" but a different
                        corpus with the trust switched off — the engine does not
                        even send `min_rank` without an observer to anchor it
                        (EventYql), so the floor a search carries is silently
                        inert. That corpus is a legitimate ask and `include:spam`
                        is how to make it; what it must not be is what a client
                        gets by SAYING NOTHING. The parse is quartz's own
                        SearchQuery — the very parser the store maps with, so
                        the gate cannot read a token differently from the query
                        planner (a quoted `"include:spam"` is a phrase, a
                        `-observer:…` an exclusion, and neither is a way
                        through). ALL filters or none: NIP-01 ORs them, so one
                        undeclared filter beside a declared one would serve the
                        undeclared question in full. NOT gated: EVENT, AUTH and
                        NIP-11. NIP-77 **IS** gated, and not by a second
                        decision — quartz's `NegSessionRegistry.open` builds a
                        ReqCmd from the NEG-OPEN's filters and runs it through
                        this same hook, so a refusal arrives as `NEG-ERR …
                        auth-required:` (measured on a real corpus; a NEG-OPEN
                        carrying `include:spam` is admitted, and
                        `NegentropyGatedTest` pins both halves). Right for a
                        relay filled FROM public relays rather than serving
                        mirrors — a reconcile hands over the corpus's whole id
                        space — and the cost is that an anonymous PEER cannot
                        mirror from here without declaring. NIP-11
                        `auth_required` stays FALSE and that is
                        not an oversight: both ways past the gate are unsigned,
                        because scores here are public. `REQUIRE_READ_LENS=false`
                        is the older relay, for a deployment with no trust data
                        to gate on
    (the search expansion)  THE SUBJECT TRAVELS WITH THE POINTER — a REQ that
                        actually SEARCHES answers with the record each Trusted
                        List / NIP-85 assertion / NIP-32 label hit points at,
                        spliced in behind it. It LIVED HERE, as an IEventStore
                        decorator, until store `68f07ce958`; it is now
                        `store/search/` in vespa-eventstore. Two things could
                        not be fixed from this side and are why it moved: the
                        reader's enrolment needed a TTL because a relay cannot
                        see the sync process feeding 10040s into the same index
                        from another JVM, and placing a subject by the
                        confidence its pointer expressed needs the pointer's
                        RELEVANCE, which `IEventStore` does not expose. What
                        stays here is the budget — `SEARCH_EXPAND_*`, handed to
                        `VespaEventStore.open()` — because a deployment's caps
                        are the operator's call and applying them is the store's.
                        The relay's own part is now not asking: a plain NIP-01
                        recall carries no terms and expands nothing, which is
                        every read the router makes.
    MultiAddressAuthPolicy.kt  NIP-42 for a relay with two front doors: a Tor
                        client signs the .onion it dialled, and quartz's
                        OptionalAuthPolicy binds exactly one url. It also
                        carries the LOGIN HOOK: quartz's `authorize` is the one
                        seam that sees a verified AUTH and that connection's
                        `send` at once
    TrustNotice.kt      what a reader is told the moment they sign in: ONE
                        NOTICE naming the first link of their chain this relay
                        does not hold, and nothing when it holds them all.
                        readiness.js's second and third links in the existence
                        form a relay can answer for itself — their own kind
                        10040, then kind 30382 SIGNED BY the `30382:rank`
                        service that list names. A chain, not a fan-out: the
                        second ask is addressed to what the first one returns,
                        which is also why a missing 10040 says one thing rather
                        than two (the same first-unmet-link rule the panel
                        holds; a column of red crosses says four things are
                        wrong when one is). The signer is what ranks, never the
                        card's `d` — a store full of cards ABOUT the reader
                        from services they never named ranks nothing for them —
                        and EVERY `30382:rank` entry counts rather than the
                        first, because the provider map credits a reader
                        through all of them: reading one told a reader whose
                        SECOND provider is fully mirrored that their scores
                        were missing, on every login. Fired once per identity
                        per CONNECTION, since an AUTH frame stays valid for its
                        whole ten-minute window and quartz accepts every copy —
                        a replay loop would otherwise be free store reads on a
                        scope the socket's close does not cancel.
                        A stored 10040 naming no usable rank entry is its own
                        answer, told apart from having none: only a PUBLIC
                        `30382:rank` with a relay hint resolves, here and in
                        the store's own provider map, so a followers-only list,
                        a hintless entry and a NIP-44 private one all leave
                        ranked search empty. A store that THREW is silence too
                        — a failed read must never be published as "you never
                        posted one". Off the AUTH path entirely: an OK is what
                        a client waits on before it reads, and quartz reads a
                        throw from that hook as a FAILED LOGIN
    SearchGate.kt       ONE RANKED READ AT A TIME PER CONNECTION — terms, a
                        phrase, a `sort:`, and the COUNTs of either queue in
                        arrival order behind the one in the engine; plain and
                        lens-only reads never wait. A relevance search takes
                        every match thread the cluster has, so two on one
                        socket share rather than overlap (measured on staging:
                        one `bitcoin` 3.8s, three at once 5.0s EACH, six 6.8s
                        each), and the search page used to stack nine per typed
                        word. `SEARCH_CONCURRENCY_PER_CONNECTION`; the permit is
                        held to EOSE, not to the end of a REQ that parks at its
                        live tail. docs/search-latency.md has the whole
                        measurement, including what the page and the store
                        changed beside it
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
    RelayProfile.kt     the relay's OWN kind 0 and kind 10002, signed with
                        RELAY_NSEC at boot: the NIP-11 name/description as a
                        profile, and this relay as its own NIP-65 inbox and
                        outbox. NIP-11 only reaches whoever asks this host —
                        a reader meeting the key on a 22242 or a 30166 has no
                        name and no way back, and events travel. Both kinds are
                        replaceable and EDITED rather than rebuilt (the NIP-66
                        rule, for the same reason): the five fields this writer
                        owns come from the env, everything else in a stored
                        kind 0 and every other relay in a stored 10002 survive.
                        Skipped when the store already says it, so a restart
                        loop cannot walk the profile forward one created_at at
                        a time — and a store that THROWS is never read as an
                        empty one, which is the only path that could replace a
                        richer profile with a freshly built one. The doc is an
                        ARGUMENT to publish(), not state: NIP-86's
                        changerelayname/description/icon rewrite the served
                        document while the relay runs, so serveRelay's
                        onInfoChanged republishes and the profile follows the
                        doc rather than the environment it booted with. Two
                        traps worth knowing: quartz's updateFromPast REBUILDS
                        the NIP-39 `i` tags through a parse that drops a
                        proof-less claim and truncates `matrix:@a:b.org` at the
                        second colon, so the held event's own `i` tags are put
                        back verbatim in the initializer; and RELAY_NAME is the
                        one owned field with a default, so unset publishes
                        "vespa-relay" — which is what the doc serves

sync/src/main/kotlin/com/nosfabrica/vespa/relay/
  maintenance/ParseAudit.kt   what quartz could not parse, grouped to a JSON
                              report — lives here because ingest is what feeds it
  util/SyncFormat.kt          fmtCount / nowSeconds, internal again
  SyncMain.kt           entrypoint; env, store, engine, block
  SyncEngine.kt         wiring, health/stats lines. Owned by NEITHER plane,
                        which is why it sits above both: it starts them
  sync/                 THE MIRROR — everything that moves events into the store
    VisitPool.kt          EVERY down stream's engine: the roster (declared
                          `urls` plus the relays the monitor's 30166 verdicts
                          admit), rotating visits (catch-up, the reconcile of
                          the past, heal drain), earned live tails,
                          yield-paced revisits
    VisitQueue.kt         whose turn it is, and when a relay may be revisited
    VisitAborts.kt        WHY a visit ended early: `abortedVisits` split seven
                          ways and said once per (stream, relay, reason), with
                          the ask and whatever the relay said for itself. An
                          abort leaves its relay unreconciled, so this number
                          IS whether the resync converges
    RelayComplaints.kt    what a relay SAID when it would not answer — the
                          NOTICE/CLOSED text quartz's PagedFetchResult has no
                          room for, kept per relay and dated so a walk can only
                          read the sentence its own REQ earned
    RelayPages.kt         …and what it SENT: the page an abort could not name,
                          because a walk aborts on `downloaded == 0` and the
                          events that would say why are dropped by the match
                          that produced the abort. ARMED per ask rather than
                          always on — it is a listener on every event of every
                          socket of both planes
    FilterWidths.kt       how many kinds each relay takes in one filter,
                          learned from its own refusal, so an over-wide ask is
                          re-sent in chunks instead of refused forever
    RosterBuilder.kt      the asks a stream makes of each relay it may dial
    RetractionAudit.kt    the deleteMissing comparison, run as a retracting
                          ask's `negentropySyncThePastSeconds` reconcile:
                          compare both ways, delete what the provider no
                          longer serves
    NegentropyPager.kt    the windowed history sweep every reconcile runs
    SweepState.kt         per-peer window sizes and the in-progress cursor
    SyncBands.kt          covered created_at bands per (relay, filter)
    IngestPipeline.kt     bounded queue -> dedup -> supersede -> verify ->
                          batchInsert, poison isolation. Dropping FIRST is the
                          point: a schnorr check is ~48us isolated and ~70-95us
                          in situ, and a mirror is offered the same event once
                          per relay holding it — and older VERSIONS of it from
                          the relays that never got the newest
    ProbeGate.kt          whether either drop-probe still earns its round trip,
                          learned from what it drops
    BisectingInsert.kt    the batch-bisecting write
    UpstreamPush.kt       dir = up: reconcile and publish what the upstream lacks
    SyncManifest.kt       what this router is CONFIGURED to mirror — the running
                          streams and their kinds — written once at boot so the
                          relay can publish it. The kind list exists in
                          router.conf and nowhere else, and a count taken
                          against this relay is wrong without it
    PressurePoller.kt     polls the relay's /pressure into ServingPressure
    RouterTuning.kt       the constants the mirror is paced by
    heal/                 pushing back what an upstream is missing
    refused/              what the store refused, so it is not re-downloaded
  monitor/              THE MEASURING PLANE — what to believe about a relay,
                        published as signed NIP-66 records. It decides nothing
                        about syncing; the mirror reads its verdicts back
    AliasMonitor.kt       the schedule the passes run on: fold, then stability,
                          then fitness
    AliasProbe.kt         the fingerprint: a relay's newest events, as ids
    AliasFolding.kt       apply() reads verdicts; measure() earns them
    RelayAliases.kt       which discovered urls are ONE relay (see below)
    ConsistencyPass.kt    the pass that measures the stability gate
    RelayConsistency.kt   which relays answer one filter the same way twice
    RelayCompliance.kt    …and which relays answer THE FILTER at all — the
                          other half, and the one the gate above structurally
                          cannot see: it compares two answers to each other, so
                          a relay serving the same wrong events to every REQ
                          scores 1.000. Graded by the fitness pass, on the ask
                          ladder it was already paying for plus ONE second page
                          — which is also what proves the relay can be walked
                          rather than merely answered once (#187)
    FitnessPass.kt        the fitness grade: `["l","prime","relay.fitness",…]` on
                          the 30166 record, earned by answering a settled-anchor
                          probe — the tag [VisitPool]'s roster selects on
    ReachabilityProbe.kt  the TCP pre-probe, and whether a url warrants one
    Silence.kt            classifying what a quiet socket actually said
    Unreachability.kt     which failures may be published as NIP-66 records
    HostStrikes.kt        per-authority strikes and eviction
    RelayFacts.kt         the DESCRIPTIVE half of the record — `n`, the two
                          rtts, `R`, `s` (+version), `N`. Absent writes no tag
                          rather than a zero, and everything it writes it also
                          OWNS, so a reading cannot outlive the dial that took
                          it. Says which fields are deliberately unwritten
                          (`rtt-write`, `g`, `T`, a `v` tag) and why
    RelayDocument.kt      the relay's own NIP-11 document, for the fields no
                          dial can measure — and the connect that carries it,
                          which IS the `rtt-open` we publish. Pure `parse`,
                          because it is somebody else's json
    StreamWorld.kt        the url universe the monitor measures: every stream's
                          sources, plus the monitor's own. Reports as the
                          `aliasSource` processor
  shared/               WHAT BOTH PLANES TOUCH, and the whole of it — the split
                        was derived from the imports, not guessed
    RelayDiscovery.kt     pulling relay urls out of stored events: the mirror
                          builds its roster from them, the monitor its universe
    RelayVerdictRecord.kt the signed 30166 edit — the monitor writes every
                          verdict through it, the mirror reads the fold and the
                          `nip77` measurement back out. OWNERSHIP IS A
                          PREDICATE, not a set of tag names, because `l`/`L`
                          are shared: the fitness writer may replace its own
                          NIP-32 namespace and must carry every other
                          labeller's through
    RelaySockets.kt       WHO IS STILL USING THIS SOCKET — the one refcount
                          across streams and probe passes; quartz closes none
                          of its own connections
    TorTransport.kt       SYNC_TOR_SOCKS: the second OkHttp client, chosen per
                          url, whose .onion names resolve INSIDE the proxy
  config/               the declarative side, read by both planes
    RouterConfig.kt       the stream model (streams, directions, the two
                          re-check clocks)
    RelaySourceConfig.kt  the relaySource model (sources, selects, bindings)
    RouterConfigLoader.kt HOCON `streams { }` parsing (strfry-shaped), and
                          every removed knob's refusal
  progress/             observability for both planes
    StreamPhases.kt       per-stream progress reporting, and the snapshot the
                          progress file is written from
    InFlight.kt           WHICH relays a stream is holding right now, which
                          those counts never said. UNBOUNDED, and the one list
                          here that is: a row is a WORKER, so the pool's
                          visitConcurrency already bounds it, and a top-N
                          answered "what is this mirror connected to" with a
                          sixth of the answer on a card that looked whole.
                          Quietest first — held is not risk
    Processors.kt         the work that is NOT a stream: the alias fold, the
                          stability gate, fitness, the rotating pool, ingest,
                          the healer, the push. Same shape as a stream — a phase
                          and a clock — plus either a pass schedule and an
                          `outstanding` count, or live gauges read through a
                          supplier
    SyncProgress.kt       what each stream is doing, republished on the
                          progress tick and read by this process's own status
                          site off the same heap. It was SYNC_PROGRESS_FILE,
                          written for the relay to read off a shared volume;
                          that knob is REFUSED at boot now
  status/
    SyncStatus.kt         the mirror's own /stats.json — the coverage fold, the
                          progress document and the glossary, in the relay's
                          envelope so both pages share one engine
    StatusRollup.kt       its timer, on its own daemon thread so a saturated
                          Dispatchers.IO cannot stop the page refreshing
    SyncCoverageReport.kt bands + sweep cursors folded into per-stream groups
                          and depth buckets. Real computation, which is why it
                          survived the move whole
    RelayStatusReport.kt  WHERE EACH PRIME RELAY STANDS — the roster joined
                          against the same band snapshot, one row per (relay,
                          stream) pair: complete / paging / refused / never
                          started, with the depth reached and the relay's own
                          sentence where it refused. Its subject is the ROSTER,
                          which is why it can report the two states the
                          coverage fold structurally cannot. TWO AXES per row —
                          the past (`syncStatus`) and the present (`behind`),
                          neither derived from the other — plus the terms the
                          relay serves us on. The join is on the unit's OWED
                          ASKS — see the trap below
    GaugeSeries.kt        the last hour of the four process gauges — the one
                          thing a single tick cannot state, and all that is
                          left of SyncProgressReport
    SyncVocabulary.kt     what every number in the `sync` section means, shipped
                          inside the document as `sync.terms`. Pinned in both
                          directions by SyncVocabularyTest: no published count
                          without a term, no term without a count


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
                        `#l` label per NIP-32), ORed in one REQ, and gives the
                        OTHER NIP-73 subjects their own prefixes — `site:`,
                        `isbn:`, `geo:`, `isan:`, `doi:`, `podcast:guid:`,
                        `podcast:item:guid:`, `podcast:publisher:` — which
                        become the comment pair alone (scopeIds spells each id
                        the way NIP-73 fixes it, plus as typed), with the page
                        state passed IN so the whole thing is testable —
                        web/src/test/js/query.test.mjs asserts the filters, and
                        RelayProtocolTest asserts the relay answers them.
                        shared/lens.js is what makes any of those filters
                        ANSWERABLE without a signature: the relay refuses an
                        unauthenticated read that names no web-of-trust lens
                        (LensRequiredPolicy), so every ask this page makes falls
                        into one of three cases and each has exactly one place
                        that decides it — the authenticated socket adds nothing
                        (the connection IS the lens), a search ranked through
                        somebody else carries `observer:` (app.js's
                        `searchString`/`askString`, and "ranking as" now works
                        SIGNED OUT because scores are public), and a fact about
                        a SUBJECT rather than the reader carries `include:spam`.
                        That third case is the module: names, faces, scores,
                        group names, reply parents and the observer list are
                        asked down the anonymous reference socket precisely so
                        the reader's own gate cannot narrow them, and there are
                        a dozen call sites — so the STAMP is on the connection
                        (`new Relay(url, { lensless: true })` in shared/conn.js,
                        monitor/cards.js's verdict reader, observer_stats.html's
                        local client) rather than at each ask, where the next
                        caller would forget it and read a CLOSED as "no such
                        event". Sockets to OTHER PEOPLE'S relays are never
                        stamped (readiness.js's askRemote, observer_stats.html's
                        remote clients): `include:spam` is this store's
                        extension, and a NIP-50 relay without it would take the
                        token for a word and search for it.
                        That whole language is also written down for the READER,
                        in the syntax sheet the `?` beside Filters opens: a
                        <dialog> of markup at the end of index.html, four lines
                        of app.js (showModal, the two ways out, and the `?`
                        shortcut), and web/src/test/js/help.test.mjs holding it to
                        this file — a prefix query.js lifts and the sheet never
                        names fails there, and so does a token the sheet names
                        that query.js leaves in the query as words, which is the
                        worse half: the page would be promising a filter while
                        searching for the literal string. Its sort values are
                        held to the Filters menu's <option> list the same way.
                        `group:<id>` is the NIP-29 one, and the only subject
                        with a PICKER: a group id is opaque (`chachi`, a hex
                        blob) where a hashtag is already the word it means, so
                        nobody can type one from memory. It becomes an `#h`
                        filter — single letter, so `tag_index` holds it exactly
                        as it holds `t` — plus the group's own kind 39000 keyed
                        by `d`, which is the half that names its HOST: NIP-29
                        has the relay sign its groups' records. A CARD MAY ONLY
                        MINT THAT TOKEN FOR AN ID THAT READS BACK AS ITSELF —
                        `groupTokenizes`, compiled from the tokenizer's own
                        `GROUP_ID` so the two cannot drift. A group id is a
                        stranger's string and two of its shapes fail silently:
                        whitespace ENDS a token, so `group:my group` asks for
                        the group `my`, and a trailing `.,;!?` is sentence
                        punctuation, so `group:hello.` asks for `hello` — the
                        wrong room, no error, under this room's name. All three
                        places that draw a group (the pill on a chat card, a
                        39000's title, a `group` tag on a list) go through
                        base.js's `groupHref`, which returns null for those and
                        leaves the label as text: a link that searches for
                        something else is worse than no link. shared/groups.js
                        is the decision behind the picker, and its whole
                        argument is a refusal. A group is the pair (id, host)
                        — quartz's `GroupId` — and this page holds that pair in
                        two incompatible spellings: a kind 10009 `group` tag
                        names the host as a URL, a 39000 names it as the pubkey
                        that signed it, and NOTHING here joins the two (no
                        per-relay provenance in the store, no such tag on a
                        NIP-66 record). So your list and the corpus stay two
                        answers, and one id on two relays stays two rows,
                        flagged `ambiguous` — because an `h` tag carries the
                        bare id and picking either row searches for both. That
                        last part is the honest limit: the picker fixes the
                        human's half (remembering the id) and cannot fix the
                        filter's. HALF A GROUP LIST CAN BE LOCKED — a 10009 is
                        a NIP-51 private-tag event, so items may sit
                        NIP-44-encrypted in `.content`, self-encrypted to the
                        author's own key — and this is the ONLY place the page
                        asks a signer for anything but a signature. The prompt
                        fires when a reader with a payload uses `group:`, not on
                        load, and three rules keep it from becoming noise: no
                        payload no ask, one prompt per reader however many
                        keystrokes (`unlockAsk` is the shared in-flight
                        promise), and a refusal is FINAL until the reader
                        clicks the unlock row — nothing re-asks on its own.
                        It is fired and not awaited, or an unanswered dialog
                        would hold the picker hostage over public groups it
                        already had; `field.refreshGroups()` is what draws the
                        answer whenever it lands. Two traps in the payload
                        itself: an EMPTY private list encrypts the empty STRING
                        rather than `[]`, so a payload is not evidence there is
                        anything in it and "we asked and it was empty" is a real
                        cached answer; and nothing records which scheme was
                        used, so `isNip04` ports quartz's shape test verbatim
                        (`?iv=` exactly 28 from the end, after the `-null`
                        strip) — a wrong guess costs a permission prompt, which
                        is the one thing this is all trying not to spend twice.
                        BOTH READS GO DOWN THE AUTHENTICATED SOCKET, and the
                        own-list one is pinned by `groups.test.mjs` against
                        app.js's source because there is a plausible-looking
                        change that breaks it. The store applies the observer as
                        a FILTER, so a reader with no scores and no 10040
                        mirrored here reads back nothing at all — their own
                        events included. Measured against a real Vespa, signed
                        in on a store with no scores: `{kinds:[10009],
                        authors:[me]}` returned 0, and returned 1 the moment a
                        provider that reader trusts scored them. Moving the read
                        to the anonymous connection makes it answer, which is
                        exactly why it must not — the picker would become the
                        one place on the page showing a reader content this
                        relay has decided it cannot rank for them. No chain
                        here, no personal groups, and no unlock prompt either
                        (nothing found means nothing sealed); readiness.js is
                        what explains that, rather than a special case in the
                        search box. Whether an observer should be gated by their
                        own trust at all is a separate question and the store's:
                        the reputation tensor is derived only from 30382s about
                        a subject, so there is no self-edge and you score 0 under
                        your own lens. Nothing here anticipates that fix — when
                        the store stops gating a reader out of their own events
                        this read simply starts answering.
                        shared/groupnames.js is the OTHER half of the same
                        problem: once a group has been picked, its `group:<id>`
                        pill draws the NAME back over the id — the person
                        chip's bargain, over a value even less readable than an
                        npub, since a group id is arbitrary as well as opaque.
                        The token underneath never changes (the url, the export
                        and the query builder all still carry the id, and the
                        hover says so), so the pill is a view and not a second
                        source of truth. What it may draw is the whole of that
                        module: your own 10009's name for the id wins outright,
                        the corpus's 39000s are used only where the hosts AGREE,
                        and two relays that signed one id under different names
                        leave the hex on the pill — the `#h` filter really does
                        return both, and naming it after either would say the
                        results are one group when they are two. Its lookup is
                        ANONYMOUS, unlike the picker's two reads right above,
                        and deliberately: "what is this group called" is a fact
                        about a subject like the kind 0 behind a face, so
                        asking it through the observer gate would leave an
                        unmirrored reader looking at the id the pill exists to
                        replace. TWO pills read that one cache, so they can
                        never disagree about what a group is called: the search
                        box's, and the one a NIP-29 chat card draws beside its
                        kind badge for the room it was said in (`postedTo` in
                        groups.js reads the `h` tag, cards/base.js's
                        `groupPillHtml` draws it, and it links to the `group:`
                        search because NIP-29 gives a room no event of its own
                        to open). A chat line is a fragment of a conversation,
                        and two rooms interleaved by timestamp read as nonsense
                        without it. Neither pill says WHICH of the groups
                        sharing an id it is, and neither can: an `h` names no
                        host, and nothing in this store records the relay an
                        event was mirrored from — so `general`, an id half the
                        relays running NIP-29 have each minted, is a name over
                        a union. Fixing that is a store-side provenance
                        question, not a drawing one;
                        shared/parents.js answers "in reply to WHO" — NIP-10's
                        rule for which `e` tag is the parent, plus the by-id
                        lookup for the author when the tag carries no hint;
                        entity.js
                        renders /npub1…//note1…//naddr1… paths — and re-renders
                        them from `rerun()` like every other view, because it is
                        the one that GATES on the reader's web of trust: a
                        permalink fetched signed in is a different page signed
                        out, and it is also the one view `$q` is empty on, so it
                        used to fall through the "nothing typed" branch and be
                        left standing under an identity the page no longer had.
                        `entitySeg`/`openEntity` are the single pair the router
                        and the re-run both open it through — with related.js
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
                        NIP-01 read, which the store answers newest-first
                        (signed OUT it carries one token and only one — the
                        lens declaration every read now owes the relay: the
                        picked `observer:`, else `include:spam`, neither of
                        which orders anything on a termless filter, so the feed
                        is the same plain read wearing what makes it
                        answerable) (a
                        stray `sort:` would rank it while the page said
                        "latest" — `sort:recent` being the one value that
                        would not, since store 5e44f1bde8 it asks for the
                        order this view already has, and the feed does not
                        lean on that), so the feed hides the Filters
                        disclosure — but NOT the kind chips, which are the
                        `kinds` of that same read and narrow the feed under the
                        hero and at /?feed=1&tab=media alike, replacing the
                        content default rather than intersecting it (four of
                        the seven narrowing chips share no kind with it); what
                        is
                        feed.js's own is the shaping — replies, future dates and
                        duplicates never reach a card;
                        paging.js is the results view's PAGER — forty to a page,
                        three pages fetched ahead of the one on screen. A page
                        is a longer ask CUT LOCALLY, because NIP-50 has no
                        offset and a ranked answer has no `until` cursor
                        either: the only way to reach page two is to ask for a
                        longer prefix of the same ranking. So the ask in front
                        of the reader stays ONE page (firstAsk) and the three
                        behind it are a second, wider ask made after that
                        answer is already drawn (app.js's `preload`, which does
                        NOT bump requestId — it is the same answer asked for at
                        greater length, and bumping would drop the names and
                        pills out of the page being read). The widened answer
                        is APPENDED, never taken whole (mergePages): an event
                        published between the two asks can rank onto page one,
                        and adopting the new order would renumber the list
                        under the reader. `?page=4` is state like the query and
                        the chip, so Back undoes a page turn and a link is what
                        the sender was reading. Two ways the pages end and the
                        page says which: the corpus running out (`drained`,
                        which needs EOSE — a read WE stopped listening to came
                        back short for our own reasons) and this page declining
                        to follow a ranking past MAX_ASK, which is a note under
                        the pager rather than a silently missing button. The
                        FEED does not page: its answer is a plain NIP-01 read
                        whose cursor is `until`, a real one and a different
                        mechanism; cards/ is the
                        kind registry — one renderer module per family, a
                        generic floor for the rest, and a render test that
                        FAILS if a kind registers without a fixture, a badge
                        label or a family tone. Each family registers TWO
                        renderings of its kinds: the card, and the one-line
                        type-ahead row the search field draws, whose registry
                        the same test holds to the card registry's key set. A
                        row has no fallback to the raw content on purpose —
                        that rung is what printed
                        `{"about":"","name":"Test group","picture":""}` in the
                        popup for every channel, product, stall, app handler
                        and NIP-66 record, and git's own `From <sha> Mon Sep 17
                        00:00:00 2001` for every patch, beside cards that had
                        been drawing all of them properly. A family with
                        nothing to say about an event says nothing and the row
                        leads with its author. The NIP-51 lists and sets are
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
                        comments claimed. What it DOES do now is answer: a
                        verified AUTH starts `TrustNotice`, which walks the
                        same two middle links and carries the first unmet one
                        to every client on the protocol as a NOTICE, rather
                        than only to this page.
                        Its quietest number is the one that needed a whole
                        mechanism: "your own posts, N% mirrored" compares OUR
                        count for you against your write relay's, and ours is
                        narrowed by router.conf while theirs is not — 35% on a
                        mirror missing nothing. shared/mirrors.js reads the
                        kinds this relay actually asks for off `sync.mirrors` on
                        GET /stats.json and puts them on BOTH counts (and both
                        newest-event reads: a reaction we never mirror is not
                        this relay being behind). Where that document does not
                        say — no rollup yet, no mirror, unreadable — `scopedTo`
                        THROWS rather than passing an unscoped filter through,
                        and the panel asks neither side and says nothing about
                        posts. The percentage says "about": both sides carry the
                        same kinds, but the denominator is a stranger's NIP-45
                        COUNT, and on one author kind 1 alone came back as
                        126,426 against 89,485 for every kind at once.
  web/favicon.{svg,ico}
                        the tab icon: index.html's own brand mark, in white on
                        an accent tile, kept BY HAND in both formats. Not the
                        mark verbatim, and the divergence is deliberate — the
                        mark is `currentColor` at 1.7-unit strokes, a tab strip
                        has no `currentColor` to inherit and comes in both
                        themes, and 1.7 units is 0.9 device pixels at 16px. So:
                        white ink, fixed ground, heavier stroke, less margin.
                        `RelayFaviconTest` holds the SVG to the mark circle by
                        circle and fails if either moves alone — but the .ico is
                        a raster and NOTHING checks that it still depicts the
                        same drawing, so an edit to the mark that is followed
                        through into the SVG alone leaves it stale with every
                        test green. Redraw both; favicon.svg's header carries
                        the tile radius, glyph scale and stroke a redraw needs.
                        All three pages link both formats (SVG first, .ico as
                        the fallback — Safari takes no SVG icon — which also
                        answers the guessed /favicon.ico for everything that
                        never read our markup: see `favicon()` in HttpServer.
                        `RELAY_ICON` overrides all of it, and the override runs
                        BOTH ways: set, it is the tab icon as well as the NIP-11
                        one, replacing those two links (not joining them — the
                        SVG would outrank it everywhere but Safari) and turning
                        /favicon.ico into a 302; unset, NIP-11 publishes THIS
                        relay's /favicon.ico, so the doc always names the icon
                        the tab shows. The trap that pairs with that: once unset
                        publishes our own url, "the doc has an icon" no longer
                        means "the operator set one", and redirecting to it
                        blindly points /favicon.ico at itself — `iconOverride`
                        is the comparison that keeps that loop shut
  observer_stats.html   an operator diagnostic carrying its own tiny relay
                        client on purpose: it must work when the app does not,
                        and asking the way a client would means it also TESTS
                        what it asks
  stats.html            the corpus dashboard: it charts GET /stats.json, and
                        THE JSON IS THE ARTIFACT — the page is one reader of it
                        (see maintenance/StatsRollup). No aggregation on it is a
                        protocol feature to test, and thirty days of
                        distinct-pubkey counts is not a question to ask over a
                        websocket, so every chart here is a rollup read.
                        The router's state is TWO cards off one `sync` section,
                        split by what each row decides. *Sync coverage* answers
                        "is the mirror keeping up": the constraint line, then
                        ONE BORDERED SECTION PER STREAM, then *the pipeline* —
                        ingest, the healer, the push and the pool's lifetime
                        counters, which move EVENTS — and last the coverage
                        bars, which are where it has WALKED rather than what it
                        is doing. THE STREAM IS THE SECTION and it is the only
                        place a stream appears: its phase, its share of the
                        roster, its caps and clocks as one row per job, and its
                        five pool tables. It was drawn FOUR times before that —
                        a phase line, a pool section repeating it, and its name
                        in the first column of two card-level tables once per
                        job — off four independent walks of `progress.streams`,
                        so the reader did the join and the two roster lines came
                        off different members and could disagree. The join is
                        `streamSections` (shared/sync.js) now, done once and
                        pinned; the card level keeps only what is not about any
                        one stream, and the pool-wide summary is drawn above
                        more than one stream and not at all below that, where it
                        would be the single stream's own line again.
                        *Relay monitor*
                        answers "which relays may we dial at all": the corpus
                        tree, then the url round-up (`aliasSource`, the store
                        walk that derives the candidate set — its own row
                        because it takes minutes and every pass waits on it),
                        the alias fold, the stability gate and
                        fitness, whose unit is a URL, whose clock is the
                        monitor's own and whose output is a signed 30166 record
                        — so it sits beside the panel that reads those records
                        back. The rule for a processor name the page has not
                        been taught is still that it DRAWS — under the router's
                        own word, off `BACKGROUND` in shared/processors.js —
                        since dropping a row to keep a card tidy is how a new
                        job runs unwatched. `splitProcessors` was that rule while
                        the two planes shared one array; each publishes its own
                        document now, so a row is on a card because that plane
                        registered it. The pin that keeps the JS honest against the
                        Kotlin that feeds it is `SyncVocabularyTest`: every
                        published member must have a term, and every term a
                        published member, because a name added on one side only
                        does not fail — the number silently stops being drawn on
                        a card that still looks complete.
                        ONE panel is not. Monitor verdicts (kind 30166) have no
                        rollup at all — `/stats.json` says how many urls folded
                        and onto how many relays, and nothing could answer "what
                        does this store say about THIS url, and when was it
                        measured", which is where every investigation of a
                        still-duplicated relay starts. That panel pages the
                        records off this relay's own websocket (shared/verdicts.js
                        parses; verdicts.test.mjs holds the tag semantics) and is
                        drawn on a BUTTON, not on the minute poll.
                        It draws the WHOLE record, not just our three verdicts
                        — `n` / `rtt-open` / `rtt-read` / `R` / `s` / `N` as
                        well, with unknown tag names counted rather than
                        dropped. Two reasons: `R: auth` is the first explanation
                        for a url that will not fold, and a row showing
                        `same-as` and nothing else is what a CLOBBERED record
                        looks like, so the panel is the production-side check on
                        the tag merge that `RelayVerdictRecordTest` can only pin
                        in isolation.
                        THREE VERDICTS, and it drew two. The fitness grade rode
                        the `s` tag, which is the SOFTWARE field to every
                        monitor in the wild, so the panel rendered `prime` and
                        `dead` in the software column — the one verdict that
                        decides whether a relay is in any roster, drawn as a
                        vendor string, with no way to show the software at all.
                        The grade is a NIP-32 label now (`relay.fitness`) and
                        the panel reads it under that namespace, never by tag
                        name: `l` also carries other monitors' country and ASN
                        labels, and matching the name would read a country code
                        as a grade. Two counters moved with it — `pageable` and
                        `nip77` were in neither the rendered nor the owned set,
                        so the panel counted the monitor's OWN writes as
                        `+2 other tag(s)`, and `not folded` counted every url
                        the fitness pass had measured as one nothing had ever
                        looked at.
                        Every verdict it draws is tested for BOTH expiry rules —
                        age and rules epoch — and both forms are tested, which
                        the cleared half was not: it drew `keep` off the tag's
                        presence alone, so a retired cleared verdict read as
                        settled while the url was queued to be re-measured, and
                        it fell out of every counter on the page at once.
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
    MirrorReport.kt  the kind set this relay mirrors, from SYNC_MANIFEST_FILE
                     — the LAST of the router's files this side reads. What the
                     mirror has walked and what it is doing moved to the sync
                     service's own status site; see `SyncStatus` for why a file
                     could not answer "is it running" and an HTTP request can
    StatsYql.kt      the grouping pipelines and the readers for what comes
                     back — pure, so both halves are tested against captured
                     engine output rather than against an assumed shape
    StatsVespa.kt    POST to /search/, refuse a degraded answer (the same
                     coverage question the store's SearchCoverage asks, and
                     for the same reason: `full` answers it in neither
                     direction)
    StatsRollup.kt   the document, section by section, each failing on its own
                     — and `StatsTier`, the two schedules it is computed on
  (:web) StatsSnapshot.kt
                     what GET /stats.json serves — held in memory with an
                     ETag, written through to STATS_FILE so a deploy does not
                     blank the page for the minutes a first rollup takes, and
                     the MERGE point for the two tiers. In :web because every
                     service publishes a document this way, not just the relay
```

**The document is computed in two passes, and the split is by measured cost.**
`STATS_COUNTERS_INTERVAL_SECONDS` (60s) runs `corpus`, `trust` and `sync`;
`STATS_INTERVAL_SECONDS` (900s, the setting that always meant this) runs `kinds`,
`authors`, `activity`, `kindActivity`, `zaps` and `relayDistribution`. The line
between them is whether cost scales with the corpus: a `count()` over a match set
does not materialise the match set, a grouping behind a *selective* `kind` filter
is bounded by that kind's population (10040/30382 are thousands of events), and a
windowed bucket grouping is bounded by the window — while `group(pubkey)` over
everything materialises the store's whole pubkey set, `distinctAuthorsBy(bucket)`
materialises one such set PER BUCKET (the shape that OOMKilled the engine twice),
a full-corpus histogram walks all 90M+ documents, and grouping `tag_index` emits
every tag pair on every matched document. **A `kind` filter bounds the group set,
not the walk**, which is why `zaps` is with the charts despite being three counts:
a mirror holds millions of kind-9735 receipts and `group(pubkey)` over them
touches every one to return a handful of LNURL services. Nothing is watched by a
zap total the way a mirror is watched by its freshness, so fifteen minutes is fine
for it. **The tier is the section, never the query**, because a section carries
one `generatedAt` for all of its members — which is what moved the store's
distinct pubkeys out of `corpus` into its own `authors` section, and why
`corpus.kinds` is gone in favour of the histogram's own `kinds.total`. The one
number that crosses the boundary is `corpus.newestEvent`: asked over a two-day
window (cheap, and freshness is what a per-minute cadence is FOR), carried
forward from the previous document when that window is empty, and published as
the maximum of the two so a quiet mirror never winds the tile back.

Every section publishes `queryMs` per query and every pass publishes
`tiers.<name>.{generatedAt,tookMs,everySeconds}`. That is deliberate and
load-bearing: which queries can afford the fast cadence is a MEASUREMENT on the
corpus in front of you, not a deduction, and a pipeline that drifts into the fast
tier does not break a chart — it quietly runs fifteen times more often than it
can afford. `StatsRollupTest` holds the invariant from the other side, against a
`StatsQueries` fake: a counters query may not group without a bound, nest a
grouping inside `each(...)`, touch `tag_index`, or lean on a `kind` filter for a
kind outside its `SELECTIVE_KINDS` — the allowlist that keeps "bounded by a kind"
from meaning "bounded", and that kind 9735 is deliberately not in.

Four of the pipelines are `EventYql`'s own shapes, reused verbatim because this
deployment has already run them; the rest extend them along `created_at`. It
does NOT build on `EventYql` itself — `grouping()` is private, its pipelines are
a fixed set, and it ships in vespa-eventstore, so every new chart would cost a
store release plus a JitPack pin bump. The duplication is the WHERE clause and
nothing else.

`docs/configuration.md` documents every environment variable, `docs/router.md`
the router config format, and `docs/migrations.md` the schema changes a store
bump can need on a cluster that already holds events — the ones a deploy
reports success for and only half applies. They are the reference; this file is
the orientation.

