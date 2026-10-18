# Dependency pins

Why each pin in `gradle/libs.versions.toml` is what it is, moved out of the
catalog so a bump is a one-line diff. Newest notes first within each pin, as
they were written. The rule that matters on every bump is the first section.

## The two JitPack pins carry no order (and `kotlin`, after this note)

```text

A NOTE ON THE TWO JITPACK PINS BELOW, because every comment on them mentions
how the hash sorts and that is easy to read as more than it is.

A JitPack version is a git commit hash. Hashes carry NO order. `c43339e92b`
is not newer than `a5507f9a4d`, not ahead of it, not a superset of it — the
two strings simply sort a particular way, decided by the first hex digit,
which is uniformly random. A commit from last year sorts above one from today
roughly half the time.

Gradle does not know that. It sees version STRINGS, and when two parts of the
graph ask for different ones it keeps the lexicographically higher — so for
hashes it keeps an arbitrary one, and says nothing. That is the whole reason
for `resolutionStrategy { force(libs.quartz) }` in every module.

So when a comment below says a bump "sorts the safe way", read it as: THIS
bump would survive a conflict if one arose. It is not a property of the pin
and it will not hold for the next bump of the same dependency. And it is not
evidence the pin took effect — ordering only decides who wins a conflict, not
whether one happened. The proof of that is always the resolved graph:
  ./gradlew :relay:dependencies --configuration runtimeClasspath
with no `->` on the line, checked per module.

The relation that does mean something between two hashes is ancestry, and git
is the only thing that can answer it: `git merge-base --is-ancestor A B`.
Dates cannot — a commit authored earlier can land on main later.

```

## `app` (versions)

```text
The relay application's version (image tag / --version display).
```

## `quartz` (versions)

```text
99d05ee122 = amethyst main @ 2026-10-17, the merge of vitorpamplona/amethyst#4522. 38 commits over
e5bdf1bd37 (below), 12 of them touching quartz/, all in the relay client and its OkHttp socket
(#4517, #4521, #4522). What this relay reaches, through peers (NostrClient over
BasicOkHttpWebSocket), sync's walks (RelayReads.page = fetchAllPages) and the monitor:

  A WALK LOSES NO EVENT WHEN ITS RELAY HANGS UP (#4517). Events of a page the relay dropped mid-way
    were skipped as already seen, and the socket's backlog was thrown away at the close: the
    socket now delivers every message received before the end (#4521), and a relay that hung up
    on a page it was serving is redialed at once.
  A FRESH CONNECTION REPLAYS ITS SUBSCRIPTIONS FIRST (#4521): what was sent while the socket was
    opening no longer races the replay, and a replaced socket's late EOSE no longer reaches the new
    session.
  AN EXCEPTION FROM A WALK'S onEvent REACHES THE CALLER (#4522); it was logged as an unparseable
    frame and the walk went on without the event. Sync's onEvent throws only on cancellation
    (IngestPipeline.submit absorbs a closed channel), so this changes nothing it does today.
  availableRelaysFlow EMITS (#4522): it held the pool's live key view and stopped emitting after
    the first relay. RelaySockets.close reads its value, which stays current.
  Also #4522: a tagsAll-only filter change is re-sent; fetchAsFlow/subscribeAsFlow stop copying
  per event; RelayStat counters are atomic. Unused here: EventCache (commons).
  SEARCH DERIVATION, through the store: none. nip50Search, nip01Core.store/core and relay.filters
  have no diff in the range.

NO ARROW: the store at 0aed31fda8e8 (below) names 99d05ee122 too, so every quartz line in all six
  modules reads it.
  ./gradlew :relay:dependencies --configuration runtimeClasspath | grep -E "quartz|eventstore"

DIRECTION: e5bdf1bd37 is a strict ancestor of this.
  git merge-base --is-ancestor e5bdf1bd37 99d05ee122   # verified

Spelled with 10 hex digits: `com.github.vitorpamplona.amethyst` serves quartz and quartz-jvm at
  99d05ee122.

LEXICOGRAPHIC: '99d0…' < 'e5bd…': would lose a conflict unforced; moot under force(libs.quartz),
  and with the store naming the same string there is no conflict.

Prev e5bdf1bd37, on this branch only (never on main): the merge of vitorpamplona/amethyst#4515.
60 commits over ce7f4db1b6, 21 of them touching quartz/. What this relay reached:

  EventIngest (#4510). LiveEventStore takes it instead of the concrete IngestQueue, which is how
    the relay's publishes reach IngestPipeline (RELAY_INGEST). IngestQueue implements it unchanged.
  OK FOR AN OVERSIZED EVENT (#4510). A frame over max_message_length got a NOTICE and no OK, so a
    publisher pipelining EVENTs waited each one out; it now gets `OK false invalid: … too large`.
  THE CLIENT POOL, reached by the mirror and the monitor (#4515). NostrClient no longer drops a
    relay from its pool on a stale sample of what it wants: that disconnected the socket under an
    EVENT just sent, whose OK never came (a one-at-a-time publisher stalled ~45 s per drop until
    the keep-alive redialed). And a relay that drops soon after its dial is redialed when its
    backoff ends, not at the next 60 s sweep. Also from #4514: listener and auth-responder
    registrations no longer lost under concurrency; fetchAsFlow completes on a refusal.
  NEGATIVE LIMITS (#4514, LimitsPolicy). `"limit":-1` is served like an absent limit (the
    default page) instead of reaching the store, where "no limit" would walk past max_limit.
  NIP-11 (#4514). A document with kind ranges in its retention or a fee above Int range parses
    instead of failing whole; the monitor reads other relays' documents.
  SEARCH DERIVATION, through the store. Additive: the OpenAgents kinds 30184 (extension listing),
    3184 (extension release) and 30180 (capability discovery) become searchable, read from their
    JSON bodies, their protocol-marker `t` tags kept out of the hashtag role. No kind already
    searchable extracts differently. Owed and written: docs/migrations.md ("OpenAgents kinds
    become searchable"). The cards (web/.../cards/openagents.js) came with #264.
  Unused here: publishStream / publishRawStream / publishRaw, fetchAllPagesResuming; the SQLite
  store's fixes in the range (this relay's store is Vespa).

(ce7f4db1b6 is a strict ancestor of e5bdf1bd37, as are 020e714858 and 96100f4309, this branch's
earlier pins.)

Prev 020e714858, on this branch only (never on main): the amethyst #4510 merge, the ingest
series above without #4514 and #4515.

Prev ce7f4db1b6 = amethyst main @ 2026-10-16, the merge of vitorpamplona/amethyst#4506: Open Markets
Prev 96100f4309200bca4a9cbacfa7bd4ab9bceaf1d1, on this branch only (never on main): the first
three commits of the #4510 series (EventIngest, OK false for an oversized EVENT, publishStream)
over main d7df3040; superseded by their merge above.


Quartz from JitPack (amethyst @ commit) — the Nostr protocol engine (RelayServerBase).

ce7f4db1b6 = amethyst main @ 2026-10-16, the merge of vitorpamplona/amethyst#4506: Open Markets
product collections (30405), shipping options (30406) and product reviews (31555), the kinds the
Conduit recommendation plan reads and the web cards (cards/market.js) render. 58 commits over
25a3f37c83, 5 of them touching quartz/: that series, an SNO (cyberspace) size-cap fix and a
dial-race test.

SCANNED PER PACKAGE THIS RELAY IMPORTS: none of them changes. What moves is reached through the
store and the parser:

  SEARCH DERIVATION. Additive: 30405, 30406 and 31555 become searchable (collection: title,
    summary, content; shipping option: title, carrier, content; review: content, catch-all),
    and no kind already searchable extracts differently in this range. 30405 and 30406 are
    tag-split in EventFactory: Shopstr's tagless encrypted carts and its escrow records on the
    same numbers parse as UnrecognizedKind30405Event / UnrecognizedKind30406Event, addressable
    (supersession and NIP-09 by `d` unchanged) and never indexed.
    Owed: docs/migrations.md ("Open Markets kinds become searchable"), which also carries the
    NIP-15 kinds the 25a3f37c83 entry missed.
  KindNames names the three kinds; StatsRollup's per-kind labels pick them up.

NO ARROW: the store at 28a6020ca1c4 (below) names ce7f4db1b6 too, so every quartz line in all six
  modules reads it. Before that store pin, 94aad0feea7e carried `quartz:68268da413 -> ce7f4db1b6`,
  checked safe then: all 124 quartz members its two jars reference resolved against
  quartz-jvm-ce7f4db1b6 (javap over every class in both jars, inherited members followed).

DIRECTION: 25a3f37c83 is a strict ancestor of this, as is 68268da413.
  git merge-base --is-ancestor 25a3f37c83 ce7f4db1b6   # verified, full-depth clone

LEXICOGRAPHIC: 'ce7f…' > '6826…' ('c' > '6'): would win the store's conflict unforced; moot under
  force(libs.quartz) regardless.

Prev 25a3f37c83 = amethyst main @ 2026-10-15, the merge of vitorpamplona/amethyst#4491. 781 commits
over 68268da413, 149 of them touching quartz/. Taken for the client's rate-limit work, which
the monitor's fold walks run through (see docs/decisions/aliases.md):

  NIP-01 BACK-OFFS (nostr-protocol/nips#2498). OK and CLOSED carry an optional params element;
    for `rate-limited:` it is the back-off in ms. Each BasicRelayClient owns a RateLimitGate fed
    every message before its listener and asked before every write, so a refused REQ, EVENT or
    COUNT is held and re-sent once the back-off passes, however it was sent, reconnects included.
  PAGED WALKS. fetchAllPages pages by page size, re-asks a throttled or suspiciously short page
    after a back-off (PageRetryBackoff), keeps a walk on one connection, and reads limit,
    throttle and concurrency notices apart. RelayReads is its one caller here.
  ONE-SHOT FETCHES. fetchAllWithHooks, the call under every AliasProbe page, re-asks a relay that
    refuses a filter's `limit` at the limit it states (up to 6 times) before ending it `closed:`.
    The probe's own FALLBACK_PROBE_PAGE retry now meets that refusal only once quartz gives up,
    and a limit refusal no longer reaches the shape ladder as a refused shape.

SCANNED PER PACKAGE THIS RELAY IMPORTS (42): what moves besides the client:

  SEARCH DERIVATION. Purely additive: 9 kinds become searchable (1624, 5128, 30024, 30031,
    30301, 30302, 33402, 36767, 38385) and no searchable kind extracts differently. The store
    calls SearchFieldExtractor at runtime, so new writes derive on arrival; a kind-scoped
    reindex is owed for what is stored, docs/migrations.md ("nine kinds become searchable").
  NIP-85. ServiceType gains GrapeRank's 30382:hops, :reporters and :muters; both example
    configs' 10040 scans name them (RouterConfExamplesTest pins the vocabulary).
  Relay side, additive: RelayServerBase.connect / RelaySession take a clientAddress,
    PolicyResult.Rejected and OK/CLOSED a params, and RateLimitPolicy / RateLimiter are new and
    unused here. Each signature gained a DEFAULTED parameter, which removes the old JVM
    overload; this relay is recompiled, and the store's two jars reference none of them (all
    107 quartz methods they call resolve against quartz-jvm-25a3f37c83, checked with javap).

DIRECTION: 68268da413 is a strict ancestor of this.
  git merge-base --is-ancestor 68268da413 25a3f37c83   # verified

ONE ARROW, TO THE PIN: the store (94aad0feea7e, its main) still names 68268da413, so its
modules carry `quartz:68268da413 -> 25a3f37c83`; `force(libs.quartz)` settles it, and the
binary scan above is why that is safe.

LEXICOGRAPHIC: '25a3…' < '6826…' ('2' < '6'): the losing direction, moot under force().

Prev 68268da413 = amethyst main @ 2026-10-06, THE STORE'S OWN PIN: vespa-eventstore 14d8c7eeed (below)
names it, and this line follows it so the graph carries one quartz. It is the hint-provider /
NIP-50 / tag-relationship series (amethyst 9c34a3bc..d0cbdaf0) plus its review follow-ups; not
amethyst's tip 3e8137ab4e, whose Crowdin-sync commits fail on JitPack (the store's note has the
detail). 515 commits over bf2fefc621, 134 of them touching quartz/.

SCANNED PER PACKAGE THIS RELAY IMPORTS (47 of them, all six modules): the relay server, its
policies and backend, NIP-11, NIP-42, NIP-65, NIP-77, NIP-86, NIP-98 and NIP-FE have NO diff in
the range. What does move:

  SEARCH DERIVATION. SearchFieldExtractor / SearchableKinds: 42 kinds become searchable and 19
    extract differently (the store's note lists both); DeletionRequestEvent (5) and
    RequestToVanishEvent (62) are SearchableEvents now, their reasons indexed — shouldVanishFrom
    is unchanged. EventFactory builds 31890 as FeedDefinitionEvent. All fed `search_*` columns:
    a re-feed is owed, docs/migrations.md ("66 kinds re-derived").
  Event.dTag() defers to AddressableEvent.dTag(). RouterRefusalSink, the one caller, asks it only
    for an addressable kind, where the two read the same `d` tag.
  RelayUrlNormalizer gains a STRICT normalizeHintOrNull for relay hints; normalizeOrNull, the
    lenient one every call site here uses (config, verdicts, relay lists), is unchanged.
  Client side, additive and unused here: RelayAuthenticator.reauthenticate(url), the pool's
    RelayReqRefusals. DeletionIndex's comparator stops comparing a pubkey to itself; unused here.

DIRECTION: bf2fefc621 is a strict ancestor of this.
  git merge-base --is-ancestor bf2fefc621 68268da413   # verified

NO ARROW: with the store on 14d8c7eeed every quartz line in all six modules reads 68268da413,
clearing the `quartz:fcd76d2075 -> bf2fefc621` the previous store pin carried.
  ./gradlew :relay:dependencies --configuration runtimeClasspath | grep -E "quartz|eventstore"

LEXICOGRAPHIC: '6826…' < 'bf2f…' ('6' < 'b'): the losing direction, moot with no conflict left
and `force(libs.quartz)` regardless.

Prev bf2fefc621 = amethyst main @ 2026-09-28, the merge of vitorpamplona/amethyst#4243 — SAME TREE as
its head cba4a2d990, which this pin held before the merge (`git diff cba4a2d990 bf2fefc621` is
empty; the merge's parents are 77cb10e and cba4a2d990). Two fixes this relay's contract IT
(RelayContractIT) found, over amethyst main 77cb10e (42 commits over fcd76d2075):

  NIP-62 MATCHES THE URL A CLIENT WRITES. RelayTag.shouldVanishFrom compared the tag to
    RELAY_URL's normalized form as a STRING, so `["relay","wss://host"]` never matched
    `wss://host/`: OK true, nothing deleted, the "vanished" events republishable. Now compared
    as NormalizedRelayUrl.
  A NIP-77 RECONCILE IS NOT A REQ PAGE. NEG-OPEN ran through the REQ policy hook, so
    LimitsPolicy stamped DEFAULT_LIMIT on it and a reconcile covered the newest 500 ids of its
    filter and called that the set. IRelayPolicy.accept(NegOpenCmd) now runs the REQ rules by
    default (LensRequiredPolicy governs NEG-OPEN unchanged) and LimitsPolicy leaves the page
    limits out: NEG_MAX_SYNC_EVENTS (1,000,000) is the one bound, answered with NEG-ERR.

THE REST OF THE RANGE, for this relay: relay-server additions (EOSE completeness hints,
EventCommandHandler, RelaySession for them, #4238's kind-not-allowed narrowing) and NIP work the
relay does not serve. No searchable kind's indexableContent moves (the diff has none), so
nothing is re-derived and no re-feed is owed.

DIRECTION: fcd76d2075 and cba4a2d990 are both strict ancestors of this.
  git merge-base --is-ancestor fcd76d2075 bf2fefc621   # verified
  git merge-base --is-ancestor cba4a2d990 bf2fefc621   # verified

ONE ARROW, TO THE PIN: the store (ff54c9beb1 -> 5379e572dc below) still names fcd76d2075, so
its modules carry `quartz:fcd76d2075 -> bf2fefc621`; `force(libs.quartz)` settles it and the
store calls nothing the range changes (full build, archTest and the contract IT ran on the
identical tree as cba4a2d990).

LEXICOGRAPHIC: 'bf2f…' < 'fcd7…' ('b' < 'f'): the losing direction, moot under force().

Prev fcd76d2075 = amethyst main @ 2026-09-27, the merge of #4231 (39 commits over a53cafd577;
quartz/ moves in nipFERelayOverHttp, RelaySession, NIP-49 and a new Bech32Transcription, the
rest is app, geode and translations). MOVED FOR THIS RELAY: NIP-FE was simplified to the text at
https://nostrhub.io (kind 30817, d=nip-fe-nostr-over-http), and quartz's handler follows it:

  ONE ENDPOINT, WHOLE FRAMES. A request is a POST to the relay's URL whose body is one client
    frame as sent on the socket (`["REQ","q1",{...}]`, COUNT or EVENT), and the answer is the
    socket's own frames, subscription id included. /req, /count, /event and the id stripping are
    gone; HttpRelayRequest has no command, HttpRelayCommand no paths.
  ONE PARSE. The handler parses the frame once (fromJsonToCommand) to refuse what HTTP does not
    carry and to know what ends the answer, and hands text and command to the new
    RelaySession.receive(text, parsed), which still runs acceptMessage on the text.
  REFUSALS BEFORE A COMMAND RUNS ARE A NOTICE LINE (400/413/429/503); a refused NIP-98 token is
    the command's own refusal (CLOSED for REQ/COUNT, OK false for EVENT), 401.
  NIP-86 SHARES THE URL, told apart by `Content-Type: application/nostr+json+rpc`; this relay's
    relayPosts does the split, geode's KtorRelay the same.
  The client side (HttpRelayClient, HttpRelayTransport, OkHttpRelayTransport) is new and unused here.

THE STORE MOVED WITH IT (vespa-eventstore #153, 9522ee0768 below), for the graph's sake only:
neither its store nor engine jar references a class in any package the range touches (scanned),
so e62745df71 ran on this jar too, with the arrow below.

DIRECTION: a53cafd577 is a strict ancestor of this.
  git merge-base --is-ancestor a53cafd577 fcd76d2075   # verified

LEXICOGRAPHIC: 'fcd7…' > 'a53c…' ('f' > 'a'), and `force(libs.quartz)` settles it regardless.
NO ARROW: with the store on 9522ee0768 every quartz line in all six modules reads fcd76d2075. For
one commit (e6a71f2) the store still named a53cafd577 and five modules carried
`quartz:a53cafd577 -> fcd76d2075`, an arrow TO the pin, which #153 cleared.
  ./gradlew :relay:dependencies --configuration runtimeClasspath | grep -E "quartz|eventstore"

Prev a53cafd577 (2026-09-27, #4214: NIP-FE's review fixes; the relay's HTTP commands moved onto
quartz's HttpRelayHandler, NIP-98 signing the session in through authenticateByTransport and the
policy chain, MultiAddressAuthPolicy overriding authorizeTransport).
Prev ec16988e3a (2026-09-27, #4222: a stale replaceable is `replaced:` / OK
false again, which this relay's store had never left; #4215 re-derives three
kinds' search fields, see docs/migrations.md).
Prev 28bf170f92 (2026-09-26, PR #4212: NIP-FE's engine half, SessionSink and
receive(Command); 110 event classes renamed to their NIP names, which moved the
store pin with it; the engine acks `duplicate:` with OK true; 142 -> 150
searchable kinds; jackson-module-kotlin, and with it kotlin-reflect, left the
classpath).
Prev a8e8778265 (2026-09-10, PR #4069: the searchable set 133 -> 144 by that
note's count, SearchableEvent's read path, the allocation-free FilterMatcher).
```

## `vespaEventStore` (versions)

```text
7d635159d7fed1aaab82b6979f7ba59b6a003c45 = vespa-eventstore branch claude/awesome-edison-x5mb5y (PR #168), five commits
over store main 0aed31fda8 (below):
  - the feed body written into a char array (JsonChars): the relay's CPU per published kind 3
    fell from 550/523 us to 513/490 (docs/ingest-benchmark.md's harness);
  - event.sd pins the document language ("en") instead of having Vespa detect it: Vespa's CPU
    per event 1,701-1,767 us -> 1,310-1,533 on a 404k-event slice of the live relay. Deploys
    with no validation override and no reindex;
  - 3-character CJK words and voiced kana reach a note body (exact recall of 3-character
    Japanese and Chinese words 3.6% / 8.3% -> 100%);
  - language:pt / language:es: the store stems Portuguese and Spanish bodies into
    search_text_pt / search_text_es (Lucene 9.12.2 light stemmers, Apache-2.0, now on this
    relay's runtime classpath via the store's engine), so a search with the hint reaches a
    word's other forms, ranked below the typed form. Two new index fields: the schema deploy is
    additive;
  - the review fixes on top: the language guess no longer lets URL pieces, bech32 strings or
    words shared with French/German/Italian vote, stemFields costs 4 us per kind 1 instead of
    26 (the relay's feed writer runs it), and language:pt-BR names Portuguese.

DEPLOY STEP: the stems of notes stored before the deploy come from the FTS reindex, which now
  re-puts every body that reads as Portuguese or Spanish (about 1% of kind 1s; on every walk,
  since the columns cannot be read back). After the schema deploy:
    REINDEX_FTS_ON_START=true REINDEX_FTS_KINDS=1   # or unset KINDS for every body kind
  git merge-base --is-ancestor 0aed31fda8e8 7d635159d7fe   # verified

GATED, the store with quartz 99d05ee122: its integration suite against a real Vespa in Docker
(:benchmark:test -Pintegration, green) on 04495876a406, and on 7d635159d7fe its unit build
(engine + store, green) plus the integration suites the review fixes reach (SearchBodyGramIT,
RankRegressionIT, VespaParityIT, green), run locally. This relay's own gate is in the commit that set this line.

SPELLED WITH 40 HEX DIGITS: the 12- and 10-digit spellings of 04495876a406 hit the transient
maven-publish resolution failure on their first builds (com.vanniktech.maven.publish 0.37.0 not found), which JitPack
keeps per version string. com.github.NosFabrica serves store and engine at the full hash, and
the store's pom names engine at it and quartz-jvm at 99d05ee122.

NO ARROW expected: nothing else in the graph names the store.

Prev 04495876a406f0a5f88268f7445a80b2c47113f6 (this branch only: the same four commits
without the review fixes). Prev 0aed31fda8e8:

Prev 437712208b = vespa-eventstore branch claude/awesome-edison-x5mb5y, one commit over 94aad0feea7e:
the feed body written in one pass instead of through a JsonObject tree (EventDoc.feedJson,
byte-identical to the tree, checked over all 65,536 UTF-16 units). Engine code only; the relay's
CPU per published kind 3 fell 3-5% (docs/ingest-benchmark.md).
  git merge-base --is-ancestor 94aad0feea7e 437712208b   # verified

GATED, on this pin with quartz 96100f4309200bca4a9cbacfa7bd4ab9bceaf1d1: the store at 437712208b,
its unit build (799 tests, 0 failures) and its integration suite against a real Vespa 8.763.13
in Docker (23 tests, 22 passed, StagingCorpusIT skipped by design, 0 failures), read off the
result XML. This relay's own gate is in the commit that set this line.

SPELLED WITH 10 HEX DIGITS: the full 40-digit spelling's first build hit the same transient
maven-publish resolution failure as 94aad0feea; `com.github.NosFabrica` serves store and engine
at 437712208b.

LEXICOGRAPHIC: '4377…' < '94aa…': the losing direction, moot — nothing else names the store.


0aed31fda8 = vespa-eventstore main @ 2026-10-17, the merge of PR #167: store main 28a6020ca1c4
(above) plus the one-pass feed writer (437712208b, below), VESPA_FEED_COMPRESSION (the feed
client's compression switch; its default stays `auto`, docker-compose.yml sets `none`,
docs/ingest-benchmark.md), and the store's own quartz pin moved to 99d05ee122, this relay's.
Store code over 28a6020ca1c4 is the feed writer and that switch; over ece7b81ca8 (this branch's
previous pin) only the quartz pin and its notes.
  git merge-base --is-ancestor 28a6020ca1c4 0aed31fda8   # verified
  git merge-base --is-ancestor ece7b81ca8 0aed31fda8     # verified

GATED, the store at 307e83f (its merged head) with quartz 99d05ee122: its unit build (803 tests,
0 failures, run locally) and its CI on that commit, lint, build and integration against a real
Vespa, all green (integration finished after the merge). The integration suite last ran locally
at ece7b81ca8 (23 tests, 22 passed, StagingCorpusIT skipped by design, 0 failures); nothing the
store calls moved between the two quartz pins. This relay's own gate is in the commit that set
this line.

SPELLED WITH 12 HEX DIGITS: the 10-digit spelling's first JitPack build hit the transient
maven-publish resolution failure again (com.vanniktech.maven.publish 0.37.0 not found), and JitPack
keeps a failed build per version string. com.github.NosFabrica serves store and engine at
0aed31fda8e8, and the store's pom names engine at 0aed31fda8e8 and quartz-jvm at 99d05ee122.

LEXICOGRAPHIC: '0aed…' < 'ece7…': the losing direction, moot — nothing else names the store.

Prev ece7b81ca8 and 75be510686 (this branch only): PR #167's head with quartz e5bdf1bd37, and
before that 020e714858. Prev 28a6020ca1c4, 437712208b (above), 94aad0feea7e.

The event store this relay serves, from JitPack (vespa-eventstore @ commit).

e1ecd7f23e = store main through PR #97, ten commits over 2bc79f5f40, and the
INVERSE of the last one. That bump moved where rows land and left the cluster
alone until the corpus was re-derived; this one moves no row and re-derives
nothing, and its one deployment step is a RESTART that the deploy reports
success without doing. What it changes is what a read COSTS — a NIP-45 COUNT
that was recalling the page it summarizes, and a NIP-50 search that was
matching on one core — plus one gate that a search operator this relay
documents had stopped reaching.

IT ASKS FOR NO NEW QUARTZ. e1ecd7f23e's own libs.versions.toml still names
cecc3287b2, the string pinned above, so the store line moves alone this time
and the note over the quartz line stands exactly as written.

WHAT MOVED, in the store's own commits — the first two are what this relay
pays for:

  A SEARCHING COUNT NO LONGER RECALLS THE PAGE IT SUMMARIZES. A ranked or
    searching NIP-45 COUNT answered with `index.rawSearch(q).size` — a full
    document summary per counted event (id, pubkey, created_at, kind, tags,
    content, sig, owner) over the wire and through the JSON decoder, to
    produce an integer — and a COUNT reaches the store carrying
    `limit: 100000`, so that is 100 000 documents at ~0.7ms each. Measured on
    the production relay (2026-09-01): `bitcoin` cost 76.0s to COUNT against
    4.9s to SEARCH the same thing, `nostr` 84.7s against 19.2s. The tell that
    the excess was DOCUMENTS and not matching is that nostr's match set is
    three times bitcoin's while the excess on both rows is the same minute.

    `EventIndex.count` now asks the engine for hits=0 and reads the response's
    own `totalCount`, which already has the trust gate's subtraction applied
    — the property that makes this legal at all, since a lensed REQ serves
    less than it matches and the plain grouping counts the UNGATED set.
    Measured against a real Vespa on 360k staging events under a real
    65k-pubkey web of trust, totalCount equalled the documents actually served
    at every floor from 0 to 95 (12 215 ungated, 4 950 at floor 20, 530 at
    floor 95) — 39ms against 4 559ms, with zero documents crossing the wire.
    Two profiles stay off that path for OPPOSITE reasons, and
    `EventYql.countProfileOf` is that decision: `recency` gates nothing, so
    the grouping already counts it exactly; `recency_gated` does gate, so it
    counts on its full-scan twin — a match phase CAPS totalCount. The
    STORE-C01 clamp to the filter's limit is unchanged, so every answer is the
    number it already was.

    WHAT IS NOT AFFECTED, because it is easy to read this as a stats change:
    /stats.json's rows are StatsYql groupings and never were NIP-45 counts,
    and readiness.js and the monitor page count plain `kinds`/`authors`
    filters, which took the grouping path before this bump and take it after.
    The 76-second row is a SEARCHING count — something a client sends us.

  THE MATCH THREADS GO TO THE ONE QUERY THAT CANNOT BE MADE SMALL, and this
    is the half that does NOT arrive by deploying — see step 2 below.
    `services.xml` moves `numthreadspersearch` from 1 to 4, but as a CEILING
    rather than as the setting every query gets: it is a per-query parameter a
    query may only lower, and `EventYql.MATCH_THREADS` asks for a single
    thread on every shape that is not a relevance search over an unbounded
    match set — plain recall, both match-phase profiles, counts, groupings,
    the snapshot walk, and the expansion's keyed lookups.

    The shape that keeps the headroom is the one no request can shrink: a
    NIP-50 search for a common word matches millions of postings and SCORES
    every one of them, and measured read-only against staging the same `nostr`
    costs 19.4s at limit 1, 19.2s at limit 50 and 23.0s at limit 500.
    `searchTrace`, new in this range, is the harness that says which half is
    the cost: it ablates one clause family at a time off EventYql's own
    assembled query and prints totalCount beside every row, so a variant that
    got fast by matching LESS cannot pass for a speedup. Its answer is that
    every loose matcher is load-bearing recall — the body's trigram phrase net
    is 12% of the plan for `nostr`, the near-attribute prefix/fuzzy clauses
    32% for `bitcoin` — so what is left to take is parallelism, which changes
    no answer.

    MEASURED (4-core container, 360k staging events, query instant pinned):
    bitcoin 44.6ms -> 25.1ms, nostr 84.3ms -> 44.1ms, lightning 23.9 -> 17.0,
    zap 12.8 -> 10.1. The served page is byte-identical at every thread count
    across 20 terms at both page depths, which MatchThreadPageIT pins against
    a real Vespa. The price is 3.8% of throughput — 85.0 q/s at one thread
    against 81.8 at four, eight concurrent clients.

  `filter:rank:gte:N` NOW DELETES A SPLICED ROW. README.md documents that
    operator, and it was being honoured for every row on a page except the
    ones a Trusted List spliced in — the one place a reader cannot see it
    failing. It is fallout from the pin this replaces: §13.1 deliberately
    stopped multiplying a member's placement by `wot_mult()`, and that
    multiply was the only thing suppressing a member below the floor.
    `spliced_member` declares no rank-score-drop-limit, so `wot_mult()=0`
    never DELETED a hit even then — it pinned the score to 0, which was at
    least the bottom of the page, and `max(member_rung(), …)` now floors that
    straight back up into the affiliation band.

    The gate returns as its own number rather than by restoring the multiply,
    because `query(min_rank)` has the other job on that profile: it anchors
    the trust curve inside `delegated_place()`, so raising it to gate would
    MOVE every member it did not drop. `query(member_floor)` gates and does
    nothing else, defaults fail-open below every real user_score, and pairs
    with a drop-limit only that sentinel can reach. It travels only when the
    reader ASKED for a floor: the default one this relay stamps on every
    lensed read deliberately does not, since a member is on the page because a
    provider the reader enrolled vouched for it, and gating that on the
    reader's own opinion of a stranger would leave the delegation serving
    nothing — the same reading, and the same DEFAULT_MIN_RANK line,
    RelayProtocolTest already pins for the declaration companion.

  THE EXPANSION'S LOOKUPS ARE PLANNED BEFORE THEY ARE SENT. A pointer's own
    relevance is a query-level rank feature, so two Trusted Lists that ranked
    differently cannot share a subject lookup — on a ranked page that is one
    lookup per list, and they went out INLINE, one after another: up to 64
    SEQUENTIAL engine queries on a single searching REQ. Every lookup a lens
    needs is now planned first, sent at QUERY_FANOUT, and folded back IN PLAN
    ORDER (`mapBounded`, not `forEachBounded`, precisely because it preserves
    list order — `putIfAbsent` still files a subject two pointers name under
    the better-ranked one, so the answer is identical). The budget moves from
    a private `MAX_LOOKUPS` to `SearchExpansionLimits.maxLookups`, still 64,
    and now cuts a DEFINED place: plan order is page order, which on any page
    that can be sorted is relevance order, so a page too wide for the budget
    loses the subjects of its worst-ranked pointers rather than of whichever
    family the loop nesting happened to reach last.

  AND TWO GUARDS ON PATHS NOTHING REACHES YET. `EventSelection.build` returns
    null for a query carrying `idWeights`/`authorWeights`: a dotProduct is a
    RECALL constraint a selection has no form for, so omitting it handed back
    a selection that streams the whole corpus while looking like it named a
    few hundred keys — the silent-superset failure that builder returns null
    to avoid. And `isPureIdLookup` no longer sends a weighted id set down the
    document-API get path, which sees neither the dotProduct nor the rawScore
    the member rung ranks on. Neither is reachable from the store today (the
    expansion's weighted lookups go to searchRanked), which is the argument
    for guarding them now rather than after something wires them up.

WHAT IT COSTS THIS REPO.

NOTHING TO COMPILE. `EventQuery.memberFloor` is defaulted and the store fills
it in itself, off the `minRank` this relay already sends;
`SearchExpansionLimits.maxLookups` is defaulted; and `EventIndex.count` gained
KDOC ONLY, no new abstract member — which is worth checking rather than
assuming, because RelayProtocolTest implements `EventIndex` by hand.

TWO DEPLOYMENT STEPS, AND THE SECOND IS THE ONE `AUTO_DEPLOY` CANNOT DO.
Both report success while doing nothing if skipped — docs/migrations.md
carries this bump as its own entry:

  1. THE SCHEMA DEPLOYS ITSELF. `event.sd` changed, and it is rank-profile
     work only — one rank input (`query(member_floor)`), one first-phase
     expression and one `rank-score-drop-limit`, all inside `spliced_member`;
     not one `field`, `indexing`, `attribute` or `match-features` line moves.
     `AUTO_DEPLOY` on boot is the whole migration and `configChangeActions`
     should carry nothing to act on. Read it anyway, per docs/migrations.md.

  2. RESTART VESPA. THIS IS THE STEP. `numthreadspersearch` is
     `restart`-flagged in Vespa's own proton.def (`numthreadspersearch int
     default=1 restart`), so the deploy NAMES the change and does not make
     it. Measured by deploying the store's package at 2bc79f5f40 and then at
     e1ecd7f23e onto one Vespa (2026-09-02): the second answers
     `activated: true` carrying
     `configChangeActions.restart[0].serviceType = "searchnode"` with the
     message `proton.numthreadspersearch has changed from 1 to 4`, and 95
     seconds later the `vespa-proton-bin` process still carried its original
     start time. (`refeed` and `reindex` came back EMPTY on that same
     response, which is step 1 confirmed rather than assumed.) That is
     exactly the trap AGENTS.md records for `<jvm options>`, one store bump
     later and on the CONTENT side rather than the container — and here
     skipping it is a silent NO-OP rather than an outage:
     `ranking.matching.numThreadsPerSearch` may only LOWER the configured
     ceiling, so EventYql's ask for four threads is clamped back to one, the
     search keeps the latency this bump exists to halve, and every log line
     and every answer stays correct. `docker compose restart vespa`,
     deliberately, after the deploy — and confirm it by the searchnode's
     START TIME, since `vespa-get-config` and proton's own `/state/v1/config`
     both report the new value on a node that never restarted
     (docs/migrations.md has all three).

  AND NO RE-FEED, unlike the two pins before it. Nothing changes what a put
  derives: quartz is unmoved, no feed-side tokenization moved, and no column
  here is written differently. `REINDEX_FTS_ON_START` stays off.

THE GATE AT THIS COMMIT, per AGENTS.md's rule — the relay's own tests run on
InMemoryEventIndex and cannot see a Vespa-only regression:
  git clone https://github.com/NosFabrica/vespa-eventstore && cd vespa-eventstore
  git checkout e1ecd7f23e
  DOCKER_MIN_API_VERSION=1.24 dockerd &          # AGENTS.md: no daemon by default
  TESTCONTAINERS_RYUK_DISABLED=true ./gradlew test -Pintegration --max-workers=1 --no-daemon
Run HERE: 48 suites, 520 tests, 0 skipped, 0 failures, 24m27s (`skipped="0"`
read off the result XML, not off `BUILD SUCCESSFUL`, per AGENTS.md — a run
whose ITs all abort on `assumeTrue(dockerAvailable())` prints the same line).
FIFTEEN ITs stood up a real Vespa, and TWO of them are this bump's own.
MatchThreadPageIT reads the page back at one, two and four match threads and
requires the same ids in the same positions, with the query instant PINNED on
every arm — without that the recency multiplier moves under the runs and a
difference proves nothing about threads, which is the trap that test was
written after walking into. SearchCountIT pins the count in PAIRS: it must
equal the page the same query serves AND be strictly smaller than the raw
match set wherever the floor bites — a grouping count passes the first half
and fails the second, which is the whole anti-regression. SplicedMemberWeightsIT
grew the case the gate above exists for: at a floor of 20 exactly the member
this observer trusts at 90 survives, placed exactly where it was (the gate
admits, it does not rank), and the floorless read still returns all four. Up
from 46 suites / 510 tests at 2bc79f5f40 — the two new suites and ten tests
are exactly this range's.

DIRECTION: 2bc79f5f40 is a strict ancestor of this (`git merge-base
--is-ancestor`, not the dates).

LEXICOGRAPHIC: '2bc7…' < 'e1ec…' ('2' < 'e'), so this sorts ABOVE the pin it
replaces — the SAFE direction, the way the last one did not go. Read it for
exactly what the note at the top of this file says it is and no further: it
records that THIS bump would survive a conflict, not that one happened and
not that the pin took. Nothing else in the graph names vespa-eventstore, all
four `api(...)` declarations take this one version ref, and there is no
`force(libs.vespa.eventstore.store)` anywhere because no conflict has ever
been possible. The proof stays the resolved graph — every eventstore line
reads e1ecd7f23e with no `->`:
  ./gradlew :relay:dependencies --configuration runtimeClasspath | grep -E "quartz|eventstore"

9f07b0b30a = vespa-eventstore main @ 2026-09-04 (PR #98, the trust descent).
THE BUMP THE SEARCH LATENCY WORK WAITED ON: a ranked search under an observer
now walks the trusted authors' documents (`author_max_rank >= T`, a rung
the store PROVES is the exact page from the profile's own constants) instead
of a common word's postings — measured on a 1.28M-note slice of staging
page-identical to this pin's, `the` 154ms -> 46ms, and estimated 0.5-1s on
staging where it is 16s. docs/search-latency.md has the whole account. On
the wire NOTHING moves: a REQ still answers its events in final rank order,
then EOSE, so no relay code changes with this line. What DOES happen at boot
is the store's own one-time walk writing `max_rank` onto every reputation
document (~340k on staging), with the descent off until it returns — the
`trust descent:` boot line in RelayMain says when.

Quartz stays at cecc3287b2: the range e1ecd7f23e..9f07b0b30a touches no
quartz pin (one commit, the descent, plus its merge).

The store's integration gate ran against c89ed18 — the branch head, whose
tree the merge commit 9f07b0b30a carries UNCHANGED (`git diff c89ed18
9f07b0b30a` is empty; main was its ancestor) — 18 tests, 0 failures,
16m42s, on 2026-09-04.

DIRECTION: e1ecd7f23e is a strict ancestor of this (`git merge-base
--is-ancestor`).

LEXICOGRAPHIC: '9f07…' < 'e1ec…' ('9' < 'e'), so this sorts BELOW the pin it
replaces — the direction that would LOSE a conflict, exactly as the note at
the top of this file warns a hash may. It costs nothing here for the reason
the previous note gives: nothing else in the graph names vespa-eventstore,
so there is no conflict to lose. It is written down so the next reader does
not take the previous bump's "safe direction" as a property of the pin.

e0e638d73f = vespa-eventstore main @ 2026-09-04 (PR #99, the descent after
its first deploy). THREE THINGS, all learned on staging the day 9f07b0b30a
went out: the `max_rank` walk RETRIES a refused engine instead of leaving
the descent off for the life of the process (the walk started as Vespa was
being OOM-killed and died on its first page); a read the trust projection
WRITES FROM — the card fetch under every derivation, the 10040 read under
the attribution map — now refuses an answer the engine cannot call full or
served short of its count (17k reputation documents were removed by a
boot-time recompute derived from a fetch that missed cards; the read path's
rounded-100 carve-out is right for a page and wrong for a delete); and
`VESPA_TRUST_DESCENT=off`, the operator's switch, because with the descent
ON staging's descending shape got SLOWER (`bitcoin` 2.7-4.4s -> 8.4-14s,
text-only and recency controls unchanged): on that two-node cluster Vespa
walks the word's postings and filters by the imported range instead of
driving by it, so each rung costs a full walk. docs/search-latency.md and
docs/configuration.md carry both accounts; the boot line in RelayMain now
says which way the switch is set, which is the one relay change here.

Quartz stays at cecc3287b2: the range 9f07b0b30a..e0e638d73f touches no
quartz pin (three commits and their merge).

The merge commit e0e638d73f carries the branch head b139c57's tree
UNCHANGED (`git diff b139c57 e0e638d73f` is empty; main was its ancestor).
Engine 174 and store 343 unit tests ran on b139c57; the integration gate's
standing on this pin is recorded below the bump that ran it, if one did.

DIRECTION: 9f07b0b30a is a strict ancestor of this (`git merge-base
--is-ancestor`).

LEXICOGRAPHIC: 'e0e6…' > '9f07…' ('e' > '9'), so this sorts ABOVE the pin it
replaces — the safe direction, and as the previous two notes say, one that
has never been needed: nothing else in the graph names vespa-eventstore.
ec2976b679 = vespa-eventstore main @ 2026-09-04, the merges of PR #104 and
#105 (the lock audit) over #103's grouping aggregate.

WHY THIS BUMP: the trust-gate split shipped with the locks in the wrong
order. A card took `writes` and then waited for `trustGate` WHILE HOLDING IT,
so for the length of a drain slice every plain writer in the process queued
behind the card — measured here at a 20s hold on `lock.ingest.hold` with
`lock.ingest.trust.wait` at 13,645,290ms. `cedd2aa` inverts it: the gate is
taken first, holding nothing, then `writes` for one short hold. It also
splits pure-record batches so only the card portion pays the gate at all,
and `364d419` makes the dirt ledger lock-free.

WHAT IT DOES NOT FIX, so the next reader does not expect it to: the trust
derive still reads inside the gate, ~25,000 documents per 50 subjects
(FETCH_CHUNK), ~14.5s per 500-subject slice. One kind-10040 provider-list
change re-derives every subject its services ever scored — three of them in
24h produced ~4 hours of gate time here. Cards stay slow; what this bump
frees is everything that is NOT a card, above all the monitor's kind-30166
verdicts, which had fallen to ~1/min with 18,157 grades still to restore.
Superseded: 8a643d4e27 = the merge of PR #103 — the grouping aggregate the
discovery fast path calls.

ON A MAIN COMMIT AGAIN. #196 merged while this line still named
`claude/distinct-tags-grouping`'s own sha, which resolved (that commit is an
ancestor of the merge) but left `main` carrying a branch dependency. The two
repos land in one order — store PR first, then re-pin, then the relay PR —
and this is the step that gets skipped when both merge together.

Superseded: 3cadb285d5 = the merge of PR #102, which
fixes a bug #101 shipped: the trust gate was taken by `insert` but not by
`batchInsert`, `transaction` or the sweeps, so the mirror's BULK card ingest —
which writes reputation cells inline — ran with no exclusion against the trust
drain. Do not pin ea7077fbbc (the #101 merge): it carries that bug. Back
on a MAIN commit: the three pins before this one named branch commits while
that PR was open, which is a dependency `main` must never carry.

What it brings, in the order it happened: the lock instrumentation
(`IngestStats.snapshot()` with calls/mean/max, a live `heldNow()` holder,
`proj.fetch` split into `.derive`/`.maxrank`), the per-slice write gate it
found, and the trust-gate split that followed. Measured on staging: a client
write 32-42s -> 0.05s, mirror ingest 0 -> 2,884 ev/s.
27d43f84c1 = vespa-eventstore main @ 2026-09-06, the merge of PR #121 — the
service walk falls back to the document-API scan when the engine cuts its
sorted read. THE FIX for the projection that stopped at 236 of 342 services.

WHAT WAS HAPPENING. The walk lists a service's card ids through SEARCH with
`order by created_at desc`. A sorted read is one Vespa is PERMITTED to
truncate — `sorting.degrading` defaults true — and it reports the truncation
as `match-phase`. The coverage guard then correctly refuses the partial
THE PULSE CAN NAME WHO MADE THE ENGINE WORK (#132), plus the package
restructuring (#131) this pin already carried.

#132 answers two questions this deployment could not, each of which cost real
time here:

WHO ASKED. CostLedger booked engine cost per rank PROFILE and nothing said
who made it do that, so attributing load meant an ablation — on 2026-09-06
the only way to find what drove ~20M docs/s was to scale the mirror to zero
and watch it fall. engineQuery now also books (activity, profile, shape),
heaviest first. Shape, never terms: the pulse page is admin-gated BECAUSE it
quotes searches, and attribution must not become a second search log.

WHAT THE ENGINE HAS LEFT. EngineResources reads proton memory/disk and
feed-block per content node from /metrics/v2/values — the SAME endpoint the
store already queries, so no new port or config. This deployment lost two
content nodes to OOM on 2026-09-07/08 (one on an app-package activation at
0.810, one on a routine restart at 0.808) and in both cases the numbers that
would have predicted it were only reachable by hand.

DIRECTION: b767bdb7a8 is a strict ancestor of this — main plus #132.
  git merge-base --is-ancestor b767bdb7a8 58011b670c   # verified

ALSO #133, which wires the headroom probe: #132 shipped EngineResources as
DEAD CODE — parsed, tested, and constructed nowhere, so the feature did not
exist and every test still passed because they drove the parser seam. The
store now references the index accessor, so deleting either end breaks the
build; the guard is the compiler, not an assertion.

LEXICOGRAPHIC: '5801…' < 'b767…' ('5' < 'b'), the unsafe direction. No
`force(…)`: nothing else in the graph names vespa-eventstore. Every
eventstore line must read 58011b670c with no `->`:
  ./gradlew :relay:dependencies --configuration runtimeClasspath | grep -E "quartz|eventstore"

Superseded: b767bdb7a8 = #131, the package restructuring (every package one
owning module, a read-only engine handle on the front door), published 1.0.1.
d0e25c3325 = #130, the trust descent removed — measured 1.6-4x SLOWER than
no descent because the rung was a range over an imported field with no
posting list to seek. ab90b08f02 = #128 (the streamed visit's server
deadline, which is what finally let the projection finish).

Superseded: 4c2b026cfc = #127. 61b39e7280 = #125. a2499fa4c1 = #126.
cba205940b = #124. 1211420526 = #122 + #123. 27d43f84c1 = #121.
52ac77e5ac = #120. 34fb127523 = #119. 02b528a85a = #118. 8f353d8d05 = #117.
2fa2b67141 = #116. a23a6a3a68 = #113/#114. aca0552c4e = #112.
53441182f5 = #108.
WIDER CURSOR PAGES (#134). An id walk is billed per PAGE, not per id: a page
is `order by created_at desc` with a limit, and proton matches every document
in the cursor's remaining range, sorts, and returns the top N — the limit
never touches the matching, which is nearly all of the cost. PAGE_IDS went
2,000 -> 20,000, which is ~5.4x less work on the largest consumer this
cluster has.

WHY IT MATTERED HERE: read off the mirror's own pulse (`engineByCaller`, made
reachable by relay #230), the Snapshot walks were 79.3% of ALL engine time on
staging — 10.9 of 13.7 core-hours in 14.5h, across 653,476 pages that matched
148,493,275,456 documents between them, on a cluster with NO USERS.

Measured on staging, same shape, only the limit varying (totalCount 2,809,568
and coverage.full at every size): 2,000 -> 0.122s/page, 20,000 -> 0.227s,
50,000 -> 0.363s, 100,000 -> 0.643s. Ten times the rows for 1.86x the time.
100,000 is not a value it may take: the fetch is PAGE_IDS + TIE_SLACK and a
page over the query profile's maxHits cap is refused outright, not trimmed.

DIRECTION: 58011b670c is a strict ancestor of this.
  git merge-base --is-ancestor 58011b670c 2e05757a41   # verified

LEXICOGRAPHIC: '2e05…' < '5801…' ('2' < '5'), the UNSAFE direction — this
sorts BELOW the pin it replaces, so a conflict would resolve away from it. No
`force(…)`: nothing else in the graph names vespa-eventstore. Every
eventstore line must read 2e05757a41 with no `->`:
  ./gradlew :relay:dependencies --configuration runtimeClasspath | grep -E "quartz|eventstore"

THE SCAN WALK IS TIMED (#135). `visitIdsByScan` had no instrumentation of any
kind while every sibling branch had it -- walk.cursor.decide, walk.ids.page,
walk.ids.tiegroup, walk.cursor.countat -- and a document-API visit is not a
query either, so it books no engine time against any caller on the pulse. A
walk could read every document in the corpus and look, on every instrument the
process has, like a walk doing nothing.

Taken to close a 44x hole in the accounting. On staging with NO USERS: sync
drives ~15.5 cores of Vespa (proved by scaling sync to 0 -- CPU fell to 0.26
cores in 90s and stayed), Vespa matches ~7.0M docs/s on the event doctype, and
the store's query ledger accounts for 160k/s. The scan is the only path to
Vespa that is neither a recorded query nor a timed stage.

It is also the standing explanation for two fixes that moved no CPU: the
age-banded schedules and #134's wider pages both touch the CURSOR path only,
and the scan cannot narrow -- a selection is evaluated per document, so
`created_at` bounds shrink what is RETURNED, never what is READ.

NEW STAGES: `walk.scan`, `walk.scan.page`, `walk.scan.unselectable`. Read them
with `walk.cursor.decide`, which counts every choice: decide-minus-scan is how
often the cursor won. MEASURES, DOES NOT FIX -- shipped to kill or confirm the
hypothesis, not on the assumption it is right.

DIRECTION: 2e05757a41 is a strict ancestor of this.
  git merge-base --is-ancestor 2e05757a41 3c6f651a1c   # verified

LEXICOGRAPHIC: '3c6f…' > '2e05…' ('3' > '2'), so this sorts ABOVE the pin it
replaces -- the SAFE direction, unlike the last two bumps. Every eventstore
line must still read 3c6f651a1c with no `->`:
  ./gradlew :relay:dependencies --configuration runtimeClasspath | grep -E "quartz|eventstore"

ELEVEN KINDS BECOME SEARCHABLE (#136). The first store pin in this file that
carries no store code at all: `git diff 3c6f651a1c..c11472eb18` is five files,
and the only one under `src` is a test. What it carries is the QUARTZ pin
above — the store's own libs.versions.toml moves cecc3287b2 -> a8e8778265, and
`force(libs.quartz)` means this repo has to move with it or the bump is a
no-op. Read the quartz note for what upstream changed; this one is about what
the relay has to DO about it.

WHAT THE RELAY GETS: kinds 31, 32, 33, 818, 30040, 30041, 30045, 30142, 31987,
32176 and 34259 now parse as `SearchableEvent`, so `SearchExtractors.extract`
derives search columns for them on put where it derived none before. Nothing
else about a put changes, no column is written differently for any other kind,
and `event.sd` does not move — so there is no deploy step, nothing for
`configChangeActions` to report, and no restart.

RECALL IS FORWARD-ONLY UNTIL THE WALK RUNS, and that is the one thing about
this bump that is not free. An event of those kinds stored BEFORE it carries
no search fields at all — not the wrong tier, none — because when it was
ingested nothing parsed it as searchable. It is therefore invisible to NIP-50,
and unlike the last two bumps' backfill that is a RECALL hole rather than a
ranking one: the search does not rank it low, it does not return it.

`REINDEX_FTS_ON_START` is the repair, for the same reason it was the repair
for the Trusted List titles: the store's drift check re-puts a document whose
extracted columns differ from the stored ones, `visitDocsPage(EventQuery())`
walks EVERY document rather than the searchable ones, and a pre-bump kind-30040
doc differs by all of them. docs/migrations.md carries the entry and the
procedure; the walk is resumable and the cursor survives a restart.

It is also the CHEAPEST such repair this file has recorded, because of the
other half of this change: no stream ever fetched these kinds, so the back
catalogue is whatever arrived by direct write to this relay, not a mirrored
corpus. sync.conf.example's `contentViaOutbox` now asks for all eleven — the
same shape as the three NIP-34 git statuses that were missing there, searchable
kinds no stream fetched — so the corpus this walk is worth running against is
the one that arrives AFTER the router is restarted with the new list.

THE GATE AT THIS COMMIT, per AGENTS.md's rule — the relay's own tests run on
InMemoryEventIndex and cannot see a Vespa-only regression:
  git clone https://github.com/NosFabrica/vespa-eventstore && cd vespa-eventstore
  git checkout c11472eb18
  DOCKER_MIN_API_VERSION=1.24 dockerd &          # AGENTS.md: no daemon by default
  TESTCONTAINERS_RYUK_DISABLED=true ./gradlew test -Pintegration --max-workers=1 --no-daemon
Run HERE on 2026-09-10: 78 suites, 685 tests, 0 failures, 27m25s. EIGHTEEN of
those suites are ITs against a real Vespa and SEVENTEEN of them ran —
VespaParityIT, SearchCountIT, SearchExactTextIT, SearchPrefixLadderIT,
SearchBodyGramIT, RankRegressionIT, FieldCoverageRankIT, MatchThreadPageIT and
the rest. `skipped="1"` read off the result XML rather than off `BUILD
SUCCESSFUL`, per AGENTS.md, and the one skip is StagingCorpusIT declining on
`Assumption failed: set STAGING_CORPUS to a capture_staging.py export` — an
absent corpus export, NOT the `assumeTrue(dockerAvailable())` abort that makes
a whole green run meaningless.

DIRECTION: 3c6f651a1c is a strict ancestor of this.
  git merge-base --is-ancestor 3c6f651a1c c11472eb18   # verified

LEXICOGRAPHIC: 'c114…' > '3c6f…' ('c' > '3'), so this sorts ABOVE the pin it
replaces — the SAFE direction, as the last one was. No `force(…)`: nothing
else in the graph names vespa-eventstore. Every eventstore line must read
c11472eb18 with no `->`:
  ./gradlew :relay:dependencies --configuration runtimeClasspath | grep -E "quartz|eventstore"

A REFUSED PROBE NO LONGER BUYS A CORPUS (#137), AND THE PROBE IS GONE (#138).

A partial answer used to be swallowed into the same `false` a dense tie group
returns, and both went to the document-API scan -- a visit with NO INDEX
behind it, whose cost is the corpus whatever the filter says. Measured on
staging: 7.1s of visiting found 4 of 8,765 matches that one indexed query
answered completely in 6ms. A full scan takes ~17.9 minutes and at ~24/hour
was holding ~7.3 of 16 cores with NO USERS on the cluster.

#138 then removed the predicting probe outright. The cursor loop MEETS the
boundary group it was guessing about and asks for it with a bound, so the
measurement replaces the guess for a query that was going to be issued anyway
-- one probe per walk saved, plus the exact-count query it paid on every tie,
on walks that were 79.3% of all engine time.

And a refusal now WAITS (four asks over 4.2s) and then THROWS, instead of
falling back to the scan. It is safe to throw because the walk cannot be
refused for size: `buildIdTime` ranks `unranked`, and `event.sd` declares
match-phase on `recency`/`recency_gated` alone. Confirmed twice against a real
Vespa -- 120,000 matches at 100% coverage upstream, 100,000 hits with no
degradation here, same shape. What is left to be refused for is a node
redistributing, which clears on its own.

WHAT THAT THROW MEANS FOR US, checked because it crosses the repo boundary:
`StoreWindowIndex.entriesFor` does not catch, so a PartialAnswer reaches
`guardedVisit`, which records VisitAborts.Reason.FAILED and retries on the
next roster pass. One visit aborts, nothing crashes, and it shows up in
`abortedFailed`.

READ AFTER DEPLOY: `walk.ties.dense` (the only thing that still reaches the
scan), `walk.scan` beside it, and `walk.refused.<degradation>` -- which names
the state that keeps reaching this walk. `walk.cursor.decide` is GONE; nothing
books it.

DIRECTION: c11472eb18 is a strict ancestor of this.
  git merge-base --is-ancestor c11472eb18 c79d2fdb2d   # verified

LEXICOGRAPHIC: 'c79d…' > 'c114…', so this sorts ABOVE the pin it replaces --
the SAFE direction. Quartz is untouched by the range (a8e8778265 both sides),
and no eventstore or quartz line may carry a `->`:
  ./gradlew :relay:dependencies --configuration runtimeClasspath | grep -E "quartz|eventstore"

THE GATE AT THIS COMMIT: 20 of 21 ITs ran against a real Vespa, 0 failures.
The skip is StagingCorpusIT, which needs a STAGING_CORPUS export.
A CUT MATCH SET TAKES THE SCAN AGAIN (#140). #138 made every refusal wait four
asks over 4.2s and then throw, on the premise that this walk cannot be refused
for SIZE. That premise holds only for a BOUNDED walk. Measured on staging
within minutes of deploying it:

  kind in (0,10002,10040), order by created_at desc, ranking=unranked
    limit  2064 -> cov=1%  docs=2,251,965 of 362M  degraded={match-phase:true}
    limit 20064 -> cov=1%  docs=2,251,965 of 362M  degraded={match-phase:true}

while the SAME shape windowed answers completely (943,949 matches at 100% over
23 hours). It is `order by created_at desc` over a wide range that trips the
limiter, not the match count -- so the walks that most needed the fallback
were the ones refused, and `match-phase` is the one refusal waiting cannot
clear. Staging aborted 7 of 146 visits in three minutes, each after sleeping
4.2s first.

Now: one ask, then the document-API visit, BOUNDED AT the cursor's own
position so nothing already emitted arrives twice. `non-ideal-state` and a
bare coverage shortfall keep #138's behaviour exactly -- full backoff, then
throw for the caller's own retry loop.

#138's win is intact and is the reason for all of this: `walk.scan` went from
583 calls and 383 CORE-HOURS a day to zero, and the cluster from ~16 cores to
~5.5, on a deployment with no users.

DIRECTION: c79d2fdb2d is a strict ancestor of this.
  git merge-base --is-ancestor c79d2fdb2d 369a3f8e1c   # verified

LEXICOGRAPHIC: '369a…' < 'c79d…', the UNSAFE direction -- this sorts BELOW the
pin it replaces, so the resolved graph is the proof. Quartz untouched by the
range (a8e8778265 both sides); no eventstore or quartz line may carry a `->`.

THE GATE AT THIS COMMIT: 20 of 21 ITs ran against a real Vespa, 0 failures.
The skip is StagingCorpusIT, which needs a STAGING_CORPUS export.

NARROW THE CUT (#141) AND SAY WHERE THE TIME WENT (#142).

#140 sent every match-phase cut to the whole-corpus visit. #141 halves the
window instead, because a cut is about the RANGE and not the match set --
measured on staging, `kind in (0,10002,10040)` ending at one instant with only
the window varying:

    30d     189,623 matches  100% coverage  UNCUT
    90d     529,357 matches   54% coverage  cut
     1y   1,106,187 matches    7% coverage  cut

A 23-hour window with 944,000 matches answers whole, so COUNT does not drive
it. The halving carries a budget counted in SPLITS, not depth: a depth of six
permits 63 splits and 64 leaves, and every leaf that still refuses scans, so a
depth bound bought up to 64 corpus reads. Six splits bound it at seven.

AND #142 IS WHY THIS ONE IS DIFFERENT. Four passes over this path today each
inferred a cause from aggregate stage totals and each landed somewhere else.
Staging had 19 calls open, the oldest 1081s, over a 15.7-HOUR window on three
kinds -- a query that answers 3,560 matches at 100% coverage in 0.05s by hand.
A SIGQUIT dump found 27 of 30 threads in `VespaVisits.streamOnce:294`,
mid-scan. Nothing joined "which call" to "which stage", and the hold registry
cannot: it is keyed by stage, one slot per name, so concurrent walks overwrite
each other by design.

WHAT TO READ, AND WHAT WOULD CONDEMN #141. `walk.cut.width.<bucket>` counts
the width of every refused window; `walk.cut.narrowed` against
`walk.cut.budget` says whether halving CONVERGES. Cuts that come back `1h`
with the budget firing mean narrowing cannot help and #141 should be reverted.
Cuts at `1y+` with the budget near zero mean it is right. #141 could not be
measured before it shipped -- it is upstream of the instrument -- so this is
the check that settles it after.

AND #144 ANSWERED IT, from the other end. The check above asked whether
halving converges; the real question was why an `unranked` walk was being cut
at all, when `event.sd` declares `match-phase` on `recency`/`recency_gated`
alone. The answer is that the cut was never the schema's: Vespa activates
match-phase degradation on ANY query whose primary sort key is a single-value
numeric fast-search attribute -- `created_at` -- on EVERY profile, `unranked`
included (`sorting.degrading`, default true, docs.vespa.ai). It is a SORTING
optimisation, which is why no rank profile exempts it and why four passes
over this path looked for it in the wrong place, this repo's comments
included ("it cannot be refused for size" is now corrected in the store).

The A/B, one query shape, only the flag varying (store PR #144, staging
2026-09-12):

  30d                            coverage   3%, cut
  30d, sorting.degrading=false   coverage 100%, 12.8M matches

WHAT THIS RELAY WAS PAYING, measured on the sync pod before the bump:
`unranked/kinds,since,until match-phase x16325 REFUSED` -- the id walk's
exact shape -- and `walk.scan calls=2345 mean=22min`, 864 hours of
document-store fallback, with both content nodes at 17.3 cores and no users.
#141's halving stays, as insurance against a serving query profile or a Vespa
release that turns the degrader back on; it is no longer the hot path.

STILL ON THE DEGRADER BY DECISION: client recall. `requireComplete` throws on
`unranked`, so a deep-past REQ errors in 0.03s today rather than answering in
4.05s. #144 leaves that alone and asks for its own measurement of what
clients would then pay; this bump does not change it either way.

DIRECTION: 6747f3ad4f is a strict ancestor of this.
  git merge-base --is-ancestor 6747f3ad4f bb25c8b15c   # verified

LEXICOGRAPHIC: 'bb25…' > '6747…', so this sorts ABOVE the pin it replaces --
the SAFE direction. Quartz untouched by the range (a8e8778265 both sides, and
bb25c8b15c's own toml names it), and no eventstore or quartz line may carry
a `->`. Resolved graph checked after the bump: every store and engine line
reads bb25c8b15c, every quartz line a8e8778265, zero arrows.

NOT GATED BY THE STORE'S INTEGRATION SUITE: Docker was down on the pinning
machine, and that suite reports BUILD SUCCESSFUL in six seconds with every
test skipped when it is. #144 carries its own staging A/B above and a mock
that reproduces the degrader; the gate is still owed.

faa18dfba1 = vespa-eventstore main @ 2026-09-26, the merge of PR #146 (two
commits over bb25c8b15c; its tree is the 1c0bfbd2df this PR first pinned):
the kind-10040 provider pass gets a bounded age
(NosFabrica/vespa-eventstore#145, #243 here). Until now each process's pass
learned only from 10040s written through its own store, so a list the sync
mirrored in reached the relay's lenses and Trusted List gate only at the
relay's next restart or socket-written 10040 -- nine days on staging, where
24 of 47 recent observers resolved no lens and got EMPTY ranked pages. The
store now rebuilds the pass every `providerRefreshSeconds` under any topology
but SINGLE_WRITER (both processes pass TRUST_PROVIDER_REFRESH_SECONDS, default
60), keeps the old pass when a rebuild fails or reads nothing, and walks a
signer whose cards THIS process skipped as unnamed once a pass names it --
so the sync projects cards for a service first named on the relay's socket,
and neither process re-walks a service the other already walked.

DEPLOYMENT: a restart of BOTH processes; no schema change, nothing re-derived.
WHAT IT DOES NOT REPAIR: cards the sync skipped BEFORE this deploy. The skip
memory starts empty at boot, so those stay unprojected; the boot reconcile
samples each service's oldest cards and is not guaranteed to find them. The
exact repair is the store's `rebuildTrust()`, which this relay does not
expose yet.

DIRECTION: bb25c8b15c is a strict ancestor of this.
  git merge-base --is-ancestor bb25c8b15c faa18dfba1   # verified

LEXICOGRAPHIC: 'faa1...' > 'bb25...' ('f' > 'b'), so this sorts ABOVE the pin
it replaces -- the safe direction, though nothing else in the graph names
vespa-eventstore (see the e1ecd7f23e note). The proof stays the resolved
graph -- every eventstore line reads faa18dfba1 with no `->`.

ebb09fa0d4 = vespa-eventstore PR #150's head (branch claude/quartz-28bf170f92),
pinned BEFORE its merge because the quartz pin above cannot move without it and
JitPack builds a branch commit as readily as a merged one; move this to #150's
merge commit when it lands (same tree). The whole change is quartz 28bf170f92:
the five renamed classes the store names, kotlin-reflect declared by the two
tests that read members by reflection, and the pin note that records the
searchable-set growth (142 -> 150, recall forward-only until
reindexFullTextSearch) and the OK-true duplicate. No schema change, nothing
re-derived; deploying it is a restart of both processes.

GATED: the store's `./gradlew build` (696 tests) and its integration suite
against a real Vespa (21 tests: 20 passed, StagingCorpusIT skipped by design,
0 failures, VespaParityIT / RankRegressionIT / ObserverGateIT / OrphanSweepIT
among them), and this relay's full build + archTest run against that tree
(1249 tests, 0 failures) before either pin moved.

DIRECTION: faa18dfba1 is a strict ancestor of this.
  git merge-base --is-ancestor faa18dfba1 ebb09fa0d4   # verified

LEXICOGRAPHIC: 'ebb0...' < 'faa1...' ('e' < 'f'), so this sorts BELOW the pin it
replaces -- the losing direction, and harmless here: nothing else in the graph
names vespa-eventstore. The proof stays the resolved graph -- every eventstore
line reads ebb09fa0d4 with no `->`.

748dc5b8c6 = vespa-eventstore main @ 2026-09-27, the merge of PR #151. It
SUPERSEDES #150, the branch the previous pin was: #151 did the same quartz
28bf170f92 bump on its own branch, took two audit rounds, then moved on to
ec16988e3a, and #150 never merged. So the previous pin is not an ancestor;
its base is.
  git merge-base --is-ancestor faa18dfba1 748dc5b8c6   # verified (#150's base)

WHAT THIS RELAY GETS over ebb09fa0d4:

  THE SAME RENAMES and the same kotlin-reflect for the two reflective tests,
    written independently; no store API this relay calls changes shape.
  A STALE REPLACEABLE STAYS `replaced:` / OK false. The branch had briefly
    followed quartz 28bf170f92 into SUPERSEDED (OK true), then went back
    (f7eeeaf) when quartz #4222 did; the net answer is what ebb09fa0d4 gave.
  MEDIA FOLLOW PACKS EXPAND (661f0ab): a reader's own kind 39092 now splices its
    members into a search the way a 30000 or 39089 does, under the same
    DECLARATIONS gate; a stranger's stays an ordinary hit. No stored data moves.
  QUARTZ #4215's three re-derived kinds (the quartz note above): the store's
    SearchExtractors takes them from the jar, so the re-feed is this relay's to
    run. event.sd and services.xml are untouched, so AUTO_DEPLOY has a no-op
    and nothing restarts for config.

DEPLOYMENT: a restart of both processes, then one boot with
REINDEX_FTS_ON_START=true (docs/migrations.md).

GATED HERE: this relay's full build + archTest on both new pins (1247 tests,
0 failures; RelayProtocolTest's declaration-kind set gains 39092, the one
behaviour change a test here can see), and the resolved graph of all six
modules: every quartz line ec16988e3a, every eventstore line 748dc5b8c6, zero
arrows.
And the store at 748dc5b8c6 itself: `./gradlew build` (719 tests, 1 skipped,
0 failures) and its integration suite against a real Vespa in Docker
(`TESTCONTAINERS_RYUK_DISABLED=true ./gradlew :benchmark:test -Pintegration`:
21 tests, 20 passed, StagingCorpusIT skipped by design, 0 failures, VespaParityIT
/ RankRegressionIT / ObserverGateIT / OrphanSweepIT among them), read off the
result XML.

LEXICOGRAPHIC: '748d…' < 'ebb0…' ('7' < 'e'), so this sorts BELOW the pin it
replaces -- the losing direction, and harmless here: nothing else in the graph
names vespa-eventstore. The proof stays the resolved graph -- every eventstore
line reads 748dc5b8c6 with no `->`.

e62745df71 = vespa-eventstore main @ 2026-09-27, the merge of PR #152 (one commit over
748dc5b8c6, same tree as its head df62335): the store's own quartz pin moves ec16988e3a ->
a53cafd577 to match the one above, and nothing else. No store code, no schema, nothing
re-derived; nothing to deploy beyond the relay's own restart.
  git merge-base --is-ancestor 748dc5b8c6 e62745df71   # verified

GATED: the store at #152's head -- `./gradlew build` (719 tests, 1 skipped, 0 failures)
and its integration suite against a real Vespa in Docker (21 tests, 20 passed,
StagingCorpusIT skipped by design, 0 failures) -- and this relay's full build + archTest
on both pins with every quartz line a53cafd577 and every eventstore line e62745df71, zero
arrows.

LEXICOGRAPHIC: 'e627…' > '748d…' ('e' > '7'), so this sorts ABOVE the pin it
replaces -- the safe direction, and moot: nothing else in the graph names vespa-eventstore.

9522ee0768 = vespa-eventstore main @ 2026-09-27, the merge of PR #153 (one commit over
e62745df71, same tree as its head 48f537b): the store's own quartz pin moves a53cafd577 ->
fcd76d2075 to match the one above, and its pin note gets back three history lines #152 dropped.
No store code, no schema, nothing re-derived; nothing to deploy beyond the relay's own restart.
  git merge-base --is-ancestor e62745df71 9522ee0768   # verified

GATED: the store at #153's head -- `./gradlew build` (719 tests, 1 skipped, 0 failures) and its
integration suite against a real Vespa in Docker (21 tests, 20 passed, StagingCorpusIT skipped by
design, 0 failures) -- and this relay's full build + archTest on both pins, every quartz line
fcd76d2075 and every eventstore line 9522ee0768, zero arrows.

LEXICOGRAPHIC: '9522…' < 'e627…' ('9' < 'e'), so this sorts BELOW the pin it replaces -- the
losing direction, and moot: nothing else in the graph names vespa-eventstore.

ff54c9beb1 = vespa-eventstore main @ 2026-09-27, the merge of PR #148 (one commit over
9522ee0768, b87910c): A GATED COUNT NOW GATES. At 9522ee0768 a ranked count asked Vespa for
`hits=0`, and Vespa 8.731 answers that without running first-phase ranking at all, so
`rank-score-drop-limit` never fired and `totalCount` came back UNGATED. A term-less COUNT through
an observer (`observer:<hex>`, with or without `filter:rank:gte:N`) therefore answered the
whole match set whatever the lens or floor. Measured on staging (2026-09-28, since=1790468839):
963,042 with the observer alone, 963,042 at gte:50, 963,043 at gte:90, against 964,435 under
`include:spam`; kind 1 alone 410,344 gated vs 410,345 unranked. A count with terms was gated
only partly (the store's note: 3,714 vs 2,676 served). The store now asks for one hit, which
forces the ranking pass the gate lives in, for one document summary. No schema, no quartz
move, nothing re-derived; nothing to deploy beyond the relay's own restart.
  git merge-base --is-ancestor 9522ee0768 ff54c9beb1   # verified

GATED: vespa-eventstore's FilterMatrixIT (its branch claude/relaxed-bohr-4h9lp3, 14043ec) -- 10,305
REQ+COUNT pairs through `open()` against an in-test oracle: NIP-01 shapes x token / NIP-42 / lens-
less observers x every `filter:rank` / `include:spam` form x `sort:` x term / phrase / exclusion.
On Vespa 8.754.14: ff54c9beb1 0 failing; 9522ee0768 1,890 failing, every one a COUNT over the
page its own REQ served, membership never wrong. So an AUTHENTICATED plain COUNT was ungated too
(kind 1 through the connection's lens: COUNT 42, served 30), not only the `observer:` token form.
And this relay's full build + archTest on the new pin: every quartz line fcd76d2075, every eventstore
line ff54c9beb1, zero arrows.

NOT main's head: 0dea23e (#147, #149) adds the speculative recency strategies on top (~2.8k
lines, a new default read plan). That is its own bump, not a count fix.

LEXICOGRAPHIC: 'ff54…' > '9522…' ('f' > '9'), so this sorts ABOVE the pin it replaces -- the
safe direction, and moot: nothing else in the graph names vespa-eventstore.

5379e572dc = vespa-eventstore main @ 2026-09-28, the merge of #155 — SAME TREE as its head
6b9a1a8d3b, which this pin held before the merge (`git diff 6b9a1a8d3b 5379e572dc` is empty; the
merge's parents are 81f2286 and 6b9a1a8d3b), over eventstore main 81f2286. The head had moved
from dfd8226ad0 to fix an audit of the PR: one
gating rule (EventYql.gateFloor) for the page, the walk, the snapshot and LiveGate; LiveGate checks
a searching rule's TEXT (quartz's Filter.match ignores `search`, so an ungated search filter
vouched for a gated sibling's below-floor events, and a live "pizza" subscription streamed every
note); a limit'd walk takes a cut tie group's lowest ids; a limit'd trust sort snapshots as its
ranked REQ. Being over main 81f2286, this bump also brings #147 / #149 (the speculative
recency strategies ff54c9beb1 deliberately left out: VESPA_RECENCY_STRATEGY, default
`speculative`, `match_phase` the escape hatch) and #154 (recency pages resolved from ids, each
document read once). Its internal existence probes now stamp RANK_UNRANKED, which is why
RelayProtocolTest's and HttpRelayTest's recording indexes skip that profile. Three commits of its own:

  FilterMatrixIT (14043ec): 10,305 REQ+COUNT(+snapshot) cases through open() against an oracle.
  A NIP-77 SNAPSHOT IS THE SET ITS REQ SERVES (80433e7): a limit'd filter walked one bounded
    search page (25 of 28 in the contract IT), and an `observer:` NEG-OPEN ran its gate with no
    lens resolved and reconciled NOTHING (0 of 20). Now every termless snapshot walks, through
    the resolved lens: the engine's id walk runs a lensed query on recency_gated_exact.
  LiveGate (e6bccf8): the observer gate for events after EOSE, which ObserverBackend now holds
    live deliveries to — a signed-in feed no longer streams an author its replay dropped.

No schema change (event.sd untouched: recency_gated_exact already exists), nothing to re-feed.
  git merge-base --is-ancestor ff54c9beb1 5379e572dc   # verified
  git merge-base --is-ancestor 6b9a1a8d3b 5379e572dc   # verified

GATED, on this pin (6b9a1a8d3b, the same tree) with quartz bf2fefc621: this relay's full build +
archTest (262 relay tests, LiveGateDeliveryTest among them); the store's `./gradlew build`, its
integration suite (23 tests, 0 failures), and FilterMatrixIT with its LIVE and LIMIT'D SNAPSHOT
columns (10,305 cases, 0 failing; 6,222 failing on dfd8226ad0); and RelayContractIT over the wire
against a relay built from both pins: 8,191 checks, 0 failing (5 on ff54c9beb1 + fcd76d2075,
1,741 on 9522ee0768).

LEXICOGRAPHIC: '5379…' < 'ff54…': the losing direction, moot — nothing else names the store.

14d8c7eeed = vespa-eventstore main @ 2026-10-06, the merge of #159, over the merges of #156
(gated-COUNT benchmark studies), #157 (live hostname in docs) and #158 — 20 commits over
5379e572dc, none in event.sd or services.xml. It moves the store's quartz pin twice
(fcd76d2075 -> d792ebc4bf in #158 -> 68268da413 in #159), and the quartz line above follows
it. What it changes for this relay:

  SEARCH FIELDS RE-DERIVED (#158, #159; quartz's SearchFieldExtractor). d792ebc4bf: 38000 split by
    tags (the d-only spam that is most of the kind stays addressable but is no longer searchable),
    decentralized lists (9998/9999/39998/39999) gain a text tier, 30267 indexes its title.
    68268da413: 42 kinds become searchable, 19 extract differently; 30030 stops indexing its
    NIP-44 ciphertext. Fed columns, so the repair is a re-feed: docs/migrations.md, "66 kinds
    re-derived". The store adds a kind-scoped reindexFullTextSearch(kinds) for exactly this;
    REINDEX_FTS_ON_START still runs the unscoped walk, which reaches the same documents.
  NIP-50 QUERIES VESPA USED TO REFUSE (#159). A phrase repeating a term six times in a row or
    eleven in all was an HTTP 400 for the whole REQ ("noooooooo" through the gram net, a quoted
    sentence, `-no-no-no-no-no-no`); a trailing `*` was a 400 ("bitcoin*"). Now cut, split into
    required pieces, or stripped. EMOJI ARE WORDS: Vespa indexes every So code point, so "⚡"
    finds notes and "-🔥" excludes, where both were dropped before; a lone U+FE0F is stripped.
    LiveGate judges streamed events by the same pieces, without allocating per word.

No schema change, nothing to deploy beyond the relay's restart; the re-feed above is the step.
  git merge-base --is-ancestor 5379e572dc 14d8c7eeed   # verified

GATED, on this pin with quartz 68268da413: this relay's full build (1,250 tests, 2 skipped,
0 failures) + archTest, every quartz line 68268da413 and every eventstore line 14d8c7eeed in all
six modules, zero arrows; and RelayContractIT over the wire against a relay built from both pins
on Vespa 8.763.13: 8,191 checks, 0 failing. And the store at 14d8c7eeed itself: its integration
suite against a real Vespa in Docker (23 tests, 22 passed, StagingCorpusIT skipped by design,
0 failures), read off the result XML.

LEXICOGRAPHIC: '14d8…' < '5379…': the losing direction, moot — nothing else names the store.

a995519bef = vespa-eventstore main @ 2026-10-08, the merge of #162 — SAME TREE as its head
8762d85ce6 (`git diff 8762d85ce6 a995519bef` is empty), one commit over 14d8c7eeed. NIP-50 ON
BODY-LESS KINDS STOPS PAYING FOR THE BODY PHRASE (vespa-eventstore #161, fix 1): a kind 0
document never carries search_text, so `search_text_gram` is empty on all of them, yet a
`kinds:[0]` search ran the trigram phrase anyway and paid 1.4-1.9 s on production (445M docs)
for it — `kind = 0 and <phrase>` took 1.77 s to match 0 documents, because Vespa drives the AND
from the phrase's smaller estimate. The compiler now leaves the phrase out when every kind a
compiled query names is in EventYql.BODYLESS_KINDS (kind 0 alone), judged per compiled query so
a reference-expansion companion keeps its body reach; the exact search_text clause still runs.
Engine query code only: no schema, no services.xml, no quartz move (the store still names
68268da413), nothing re-derived; nothing to deploy beyond the relay's own restart.
  git merge-base --is-ancestor 14d8c7eeed a995519bef   # verified

SPELLED `com.github.nosfabrica` (below): JitPack's `NosFabrica` entry for this commit holds a
failed infra build (JAVA_HOME pointing at a missing JDK, exit 1) and 404s the jar, while the
lowercase entry serves it — docs/traps.md, "JitPack caches builds per group-spelling".

GATED, on this pin with quartz 68268da413: this relay's full build (1,257 tests, 2 skipped,
0 failures) + archTest, every quartz line 68268da413 and every eventstore line a995519bef in all
six modules, zero arrows; and RelayContractIT over the wire against a relay built from both pins
on Vespa 8.763.13: 8,191 checks, 0 failing. And the store at a995519bef itself: its integration
suite against a real Vespa 8.763.13 in Docker (23 tests, 22 passed, StagingCorpusIT skipped by
design, 0 failures; SearchBodyGramIT among them), read off the result XML.

LEXICOGRAPHIC: 'a995…' > '14d8…': the safe direction, moot — nothing else names the store.

Prev 14d8c7eeed, 5379e572dc and ff54c9beb1 before it (all above).

d1adf0ff91 = vespa-eventstore main @ 2026-10-13, the merge of #164 — SAME TREE as its head
a1dd4b28fb (`git diff a1dd4b28fb d1adf0ff91` is empty), two merges over a995519bef:
 - #164, ScoredSearch + NostrSemanticsStore.scoredProfileSearch: kind-0 hits with their own
   engine score under a named observer, and that observer's lens (READY / NO_LIST / NO_CARDS),
   which OpenRankingRoutes answers ORE-05 from. Store code only.
 - #163, the body edge n-gram PROTOTYPE (#161), OFF by default: ONE ADDITIVE SCHEMA FIELD,
   `search_text_edge` (index only, no attribute, no summary), plus its matchCount() in
   tier_body_match and offname_match. The relay's SchemaDeploy carries it on the next boot; it
   is fed only under VESPA_BODY_EDGE and queried only under EventQuery.bodyEdgeMatching, neither
   of which this relay sets, so the field stays empty and both expressions add 0. Nothing to
   re-feed; SchemaFallbacks demotes the column if a serving schema predates it.
No quartz move (the store still names 68268da413), nothing re-derived.
  git merge-base --is-ancestor a995519bef d1adf0ff91   # verified

SPELLED `com.github.nosfabrica` (below): the lowercase entry serves store and engine at
d1adf0ff91, and its pom names engine under `nosfabrica` too.

GATED, on this pin with quartz 68268da413: this relay's full build (1,409 tests, 2 skipped,
0 failures) + archTest, every quartz line 68268da413 and every eventstore line d1adf0ff91 in all
six modules, zero arrows. And the store at d1adf0ff91 itself: its integration suite against a
real Vespa 8.763.13 in Docker (23 tests, 22 passed, StagingCorpusIT skipped by design,
0 failures; the schema with search_text_edge deploys), read off the result XML. Not run:
RelayContractIT; and no store IT drives scoredProfileSearch on a real Vespa (its tests are unit).

LEXICOGRAPHIC: 'd1ad…' > 'a995…': the safe direction, moot — nothing else names the store.

8c95193705 = vespa-eventstore branch claude/eloquent-cannon-4nj7p8, ONE commit over d1adf0ff91;
re-pin to its merge commit before this merges. scoredProfileSearch STOPS ASKING WHETHER THE
OBSERVER'S SCORES ARE READY: each call used to sample the rank service's 8 newest kind-30382
cards (a second engine query, sorted over every card the service publishes) and could not tell
from 8 cards anyway. Now it is one query; the observer's 10040 resolving a rank service (the
cached provider map, no query) is the whole lens. ScoredSearch carries `rankService` in place of
Lens/LensStatus, so OpenRankingRoutes has no 202 and no grace. Store code only: no schema, no
quartz move, nothing re-derived.
  git merge-base --is-ancestor d1adf0ff91 8c95193705   # verified

SPELLED `com.github.NosFabrica` (below) for this pin: the lowercase entry holds a failed infra
build (the maven-publish plugin unresolvable, 15 s in), the capitalized one serves store and
engine, and its pom names engine under `NosFabrica` too.

GATED, on this pin with quartz 68268da413: this relay's full build (1,412 tests, 2 skipped,
0 failures) + archTest, every quartz line 68268da413 and every eventstore line 8c95193705 in all
six modules, zero arrows. And the store at 8c95193705 itself: its unit build (795 tests,
0 failures) and its integration suite against a real Vespa 8.763.13 in Docker (23 tests, 22
passed, StagingCorpusIT skipped by design, 0 failures), read off the result XML. Not run:
RelayContractIT; still no store IT drives scoredProfileSearch on a real Vespa.

LEXICOGRAPHIC: '8c95…' < 'd1ad…': the losing direction, moot — nothing else names the store.

a34cc5df39 = the same branch, one commit over 8c95193705, from the audit of both PRs: the lens is
resolved ONCE (a 10040 write between two lookups could query under another service than the
rankService the answer reports), and the store's own require checks the observer's length too
(quartz's isHex64 ignores everything past 64). Store code only. Both group spellings serve it;
the capitalized one stays. Re-pin to the merge commit of NosFabrica/vespa-eventstore#165.
  git merge-base --is-ancestor 8c95193705 a34cc5df39   # verified

GATED, on this pin with quartz 68268da413: this relay's full build (1,412 tests, 2 skipped,
0 failures) + archTest, every eventstore line a34cc5df39 and every quartz line 68268da413 in all
six modules, zero arrows. And the store at a34cc5df39: its unit build (796 tests, 0 failures) and
its integration suite against a real Vespa 8.763.13 in Docker (23 tests, 22 passed,
StagingCorpusIT skipped by design, 0 failures), read off the result XML.

94aad0feea7e = vespa-eventstore main @ 2026-10-14, the merge of #165 — SAME TREE as its head
a34cc5df39 (`git diff a34cc5df39 94aad0feea` is empty; the store jar is byte-for-byte the same
size), so everything GATED above for a34cc5df39 holds for it.
  git merge-base --is-ancestor a34cc5df39 94aad0feea   # verified

SPELLED WITH 12 HEX DIGITS, not the usual 10: JitPack holds a failed infra build for
`94aad0feea` under BOTH group spellings (the maven-publish plugin unresolvable, as for
8c95193705's lowercase entry), and the version string is part of its cache key, so the longer
spelling of the same commit is a fresh build. `com.github.NosFabrica` serves store and engine
at 94aad0feea7e, and its pom names engine at 94aad0feea7e too.

LEXICOGRAPHIC: '94aa…' < 'a34c…': the losing direction, moot — nothing else names the store.

Prev a34cc5df39, 8c95193705, d1adf0ff91 (above).

28a6020ca1c4 = vespa-eventstore main @ 2026-10-16, the merge of #166 — SAME TREE as its head
00da4d38f1 (`git diff 00da4d38f1 28a6020ca1c4` is empty). That PR only moves the store's quartz
pin to ce7f4db1b6, this relay's own (above): 12 kinds become searchable, among them the Open
Markets 30405 / 30406 / 31555, and NIP-15 bodies parse leniently. No store code changes. Taken
so the graph carries one quartz: it clears the `quartz:68268da413 -> ce7f4db1b6` arrow.
  git merge-base --is-ancestor 94aad0feea7e 28a6020ca1c4   # verified

GATED, the store at 00da4d38f1 with quartz ce7f4db1b6: its unit build (796 tests, 0 failures)
and its integration suite against a real Vespa in Docker (23 tests, 22 passed, StagingCorpusIT
skipped by design, 0 failures), read off the result XML. Same tree here, so it holds for the
merge commit.

Spelled with 12 hex digits like the pin it replaces. com.github.NosFabrica serves store and engine
at 28a6020ca1c4, and the store's pom names engine at 28a6020ca1c4 and quartz-jvm at ce7f4db1b6.

LEXICOGRAPHIC: '28a6…' < '94aa…': the losing direction, moot — nothing else names the store.

Prev 94aad0feea7e (above).
```

## `okhttp` (versions)

```text
The router mirrors kinds from upstream relays. OkHttp drives the outbound
websockets (quartz's BasicOkHttpWebSocket) and typesafe-config parses the
strfry-style `streams { }` HOCON. Both already ride in transitively from
quartz / vespa-eventstore; pinned here so the router's direct use is explicit.
```

## `vespa-eventstore-store` (libraries)

```text
The event store, from JitPack. `store` is the IEventStore + open() front door + the
ObserverContext/OriginalFilters this relay stamps; it api-exposes the `vespa` engine
jar transitively (InMemoryEventIndex, EventYql — used by the relay's protocol test).
The group's SPELLING is per pin: JitPack caches builds per spelling, and either
entry can permanently hold a failed infra build while the other serves fine.
Lowercase served the pins through 5379e572dc (the capitalized entry had failed);
for 14d8c7eeed it is the reverse — `nosfabrica` failed opening gradle-wrapper.jar,
`NosFabrica` built (its pom also names engine under `NosFabrica`). For a995519bef
it flips back: `NosFabrica` failed (JAVA_HOME at a missing JDK), `nosfabrica` built
(its pom names engine under `nosfabrica`), and d1adf0ff91 too. For 8c95193705 it flips
again: `nosfabrica` failed (the maven-publish plugin unresolvable), `NosFabrica` built.
For 94aad0feea BOTH failed, and a 12-digit version of the same commit built: the version
string is part of the key too. Same repo, same artifacts — different cache key. On the next
bump, check both spellings, and a longer hash if both fail.
```

## `ktor-server-cors` (libraries)

```text
NIP-11 tells relays to serve the info doc with CORS headers, and browser-based
NIP-86 admin tools need the Authorization header preflighted.
```

## `ktor-server-compression` (libraries)

```text
gzip/deflate for the pages and ES modules — ~117KB of text over the wire otherwise.
```

## `ktor-server-test-host` (libraries)

```text
serveRelay bundles the Netty engine (batteries-included) so a consumer — or the
standalone main() — can bind a port without wiring an engine itself.
testApplication drives the /web route in-process — the module directory is the
whole page, so its route, its 304 and its content type are worth asserting
rather than eyeballing.
```

## `okhttp` (libraries)

```text
Outbound upstream websockets for the router (quartz's BasicOkHttpWebSocket
takes an OkHttpClient factory) and the HOCON parser for the streams config.
```
