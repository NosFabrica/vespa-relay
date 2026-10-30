# Peers decisions

The history behind `peers/.../` (`PeerClient`, `RelaySockets`, `Sockets`,
`DialGate`, `TorTransport`, `RelayUrlCache`, `RelayDiscovery`,
`RelayVerdictRecord`, `RelayFacts`, `Verdict`), moved out of the source so
the code reads on its own. One paragraph per decision; `git log -L` on the
function finds the commit.

**The OkHttp dispatcher is 4,096 wide, not the stock 64.** An open websocket
holds a dispatcher slot for its whole life, so at 64 every stream's
`concurrency` silently stopped meaning anything: a 20,340-relay cycle
projected an ETA of 330 hours. `PeerClient.socketLoad` publishes the queued
count because `queued > 0` is the one direct sign the budget is the
constraint; everything else about a slow mirror looks alike from throughput.

**4,096, because the prime list outgrew 1,024.** `LiveFleetProbe` held one
live REQ on every clearnet prime relay at once (1,382 of production's 1,527
in October 2026). At 1,024 the dispatcher filled and the rest queued forever:
1,088 ever opened, and after a quarter of the servers went silent only 1 of
240 came back, because the slots they freed went to dials queued ahead of
them. At 4,096, 1,334 opened and 324 of 325 came back. A socket is also one
reader thread for its life, so the budget is a thread budget too.

**DNS is cached and bounded (`PeerClient.relayDns`).** Dialling the fleet at
once sends ~1,300 lookups at the resolver together; in a sandbox resolving
through 8.8.8.8 a burst of 1,267 failed 326 of them, while the same names 128
at a time failed none and took the same 25 seconds. quartz reads any
`UnknownHostException` as a gone domain and waits five minutes, so after a
reset of every socket 366 of 1,024 relays stayed down for that long. quartz's
`SurgeDns` (what Amethyst's relay pool and CLI use) caches answers for a day
and serves stale while it refreshes, so a reconnect storm resolves nothing;
`BoundedDns` under it keeps the cold start from being the burst.

**The ping is 30 seconds, not 120.** The ping is the only thing that notices
a path that went quiet without closing; at 120s a blackholed relay took p50
192s to notice, its events lost all along. Amethyst's 120s is a battery
trade-off a server does not make, and its study of relay idle timeouts found
a ~60s tier that only a shorter ping keeps open.

**There is no passive NIP-66 writer.** quartz's `RelayMonitor` used to hang
off the shared client and sign a kind-30166 for every socket the fan-out
opened. Because a 30166 is addressable per (author, url), it rewrote
`created_at` on a five-minute flush for every relay being synced, so the
record's clock said "we talked recently" instead of "we checked this", and
every consumer needed a private freshness convention. The monitor's passes
are the only writers.

**The socket refcount is the stream's, and a release nobody claimed is
dropped.** `fetchAll` sends CLOSE and leaves the connection in the pool, and
quartz never closes one, so a probe pass left one open socket per url it
fingerprinted against a 1,024-socket dispatcher with a 20-per-host cap.
Treating an unclaimed release as a 1-count turned a bookkeeping bug elsewhere
into disconnecting a socket its real holder was still on.

**`RelaySockets.close` checks pool membership before disconnecting.** A bare
`getOrCreateRelay(url).disconnect()` on a url quartz's 300ms `updatePool`
reconcile had already dropped constructed a fresh, unsubscribed
`BasicRelayClient`, which the 60-second keep-alive then dialled and nothing
ever removed. Measured on a staging pod: 105 `RealWebSocket.loopReader`
threads, about 100 of them left over from probe passes that had finished,
the oldest silent for over ninety minutes.

**One dial gate per transport.** The monitor passes shared one
`Semaphore(dialConcurrency)` over clearnet and Tor. Measured on staging at
`dialConcurrency = 100` during a 936-host `aliasFold` pass, `.onion` urls were
10% of the population (357 clearnet / 42 tor in 400 sampled records) and held
60-74% of a saturated gate, with 30-40 of those permits queued inside a
32-wide Tor dispatcher while occupying a clearnet slot. The Tor gate is now
`min(dialConcurrency, maxSockets)` and the clearnet gate is the knob alone.

**`probeIdleMs` sums the Tor connect budget and the clearnet idle budget.**
quartz's `idleTimeoutMs` runs from the start of the fetch, before the
connection, so a hidden service allowed 90s for its circuit but given the
clearnet 20s window returned an empty result indistinguishable from an empty
relay. An empty window folds nothing and clears nothing, so every url on that
host stayed unmeasured and was re-dialled every pass forever.

**The Tor client replaces the dispatcher and resolves the proxy per
connection.** `newBuilder()` shares the dispatcher object, which would let
onion dials draw down the clearnet budget while queueing behind it. A fixed
`proxy(InetSocketAddress(host, port))` resolves once at construction, so a Tor
container restarting on a new address left the router dialling the old one
until someone restarted it. `SYNC_TOR_ALL` is a different deployment, not a
better default: a 20k-relay cycle over Tor is a fraction of the throughput and
several large relays refuse exit traffic.

**`socksAnswers` re-checks the TTL after winning the flag.** Reading the TTL
and taking the CAS are two steps; a caller descheduled between them can win
the flag after another prober has finished and open a second connection for
an answer already in hand. The timestamp is written after the verdict, or a
reader could hold a stale answer for a whole TTL.

**Before the first probe lands, a loser waits for it.** A loser taking the
previous answer is right once there is one; before that, `probeSaid` is its
`false` default, and every caller in the first fan-out that lost the flag was
told the proxy was down while it was being found up. The wait is bounded past
the probe's own connect timeout.

**Relay-url normalisation is memoised per spelling, keyed without the onion
gate.** A corpus with 19,844 known relay urls (issue 182) hands the parser
the same few thousand spellings once per author. `allowOnion` is a property
of the deployment, not the string, so keying on it would double every entry;
the entry carries whether the url is an onion and the gate is applied on the
way out. The map is dropped whole at 65,536 entries because the keys come
from strangers' kind 10002s.

**A `ws`/`wss` scheme is demanded before and after the normalizer.** The
scheme was once demanded only when a select named no tag. Measured on this
store's kind-10002s: 1,749 wss, 103 ws, 2 with no scheme, 0 http, so demanding
it costs 2 urls in 1,854 and refuses every `http://` entry riding in on other
sources. The second check exists because the normalizer repairs as well as
canonicalises: 143 urls carried a nested scheme
(`wss://https//nostr.watch/relay/x`) and came out as `https://` web pages
dialled once a cycle forever; a diagnostic run found 116 such "relays"
returning 0 events between them.

**Discovery pages the search index and never the tags projection.** The
`document/v1` visit evaluates its selection per document with no index, so
its cost is the corpus, not the answer. Measured on staging (issue 182),
reading 364 kind-10040 declarations in a 319,426,563-event corpus: `/search/`
0.0058s, `/document/v1` 75.0260s, a 12,800x gap, paid once per tag name, and
the monitor's 10040 source named 38 of them. Those walks shared the document
API with the ingest dedup probe, which is the mechanism behind the wedges in
issue 167. No size rule chooses between the two because the crossover
(~100k matches at 319M events) rises with the corpus and is never reached.

**`maxAgeSeconds` combines with a caller's `since`, never replaces it.**
`StreamWorld.candidatesSince` narrows the source filters at run time for the
fast lane; a bound that overwrote it handed back each source's whole window,
so the fitness pass re-dialled the entire roster every `fastLaneSeconds`
instead of the handful of new urls.

**The relay-list cap counts only tags a select would extract, `where`
included.** A NIP-65 select carrying `marker = "write"` counted the read-only
`r` tags it then discarded, so a 10002 listing 200 read relays and three
write ones tripped a cap of 50 and lost all three. Measured on this store:
9,418 pubkeys named a list of 6-20 entries and 148 named one of 100-10,591,
the largest carrying no other tag and no content. An oversized list is
dropped whole so the author cannot choose which relays we see by ordering.

**`undialable` holds out `dead` alone, scoped to named monitors, bounded by
`among` for the fast lane.** The other refusals are verdicts a relay earned
by answering; holding them out would stop the fold and the stability gate
ever re-measuring them. Unscoped, any stranger whose 30166s we mirror could
take a relay out of every pass for good. Unbounded, the fast lane materialised
every dead record in the store (five figures) every two minutes to answer a
question about a dozen urls.

**`recorded` is scoped to our own key.** Drawn from the wider trust set, a
deployment mirroring a busy foreign monitor's 30166s would make that
monitor's whole world the mouth of its coverage tree, shrinking every bar
under it to a sliver of a corpus this router never touched.

**The scan clamps the doubled page to Int range before narrowing.** An
unbounded scan's budget is `Int.MAX_VALUE + boundaryIds.size` as a Long; the
doubling wrapped to a negative `ask`, which the store reads as its
matches-nothing sentinel, and the walk restarted from the top forever.

**The fold's tag is `same-as`, not `redirect`.** A redirect is a directed edge
carrying authority the relay never gave. A fingerprint establishes an
equivalence, so a consumer running union-find over the tags gets the right
partition without sharing `RelayAliases.PREFERENCE`.

**Verdicts are 30166 events, not a state file, and `distinct` is persisted.**
The kind is addressable, so re-probing replaces rather than appends, and the
verdict is served with its evidence. Without persisting "measured, nobody's
duplicate", every boot re-fingerprinted all the non-duplicates: 59 of them in
the live run against a store already holding 128 folds.

**A failed verdict read throws; it is not "no verdict".** `load` used to
swallow a failed chunk into an empty result while `AliasFolding.adopt` forgot
every verdict before adopting what came back, so one unlucky query unfolded
up to 500 urls for that cycle and nothing said so. The same rule on the write
side: `edit` aborts when the current-record read fails, because a fresh
record built from `add` alone erases every other writer's tags at a newer
timestamp with nothing looking wrong.

**`loadAll` groups the world, not the candidates.** A duplicate is a property
of a url next to another one, and a url's siblings can be absent from a
candidate set for reasons unrelated to the fold. A group of one is dropped
unresolved, so the new url was dialled as its own relay forever while a
signed record naming its survivor sat unread.

**A verdict ages by its own measured-at stamp, never the event's clock.** The
record's `createdAt` was bumped by every writer on every connection, so a
relay we kept was re-measured never and a relay we refused was re-measured on
schedule: exactly backwards. A fallback from a missing stamp to the event
clock was the same trap, and made every pre-stamp verdict on a live relay
immortal; an unstamped tag is stale.

**Each verdict carries a rules epoch, and the fitness epoch is enforced by
retraction, not by readers.** `FOLD_EPOCH` went to 2 when hosts' urls were
compared to each other rather than only to the leader (which had signed
genuine duplicates as distinct, six at a time on `haven.calva.dev`), any url
could hold the ruler, and a yardstick had to reproduce its own window before
a negative claim. `FITNESS_EPOCH` went to 2 for the compliance check. A
read-side epoch check meant no standard NIP-66 record could satisfy a gate,
so `FitnessPass.retireStaleEpochs` takes old verdicts back at boot instead.

**The fitness grade is a NIP-32 label, not the `s` tag.** `s` is where every
monitor in the wild publishes the relay's software (a repository url on 172
of 400 records sampled off `nos.lol`; 539 of 800 across 12 monitors carry it),
so our records said `s: dead` where a reader expected strfry's git url. The
grade rides `l` under `relay.fitness`, and the pass owns only its own
namespace inside `l`/`L` because foreign monitors label the same record with
country and ASN. `LEGACY_STATUS_TAG` survives because 4,000 of 4,000 records
still carried a grade there and the migration queries it.

**An inherited verdict is not re-signed.** The fold's alias and the stability
gate's refusal reach `publishFitness` without a dial; re-signing one stamps
`measured-at = now` on a relay nothing tested, which makes the verdict
immortal. `fitnessGrades` asks first and carries the evidence because `alias`
onto a different canonical is a different public statement.

**Empty NIP-66 facts were not neutral.** quartz's own convention reads a
30166 inside the TTL with no `rtt-open` as "checked, could not open", so every
record this monitor signed, `prime` ones included, told foreign readers the
relay was unreachable. `rtt-write` is in `OWNED` without a field so a reading
left by the old passive monitor is cleared. No `v` tag: sampled across 12
monitors and 800 records, zero occurrences; the version rides as the third
element of `s`. Requirements sampled on 800 records: `!auth` 681, `!pow` 663,
`!payment` 619, `payment` 80, `auth` 18, so the negated form is how most
records say "open to read".

**`publishDistinct` names what was actually compared.** The evidence once
read "of N peers on this host", which counted comparisons that never
happened; members are compared to the leader only, so two paths that are
duplicates of each other but not of the leader are both recorded distinct.

**`edit` is bounded by a two-minute deadline.** The store's HTTP client
carries no read deadline, so a response that never came held a fitness pass,
the sweep behind it and the fast lane behind that for ten hours on staging
(issue 165). The floor is wider than `FitnessPass.PUBLISH_DEADLINE_MS` (one
minute) so the instrumented clock always fires first, and wide enough that a
write queued behind the mirror's bulk commits (~10s per 20k-event batch) is
never lost to it.

**The verdict word is `prime`, not `syncable`.** The old word named our use of
the relay on a record published for everyone; a crawler, an archiver and a
client choosing read relays want the same composite and none of them sync.

**Discovery lets the engine's `tag_index` aggregate answer a source, and the
preconditions are the whole of the safety.** `RelayDiscovery.aggregable` is
pure and public so each condition can be tested on its own, because a wrong
yes does not throw: it answers a superset, urls nobody named, dialled as if
they had been. `tag_index` is lossy three ways at once (single-letter names,
first values only, nothing of the rest of the tag), and every clause is a way
the caller's question stops surviving all three: `maxRelaysPerList` drops an
event for naming too many relays and an aggregate has no events to drop, so
the multi-thousand-entry synthetic lists would come straight back in; a
`where` is a condition on another element of the tag; `bindings` pair a url
with a value from the same tag occurrence and an aggregate has no occurrences;
a multi-character tag name is not in the index at all and `urlIndex != 1`
reads a value it does not keep. Where it applies, one grouping per select
replaces a walk of the corpus (about a second against minutes over millions
of NIP-65 lists), and the raw urls take the same `normalize` the paged path
applies per tag.

**`RelayFacts` writes no `g` and no `T`.** Both need something this process
does not have: a GeoIP database for the geohash and a classifier for the
relay type. A tag left out is honest; a guessed one is a signed claim.

**One predicate decides whether a fitness label is current.**
`RelayDiscovery.undialable` read the `dead` label by hand and aged it only by
the record's `created_at`, which every writer bumps, so a grade under older
rules or measured long ago held a url out for as long as anything kept
re-signing its record. It now asks `RelayVerdictRecord.currentFitnessLabel`,
the test `load` applies: `FITNESS_EPOCH` and the label's own measured-at stamp.

**A failed verdict edit is said, and its read is booked.** `edit` returned
null on a failed read, signature or insert without a word, and its record read
called the store directly, so a read the store hung on showed nowhere under
`store` on `/stats.json`. Failures are now logged with the url and the stage,
at most once a minute, and the read goes through `storeCall` as
`monitor.publish`.

**Discovery refuses what no stranger's list can make a relay of.** `RelayUrlCache.decide`
refused `isLocalHost`, which until quartz 2.7.3 missed Tailscale's `100.64.0.0/10`, single-label
hosts and `.lan`-style suffixes: 305 Tailscale addresses from production's relay lists were dialled
and graded `silent`, 456 single-label and local-suffix hosts `dead`. It now asks quartz's
`isDialableFromOthers`, the rule for a url found in someone else's data, which also refuses
documentation and multicast literals and names that cannot be a host (`wss://wss/nos.lol`, a scheme
pasted twice). Every url `decide` sees comes from someone else's relay list; the urls a deployment
declares in `sync.conf` never pass through it, so `ws://localhost:7777` still works as a stream's
own upstream.

**Websocket reader threads are virtual.** OkHttp runs each websocket's reader
loop on a dispatcher thread for the socket's life and starts that thread inside
the dial's caller, so a fleet's dials queued behind one platform thread start
each: 1,382 dials took 1,839ms to start against a silent local server, and
3.6s live. On `Executors.newVirtualThreadPerTaskExecutor()` the same dials
start in 318ms locally and 1.2s live, with no platform thread per socket.
OkHttp 5 locks with `ReentrantLock`, not `synchronized`, so a reader blocked
in a read does not pin its carrier; `LiveFleetProbe` split the fleet between
the two models under `-Djdk.tracePinnedThreads=short` and saw no pinning and
the same drops (198 on 102 relays against 180 on 97). Opening the sockets is
another matter: the sandbox admitted ~55 connections a second, so there the
fleet still took ~28s to open either way.

