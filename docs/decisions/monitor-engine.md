# Monitor engine decisions

The history behind `monitor/.../MonitorEngine.kt`, `StreamWorld.kt`,
`RelayDocument.kt` and `MonitorStatus.kt`, and the small `:common` and
`:web` files that serve every plane, moved out of the source so the code
reads on its own. One paragraph per decision; `git log -L` on the function
finds the commit.

## Monitor

**The monitor is its own class with its own `Processors`.** It was 278 lines
inside `SyncEngine`, and one class started two planes that ask different
questions on different clocks in different units. Both planes' rows landed in
one document and the page split them back apart by name (`splitProcessors`
in the JS). What the monitor still takes from the mirror is `ingest`,
`sockets` and `pinnedUrls`; cut those and it is a separate process.

**The corpus is declared, never inherited from the streams.** Every discovery
stream used to be a monitor source by existing, so adding a `relaySource` to a
stream silently widened the set of servers this deployment signs public
kind-30166 claims about. The rule the router already held for negative verdicts
— `Unreachability.proves()` stays quiet on a failure it cannot attribute, because
being wrong costs a false statement about somebody else's server — applies to
the corpus itself. The monitor names the relay lists it scans, in its own terms.
Writing the set down is also what showed that two of the shipped example's three
discovery streams source from a verdict query, so inheriting them had been
feeding the monitor its own output.

**The boot refuses the ambiguous config rather than picking a reading.** A
deployment with discovery streams and no monitor declaration used to measure all
of their sources and now measures nothing, and both are legitimate deployments.
The refusal names the streams and where the declaration goes, per the rule that
a configured component must never be silently inert; `sources = []` is how an
operator says "measure nothing" as a declaration rather than an omission. It
sits at boot (`RouterConfigLoader.refuseUndeclaredMonitor`) rather than in
`parse`, because a config with no monitor is well formed — it is the process
that would run doing nothing.

**Two files, and neither names anything in the other.** The monitor's config is
`monitor.conf` (`MONITOR_CONFIG_FILE`), read into the same `RouterConfig`. A
`monitor { }` block in the sync config still works and declaring both is
refused, because two declarations cannot both be the truth and picking one
silently is the failure this whole rule exists for. The first cut of the split
kept an `inheritStreams` key naming streams from the other file, which was the
one thing still coupling them: a cross-file name reference is the same coupling
with more steps, and it made the parse-time check need both documents. Where a
stream scans a list the monitor should measure too, the `select` block goes in
both files, or an `include` they share — the source list, not a pointer to one.
`monitor.conf` is the block's contents with no wrapper, and a pasted-in wrapper
is refused: read past, it parses to a monitor with no sources.

**The plane knows no mirror type.** `MonitorEngine` took the whole
`RouterConfig` and read four things out of it, and `StreamWorld` took
`List<SyncStream>` for two unrelated jobs: deriving candidates, and attributing
an event a probe dial happened to see. With the derivation declared, the
attribution is the only stream reader left, and it belongs to the mirror — it
has to agree with the mirror's own ingest — so it is a sink `SyncEngine` closes
over. `SyncStream`, `RouterConfig` and `IngestPipeline` are gone from `:monitor`
entirely; what crosses the seam is the `monitor { }` block, labelled
derivations, a timeout and that sink.

**A probe dial is not an upstream, so its events are always verified.** The sink
used to pass `wanted.all { it.trusted }` as `skipVerify`. `trusted` vouches for
one declared upstream's events; an event here came off whichever relay a pass
happened to dial, and any relay it probes can serve anything. Widening the sink
from the discovery streams to all of them turned that into a hole a static
`trusted` stream opened for the whole probed corpus — a forged event matching
its filter would have been stored without a signature check. The events are a
handful per relay, so verifying them all costs nothing worth having.

**The probe-event sink asks every stream, where it used to ask the discovering
ones.** `StreamWorld` was handed `discoveryStreams` for both jobs, so an event a
probe dial happened to see was attributed against those streams alone — which
meant a pure-monitor deployment (static `urls`, candidates through
`monitor { sources }`) had no streams there at all and dropped every one. That
was a proxy for "the streams the monitor is about", and there is no such
relationship any more. The sink asks the mirror, and the mirror's answer is any
stream whose filter matches. The volume is bounded by what the passes fetch,
which is a handful of events per relay.

**The fitness pass re-arms its position for the writes.** It dials one set and
writes a bigger one — a url folded onto another, or already failed by the
stability gate, is graded without a socket — and the position was armed for the
dials alone. So once the last dial landed the row sat at `n of n` with
`quietForSec` climbing for as long as the writes took, which on a live stack
under a saturated ingest queue was minutes: the page's own "nothing finished
for 5m" warning firing on a pass writing thousands of verdicts. Watched on a
real run: 2016 of 2016 for six minutes while `monitor.publish` went from 2138
to 2445 store calls. The write loop now declares its own set in a `verdict`
unit and ticks each url through, skips included; the batch guard's `break`
deliberately does not tick, so a stopped batch reads as stopped rather than
full.

**The start gate is `hasMonitorSources`, not `discoveryStreams.isNotEmpty()`.**
A pure-monitor deployment (streams on static `urls`, every url entering
through `monitor { sources }`) has no discovery streams, so `aliasMonitor
.start()` was never called and four rows read `off` for the life of the
process while `StreamWorld` derived the urls correctly. The rule is a function
over the config so a test can hand it that deployment.

**Two nulls in `fastLaneSeconds` mean opposite things.** With no `monitor`
block, `config.monitor?.fastLaneSeconds` carried null out and turned the lane
off, so a new relay waited a full `sweepSeconds` for its first `prime` on the
configs least likely to notice. A null inside a block is the operator's
`fastLaneSeconds = 0`, the documented off switch, so a plain
`?: DEFAULT` would restart a lane somebody turned off by hand.

**The fitness row gets its `OFF` phase too.** Fitness only sets a phase from
inside `measure`, so on a deployment with no sources it sat at `starting`
forever, on the row an operator checks first.

**The stale-verdict retraction is off the boot path.** On staging
(2026-09-04) it ran synchronously from `SyncEngine.start()` and queued behind
twenty-odd deadline-less store reads (#167): the mirror never opened a
stream, ingest sat at 0 ev/s and the status sites were never bound. It is a
coroutine now; the passes, which sign on what they read, wait for it, and the
roster's first rebuild may select on a verdict it is about to retract for one
interval. It is a paged walk of the whole graded corpus, not "one indexed
query returning nothing".

**The two retractions have two guards.** One shared `runCatching` meant a
throw in the epoch walk silently skipped the legacy-grade walk and reported
it under the first one's name.

**`booked` takes only the `StoreCalls` element, never the whole context.**
The retractions ran under `runBlocking` once, where the scope's element did
not reach them; `scope.coroutineContext` would also bring the `Job` (a
cancelled parent refuses to run) and the dispatcher.

**Pass order is fold, consistency, fitness.** A stability pass measures
survivors rather than urls about to be folded away, and fitness turns both
standing verdicts into refusals without re-dialling. The fast lane runs
stability then fitness only; the fold needs a host's whole group, which a
since-bound set never holds.

**The probe's idle budget is per url.** A `.onion` fingerprint on the
clearnet budget timed out while its circuit was still being built and came
back as an empty window, which folded nothing and left every url on that host
in the fan-out forever.

**The fitness pass is handed `aliases` alone, never `standIns`.** It signs
`l=alias` for every entry, and a stand-in elected for an absent survivor was
never measured. `foldedAway`, which only decides which socket to open, gets
both.

**The source row publishes nothing until a walk has run.** Zeros would say
`0 url(s) named` for the two minutes before the first sweep, indistinguishable
from a store holding no relay lists.

**`STABILITY_PROCESSOR` is spelled `consistency`.** The class is
`ConsistencyPass`, the state `RelayConsistency`, the tag `self-consistent`; a
fourth word would be one nobody could grep from the document to the code.

## Stream world

**The candidate set is derived from the store per pass, not pushed by the
streams.** A 16-url stream finished discovering in one second, the first pass
ran two minutes later against those 16, and two 17,499-url streams submitted
190 seconds after that: 34,997 urls waited six hours for a pass they missed
by three minutes.

**The hold-out is scoped to the operator's monitors; an unscoped source
contributes nothing.** Admission is a positive claim that still has to survive
a dial, so the roster reads unscoped sources freely. A hold-out forecloses:
one `dead` grade from anybody would starve a relay out for good, never dialled
and never re-measured. See `ForeignMonitorTest`.

**`recordedOnly` is scoped to `self`, narrower than the hold-out.** "How big
is our corpus" over the wider set is somebody else's corpus: a deployment
mirroring a busy foreign monitor would draw that monitor's world as the mouth
of its own coverage tree.

**The corpus is the union of what was named and what we hold records about.**
One staging round yielded 127 urls from a store holding 3.09M relay lists and
records for 19,844 relays, and the card reported the other 19,717 as urls no
relay list names now. A url we signed a record about is ours to re-measure on
our clock; a short read now costs freshness, not the population.

**A derivation that halves is called out, not adopted.** A content node still
loading answers with `coverage: 100, full: true` over zero documents, and a
degraded search returns a partial answer that looks like a small one; staging
went from ~17,000 urls to 127 without a line of log. `SHRINK_SHARE` is half
and `SHRINK_FLOOR` is 100 so the line is believable when it fires.

**Discovery is asked for the unfiltered set.** Letting
`RelayDiscovery.discover` apply `exclude` first pinned the funnel's `excluded`
row at 0 for as long as it existed and made `sourced` a post-exclusion number.

**Dead is our own `dead` verdict, not quartz's absence convention.** Inferring
death from a 30166 with no `rtt-open` counted every record quartz's passive
monitor wrote about a relay it merely failed to reach.

**The fast lane derives first and asks the hold-out about what it found.**
Reading the whole dead set before deriving cost one unbounded materializing
query per tick, thirty an hour at the stock 120s, to decide a question about
a dozen urls; most ticks find nothing.

**A probe event is submitted once, not once per wanting stream.**
`IngestPipeline.submit` queues before the store dedups, so a per-stream loop
spent one slot of a bounded queue per match.

**`DEAD_TTL_SECONDS` is 24h.** Quartz's `RelayReachabilityStore` used the
same; shorter than a sweep re-dials the corpse every pass, much longer
starves a host that came back.

## Relay document

**`rtt-open` is the document fetch's connect.** The document shares the
websocket's host, port and TLS, so `connectStart` to `connectEnd` measures
the same legs an explicit open probe would; a pooled connection reports null,
because a naive zero advertised a relay across the world as the fastest in
the store.

**The document publishes claims as claims.** No verdict is read off NIP-11,
but for `software`, `supported_nips` and the limitation flags there is no
instrument at all, and publishing nothing left our records the least
informative on the network. The measured half overwrites where they disagree.

## Monitor status

**The monitor has its own page.** Sync coverage answers "is the mirror
keeping up" in events; this answers "what is out there" in relay urls. The
verdict panel dials `relayUrl` from the document because the page is served
on the monitor's port, and `location` there found no relay.

## Common

**`ServingPressure.record` floors at 1ms.** A run of cache hits would zero
the mean, and the first-sample check then adopted one straggler wholesale.
The threshold (2s) sits above a healthy read (~400ms against 52M documents)
and below the point a client gives up.

**`STORE_WRITERS` is `SHARED_STRICT`.** The relay and the sync process write
the same authors, so a cached guard-owner view admits an event the other's
tombstone covers, and nothing repairs it afterwards. The cost was measured:
per-event insert ~143 to ~137 ev/s (-4.5%), p50 unchanged, no difference on
`batchInsert`.

**The schema deploys on every boot.** A drifted schema had Vespa answer every
write with `Field 'name_parts' is not defined` while the router counted,
dropped and carried on; 2.3M good events were lost in one run.

**`RELAY_NSEC` takes only `nsec1`.** Bare hex has no checksum, and a mistyped
hex key would sign as a stranger with nothing to indicate it. The uppercase
BIP-173 spelling (QR exports) is normalised first, or it died in `describe`
as "63 characters".

**`canonicalRelay` normalises before trimming the slash.** `wss://nos.lol`
and `wss://nos.lol/` were two rows, and a relay split three ways sat below
relays it outnumbered. A rejected url is kept, since a vanished row
understates `total`.

**`Counts.kt` is not a second `Format.kt`.** Two files of one name in one
package across modules compile to the same `FormatKt` facade class and
collide on the classpath.

**The pulse document publishes counters and gauges, never rates.** Every total is cumulative
since the process started, so the page differences two consecutive polls to recover a rate and
any number of readers may poll without consuming anything; `gauges` (a queue depth, calls in
flight) are instantaneous and named apart precisely so a reader cannot difference them into
nonsense. There is no rollup thread and no `stale` member because nothing here can go stale.
It is per process, not per cluster: the relay and the mirror hold separate stores over one
Vespa, so each has its own ledger and page. Vespa's own resource use (memory, disk, transaction
log) is deliberately absent: its metrics proxy already reports it, and a second, worse source of
truth would be the wrong thing to build.

**`startedAtMillis` is the store's start, stamped once.** Defaulting it to the moment the
reader was built named a window shorter than the one the counters covered, by two minutes on a
relay that deployed a schema first; stamped once rather than read per call so the window only
ever grows.

**Engine milliseconds are decimal.** The page divides them by the query count, and a store whose
engine answers in under a millisecond published every profile as a flat zero, the same
precision the health loop once lost rounding `%.2fs`, one boundary further on. Slow reads are
published newest first because the page draws the table in document order under a "newest
first" heading; it drew it backwards until an audit, the wrong end of a ring for somebody who
opened the page during an incident.

**The slow-read threshold is honoured only where the page will show it.** The slow-read ring is
the one place the store retains a query string, and a query string is what somebody typed; an
operator who set `PULSE_SLOW_READ_MS` but left the client sections off would be keeping that log
for nobody to read, so `pulseSlowReadMs` says so on stderr and keeps nothing. A value that does
not parse stops the boot: silently keeping no log is exactly what the operator was trying to
change.

**The pulse's operational numbers also go to the log, and nothing sensitive does.** The page is gated because it quotes search terms, which made the ordinary questions (which activity spends the engine's time, what holds the write gate, is a background walk moving) unanswerable from a terminal; diagnosing a store matching tens of millions of documents a second while retiring almost no work took hours at Vespa's own metrics endpoint because of it. `StoreMetricsLog` prints activity and port counters, stage timings, lock holders and gauges, never `topTerms`, `topObservers` or `slowReads`, since a log line is read by anything that reads container logs. Counters, not rates, so two lines subtract to an interval. On by default, on a daemon thread, because a diagnostic nobody enables is the reason it was missing.

## Web

**Pages carry content-derived ETags.** A jar entry's mtime is the build's, so
two deploys of an unchanged module still missed; a returning reader now gets
23 empty 304s instead of ~40KB, and each module is read and hashed once per
process rather than re-gzipped per request.

**Every page is compressed.** ~117KB of text over a Cloudflare tunnel cold
loaded in 1,513ms for 13 requests; gzip gives back roughly 4x on the link,
which is where the time goes.

**A service serves its own status site.** The mirror once wrote JSON files a
volume the relay read back and re-narrated, ~2,500 lines whose job was to
re-derive what the writer knew, and a file cannot say whether its writer is
alive. An HTTP request answers that by whether it answers.

**`/stats.json` is a merge of two tiers, and `stale` is per tier.** Counters
publish about once a minute and corpus-wide groupings about every fifteen; a
counters pass that cleared the notice would hide a charts tier failing all
night. Before the notice, four hours old and four minutes old were the same
picture.

**`iconOverride` returns null for the service's own icon.** With `RELAY_ICON`
unset the NIP-11 doc publishes `/favicon.ico`, and treating that as an
override pointed `/favicon.ico` at itself; a browser following the redirect
looped until it gave up.

**Icon links are replaced, not appended.** The pages hint the built-in SVG
first, and Chrome, Firefox and Edge prefer SVG to `.ico`, so an appended
override worked only in Safari.
