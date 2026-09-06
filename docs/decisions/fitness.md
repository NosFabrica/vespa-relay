# Fitness pass decisions

The history behind `monitor/.../FitnessPass.kt`, `ConsistencyPass.kt`,
`RelayConsistency.kt`, `RelayCompliance.kt`, `ReachabilityProbe.kt`,
`Silence.kt`, `HostStrikes.kt` and `Unreachability.kt`, moved out of the
source so the code reads on its own. One paragraph per decision; `git log -L`
on the function finds the commit.

**Nothing is published when the instrument returned nothing.** A pass whose
own socket layer was failing graded 3,945 relays `silent` in one sweep; a
re-dial found more than half of them answering a REQ in under two seconds,
most with an immediate CLOSED, and every one of those verdicts had taken its
url off every roster. So a url with no EOSE, no CLOSED and no transport word
on any rung is counted and named, never graded, and the same rule covers the
pre-probe throwing (formerly `dead`), the dial throwing (formerly `silent`)
and the per-url deadline firing. A relay that genuinely never answers costs
one re-dial a pass.

**The batch guard refuses a whole pass, clean-looking verdicts included.**
Every other rule is per url, and the failure it exists for is not: when our
dialling breaks it breaks for all of them at once. `GUARD_SHARE` is a quarter,
deliberately well below the roughly-half share the incident produced, because
a dead url answers (refused, NXDOMAIN, TLS failure) and lands in a verdict;
one dial in four coming back with nothing has never been normal here.
`GUARD_FLOOR` keeps it off batches where one dead host is a large share.

**The silent branch published `restricted` for eleven months, swapped.**
`restricted` means the relay answered and none of the answers was a window,
so it needs a terminal reason; the no-reason case is `silent` word for word.
`quietplace.xyz`, which accepts a socket, serves a NIP-11 document and then
answers no REQ ever, was being published to the network as a relay with a
narrow query policy. `Verdict.RESTRICTED` now has no path to it, on purpose:
a relay answering only shapes we cannot send grades `prime`, because a CLOSED
makes it "speak" and an empty window is read as a drain. Telling those two
empties apart needs a signal `AliasProbe.Page` does not carry.

**The second page is the whole of #187.** 137 relays the mirror aborted for
ignoring the cursor were all graded `prime` and `pageable: true`, because the
pass had only ever asked one page. And `pageable: true` was granted on an
empty first page (26% of the 137), which proves the relay answers and cannot
prove it can be walked; now nothing came back to page from means no claim.
Page two is asked through the rung that answered, since a page two of a
different shape proves nothing, and under its own clock so a cut costs a fact
and not a url.

**The ladder's verdict is handed over before the second page.** When the
`prime` was only returned at the end, the per-url deadline firing during the
second page left the url with no verdict; those urls counted `abandoned`,
`abandoned` feeds the batch guard's blind share, and a slow batch could refuse
to publish any of its own verdicts. The same handover covers the cut-late
branch, which used to throw a settled verdict away with everything else.

**Cut-late was the first theory for #172, and it was wrong.** The NEG-OPEN is
the last step and had no wall clock, so the theory was that slow relays lost
their verdict to the budget every sweep. `FitnessBudgetLiveProbe` refuted it
against real relays: the whole job ran 1.1-11.9s against a 240s budget, the
NEG-OPEN's share never exceeding its own 10s idle window (59ms to 10.1s across
nine relays). #172 was the write loop. The cut-late hole was real and is
shut, and `NIP77_DEADLINE_MS` exists because the idle window is re-armed by
every round and so bounds nothing on a relay that keeps answering slowly.

**Every verdict write has a wall clock (#165).** The store's HTTP client
carries no read deadline by design, so a response that never comes suspends
its caller forever, and a suspended coroutine holds no thread for a dump to
name. A 13,560-url pass on `vespa-eventstore-staging` finished every dial in
40 minutes and then sat in `measuring fitness` for ten hours with
`attempted == toProbe`; because the sweep holds the monitor's pass gate, the
fast lane stopped too (`passesRun: 0` for the life of the process), masked by
quartz's passive record refresh keeping verdict ages healthy. The deadline is
a minute because a write queued behind a 20k-event bulk commit at ~500µs an
event legitimately waits tens of seconds. `STAGE_PUBLISH` names the write in
the held set so the ten hours become one url on one stage.

**The write result is three-valued.** The first cut collapsed "the store
answered and the write failed" into "published", so a store failing every
write promptly reported a clean pass, the fast-fail shape of the very outage
the loop exists to make loud. A decline also resets the consecutive-wedge run:
a store alternating timeout and decline reached "three in a row" having never
timed out twice in a row.

**The wedge budget is a duration, not a count.** It was twenty timed-out
writes, justified as "twenty minutes lost", the right quantity through the
wrong variable: the cost of tripping is every verdict after the trip, so on a
500-url lane tick twenty is generous and on a 20,000-url sweep it lets 0.1% of
writes straggling forfeit the other 40%. That is how a limit written for a
wedged store fired on a merely busy one every sweep (#172). The consecutive
limit alone would let an alternating store spend four and a half days of
deadlines on one batch while technically making progress.

**The write loop is ordered by url and resumes where the last batch stopped
(#172).** `outcomes` is a `ConcurrentHashMap`, whose iteration order is
arbitrary but stable across passes for a stable url set, so every cut batch
dropped the same tail. `WriteOrderForensicProbe` rebuilt the map from the
20,075 graded records on `search-staging.brainstorm.world` and walked it: the
verdict ages were a staircase (positions 0-12,043 stamped 1.2-1.5h ago,
12,043-15,555 at 29.1-29.9h, 15,555-20,072 at 72.3h), 735 crossings between
written and unwritten where a per-url cause predicts ~9,700, and the same
fresh share in every grade (`prime` 55.8%, `dead` 57.7%, `alias` 61.8%). The
7,881 urls past the newest cut had gone un-regraded for three days. The cursor
is in memory only; a restart starts at the top, which is the same guarantee
from a different offset.

**The write cursor is keyed by label.** One `FitnessPass` serves the sweep
over the whole corpus and the fast lane over a handful of urls every
`fastLaneSeconds` (120 by default), the same object in both lists. A single
cursor was written ~180 times between sweeps by three-url batches, and a lane
tick that wrote its whole batch cleared the sweep's resume point every sweep,
leaving the rotation dead code that looked alive.

**Inherited verdicts are re-signed only when they would change something.**
`measured-at` is the mechanism by which a verdict ages, so stamping it on an
`alias` or `inconsistent` this pass never dialled makes an untested verdict
immortal. Skipping the unchanged ones also cut a third of the loop: 6,192 of
20,075 records (31%) on staging are one of the two free refusals. The
evidence must match as well as the grade, since a url re-folded onto a
different canonical is `alias` both times and not the same statement.

**`rtt-read` is the first page of the rung that answered.** Billing the rung
the relay declined would publish our ladder's shape as its latency, and
timing the whole walk billed a relay capping at ten events for the two round
trips our twenty-event target then costs.

**`auth` is measured only in the positive direction.** Quartz reports
`authRefused` and nothing else, so a relay that challenged us and accepted
our signer is indistinguishable from one that never challenged; publishing
`!auth` off a clean read would tell every reader without our key that a gated
relay is open. Symmetry needs a "was challenged" signal from the client.

**The descriptive half of the record comes from NIP-11 even though no verdict
does.** Applying "nothing from the document" to the whole record produced a
30166 with a verdict and nothing else, which quartz's own convention reads,
inside the TTL, as "checked, could not open". NIP-11 still decides nothing:
this corpus has a relay serving REQs over its declared `max_message_length`
and a fleet publishing no document at all.

**The epoch check moved from every read to a boot-time retraction.** As a
read-time check (`s[4] == FITNESS_EPOCH`) it put our private versioning in
the way of everyone's records: a standard NIP-66 monitor carries no such
element, so no foreign verdict could ever pass whatever the config said about
whose to trust. The claim is ours, so we retract it at boot, the only moment
the constant can have changed, and the read asks only whether the url holds a
verdict.

**`LEGACY_GRADES` is spelled out, not derived from `Verdict`.** A migration
query built from `Verdict.entries` asks for the vocabulary of the build doing
the asking, which is the build whose records do not need migrating. Written
that way it missed every `["s","syncable"]` record, the admitting grade and
the largest group (1,716 of 4,000 on this deployment), while the refusals
survived because their spellings had not changed, which made the miss look
like a working migration.

**The boot retractions are paged and concurrent.** Both read every graded
record because neither the epoch nor the legacy tag can be asked for in a
filter, and both used to do it in one unbounded query (12,374 records on
staging) inside the `runBlocking` the roster's first rebuild waits on. Serial
read-modify-writes over the 17,189 records of the first boot after the grade
move cost minutes of a router that had not started mirroring; sixteen in
flight is safe because every url is a distinct addressable record.

**The consistency anchor is a week, and it took two runs to know.** Walking
each url twice at four depths, the stable relays (`nos.lol`,
`nostr.oxtr.dev`, `relay.lightning.pub`) scored 1.000 at every depth in every
run. The first run alone suggested `multiplexer.huszonegy.world` merely needed
time to replicate (0.446 at one minute climbing to 0.964 at a week); the
second run scored the same relay at the same depth 0.654, so its score is not
reproducible and it is unstable at every depth, as is `fiatjaf.com`
(0.618-0.826). Nothing measured lands between 0.92 and 1.000, which is why the
0.9 bar costs a good relay nothing. A single run would have shipped the wrong
constant; run `RelaySelfConsistencyProbe` more than once before moving either.

**An inconsistent relay is excluded, not downgraded.** A relay whose window
is a fresh random slice has no stable band, so every cycle re-downloads what
the last one took. Measured on this mirror: millions of duplicated events and
cycles stretched from two hours to five by relays re-serving what we already
held. The verdict expires, so a relay that settles comes back on its own.

**`RelayConsistency.replace` moves each url straight from its old verdict to
its new one.** A bulk forget followed by a bulk adopt left a multi-second
window on a fan-out of thousands in which every refusal was gone and none
re-adopted; this object is shared by every stream, and another stream's
`unusable` call inside that window put every inconsistent relay back into its
cycle.

**The consistency pass has no per-pass budget, and it filters out folded
urls.** The budget it once carried stopped a pass measuring its whole set. A
folded url is never dialled, so measuring it is spent on a question nobody
asks; on a polluted store two thirds of a candidate set folds away, and
letting them through delayed the survivors' verdicts by several six-hour
passes.

**The undecided urls are a partition, with a cause under `silent`.** A pass
over a discovered corpus decides a few hundred urls of several thousand, and
the rest were one number that read as a gate getting nowhere when most of it
was a dead corpus re-asked every six hours. `report` walks the candidates once
where it used to make four passes and then a filter of the whole map per row,
seven times over five thousand urls. `dialled` counts sockets opened, not
`wanted.size`, since a url `canDial` held back is work that never happened.

**An auth refusal ends the filter ladder.** On `filter.nostr.wine` the first
ask is refused in 1.6s and every later ask on that connection is answered
with nothing, each waiting out the full idle window; the pass used to fall
through to the kinds pair regardless because the refusal was flattened into
"proved nothing".

**The consistency verdict write is not wrapped in `runCatching`.** It runs
inside the per-url deadline, and a swallowed cancellation let `measureOne`
return normally while the job was cancelled: the url was filed `abandoned`
after `decided` had counted it, and the published partition stopped summing.
Verdicts are written per url so a restart mid-pass keeps what was proved.

**The paired walk was once staged, with a comment claiming concurrency.** The
two REQs are now genuinely in flight at the same instant over one connection,
so "the answer changed" cannot be blamed on elapsed time.

**The compliance bars are provisional.** Ten percent and three events have
not been taken against the network; `RelayComplianceProbe` is the instrument
and it asserts nothing. Both must be crossed because a share alone refuses a
relay that got one of three wrong and a count alone refuses a firehose that
got three of five hundred wrong; the relay this exists for gets essentially
all of them wrong. The evidence string names strays below the bar so the
corpus of records is how the bars get re-taken.

**The TCP pre-probe short-circuits only on proof.** `TcpProber` answers with
a Boolean, so a refusal and a timeout arrive as the same value; acting on it
this pass called 5,001 urls unreachable in an hour, of which 732 across 423
hosts answered a REQ perfectly well. A failure is re-run once for its cause
and believed only for what `Unreachability` accepts. The probe once signed
its own records through quartz's `RelayMonitor.observer`, which made two
writers for one fact and rewrote `created_at` constantly because that monitor
listened to every socket the fan-out opened; the fitness pass is the single
publisher.

**`Silence` matches timeout before refused.** Quartz relays the socket
layer's text as `WebSocket Failure: <message> (<ExceptionName>)`, and for a
refused port the message is `Failed to connect to localhost/127.0.0.1:1`,
which carries no "refused" at all; the `(ConnectException)` suffix is the
whole evidence, so an explicit timeout in the same string must win.

**`HostStrikes` reports which of its two skips applied.** The two were one
number, and they carry opposite retry policies: a known-dead url is out until
our signed `dead` verdict ages past its 24h TTL, a struck-out one only for the
rest of this cycle. The durable reason is checked first because it is the one
with the longer reach.

**The TCP pre-probe is skipped for anything Tor routes.** `TcpProber` resolves
and connects directly from this box, so for a Tor-routed url it answers about a
path the transfer never uses: a `.onion` fails name resolution while up, and
under `SYNC_TOR_ALL` it would connect to every discovered relay in the clear.
`shouldPreProbe` is the one predicate for that, and the websocket dial is the
only verdict on those urls.

**`HostStrikes` counts failures per authority, not per url.** The outbox model
mints one url per user on a filtering relay, so a per-url counter never reaches
a threshold. The authority is `host[:port]`: a subdomain is not folded into its
parent, and two ports are two relays. A host that delivered anything this cycle
is never treated as dead, whatever its siblings did.
