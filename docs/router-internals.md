# The router, in one pass

Moved from AGENTS.md on 2026-09-04, unchanged. This is the long form of the AGENTS.md section "How the router works".

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
would be a mirror that quietly stopped mirroring. A stream's relays come from
one of two places and are walked the same way after that:

- **declared** — relays listed in `urls` in `router.conf`
- **discovered** — relays found in stored events via `relaySource` (NIP-65
  outbox lists, NIP-85 provider lists, relay hints), admitted by the monitor's
  verdicts

**ONE ENGINE walks both** — `VisitPool`, since the crossing finished. Declared
urls skip discovery, the gate and the fold and go straight into the roster;
everything downstream is identical, which is the point. `StaticBackfill` is
gone with the `sync` knob, `SYNC_NEG_MIN_EVENTS` and `PagingProgress`: it
walked a declared relay ONCE per process and then live-tailed, so
`negentropySyncThePastSeconds` and `refetchThePastSeconds` could never mean
anything on those streams, and both are refused at parse time now rather than
accepted and ignored. Pruned with it: `StreamPhases` is two phases now
(`starting`, `rotating`) where it was eleven plus the passes each cycled
through, `CycleTally` is gone, and the stream row on `/stats.json` carries the
phase word, its clock, `roster`/`tails` and the in-flight legs — no cycle, no
passes, no fraction, no ETA. A stream has no walk to be a phase OF, and where
each relay has got to is per relay, which is what the phase words were standing
in for. The card and the glossary followed: 30 terms describing the fan-out's
vocabulary went with the members they described.

**Dynamic streams run on two planes.** The MONITOR plane (`AliasMonitor`'s
passes: the fold, then stability, then `FitnessPass`) measures every url the
streams' sources surface and writes what it finds on signed NIP-66 kind-30166
records — culminating in the `["l","prime","relay.fitness",…]` verdict a relay earns by
answering a settled-anchor probe. The SYNC plane (`VisitPool`) never decides
whether a relay is worth dialling; it reads the verdicts back. A relaySource
entry is either a **verdict source** (`filter = { "kinds": [30166],
"#l": ["prime"] }` — the verdict list IS the relay list) or a **scan**
(`select` over stored lists like 10002/10040). They are ONE TYPE and one read
path: a verdict query is a scan whose select is NIP-66's `d` tag, which the
loader supplies. `VerdictSource` and its separate verified read are gone —
they differed in the questions they were allowed to answer and in almost
nothing else once the epoch and the tag-stamp freshness left.

**NO PROBE PASS MAY JOIN ON ITS SLOWEST URL, and this is the rule to know
before touching one.** The three monitor passes fan out with `coroutineScope`,
which joins on every child — so one url that never returns holds the pass, and
because a stream admits a relay only on a `prime` verdict and only the fitness
pass writes one, it holds the whole dynamic mirror with it. Measured on staging:
one url of 12,374 sat at `attempted: 12,373` for 74 minutes while `roster: 0`,
`tails: 0` and three streams reported `0 certified relay(s)`. For scale, the
same pass had cleared the first 11,879 urls in 37 minutes.

Every outbound call the job makes was already bounded — a 5s TCP pre-probe, a
10s NIP-11 document, an idle window per rung of the ask ladder, a 10s NEG-OPEN —
and that is exactly the trap: **an idle window is not a wall clock.** (The
NEG-OPEN's 10s was one too, and it was the LAST step of the job: a
reconciliation is rounds, and every round re-arms the window. It carries its own
`FitnessPass.NIP77_DEADLINE_MS` now — see #172 below for what it cost while it
did not.) Quartz says
so in its own header for the call these walks are made of — *"there is no
wall-clock ceiling parameter … a hard deadline composes at the call site"* — and
two paths inside that fetch loop are outside the window by construction. A relay
that never stops sending keeps the timeout disarmed (it is armed only when both
channels are dry), and the suspending `onEvent` hook is deliberately run outside
the timeout scope so a stalled write is not cancelled mid-event — which for this
router means a full ingest queue blocks it.

So every pass now puts `AliasProbe.deadlineMs` around its unit of work, and four
things about it are load-bearing:

- **It is derived from the very window it bounds** — `WINDOWS_PER_URL` (12) times
  the url's own `probeIdleMs`. Sized from a constant it would cut every `.onion`
  the fold measures, since a hidden service is allowed its circuit budget on top
  of the clearnet one. Four minutes at the default `connectionTimeout = 20`,
  against a job whose own bounds sum to about ninety seconds.
- **It goes INSIDE `gate.withPermit`, never around the `launch`.** Out there it
  would be timing the wait for one of `dialConcurrency` permits, which on 12,374
  urls is most of a job's life, is the pass's own shape rather than any relay's,
  and would cut the urls at the back of the queue first.
- **NOTHING THE CUT DECIDED is published about a url.** A deadline is our
  instrument giving up, not a fact about the relay — the same rule
  `ConsistencyPass.Unmeasured.FAILED` already carried for a probe that threw. The
  url is counted, named in the pass's log line, and measured again next pass.
  That is what makes the number safe to set close: cutting a relay that would
  have answered costs one more pass, and not cutting one costs the mirror.
- **…AND A VERDICT THE DIAL ALREADY EARNED SURVIVES IT.** The converse of the
  rule above, and the half that was missing. The relay answered the ladder; our
  clock firing one step later does not un-answer it, and a pass that threw those
  away would re-grade the corpus at a cadence set by how long each url's JOB ran
  — the slowest relays last and, past a point, never. So the `prime` path hands
  its outcome over the moment the ladder settles it (`FitnessPass.dialVerdict`'s
  `settled`), one line before the NEG-OPEN, and `measure`'s cut-late branch
  publishes it and counts the url apart from the abandoned ones. Same rule the
  walk itself already follows a level down: keep what was proved, publish
  nothing that was not.
  **This was the first theory for #172 and it was wrong** — see the write-loop
  section below for what #172 actually was. `FitnessBudgetLiveProbe` is what
  separated them: against the nine relays the issue names, a whole url job runs
  1.1-11.9s against a 240s budget, the NEG-OPEN's own share never exceeding the
  10s idle window it already had. The hole is real and is shut; it was not the
  cause.
- **The held urls are published** (`processors[].inFlight`), because the wedged
  url was not nameable from anywhere: not from the position, not from the log
  (420 router lines over twenty minutes, none about fitness), and not from a
  thread dump, since a suspended coroutine has no frame. It was recoverable from
  OkHttp thread names, which is not a diagnostic anyone should need. Longest-held
  FIRST, which is the reverse of a stream's `inFlight` — there held is not risk,
  here every leg is bounded and a long one is the anomaly.

**The thirteen urls that pass was holding were dialled, and none of them
reproduces the hang.** Worth recording, because it is the evidence that says the
deadline is a backstop and not the fix for a specific relay. Every one of them
terminates every rung of the ask ladder today: eight EOSE promptly at 500 events
with nothing above the anchor, two refuse the bare filter with `blocked: can't
handle empty filters` and answer the kinds rung, one is kinds-restricted, one
(`relay.ltgnet.work:8443`) will not open a socket at all, and one
(`quietplace.xyz`) accepts a socket, serves a NIP-11 document, and then answers
NO REQ on any rung and no NEG-OPEN either. The worst case among them is that
last one at ~60s of ladder, comfortably inside the four-minute deadline.

So the hang is not a property of any of these relays as they behave now, which
leaves the path the probes cannot exercise from outside: **the `onEvent` hook**.
It is run outside quartz's timeout scope by design, this router hands it to a
bounded ingest queue, and a job parked in it produces exactly what the thread
dump showed — a socket in `loopReader` with NOTHING arriving, because we stopped
draining, the TCP window closed and the relay stopped sending. A relay-shaped
hypothesis predicts a socket busy with frames; the dump showed the opposite.

`measuring.quietForSec` is the other half. `etaSec` read `0` throughout the
stall, which is correct arithmetic on a rate that has gone to zero and actively
misleading as a signal — it is the same `0` a pass one url from done reports. The
seconds since a unit last ENDED is what separates them, and the card draws it
only past `STUCK_PASS_SEC`.

**The STORE is the pass's other unbounded dependency, and its writes get the
same treatment as its dials.** The fitness pass defers its record edits to
after the join — the batch guard has to see the whole batch before anything may
publish — and that loop was the one stretch of the monitor plane with no
deadline on it: the store's HTTP client deliberately carries no read deadline
(an unlimited query may take as long as it takes; dead connections are caught by
HTTP/2 pings), so a request whose response never comes while the connection
stays healthy suspends its caller for the life of the process. Measured on
staging (#165): a 13,560-url pass finished every dial in 40 minutes —
`attempted == toProbe`, `quietForSec` climbing 1:1 with wall clock — then held
`measuring fitness` for ten hours with no thread in any monitor frame, because
a suspended coroutine has none. And since the sweep runs under `AliasMonitor`'s
pass gate, the one suspended write also stopped every future sweep AND starved
the fast lane on the same mutex, silently — while quartz's passive
`RelayMonitor` kept bumping `createdAt` on every connected relay's 30166, so
verdict ages read fresh and the outage was invisible from freshness. So each
verdict write now runs under `FitnessPass.PUBLISH_DEADLINE_MS`, is named in the
held set (`verdict write`) while it runs, and after `PUBLISH_WEDGE_LIMIT` cut
writes the pass drops the rest of the batch and ends loudly — a wedged store
fails every write the same way, and a minute apiece over 13,560 verdicts is
nine days of a pass that should end in three minutes. A cut write publishes
nothing and the url re-earns its verdict next sweep, the same bargain an
abandoned dial gets.

**The ceiling beside it is a DURATION, not a count.** The consecutive limit
catches a wedged store; it cannot catch one that ALTERNATES, because a success
clears the run each time — so a second bound is needed, and the thing worth
bounding is wall time. `PUBLISH_WEDGE_BUDGET_MS` is twenty minutes of writes
that never came back, measured rather than derived from the deadline. It was
twenty timed-out WRITES, justified in its own words as "twenty minutes lost",
which is the right quantity named through the wrong variable: the cost of
tripping it is every verdict after the trip, so a count says something
different on every corpus — generous on the handful a fast-lane tick carries,
and on a 20,000-url sweep letting 0.1% of writes straggling forfeit the other
40%. That is how a limit written for a wedged store came to fire on a merely
busy one, every sweep. Twenty minutes is twenty minutes at any batch size. The
report now names which of the two stopped the batch, because they want
different answers: a run of unanswered writes is a store that has stopped, a
spent budget is one answering some of them slowly, which is load.

**AND THE WRITE LOOP RESUMES WHERE THE WEDGE STOPPED IT. THIS IS #172.**
"Re-earns it next sweep" was true of the urls at the front of that loop and
false forever of the ones at the back: the loop walked `outcomes`, a
`ConcurrentHashMap`, so it went in hash order — arbitrary, and STABLE across
passes for a stable url set. Every cut batch therefore dropped the SAME tail,
and a healthy relay whose url happened to hash late could never be re-graded
however often the sweep ran. So
the loop walks the urls in URL order and starts at the write the last batch's
wedge stopped on (`FitnessPass.writeCursors`, in memory — a restart starts at
the top, which is the same guarantee from a different offset). What a wedge
costs this pass is the next pass's head.

**The cursor is PER LABEL, and a single one would have been dead code that
looks alive.** One `FitnessPass` object serves two callers on two clocks —
`MonitorEngine.fitnessEntry` is in `passes` AND in `fastLanePasses` — so between
two sweeps the lane runs it ~180 times over a handful of urls each. Every lane
tick that writes its whole batch clears the cursor, so the sweep's resume point
would never survive to be read.

**The evidence, because #172 was diagnosed twice and only the second one was
right.** `WriteOrderForensicProbe` rebuilds that very `ConcurrentHashMap` from
the 20,075 graded records `search-staging.brainstorm.world` was serving and
walks it in bucket order. The verdict ages come out as a STAIRCASE, not a
spread: positions 0-12,043 stamped 1.2-1.5h ago, 12,043-15,555 stamped
29.1-29.9h, 15,555-20,072 stamped 72.3h — each cohort contiguous, and ages
decreasing across each one exactly as a loop stamping `now` as it walks would
leave them. 735 crossings between written and not, where a per-url cause
predicts ~9,700. Three sweeps, each starting at position zero, each reaching
less far than the last (20,072 -> 15,555 -> 12,191); the 7,881 urls past the
newest cut had gone un-regraded for three days. And the fresh share was the same
in every grade — `prime` 55.8%, `dead` 57.7%, `alias` 61.8%, `silent` 57.7% —
which is what rules the relay out of it entirely. Replayed through the real pass
on the real url set with the same cut, coverage goes 60.7% -> 100% by the second
pass where production sat at 58.8% forever.

**The reported `rtt-read` correlation is a confound, and reading it as a cause
cost a whole diagnosis.** The rtt and the verdict stamp are written by the same
edit, so a record that has not been re-taken carries the rtt from whenever it
last was. Binned by verdict AGE — which is the actual symptom — the median read
is flat: 664ms fresh, 675ms at 11h, 680ms at 29h, 937ms at 48-96h. The 20x tail
in the issue is what binning by the rtt ITSELF surfaces out of 74 records.
`rtt-open` does not show it because it is measured by the NIP-11 read on a
different client, so it does not move with whatever inflates a read.

**A PASS RE-SIGNS WHAT IT TESTED, AND ONLY THAT.** A measured verdict is always
written — the point of dialling is to put down what the relay did, and the
`measured-at` riding with it is true. An INHERITED one is not: the fold's alias
and the stability gate's refusal reach the fitness write loop having cost it no
socket, and stamping `measured-at = now` on those claims a measurement this pass
did not make. That stamp is the whole mechanism by which a verdict ages
(`RelayVerdictRecord.current`), so refreshing what was not tested is how an
untested verdict becomes immortal — the trap that header already describes for
the event clock, one field over. So an inherited verdict is written only when it
would CHANGE something: the record carries no such grade, carries a different
one, or carries it under a superseded epoch or aged past the TTL.
`RelayVerdictRecord.fitnessGrades` is the one chunked read that answers it, and
a read that FAILS falls back to writing everything — skipping a write the record
needs is the worse of the two mistakes. It is also most of a third of the loop:
6,192 of the 20,075 graded records on staging are `alias` or `inconsistent`.

**Two things about #172 that are NOT the fix, recorded so they are not tried
again.** Raising a stream's `gatedBy.maxAgeSeconds` is not: it had already gone
14h -> 48h for the same class of problem, the affected relays were past 48h too,
and at their latency band's 22-day median verdict age no bound short of "never
expire" holds one — while each bump weakens the gate for every relay that
genuinely went bad. Raising `WINDOWS_PER_URL` is not either, for the same shape
of reason one level down: the budget is a backstop against a hang, and a cadence
that depends on it at all is the bug.

**THE MIRROR'S READS HAVE THE SAME HOLE AND ARE DELIBERATELY LEFT WITH IT
(#167).** Same deadline-less client, same failure — a response that never comes
suspends its caller for the life of the process — but on the read side the
caller is one of eight `IngestPipeline` workers, and eight is the whole of
ingest. On staging the queue sat at 8206/8192 backpressuring every download
while three events were accepted in nine minutes. The write side's answer does
not port: a verdict write that is cut publishes nothing and the url re-earns it
next sweep, whereas an ingest pass that is cut DISCARDS twenty thousand
mirrored events that nothing re-offers before the next full resync. A router
quietly bleeding a batch every few minutes while the store is sick is a worse
failure than one that stops. **So nothing bounds these reads, and the wedge is
reported instead — the remedy is at the store.** Read that as a decision, not
an omission; the deadlines were written and removed.

**The thread dump for it reads as the exact opposite of the truth, and that is
what the reporting is for.** All eight ingest threads were parked in
`LinkedBlockingQueue.take` on an empty pool queue, having burned 1-5ms of CPU in
398s — which reads as workers STARVING while the queue is reported full, a
contradiction with no explanation. There isn't one: the workers were suspended
inside a store round trip, and a suspended coroutine has no frame, so its pool
thread goes back to `take()` looking idle. The queue depth cannot tell that
apart from honest backpressure either, which is why `bottleneckOf` said `ingest`
— "downloads are backpressured", the busy-mirror reading — for a pipeline that
had stopped. So `IngestPipeline` marks `busySince` around the whole batch pass:
`inBatch`/`oldestBatchMs` settle it from outside the process, a full queue whose
oldest pass is past `WEDGE_AFTER_MS` gets its own bottleneck word (`wedged`) on
the health line and the card, and a worker loop that exits says so rather than
silently taking its share of the queue with it. Also worth knowing before
theorising off that dump: `router: ingest probes id 99% dropped` is the probe
WORKING — `dropped` is duplicates it caught, so 99% is a converged fan-out —
not the probe being dropped. And the store's VISIT path is not the hole: it has
carried a 120s read deadline and a cancellation guard since before that pin, so
threads in `VespaVisits.streamOnce` are a streaming walk, not a wedge.

**NOTHING IN THE SYNC PLANE KNOWS THAT `l` IS THE TAG OR THAT `prime` IS THE
VALUE.** That is the whole point of a gate being a filter: another monitor
spelling its opinion `["l", "online", "monitor.example"]` — or on a tag of its
own entirely — needs no code here, only a different filter. Ours is a NIP-32
label under `relay.fitness`, which is itself a spelling and not a privilege. Keying anything on `l`/`prime` — a default tag, a refusal of other
values, an inferred freshness bound, a "this source vouches for itself"
predicate — hands our vocabulary back to every operator who wanted theirs, and
each one was tried and removed. What the loader MAY key on is kind 30166,
because NIP-66 fixes two things about that kind and neither is a semantic
guess: the url is in the `d` tag, and the author is a monitor identity. Whose
verdicts count is the
source's `authors`, and **absent means unscoped** — every monitor whose
30166s reached the store, exactly as an absent `authors` means on any NIP-01
filter. There is deliberately no fallback to the router's own signer: it made
the trust anchor rotate with `RELAY_NSEC` (emptying every roster, silently,
until the new identity finished a sweep) and it narrowed the one deployment
that had mirrored a foreign monitor's verdicts on purpose. Admitting is safe
unscoped because everything admitted is still dialled and measured; **the
hold-out read is the asymmetric one and stays author-bound**. `StreamWorld`'s
dead query — records carrying OUR `dead` grade, matched on the label's
namespace and not just its value, since `l` also carries other monitors'
country and ASN labels — is scoped to our signer plus the keys the config
names, because unscoped, one
record from anybody starves a relay out of the candidate set permanently: held
out it is never dialled, never re-measured, and the mark never clears.
`ForeignMonitorTest` pins that quartz's own `deadSet()` is NOT scoped, which is
why the router does its own author-bound read instead of using it. **Admitting
widens, holding out forecloses — do not give them the same default.** The pool
then rotates VISITS — whose unit is a (relay, STREAM) pair, so many streams work
one relay at once over one refcounted socket while each stream sees it in one
state (`VisitPool.VisitKey`) — running per-ask catch-up, the
`negentropySyncThePastSeconds` audit and the heal
drain, and holds live tails, revisit-paced by each relay's recent yield. Every
width is the STREAM's — `visitConcurrency`, `maxLiveConcurrency`,
`maxLiveConcurrency`, `refetchConcurrency`, `negentropyConcurrency` — taken as
permits by `PoolLimits`, and the pool's worker count is their sum
(`VisitPool.workersFor`). The router-wide `visitConcurrency` / `tailBudget` are
refused at parse time. A scan whose select binds
`authors` becomes ONE ASK PER BOUND AUTHOR (`VisitPool.asksOf`) — the
`(relay, provider)` granularity NIP-85's tags already chose, and the band key
that stays valid however many providers join. A retracting stream
(`deleteMissing`) runs its comparison as its audit — `RetractionAudit`, below.

**A visit that aborts leaves its relay UNRECONCILED, and that is why the abort
partition exists.** One unclean ask ends the visit (`refusedOutright`), and the
next visit re-asks from the same first ask — so a relay that refuses
deterministically never completes, however many times the pool visits it.
Measured on `vespa-eventstore-staging`: 92.5% of visits aborting over one
incremental minute, `contentViaOutbox` moving +15 reconciles in fifteen hours
against 1,131 outstanding, and ~90% of those aborts emitting no line at all —
the `!clean` path returned naming neither the relay, the stream, the ask nor the
reason, and the refusals an operator COULD see were the relay client's own
connection logging, undercounting by ~10x. `abortedVisits` said a problem
existed and no instrument in the process could name it, which is the same shape
as `oldestBatchSec` before `StoreCalls`. `VisitAborts` is the answer: eight
counters that partition the total exactly (`abortedAuthRequired`,
`abortedClosed`, `abortedQuiet`, `abortedUnreachable`, `abortedUnpageable`,
`abortedGaveUp`, `abortedFailed`, `abortedBackpressured`), plus one line per
(stream, relay, reason) per half hour carrying the ask and — through
`RelayComplaints` — the sentence the relay refused with. **The sentence is the half that could not be
inferred**: quartz reports how a walk ENDED, which is the right shape for the
one decision it exists for (a `CLOSED` licenses no coverage claim whatever
caused it), and a policy refusal, a rate limit and a filter the relay thinks is
too wide are one ending with three different remedies.

**AND NINETY PERCENT OF THE ABORTS WERE THE MIRROR DESCRIBING ITSELF IN THE
RELAY'S WORDS.** Staging, 2026-09-04, one 8-hour process: `visitsRun` 81,868,
`abortedVisits` 76,485, of which `abortedUnpageable` 46,104 and `abortedQuiet`
23,041 — and 487 of the 1,000 rows on the mirror's page reading `the relay
ignored the paging cursor`, most of them `complete` with a band edge in 2024 or
2025, on relays the monitor had graded `prime`. That reads as the two planes
disagreeing about the relays. They were not: every one of those relays, dialled
by hand with the mirror's exact leg (`kinds` at `since = coveredTo`), answered
inside two seconds with exactly ONE event — the band-edge event, which `since`
includes — and `UnpageableLegLiveProbe` puts the same leg through quartz's own
`fetchAllPages`: `DRAINED`, one downloaded, three pages, every relay. The
relays the page called quiet EOSE in under two seconds too.

What was actually happening is on the same page, one row up: the ingest
processor at `queued: 16520` against `capacity: 16384` for the whole hour of
samples, `eventsPerSec` at 0 between bursts, `ingest.write` calls 116s
outstanding, `bottleneck: ingest`. Every producer hands its events to
`IngestPipeline.submit`, which suspends on a full queue — that is the
backpressure working, and it is deliberate (#167). What nobody had followed
through is HOW that suspension reaches quartz's pager. quartz drains each
socket through ONE consumer coroutine (`BasicOkHttpWebSocket`: an unbounded
channel and `for (message in incomingMessages) out.onMessage(message)`), which
awaits every listener before reading the next frame. So one producer of ours
parked in the queue — a walk's, a tail's, an audit's, the monitor's, on any
subscription of that socket — stops every subscription on the socket: no EOSE,
no events, no CLOSED, no OK to an AUTH. `fetchAllPages` then reports the only
two things it can see. A walk whose own first event went into the parked hook
has `received = 1` and `delivered = 0` (delivered is counted AFTER the hook
returns), and on a first page that is a `break` with `end` still at its
initial `UNPAGEABLE` and `downloaded = 0`. A walk on a socket some OTHER hook
is holding sees nothing at all inside its 30s window: `IDLE`, `downloaded = 0`.
Both satisfy `refusedOutright`, both were filed against the relay, and the
status row marked it `refused` and at fault for as long as the queue stayed
full — which was the life of the process. The visits kept coming at full rate
(one per unit every seven minutes, 96 at a time), each re-dialling a healthy
relay to park one event and blame it.

So `IngestPipeline.parkedOn` counts, per relay, the producers currently
suspended in `submit` — in the pipeline and not at the pool's call sites,
because the pool is not the only producer on a shared socket: `RetractionAudit`
and the monitor's `StreamWorld` hand events to the same queue, and a wrapper
each caller had to remember was a stall the classification could not see. It
counts only a send that actually suspends (the fast path is tried first), so a
hook merely passing through at the instant a walk gives up is not read as a
stall. A walk that comes back refused is re-read at that instant: an `IDLE` or
an `UNPAGEABLE` while a producer of ours is parked on that socket is
`abortedBackpressured` — ours, counted and spoken like the others, and NEVER
written on the relay's row (`VisitAborts.Reason.ours`), nor does it clear what
the relay last said. Only those two endings are re-read: a `CLOSED`, an
`AUTH_REQUIRED` or a `CANNOT_CONNECT` came through the same consumer, so the
consumer was not parked when it was said. Nothing else about the abort changes
— no band, no tail, the leg stays outstanding — because the walk really did not
happen. `VisitPoolBackpressureTest` stages it against a pipeline filled to
capacity and never started, including a producer outside the pool.

**And a visit is not dialled into a queue that cannot take it.** `visit` reads
`IngestPipeline.isFull` before the dial permit and returns if so — skipped, not
queued, exactly as a refused permit is, counted as `visitsHeldByIngest` on the
visits row. A download into a full queue does one thing: parks its first event,
stalls the socket for everyone on it, and comes back `abortedBackpressured`
thirty seconds later, having cost the relay a handshake and a REQ for nothing —
per unit, per revisit, 96 at a time, for as long as the store is behind. The
tails already open stay open: a tail that is not draining is honest
backpressure, and a tail closed is coverage lost. The classification above is
still needed for the queue that fills DURING a visit, which is the common case
on a mirror that is merely busy rather than wedged.

**One thing this does NOT do, and one it found.** It does not fix the ingest:
the store was accepting a 2-worker batch every two minutes with
`lock.ingest.wait` at 25 hours cumulative, which is #167's territory and the
store's. And the monitor's four processors on the same page read `starting` for
the whole 28,591 seconds of that process — the passes wait on
`retireOwnStaleVerdicts`, a paged walk of the graded corpus through the same
store, and `monitor.publish` showed 3,493 of 4,133 calls cancelled — so every
`prime` the streams were selecting on came from an earlier process. The
verdicts were still right; the plane that signs them was not running.

**One of those refusals the pool can take down itself: filter WIDTH.**
`contentViaOutbox` asks for 139 kinds in one filter, and relays that cap width
reject the whole REQ rather than trimming it — nine of them on the same window,
answering `too many kinds in filter: 139`, `too many kinds (max 100)`, or just
`too many kinds in filter`. `FilterWidths` reads that sentence, learns what the
relay takes, and the leg goes back out as chunks of kinds — the catch-up AND the
live tail, since a tail carrying the filter the relay just refused is one that
silently never delivers. **The cap must be the RELAY's**: those three messages
are three different limits and one of them states no number at all, so a
constant is either above somebody's limit or below everybody's. A refusal naming
a number BELOW our ask is adopted; anything else halves, including a relay that
quotes our own 139 back at us — which is the trap, since adopting it would learn
nothing and re-send the identical REQ forever. Every path narrows strictly or
declines to narrow at all, which is what makes the loop terminate;
`MAX_NARROWINGS` bounds what ONE visit pays, and the cap outlives the visit so a
relay that names no limit converges across a handful of them. Splitting on KINDS
and nothing else is what keeps it free of consequences downstream: the band is
per kind, so the chunks of one leg widen one band between them and each carries
evidence for exactly the kinds it walked. **The gate on the sentence is
deliberately narrow** (`too many kinds`, nothing looser): 50 relays on that same
roster refuse with `auth-required:` and 21 are outright blocked, and chunking
THOSE asks would spend three extra round trips per leg, forever, on relays that
will never serve us.

**NIP-42 on upstream reads was ALREADY WIRED, and the A/B is what proves it.**
`PeerClient` attaches quartz's `RelayAuthenticator` whenever `RELAY_NSEC` is set,
and `fetchAllPages` waits out an `auth-required:` refusal on the AUTH's own
verdict rather than on a timeout. `RelayReachLiveProbe` dialled all fifty relays
#185 lists as "unreadable for want of NIP-42" with an EPHEMERAL key: sixteen
served us, and re-run with `-DreachNoAuth=true` over those sixteen, THIRTEEN
fall back to `AUTH_REQUIRED` with nothing delivered. The deployment sets
`RELAY_NSEC`, so it was already reading those thirteen while the log said
`auth-required:` — the sentence is the relay's FIRST word on the connection, not
its last, which is exactly the misreading a log without an instrument produces.
What is left is six relays: four ACCEPT our AUTH and still refuse (`you are not
authorized to perform reqs`), which is `abortedAuthRequired` and wants a key
they allowlist, and two never get an AUTH through. What was missing was the
ability to tell any of that apart from never having answered — an anonymous
deployment printed NOTHING at boot, so "we authenticate and they refuse us" and
"we have no signer" were the same log. `SyncMain` now says which it is either
way.

**…and the same probe says what the width narrowing is actually worth: three of
the nine.** git.cloistr.xyz (cap 17), purplerelay.com (cap 8) and
relay.internationalright-wing.org (cap 35) narrow and then deliver, where before
they could never finish an ask. The other six say `too many kinds` all the way
down and then change their answer — nostria's two discovery relays reach a width
of TWO and say `kind not allowed: 0`, mostro-p2p and whitenoise's two say `kind
not allowed: 1`, hsuite answers `blocked: kinds 0, 1, 5, …`. `FilterWidths.learn`
stops there because the sentence stopped being about width, which is the gate
working and the reason it had to be written against the sentence and not against
the ending. **Two more shapes are sitting in #185's "permanent" lists and are
not permanent**: six `groups.*` relays refuse with `blocked: it's not allowed to
mix metadata kinds with others` — kind 0 in one filter with everything else,
which a split would take down the way a width cap now is — and
mercury-relay.imwald.eu answers `invalid: Invalid kind in filter: '40002' must
be in the range [0, 40000)`, so five kinds this router's own config asks for
(40002, 40100, 45001, 45003, 48106) cost it that relay outright. Both want their
own change.

**No stream declares a transport any more.** `sync` (negentropy / fetch / auto)
chose one for the engine that is gone, and the pool has one shape: page forward
from the band's edge, live-tail, and re-check the past on the two clocks. Which
transport re-checks a given relay is decided per RELAY by the monitor's `nip77`
verdict, not per stream by a config line — the argument that used to justify the
knob is why: it was "a property of the data AND of how the stream asks", and
NIP-85 assertions were the standing example of `fetch` (per-provider, millions
each, no overlap) until the ask narrowed to (relay, provider), where the same
data overlaps almost entirely and `negentropy` became right. A declaration that
inverts when a filter changes shape was never the right place for the answer.

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

**…and a probe pass gates the two transports SEPARATELY** (`DialGate`), because
they are two budgets: the clearnet dispatcher is 1,024 sockets wide and the Tor
one is `SYNC_TOR_MAX_SOCKETS` (32). One shared `Semaphore(dialConcurrency)`
covered both, and on staging at 100 permits the gate ran saturated with `.onion`
— 10% of the candidate urls — holding 60-74% of it, at a median hold of 34-72s
against clearnet's 9-21s. It is the summed budget doing that (`probeIdleMs`, 90s
+ 20s against 20s) and the summed budget is right; charging it to the clearnet
fan-out was not. Worse, 30-40 of those permits had NO SOCKET behind them: past
the 32nd, an onion sits in the Tor dispatcher's queue while still holding a probe
slot a clearnet relay would have finished with in nine seconds — so raising
`dialConcurrency` could not buy Tor throughput, only convert clearnet permits
into Tor queue slots. The Tor gate is now `min(dialConcurrency, maxSockets)`, so
a permit there means a socket, and `dialConcurrency` means dials again. Which
gate a url waits on is `TorTransport.routes` — the same predicate that picks its
OkHttp client, so the gate and the dispatcher cannot disagree, `SYNC_TOR_ALL`
included.

**Sync bands** record the `created_at` span already walked for a
`(stream, filter, relay)` triple, so a re-run asks only outside it. Keyed by the
*whole filter* deliberately: edit a stream's filter and its band is invalidated,
which is the intended way to force a re-walk. Keyed by the *stream* too: two
streams asking one relay the same filter walk it at their own moments and to
their own depths, and neither may resume from the other's claim. A paged fetch
records `complete = false`; only a finished negentropy reconcile records
`complete = true`.

**An EMPTY reconcile records a band; an empty page does not.** The distinction
is evidence, not bookkeeping: a completed reconcile compared both sides and
proved them level, so it can claim coverage through the moment it started even
with nothing to show for it, while an empty page only proves that one window
was empty. The legacy deleteMissing engine used to fall in the crack between
them — it reconciled through `negentropyReconcileIds` (a different quartz
call, because the delete side needs `haveIds`) and recorded nothing on that
branch, so only its paging fallbacks ever wrote a band and only when events
came back. The `assertions` stream therefore charted the providers that
happened to hand over an event and none of the ones it was in sync with, which
is the whole population of a mirror that is keeping up. `RetractionAudit`
records the reconcile band (`reconciledThrough`), and the band's shape is
pinned hermetically in `SyncBandsTest`. The audit's clock is the router's own
`SyncBands.verifiedAt` stamp, advanced by every `reconciledThrough` record and
persisted beside the band — NOT quartz's `fullAt`, which `Band.widen` freezes
on every non-stale merge (it means "last walk from nothing"; read as "last
verified" it made every audit re-fire on each visit — 13 sweeps of one relay
in 40 minutes, measured). Callers fall back to `fullAt` where no stamp exists,
so a fresh ask that DELIVERED pages still runs its first audit one
`negentropySyncThePastSeconds` after its catch-up, and one whose pages came back empty (no
band) audits on its very first visit. `VisitPool.attemptSpacingSeconds` is the
other half: an audit that cannot COMPLETE advances no clock, so attempts
themselves are spaced instead of retried on the revisit floor.

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

**Both state files are read by the ROUTER'S OWN status site**, on its own port,
and charted there as the *Sync coverage* card — see `SyncCoverageReport`. They
used to be read by the relay off the `/var/lib/vespa-relay` mount both
containers share; `SyncStatus` has the account of what that boundary cost. The
router is still the only writer either way, and the files stay on the volume
because they are what a restart reloads.
A third file rides the same mount for a different job, and is the ONE the relay
still reads: `SYNC_MANIFEST_FILE` is
CONFIG, not state — the streams this router runs and the kinds they ask for,
written once at boot (`SyncManifest`) and published as `sync.mirrors` by
`MirrorReport`. It exists because the mirror is a *filtered* subset and nothing
outside this process can know which kinds: a client counting our events for an
author against that author's own relay's total measured 31,118 of 89,485 and
drew *35% mirroring* on a mirror missing nothing, the whole gap being kinds
3/4/5/6/7/1059 that no stream asks for. Scope both sides to `mirrors.kinds` and
the number means something — `web/shared/mirrors.js` is the one consumer, and
the rule it holds is that an unreadable manifest suppresses the question rather
than falling back to the unscoped count. **It reads `sync.data.mirrors`**: every
section of `/stats.json` is wrapped in `StatsRollup.section`'s status envelope
and the payload is `data`. Read one level too high — which it was, on ship — the
member is undefined against every real document, the scope answers null, and the
panel silently stops asking. That failure is CLOSED, which is why nothing looked
wrong: no bad number is published, the right one simply never is. Its fixture had
the same wrong shape, so the tests agreed with it; a fixture that is not the
shape of the thing it stands in for tests the fixture. Do not fold it into the band file:
bands are rewritten every 30 seconds and this changes only on a restart.

**THE ROUTER'S OWN PAGE says what it is DOING.** `SyncProgress` republishes, on
every progress tick, each stream's phase and the disposition of every url its
current cycle took on; `SyncStatus` serves it at `SYNC_STATUS_PORT` (7778).
This was a FOURTH file — `SYNC_PROGRESS_FILE`, read back and re-narrated by the
relay through a `SyncProgressReport` that no longer exists — and the knob is
refused at boot rather than ignored. Three things it fixes, all of which were
unanswerable from the serving side:

- **"Is it running" is answered by the REQUEST.** The document used to carry a
  `writtenAt` heartbeat and the relay turned it into a `staleForSec`, because a
  file says nothing about whether the process writing it still exists — without
  them a mirror down for a day published exactly the card a mirror mid-cycle
  did. A page served by the process it describes needs neither, and neither is
  in the document any more.
- **`urls` and `taken` are a PARTITION.** `discovered = foldedOntoAnother +
  refusedUnstable + excluded + taken`, and the ten outcomes under `taken` sum to
  it exactly, with
  `pending` DERIVED from the other eight so the identity closes mid-fan-out. A production
  document reported 16,752 discovered against 5,323 band-bearing and published
  no account whatever of the ~11,400 in between; every one of them had a
  disposition the router knew at the time. `balanced` is the router's own check;
  the page recomputes it as `accountedFor`, and the two disagreeing localises
  the fault to the read or to the writer.
- **`outcome` is `running`/`completed`/`failed`.** A cycle that aborted at 80%
  and one that finished left the identical trace — both simply stopped saying
  anything.
- **`pending` on a `completed` cycle is usually REAL, and the card said the
  opposite.** A pass ends when its last url is handed out, not when its last
  worker returns, so a completed pass routinely leaves live legs — but
  `pendingLabel` keyed off `outcome` alone and drew *"285 never got a verdict"*
  directly above a line naming three of those 285 downloading at 20k events
  each. Caught on the live run, not in review. It reads `inFlight` now: held
  urls are in flight whatever the outcome says, and "never got a verdict" is
  reserved for the case it was written for — a cycle that stopped with nothing
  running.
- **`inFlight` NAMES the relays a stream has workers on**, longest-held first.
  `pending` read 2 on a production stream that had received two events in eleven
  and a half hours, and nothing in the system recorded which two urls: the count
  is derived by subtraction, a leg still running has earned no band so the
  coverage card cannot draw it, the `SYNC_DIAGNOSE` line fires only for the one
  stream it names, and container stderr rotates inside the hour. `RelayRotation`
  was holding both the whole time and published only their number.
- **`passes` publishes EVERY WALK still running, not only the newest.** A walk
  ends when its last url is handed out and its slowest legs run on past it, so
  the ordinary state of a rotation is the previous pass finishing while the new
  one hands out. `StreamPhases` held ONE cycle slot, so the moment a pass opened
  the previous one stopped being published: its counters went on moving against
  a partition nobody served, and its live legs surfaced only as the new pass's
  `busy`. The slot is a bounded list now — a pass retires itself when its
  `pending` reaches 0 (`Cycle.retired`), capped at `MAX_TRACKED_CYCLES` for the
  pass whose legs never return — and each row carries its own `number`, `owner`
  and partition, checked on its own. `cycle` is still the newest of them, so
  nothing that reads this document had to learn about `passes` to keep working.
  The same change ended the static/dynamic ARBITRATION: one stream name can
  carry both `urls` and `relaySource`, and where the loser used to publish
  nothing at all, both halves now get a row and `owner` says which is which.

**THE PHASE PUBLISHES ITS NUMBERS, not only its word.** `phase` said
`fetching` and `phaseForSec` said how long, while the phase OBJECT behind them
carried everything an operator wanted — and that reached a container's stderr
and stopped there. `StreamPhases.Detail` is those numbers, flat beside the word:
`returned` (legs that CAME BACK, denominator `urls.taken`), `running` against
`transferring` (the admission gate is 512 wide where the transfer pool is 100 —
which is why `pending` is five thousand and not a fault), `fraction`, `etaSec`,
`reached`, `collected`/`collectedTotal`, `slotsFree`/`slotsNeeded`, `nextInSec`,
`retryInSec` and `reason`. Only what a phase can answer is written.

**`rotating` had no numbers at all until it was given its own two.** A
visit-mode stream has no pass, no fraction and no cycle — its world is the
monitor's verdicts and its engine is the pool — so every member above is absent
for it and the card drew `rotating for 58m` and nothing else, identically for a
stream riding four hundred relays and for one riding none. It publishes `roster`
and `tails` now, the pool's own words for both quantities. It used to publish
the roster size as `running`, which is defined as "relays this stream has a
WORKER on right now" and is the opposite of what a roster is: the pool visits a
handful at a time and most of the list sits between visits. **An empty roster is
the reading worth having** — that is a stream waiting on the fitness pass to
sign its first `prime`, which is the state that looked exactly like a busy
one, and the card says so in the fault tone.

**`reached` is the one that matters**, and it wants drawing on the coverage
axis rather than in a sentence. It is the oldest `created_at` a stream's paged
walks have got to: on an unbounded walk `fraction` rounds to zero for hours
while this date moves every page, so it is the only live evidence a deep walk is
going anywhere. It is the running counterpart of a band's floor — same
quantity, same axis — which is the one join on this card that would make its
two halves one picture.

**Two members were checked and deliberately NOT published.** `Fetching.total`
is the cleaned relay list and `CycleTally.taken` is that same set by
construction; `Fetching.events` is incremented from the same line as
`cycle.received`. Both were already in the document under another name. Check
the source before adding a member here — the same check killed a
`relayDistribution` join (it counts how many stored NIP-65 lists NAME a relay,
which is corpus popularity, not events this mirror pulled from it) and
established that there is no per-relay provenance in the store at all: only a
live leg knows its own yield.

**Three facts about the PROCESS, not about a stream.** `fatals` is published
including zero — a `VirtualMachineError` kills whichever thread allocates next
and is caught by nobody, so the router carries on looking merely quiet, and four
once passed unnoticed. `lostToStore` is the only counter in this system that
means data loss (events that passed every check and could not be written), so
the card draws it alone, loud, and only when non-zero. And `rejections` splits
the largest number here into the reasons that make it readable: 20M rejected
against 1.6M accepted looks like damage and is a mirror being offered the same
event once per relay holding it.

**A held url names the walk that handed it out.** `RelayRotation.Hold` stamped
the claim time, the transfer clock, the events and the quiet — but not the pass,
so with two walks live nothing could say which one a leg belonged to. It does
now, and `inFlight` rows carry `pass`.

**…and a FIFTH member says what is running that is not a stream at all.**
`processors` (`Processors`, published under `sync.progress.processors` and drawn
as *the pipeline* on the coverage card) is the other half of "what is this
router doing". A stream is the part an operator CONFIGURED; these run beside
them with nothing configured about them, and until they were published the only
trace any of them left was a stderr line on a container whose logs rotate inside
the hour.

The `visits` row is the exception, and what it DRAWS is not what it publishes:
`roster`, `rosterVisits`, `awaitingVisit`, `visiting` and `liveHeld` are the
five members the card's pool summary partitions, so the row drew them a second
time as a sentence, in a second vocabulary (`24 visiting` against `24 with a
worker now`), a few hundred pixels below the line that partitions them. The
summary owns the split — it states the arithmetic they are parts of, and it can
be cut per stream — and the row keeps what nothing else counts, which is what
the pool has DONE since boot. All five are still published, and the terms test
still pins them; `processorFact` is where the choice lives:

| processor | what it is | how far along it is |
|---|---|---|
| `aliasSource` | `StreamWorld.candidates` — walks the store for every url the relay lists name, drops what an operator excluded and what a signed `dead` record holds out, and hands the rest to the three passes | `attempted` of `toProbe` in `source`s (one configured relay-list block each), then `sourced`/`excluded`/`heldOutDead`/`candidates`/`recordedOnly` |
| `aliasFold` | `AliasFolding.measure` — fingerprints one host's urls against each other and signs `same-as` | `outstanding` of `subjects`, plus `undecided` by reason |
| `stability` | `ConsistencyPass.measure` — asks one relay the same filter twice and refuses the ones that answer differently | same, and it reaches `outstanding = 0` for most of its monthly TTL |
| `ingest` | `IngestPipeline` | `queued` against `capacity`, `accepted`, `rejected`, and `inBatch` of `workers` with `oldestBatchSec` — what tells a full queue apart from a stopped one |
| `heal` | `HealQueue` + `Healer` | `queued`, `pushed` — registered only where a stream opted in |
| `upstreamPush` | `UpstreamPush` | `pushed` |

**Are the fold and NIP-66 the same thing? The RECORDS are; the processors are
not.** The passes write tags onto the same addressable kind 30166 record per url
— `same-as` from the fold, `self-consistent` from the stability gate, `s` /
`pageable` / `compliant` / `nip77` from the fitness pass — which is why
`RelayVerdictRecord.edit` is a read-modify-write and why `AliasMonitor` runs its
passes SEQUENTIALLY (two writers on one record drop whichever tag was written
between the other's read and its store, silently, since the result is still a
valid signed record that simply says less). The verdicts panel on `/stats.html`
is where one url's whole record is read back, and it exists because that merge
is only pinnable in isolation by `RelayVerdictRecordTest`.

**The gate is stream-level and it is an ordinary source.** `gatedBy` sits beside
`exclude` and takes the same `{ select, filter, maxAgeSeconds }` entries a
`relaySource` does; every source's discovery is intersected with the union of
what they find. Beside `exclude` for a reason: `exclude` says which urls are
forbidden however many sources name them, `gatedBy` says which are permitted
however many sources found them, and **neither is a property of HOW a url was
discovered** — which is all a source describes. It replaced per-source
`certified = {}`, which could only ever mean "a fresh `prime`" and — because
the read behind it enforced our own rules epoch — could only ever mean OUR
monitor's, whatever identity the block named. Both of those are gone, so a
third-party NIP-66 monitor works as a gate, and so does something that is not a
monitor at all.

**A `filter { }` block IS a NIP-01 filter, so its `ids` and `authors` are raw
hex.** Copy one out of a REQ and it works here; paste one from here into a REQ
and it works there. Bech32 belongs to the settings that are OURS to define —
`RELAY_NSEC`, `RELAY_ADMIN_PUBKEYS`, `ALLOW_PUBKEYS` — where a checksummed
spelling is a free guard on a value a human typed. Inside the protocol's own
object it is a category error, so `RouterConfigLoader.hexKeys` refuses an
`npub1…` rather than decoding it: accepting it would make the block "mostly
NIP-01", the worst of both. The guard that survives is shape — 64 characters of
hex, uppercase lowercased — because NIP-01 matches these exactly and a malformed
one selects nothing and says nothing.

**Two knobs are not filter fields, and both exist because a config outlives the
day it was written.** `maxAgeSeconds` is the relative form of `since`, whose
absolute instant a file cannot hold; writing both is refused, since the relative
one wins and the other would be read by a human and by nothing else. It defaults
to UNBOUNDED and nothing infers otherwise: a NIP-65 relay list is replaceable and
timeless, so one published in 2023 that nobody revised says what its author
still means, while a verdict nobody has re-taken for a month is how a dead relay
stays in the fan-out for a month — and which of those a given filter is asking
for is the operator's knowledge, not a thing to read off a kind or a tag.
`RelaySource.DEFAULT_MAX_AGE_SECONDS` is a documented number to reach for, and
the shipped config writes it explicitly on every verdict read. `refreshSeconds` is
per source for the same kind of reason — see the cadence note below.

**An ungated stream is a warning, not a parse error.** The old rule — a scan
needs `certified`/`gatedBy` unless every source is a verdict query — could only
be stated by deciding which tag and which value constitute a vouching, which is
exactly the operator's call. A filter that gates and a filter that scans are
indistinguishable from here, so the loader says which streams have no `gatedBy`
at boot and the config is the authority. **This is a deliberate safety
downgrade**: it was a hard error and is now a line on stderr, bought in exchange
for gates the router does not have to understand.

**The fast lane runs the STABILITY GATE and then FITNESS, and it used to run
fitness alone.** The lane's whole reason to exist is that `prime` is the
admission decision for every visit-mode stream and the sweep is on a six-hour
clock, so it grades urls named since its last look straight away. But the
stability gate rode the sweep, so a url could be graded `prime` — and admitted
to every roster — before anything had asked whether it answers the same question
twice. `Verdict.INCONSISTENT` exists because such a relay *"would poison bands
and coverage"*, and the window was up to a whole `sweepSeconds` of one doing
exactly that. The gate is per-url (two REQs, compared against nothing but that
url's own second answer), so the lane can run it; the FOLD cannot go here,
because it elects a leader out of a host's whole group and a since-bound set
holds whichever urls of that host were named in the last tick.

It costs no extra dials over a sweep: a stability verdict stands for
`RelayVerdictRecord.DEFAULT_TTL_SECONDS`, so a url the lane measures is one the
sweep then skips — the work moves earlier, it does not repeat. Both halves are
pinned in `FastLaneTest`, including the control that shows the shuffler passing
the fitness ladder on its own, which is why the lane could not see it before.

**Cheap sources must be allowed to run far more often than expensive ones.** A
kind-30166 read is one indexed query bounded by `maxAgeSeconds`; a 10002 scan
walks a corpus. Cached alike at the stream's six-hour default, the cheap one
would hold a newly-certified relay out of the fan-out for six hours — against a
monitor fast lane that verdicts a new url in two minutes and a documented promise
that it joins on its first `prime`. So `refreshSeconds` is settable per
source and the shipped config sets 120 on its verdict queries. This used to be
implicit: verdict sources bypassed the scan cache entirely because they were a
different type, and collapsing the types without stating the cadence would have
turned "minutes" into "six hours" silently.

**The monitor runs when there are SOURCES, not when there are discovery
streams.** `SyncEngine.hasMonitorSources` is the gate — it reads
`discoveryStreams` OR `monitor { sources }` — and it used to read the first
alone. A deployment that took the block's offer all the way (static `urls` on
every stream, all candidates arriving through `monitor { sources }`) therefore
never called `aliasMonitor.start()`: `StreamWorld` unioned the block's urls
correctly and nothing ever ran over them — no fold, no stability gate, no
fitness, not one `prime` signed, and four rows reading `off` for the life of
the process, which is also what those rows correctly say on the static config
the gate was written for. One rule, read by the start gate and by the `off`
rows both, because rows marked `off` under a monitor that is running is the
silent half of them disagreeing. `MonitorGateTest` puts the deployment that
broke in front of it.

**The fast lane's hold-out read is bounded by its own subject.**
`RelayDiscovery.undialable` takes `among`: null for a sweep, which is about to
walk the corpus and wants the whole dead set, and the lane's own handful
otherwise. Unbounded there, a lane tick materialized every `dead` record in the
store — five figures on a discovered corpus, and one of the "no limit" reads the
`maxHits` cap rejects on multi-node — every `fastLaneSeconds`, thirty times an
hour, to answer a question about a dozen urls. It is `#d`-chunked like
`RelayVerdictRecord.load` now, and the lane derives BEFORE it reads, so a tick
that found nothing (most of them) costs no read at all. Same answer either way:
the hold-out only ever applied to the urls the lane found.

**The rules epoch is retracted, not re-checked.** `FitnessPass.retireStaleEpochs`
runs at boot — the only moment `FITNESS_EPOCH` can have changed, since the
constant is a source edit and a source edit is a restart — and strips `s` /
`pageable` / `nip77` from every record of ours written under older rules. Those
urls then read as unmeasured, which is the state that gets a candidate
re-measured on the next sweep and correctly stops admitting one that has left
every relay list. It was a check on every READ, which put our private versioning
scheme in front of everybody's records: a standard NIP-66 record carries no such
element, so no foreign verdict could pass however the config was written. **A
claim you no longer stand behind is yours to withdraw; do not ask every reader
to know why it is worthless.**

**There is no passive writer any more.** quartz's `RelayMonitor` used to be
attached to the sync client as a connection listener, signing a 30166 for every
socket the fan-out opened. It was a second publisher of facts the passes already
state, and — since every writer edits the same record — it rewrote `created_at`
on a 5-minute flush for every relay we were actively syncing. Removing it is what
makes the record's own clock mean "when the monitor last checked this relay",
which is what every other NIP-66 consumer reads it as, and what lets a stream
bound verdict freshness with a plain NIP-01 `since` instead of a private
convention. The passes are the only writers; the fitness pass is the only writer
of `s`, and it is where the `dead` verdict that holds a url out of the candidate
set now comes from.

Two rules the processor rows follow, both the same ones the rest of this
document does. **A processor that is not registered is one this router does not
run** — a deployment with no signer has no fold and no monitor at all — so an
absent row is a fact rather than missing data, and a zeroed one would say the
opposite. And **the gauges are read through a supplier at snapshot time**, never
pushed: they are live atomics owned by the component, and a copy kept in step by
hand is the shape that produces a report disagreeing with the thing it reports
on. NOTHING RE-DERIVES THESE ANY MORE, and the allowlist that used to went with
the boundary. `SyncProgressReport` rebuilt the document member by member on the
RELAY's side, which was right while the relay was reading another process's
file: a hand-edited or half-migrated one must not put arbitrary JSON into a page
served under the relay's name. The mirror serves its own page, so the writer and
the reader are one object on one heap, and ~700 lines of re-copying our own
members are gone. What survived is `SyncVocabulary`, pinned in both directions:
a member with no term is a number a reader needs the source to understand, and
now that nothing filters on the way out, the pin is a stronger claim than it was.

**A CAP IS FOR A LIST THE NETWORK CAN GROW, and for nothing else.** The rollup
bounds `foldedOnto` and the undecided reasons because discovery decides how long
those get; it bounds NOTHING whose length is decided by our own source — the
processor rows (a registration in `SyncEngine`), a processor's stream rows (a
line in `router.conf`), the in-flight rows (a worker, so `visitConcurrency`).
Capping those only picks which rows an operator is not shown, and the processor
one had the worst version of that: `splitProcessors` deliberately draws a
processor name the page has not been taught rather than dropping it, because
"dropping a row to keep a card tidy is how a new job runs unwatched" — and a cap
in the rollup dropped that row before the page could apply the rule, silently,
since neither had an `omitted` to disclose it with. The invariant is now held on
both sides rather than defended on one and undermined on the other.

**An audit of the monitor and pool planes turned up five things worth knowing,
and the first is a second route to #139's symptom.** Recorded because four of
them are invisible from the outside and one had a test asserting it.

- **A candidate set of ONE was never graded at all.** `AliasMonitor` gated its
  whole work list on `urls.size < 2`, written when this class ran the fold and
  only the fold — one url cannot be held up against another. The stability gate
  and the fitness pass joined the list since and neither compares urls to
  anything, so a router discovering exactly one relay never wrote a `prime`
  grade for it and every visit-mode stream had a permanently empty roster. The
  fold still refuses a world of one, inside `AliasFolding.measure`, against the
  world it actually assembles. The fast lane does not rescue this: it only sees
  urls named SINCE its last look, so a url present at boot is never picked up.
- **The fast lane was doing a store-wide read every 120 seconds** — the one
  thing its own comment says it exists not to do. Found independently by this
  audit and by the work that landed as `79c182e`, which is the fix in the tree:
  see *The fast lane's hold-out read is bounded by its own subject* above. The
  audit's own version was dropped at the merge in favour of it.
- **Both boot retractions walked our own corpus in one unbounded query**, inside
  the `runBlocking` the roster's first rebuild waits on. Neither can ask its
  question in a filter — the epoch and the legacy `s` tag are decided per record
  — so both read every graded record; they page now. The call site's comment
  claimed "one indexed query returning nothing", which was half right: indexed,
  and it returns everything.
- **…and they shared one `runCatching`**, so a throw in the epoch walk silently
  skipped the legacy one and reported it under the wrong name. Two guards now.
- **A revisit timer outlived the tail that sized it.** The delay is read once,
  at arming, so a url armed while tailed carries `REVISIT_TAILED_MS` (30 min)
  against `REVISIT_UNTAILED_MS` (5 min). Eviction requeues promptly, but the
  visit after it found the old timer standing in `armed` and armed nothing — so
  the relay that had just lost its live feed waited out the cadence it earned
  while it still had one. `dropTail` disarms; the "exactly one timer" rule is
  intact because disarming removes one rather than adding a second.
- **`parked` had a check-then-act race.** A worker's failed `inFlight.add` and
  its `parked.add` were two steps, as were the finishing visit's two removes, so
  the prompt requeue this class promises as its second invariant could be
  downgraded to a timer wait and the leftover park bought one spurious
  back-to-back visit later. Both blocks are pure collection work and neither
  suspends, so one monitor closes it.

**The rule was audited against every cap in the two rollups, and three failed
it.** They are gone; what is left is the list of caps that earn their keep, and
a new one has to argue its way onto it:

| cap | length decided by | verdict |
|---|---|---|
| held urls per processor | `dialConcurrency` — a job, our config | REMOVED |
| `undecided` reasons | two enums in our source (5 + 13) | REMOVED |
| `passes` on the relay side | `StreamPhases.MAX_TRACKED_CYCLES` — our source | REMOVED |
| hosts named under a reason (100) | the host universe — discovery | kept |
| `foldedOnto` survivors and their sample urls | discovery | kept |
| ingest rejection reasons | store error strings, already overflow-bucketed at source | kept |

Two things the audit made explicit. **A cap on a list our own source bounds is
not a safety ceiling, it is an editorial one**, and the editorial cut belongs at
the display layer where it can be widened without a redeploy of the writer —
`sync.js` still draws a few held urls with `+N more`, and the record it draws
from is whole. **And a re-cap on the relay side of a list the router already cut
is worse than either**, because it is a number over here that has to be kept in
step with a number over there in order to go on doing nothing; the undecided one
was one short twice, at six and at eight, both times because a reason list grew
and the number did not. `omitted` survives every removal as the schema's promise
— absent cannot be told from "nothing dropped".

`AliasMonitor.runPass` runs ONE PASS over every stream rather than one stream
over every pass, which is what makes a pass a clocked unit — `lastPassAt`,
`lastPassSec` and the `measuring` phase describe the whole pass instead of
whichever stream was last. It strengthens the ordering the passes already
depended on: the fold now finishes on every stream before the stability gate
measures anything, so no stability walk is spent on a url another stream's fold
was about to remove.

**A PASS IN FLIGHT PUBLISHES WHERE IT HAS GOT TO** — `measuring: {unit,
attempted, toProbe, etaSec, quietForSec}`, beside `inFlight` naming the urls it
is holding, and those two are the only members of a processor row that
move between passes; every other one describes the pass that ENDED. It exists
because the row went blind exactly when it became interesting: a stability pass
runs for hours, `lastPassSec` belongs to the pass before it, and a SWEEP unsets
`nextInSec` while it runs (a pass takes as long as it takes, so nothing has
computed when the next one is due — a fast-lane pass carries both, and both are
true). What was left was the word
`measuring` with no size, no position and no end. Each pass declares its set
with `Processors.Handle.measuring` the moment it derives it and ticks a unit off
with `attempted` from the job's completion — from `invokeOnCompletion`, not from
the bottom of the body, because most of a pass over a discovered corpus ends in
an early return and a position that only counted successes would sit still while
the pass worked hardest. The `unit` is carried because the passes do not count
the same thing: the gate and the fitness pass decide a `url`, the fold decides a
`host` and dials every url of one to do it. `toProbe` is the pass's OWN set, not
`candidates` — both passes drop everything already carrying a verdict — and
`etaSec` is withheld until a unit has landed, for the reason the paging ETA is
remembered for. The fold walks its groups widest-first, so its estimate reads
long and improves, which is the safe direction for a number someone uses to
decide whether to wait.

`quietForSec` is the member that says the pass has STOPPED, and it is there
because `etaSec` cannot: a pass whose last url has wedged reports `~0s left`,
which is honest arithmetic on a rate that has gone to zero and is the same `0` a
pass one url from done reports. A production fitness pass read `12,373 of
12,374, ~0s left` for 74 minutes and every number on the row agreed with every
other one — see the monitor-plane deadline section above for what was holding
it, and `inFlight` for which url.

**Three words, and they are not synonyms.** "Done" covered all three, and the
least meaningful of them was the one being read as progress:

| word | means | where |
|---|---|---|
| **returned** | a fan-out leg started and CAME BACK — including unreachable, capped, out of budget | `fetching 16747/16752 relay(s) returned` |
| **settled** | nothing outstanding below the span this stream walked here | `complete` on a band, `reconciled` on a group |
| **evidence** | the span in which EVERY kind in the filter has produced an event | `everyKindMin`/`everyKindMax` |

**A DURATION IS NOT A DIAGNOSIS — each in-flight row carries four numbers.** A
relay with a real backlog and a walk that cannot terminate are both "held for
hours", and they want opposite responses. `heldForSec` runs from the rotation's
CLAIM (before the strike checks, the TCP pre-probe and the queue for a slot);
`transferringForSec` runs from the socket and is ABSENT when there is no socket,
which is where most of a fan-out's workers are at any instant; `events` is what
that leg has received, counted as they ARRIVE rather than when the leg returns —
the leg worth watching is the one that has not returned, so a boundary counter
reports zero for exactly as long as the fault lasts; and `quietForSec` is the one
that decides, running from the last event or from the claim if none ever came.
The measured shapes, from a live in-flight report probe against real relays:
directory.yabu.me streamed **84,359 events in 42s** (~2,000/s) with `quietForSec`
pinned at 0 for the whole run — a real backlog, slot well spent — while the
purplepag.es loop is `transferring` for hours with `events` frozen and
`quietForSec` climbing, because quartz's own matcher discards those pages before
our callback, so the leg reads as genuinely silent. That is the true finding
rather than a missing one. `events` was checked against `fetchAllPages`'
own `downloaded` on every leg that finished and agreed exactly (200/200, 0/0).

**`doing` names the JOB and then the TRANSPORT, and neither implies the other.**
`catching up (paging)` is what is new since this relay's last pass; `negentropy
sync of the past` is the whole past re-checked on the stream's
`negentropySyncThePastSeconds` clock, whose purpose is to find what no catch-up ever saw; and
`negentropy sync of the provider's own records` is the retraction comparison,
the same clock and the same full-past sweep.

**THE BANDS CLOCK THAT SWEEP; THEY DO NOT BOUND IT**, and nine places in this
repo had drifted into saying "reconcile the covered past" — a phrase that reads
as the opposite. The paging pools take their legs from `SyncBands.legs`, so
outside the band is `catching-up` and inside it is `re-fetching`; the audit
hands `ask.filter` to `NegentropyPager.sweep` VERBATIM, and the range is
`filter.since ?: PLAUSIBLE_FLOOR` up to `now - slackSeconds`. Nothing subtracts
what is already covered, and it must not: a relay that back-filled behind a
catch-up leaves the band claiming that ground and the store missing the events,
so a sweep narrowed to what the band does not cover could never find them —
which is the pass's entire reason to exist. The bands decide only WHEN, through
`auditDueAt` off the last `reconciledThrough` stamp, and an ask with no stamp is
`AuditClock.NEVER_AUDITED`, which is ALWAYS due — so a relay's first audit
reconciles a history the bands cover none of. What makes the full range
affordable every time is that a reconcile compares ID SETS: a range that already
agrees costs round trips and no events. Negentropy is NOT a synonym for the
audit: the sweep pages any window a peer will not reconcile, and a static
stream's whole backfill goes either way on `sync` — so "reconciling" alone never
told a reader which of the two jobs was running, which is the half they were
asking about. The `negentropyRunning` gauge counts rows by the two stages, so the
strings live in `VisitPool`'s companion and a reword goes through it or silently
zeroes the gauge. The page never groups on them either — the stable `pool` word
beside the sentence is what the four tables are cut by, exactly so a reword
cannot move a row.

**Verified end to end against a real Vespa and real relays**, not only by
probe: `docker compose` with the schema deployed, a `profileViaOutbox` stream
discovering 579 urls off stored 10002s, and 348,770 events mirrored. What the
live `/stats.json` showed, and what each thing confirms:

| observed | confirms |
|---|---|
| `indexers` (static) publishes NO `inFlight` | a stream with no rotation makes no claim, rather than claiming nothing is running |
| `inFlight: 20 named, 279 omitted`, and `pending` = 299 | `omitted` closed the arithmetic exactly. THE CAP IS GONE — that reading is from the legacy fan-out, whose admission gate ran far wider than its transfer pool; under the pool a row is a worker, so the list is published whole and `omitted` reads 0 |
| four rows `transferring` with `events` climbing and `quiet 0s`; the rest `transferring` ABSENT, `events 0` | absent means *queued for a slot*, and here it was our own pool being the constraint — 128 workers against 4 slots |
| ties broken by url, `ws://` before `wss://` | the ordering is total, so two rollups of one state diff cleanly |

**The pool gate only runs BETWEEN passes, so a stream whose walk cannot finish
never reaches it** — worth knowing before reading a `holding` that never
appears. Measured: at `concurrency = 4` the admission gate is 128 (the floor in
`admissionWidth`), so 128 workers queue for 4 transfer slots and the walk was
still handing out at 63/237 after five minutes. `awaitPoolHeadroom` is never
called because `runPass` never returns. That is not a fault — nothing is
starting a redundant pass either, which is what the gate is for — but it means
the gate earns its keep at the ratios the config actually uses. At
`concurrency = 30` the walk handed out all 579 urls at once and the gate engaged
immediately, holding for 11 minutes with `0/15 slots free` while the pool ran
productively.

**`transferringForSec` is the transfer SLOT, not the socket, and the probe is
what caught the difference.** A url that could not be connected to at all
reported `transferring 0s` for its whole life and ended `CANNOT_CONNECT`: the
websocket connect happens INSIDE the span the transfer clock wraps.
So ABSENT means "not admitted to the pool" — in the guards, or queued behind
other legs — and absent with a long `heldForSec` is a statement about OUR pool
being saturated, not about their server. The docs said the opposite before the
probe ran, which is the whole argument for running one: the plumbing was right
and the description was not, and no hermetic test can tell those apart.

**The legacy engine's next pass would not start until half the transfer pool
was free** (`poolHeadroom`, `awaitPoolHeadroom` — gone with it, kept here for
the failure mode). Passes overlap by design, but
a pass started against a COMMITTED pool is not parallelism: it re-derives the
relay list, opens a tally, walks the whole list and hands every url to a slot
that does not exist. At `recycleSeconds = 1` against `concurrency = 100` that is
a pass a second producing log lines and a `taken` count nobody can act on. The
wait is its own phase — `holding`, with an elapsed clock and the url of the leg
holding the slots — never a longer `Idle`, because "nothing to do until the
timer" and "the timer fired and we are declining" are different states. **It is a
real change in failure mode**: a stream whose legs never return now stops passing
rather than passing uselessly, which is exactly why `holding` names the culprit
instead of only counting it.

**Two not-dialled states are not one state.** `HostStrikes.isDead` was true for
two reasons with OPPOSITE retry policies, reported as one number: a
`hostStruckOut` url is dialled again on the very next cycle (a strike is
cycle-local, nothing persists), while a `knownDead` one waits out a signed NIP-66
`dead` verdict's TTL — 24h, `StreamWorld.DEAD_TTL_SECONDS`
— or its host delivering something. `whyDead` returns which; do not collapse
them again.

**The fold publishes WHICH urls, not only how many.** `foldedOnto` groups them by
the survivor that absorbed them, bounded to the biggest few with `omitted` naming
what was left out — the full list is thousands of urls on a document fetched
every poll, the same reason a discovery filter's `authors` never reaches
`/stats.json`. The complete per-url verdict stays where it was earned: a signed
30166 `same-as` record in this store. Note that a folded url has no row in the
coverage section at all — `SyncBands.dropFolded` removes it — so the count and
`foldedOnto` are the only places it appears.

And a fourth thing none of them is: **holdings**. A band is walk state — where a
stream has asked — not what the store contains. A band floor newer than the
oldest stored event of that kind is ordinary; another stream put those events
there. `SyncVocabulary` ships all of this inside `/stats.json` as `sync.terms`,
because a definition that lives only in a KDoc is one the reader of the JSON
does not have.

**A finished paged walk stays in `PagingProgress`'s denominator.** It used to be
removed, which made the percentage run BACKWARDS — every relay that drained left
the numerator and the denominator together, so a stream whose fast relays
finished first fell from 60% to 20% while strictly gaining ground. `finish` now
retains the walk and `reset` clears it at the next cycle boundary; a walk that
DRAINED counts 1.0 and anything else counts the share it really reached, which
is also the only place a failed leg and a successful one stop looking alike.
`reset` deletes only FINISHED walks, because one stream name can carry both
`urls` and `relaySource` and a blanket clear would delete a live walk out from
under the other half — every later `mark` and `finish` on a removed key
is silently a no-op.
Three traps if you touch either format: both files nest **stream → filter →
relay**, and the sweep file's filter also strips `since`/`until`/`limit` (time is
what a sweep varies) while the band file's keeps them, so joining the two still
means reducing both to a common shape; a band's `min`/`max` are the outer edges
across every kind — the card draws the per-kind *intersection* on top, which is
the multi-kind bug below made visible rather than charted as coverage; and **one
stream is many filters**. A `relaySource` whose select binds `authors` becomes
one ask PER BOUND AUTHOR per discovered relay (`VisitPool.asksOf`, via
`DiscoveredRelay.narrowed`), so a stream configured as one filter reaches the
file as thousands of them, all under its one name. The report groups by that name,
publishes the members every leg *agrees on* (never `authors`/`ids`/tag values —
they are named in `narrowedBy` instead), and merges the legs that land on one
relay (edges union, `complete` ANDs).

**A file written before the format nested carries flat keys that name no
stream, and the two writers now treat them differently.** `SweepState` holds
them aside and hands each to the first stream that asks for that (relay,
filter) — the stream that wrote it — writing back whatever is still unclaimed,
flat, so a restart does not lose it. `SyncBands` **prunes them on load**: that
claim needs a live stream to dial the url, and the keys still in the file were
the ones no stream ever asks about — 2,624 of 2,628 top-level keys on staging,
2.5MB of a 13.8MB file, every one a subpath alias the fold had taken out of the
fan-out, none written to since the day the format nested. They could not drain,
so they sat there being charted as three unnamed groups with `reconciled=0` and
a frozen `max`, which reads as streams failing to reconcile. Pruning costs one
re-walk per key a stream would still have claimed. What is left to delete is
`SweepState`'s shim and `SyncCoverageReport`'s — grep `MIGRATION SHIM`, and
note the report's BAND branch is now only for the window where the relay reads
a file a router still on the older build wrote. They go together once every
deployment has booted once on a build that writes the nested shape.

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
second no window size will fit through the hook this class passes it. Engaged by every
audit the pool runs, and by any reconcile whose own count passes
`SYNC_NEG_PAGE_TARGET`.

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

**A kind that never returns an event never closes its leg — so asking for one
costs a full-past walk on EVERY visit.** A span is earned by observing events,
and an empty walk records no band at all (`an empty fetch records nothing`:
recording it would fabricate coverage). Both rules are right on their own, and
together they mean a kind the relay does not serve stays outstanding forever.
Measured through `SyncBands` with the `assertions` ask shape — filter
`[0, 10002, 30382]`, authors bound to one provider — after a DRAINED walk that
saw only 30382s:

```
kinds=[0, 10002]  since=null      until=null    ← the whole past, every visit
kinds=[30382]     since=1787068103 until=null   ← correctly narrowed
```

The drain is evidence the walk reached the bottom for every kind it asked for,
and it is thrown away because no kind produced an event to hang a span on. Two
things follow. Fixing it properly is upstream (`record` would have to be told
which kinds the leg ASKED for — the call here keys the band by the whole ask,
not by the leg). What this repo did instead is stop asking: a stream asks for
exactly the kinds its upstream owns, and the one thing that had kept kind 0 and
10002 in the `assertions` filter — the retraction cascade — is gone with them.
Before assuming a stream is re-walking history for a reason, check whether one
of its kinds simply never answers.

Two more ways a paged leg reaches back that are NOT this one, worth separating
before theorising: a `reconciledThrough` band records against the filter the
reconcile actually COMPARED, so a retraction audit whose `ownedKinds` are a
strict subset of the filter's stamps a different band key than the catch-up's
ask reads, and cannot narrow it — which is a second reason to ask only for what
the audit compares. Where the two coincide, as they now do on `assertions`
(`filter.kinds == ownedKinds`, so `ownedAskOf` is identity), the daily reconcile
closes the catch-up's own older leg and the deep re-walk stops. And every
`refetchThePastSeconds` — per stream and ONLY per stream, unset meaning never,
since nothing this expensive runs on a period nobody chose (the env names that
used to carry it are refused at boot) —
a band is STALE and `legs()` hands back the whole filter, floored on the
wire to `PLAUSIBLE_FLOOR` (2020-01-01). `isStale` reads `fullAt`, which
`Band.widen` freezes on every non-stale merge, so it means "last walk from
nothing" and a stale band is REPLACED rather than widened — a completed
reconcile does NOT reset it, which is why an audited stream still expires on
this clock. The catch-up runs before the audit inside a visit, so a stream
whose two periods coincide re-pages its whole history and then reconciles the
same ground; the example runs the outbox streams monthly against their weekly
audit, and the loader warns when a period sits at or under its own
`negentropySyncThePastSeconds`.

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

**And the `IDLE` in that table is not what it looks like — read this before
theorising about purplepag.es again.** "It stops EOSEing after a while" is the
natural reading and it is wrong. purplepag.es EOSEs *every single page*, promptly,
for as long as you keep asking; it is our walk that cannot stop. The mechanism,
measured end to end at the wire (`onepage.py`/`pagewalk.py` reproductions, then
`RealRelayDrainProbe.reportWhetherAnUnflooredLegCanEndAtAll` through the real
quartz client):

1. `fetchAllPages` cursors newest-first by `until = the oldest created_at it saw`.
2. purplepag.es holds **twelve kind 10002 events stamped `created_at = 0`**. They
   are not below some floor waiting to be reached — a single page anywhere under
   ~`1.6e9` carries them — so the cursor lands on `0` the moment the walk gets
   that deep.
3. purplepag.es treats **`until <= 0` as no `until` at all** and answers with its
   500 NEWEST events. (`{"kinds":[0,10002],"until":0}` → `max created_at`
   1800000000. Same for `-1`, `-100000`.)
4. Quartz checks `until` client-side (`FilterMatcher`), so none of those 500
   matches: the page *received* 500 and *delivered* 0, which is quartz's
   "boundary second is stuck" case, so it steps strictly past — `until = -1`.
5. Repeat. `until` marches one second further negative per page, **forever**:
   ~5.5 pages/s, 500 events pulled and discarded on each, EOSE on every one.

Measured from a cold start, the full `indexers` leg on purplepag.es downloads
**1,490,010 events in ~10.8 minutes** — the real corpus, arriving fine — and then
spends the rest of the process's life in that loop. `fetchAllPages` never returns,
so the band is never recorded, so the next boot walks all 1.49M again. The stream
shows `Fetching` with a frozen event count (the loop delivers nothing, so nothing
ticks) while the socket is saturated. It is not anti-DDoS: ~1,000 pages produced
no `NOTICE`, no `CLOSED`, no throttling. The other four indexers hold nothing at
all below `1.5e9` and drain in one page, which is why this is purplepag.es-only.

The fix is `flooredForPaging()` — every filter handed to `fetchAllPages` carries
`since = PLAUSIBLE_FLOOR` when it has no floor of its own, so the cursor can never
reach zero and the page below the floor is an EOSE'd empty page, i.e. a DRAIN.
Nothing is lost: `isPlausible` already refuses everything under that floor, and
every paged call site already spelled the same floor out for its progress line.
Quartz's half — a paged walk that cannot terminate against a relay ignoring
`until` — is fixed upstream too, in amethyst#3889 (merged as `a5507f9a`): the
cursor floors at epoch 0, and a relay answering ABOVE the boundary is
`UNPAGEABLE` rather than something to step past. **We are on it** — the `quartz`
pin moved to `a5507f9a4d` alongside the store's own bump to `685b059d37`, which
compiles against the same hash.

Keep both. They buy different things. A `since` cannot
bound the STEP path — `until = boundary - 1` ignores it — so quartz's guard is
the structural one, the thing that stops a cursor-ignoring relay walking from
any window floor down to 0. But quartz's guard alone ends the leg `UNPAGEABLE`,
which settles nothing and re-walks next boot; the floor is what makes the same
walk end at real data, DRAINED, with the leg closed.

Do not "simplify" this by flooring inside `SyncBands.legs`. The legs feed the
negentropy branches too, and narrowing the remote filter while the local id
snapshot stays wide is a false diff — on the `deleteMissing` path, a retraction.

`fetchAllPages` returns a `PagedFetchResult` now
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

**But the preferred leader is not the only url that can hold the ruler, and
treating it as such lost whole hosts.** `PREFERENCE` picks the pathless url
because that is the right SURVIVOR — everyone else's relay lists name it — and
the pass then used it as the only candidate yardstick. When *that one url* would
not answer, a group whose every member serves an identical window was abandoned,
wrote nothing down, and came back widest-first next pass to fail the same way.
Reported live on `wss://asia.azzamo.net` and on a hidden service, both wearing
minted paths that stayed in the fan-out indefinitely. Measured on the azzamo
host, all 12 discovered urls: every pair at containment **1.000**, self-walk
1.000, **11 folds in 5 seconds** — so the fingerprint was never the problem and
any one of the twelve is a perfect yardstick. `AliasFolding.YARDSTICK_ATTEMPTS`
walks down the preference order while urls stay SILENT, three deep. The survivor
then becomes the best url that could actually be *measured* rather than the best
url in the abstract, which is the correct reading: nothing was proved about the
silent one, so it stays in the fan-out on its own.

Silence is the only thing worth retrying, and the distinction is load-bearing.
A url answering with a window under `minSample` has told you about the HOST — it
holds a handful of events and its siblings hold the same handful — so walking
further buys three thin windows instead of one. Silence is about that url alone.
`a leader too thin to be a yardstick does not drag its group onto the wire`
pins the first half; `a host whose preferred survivor will not answer still folds
onto one that will` pins the second.

**A NIP-29 relay refuses BOTH general filters, and that pair of refusals is a
signature rather than a failure.** khatru in groups mode answers any unscoped
query with `CLOSED blocked: invalid query, must have 'h', 'e' or 'a' tag` —
whatever kinds are named — so the bare filter and the `{"kinds":[1]}` fallback
both come back empty, the group has no yardstick, nothing is written down, and
it returns widest-first on every pass forever. `groups.satsdisco.com` was the
reported case and had been the standing example of a host that "still says
nothing"; it was in fact perfectly foldable through a filter the ladder never
sent. So there is a third rung: `RelayAliases.GROUP_METADATA_KINDS`, kind 39000,
which such a relay serves unscoped and unauthenticated.

Measured against a corpus of 40 hosts drawn from live kind-10009 group lists,
21 of which serve 39000:

| host | bare | `[1]` | `39000` | minted path |
|---|---|---|---|---|
| `groups.0xchat.com` | refused | 0 | 1,302 | 1.000 |
| `groups.satsdisco.com` | refused | 0 | 55 | 1.000 |
| `relay29.notoshi.win` | refused | 0 | 27 | 1.000 |
| `groups.fiatjaf.com` | refused | 0 | 16 | 1.000 |
| `groups.hzrd149.com` | refused | 0 | 7 | 1.000 |

Every minted path that answered served the **identical** list — containment
1.000, 6 of 6 across the sweep — so a group list is a fine fingerprint for the
one question the fold asks, and an unusually stable one: addressable events that
change when somebody edits a group, not a slice of a moving feed.

**The rung is worth nothing without the floor, and the floor is a property of
the FILTER.** A 39000 window is a relay's complete list of groups, so it is short
by nature: over those 21 hosts it is min 1, median **9**, max 1,302, and
`DEFAULT_MIN_SAMPLE` admits only 7 of them. Two of the five hosts above — 7 ids
and 16 — sit under it, so the strict floor would have thrown away the hosts the
rung exists to reach. `RelayAliases.foldBar` reads the leader's filter and
applies `DEFAULT_GROUP_METADATA_MIN_SAMPLE` (**3**) to a group-metadata window.
Three rather than one because a host serving one or two groups hands over one or
two ids, and at that width "both urls returned the same list" is exactly the
coincidence the floor exists to refuse. `minOf`, so the group floor only ever
lowers the window bar and never raises one an injected `minSample` set below it.

**A small window floor needs a SHARED-COUNT floor beside it, because a ratio
alone cannot close the hole it opens.** At a window floor of 3 against
`minOverlap` 0.5, the least a fold could ever rest on is TWO shared ids — so a
path serving `{a, b, x}` scores 0.667 against a leader serving `{a..g}` and folds,
taking `x`, a group nothing else on the host serves, out of the fan-out for the
whole TTL. That is the fold's one unforgivable failure — silently not mirroring
something — bought for a two-id coincidence, and it is exactly what the audit of
the first cut of this change caught. So `Bar.shared` demands the two urls
genuinely have `floor` ids in common as well, which costs the honest case nothing
(every live pair shared its list entirely) and is `0` for a general window, where
`minOverlap` on a 500-id window already implies 250 shared. `a group list that
shares only part of itself is not folded away` pins it.

**It lowers the bar for FOLDING and for nothing else.** Every negative claim
keeps `minSample`: entry to `unmatched`, and the leader's own clear. A thin
window can support "these two urls returned the same list of groups"; it cannot
support "this url is a relay in its own right", which is signed for a month and
is the `relay.damus.io/lantern-oscar-dynamo` lie in miniature — sharing none of
three ids is what a url that answered almost nothing looks like. So a NIP-29
host whose paths serve genuinely different groups ends the pass `NOTHING_COMPARED`
and takes the 24h cooldown, which is the honest outcome. `a group-metadata window
may fold a url but never clear one` pins it.

**And the third rung is free where it would cost the most.** It is skipped when
BOTH rungs above returned null — a refusal is the relay answering, silence is our
transport giving up, and a dead url or an onion whose circuit never built says
nothing twice. That matters because the attempts are sequential and
`YARDSTICK_ATTEMPTS` multiplies them by three. One answer is enough to earn the
ask (`&&`, not `||`): a url whose bare walk was cut short by our own transport
while its kinds walk came back refused is still a live server declining the shape
of the question, and dropping it on our own blip would cost a host to save one
dial. `a url that never spoke is not asked for group metadata` pins it.

**What a REAL pass decides, run through `AliasFoldLiveProbe` against the live
hosts** — the whole `AliasFolding.measure` path, the router's own NIP-42 wiring,
an in-memory store, no Vespa and no Docker needed:

```
groups.hzrd149.com   leader 7 id(s) via kinds=[39000], self 1.000   2 fold(s) from 3 dial(s) in 2s
groups.fiatjaf.com   leader 16 id(s),  self 1.000                   1 fold  from 2 dials  in 3s
pantry.zap.cooking   leader 7 id(s),   self 1.000                   1 fold  from 2 dials  in 1s
relay29.notoshi.win  leader 27 id(s),  self 1.000                   2 folds from 3 dials  in 6s
groups.satsdisco.com leader 55 id(s),  self 1.000                   2 folds from 3 dials  in 3s
groups.0xchat.com    leader 500 id(s), self 1.000                   1 fold  from 2 dials  in 3s
nos.lol (control)    leader 500 id(s) via BARE filter, sibling 0.994 1 fold — rung 1 still wins
```

Every sibling scored **1.000**, every leader reproduced itself at 1.000, and every
group ended `0 sockets still held`. The group list is the most stable fingerprint
in this file — addressable events that change when somebody edits a group, where
a firehose slice drifts between two dials (`nos.lol` self 0.998, `fiatjaf.com`
0.638).

**The floor is doing most of the work, not the rung.** Sweeping every host named
in live kind-10009 group lists: 34 never reach rung 3 at all (a general filter
answered, so the lowered floor cannot touch them), and 6 reach it AND serve a
group list. **Four of those six are under `DEFAULT_MIN_SAMPLE`** —
`groups.hzrd149.com` 7, `pantry.zap.cooking` 7, `groups.fiatjaf.com` 16,
`relay.pana.social` 3 — so shipping the rung at the firehose floor would have
recovered a third of what it can reach. That is the measurement behind
`DEFAULT_GROUP_METADATA_MIN_SAMPLE`, not a feel for the number.

**The floor of 3 bites on a real host, and cost nothing to do it.**
`groups.sharegap.net` serves exactly 2 groups; the pass refuses it
(`under minSample(3) — decides nothing`) and reports `no url that could be a
yardstick`. Its paths served nothing anyway, so no fold was lost — and no rung-3
host was observed at 1 or 2 groups where a lower floor would have gained
anything. Do not lower it without re-running this sweep.

**The shared-count guard is UNEXERCISED in the field, and that is worth knowing
before trusting it.** Across every rung-3 host measured, a minted path served
either the identical list (containment 1.000) or nothing at all — **zero**
partially-overlapping pairs. So the `{a, b, x}` case the guard exists for is
constructed, not observed. It is insurance against a multi-tenant groups host,
kept because the failure it prevents is the fold's worst one; treat it as
correct-but-unproven, the same standing as the cleared form.

**What the rung costs, and who pays it.** A large tail of NIP-29 hosts is
auth-gated and answers `CLOSED auth-required` to everything — and the router's
NIP-42 wiring does NOT rescue them (verified on `buzz.relay.tools` and
`relay.andotherstuff.org`: nothing, even authenticated). A refusal is an answer,
so these DO earn the third ask and pay one extra dial: `buzz.relay.tools` spent
21s over 2 urls. Bounded twice over — the `undecidable` cooldown means such a
host is dialled on one pass in four, and a host that is merely dead returns null
twice and is never asked at all.

**A fold decided on a group list says so, and does not borrow the sentence the
containment form uses.** `RelayVerdictRecord.publishGroupList` writes *"same group
list as wss://x: 7 of 7 group definitions shared"* rather than `publish`'s *"7
newest events, 7 shared"*. The numbers are identical; what changes is that a
reader is not invited to check seven against a relay serving thousands and read
the fold as resting on a far thinner sample than it does. Same reasoning as
`publishSecureTwin`, which exists for exactly this.

Two things this was NOT, both of which the verdict card made look guilty: the
relay does send an AUTH challenge, but 39000 reads fine unauthenticated, so `R:
auth` was a red herring; and the 214-second `rtt-read` on those rows is quartz's
passive monitor's number, while every fold dial measured here returned in about
a second.

**A REFUSED CREDENTIAL ENDS THE LADDER, and finding out why cost two wrong
theories.** A pass against `filter.nostr.wine` spent 184s on three urls. The first
explanation was "one `idleTimeoutMs` per rung" — wrong: a single ask on a fresh
socket comes back in ~1.5s at every host measured. `AuthRefusalProbe` times the
ladder ask by ask, on one connection, through quartz's own client and the
router's NIP-42 wiring:

```
filter.nostr.wine  rung 1 bare  limit=500:  1601ms  authRefused=true
                   rung 1 bare  limit=100: 20007ms  reason=null
                   rung 2 [1]   limit=500: 20004ms  reason=null
                   rung 3 [39000]        : 20004ms  reason=null
buzz.relay.tools   every ask:               ~85ms   authRefused=true
nos.lol            every ask:              ~170ms   eose
```

1.6 + 20 + 20 + 20 is the 61s per url, exactly. **The first ask is answered
properly and every one after it is answered with NOTHING** — no CLOSED, no EOSE,
no `doneReason` — so `AliasProbe.over` reads a url that never spoke and the walk
waits out the window. On the wire, once this relay rejects our AUTH (`OK <id>
false restricted: user unauthorized`) it ignores every further REQ on that
socket. `buzz.relay.tools` is the contrast: it repeats `auth-refused` to every
ask, which is why it cost 21s for two urls where this cost 121s.

**Quartz needed no change.** It reports the refusal terminally and
machine-readably on the FIRST ask — `doneReasons[url]` starts with
`auth-refused`, which is all `FetchAllResult.authRefused` is derived from, and
`AliasProbe.over` already read that map to tell `cannot:` from a real answer.
Waiting out the window on the later asks is the only thing quartz COULD do; the
relay sends nothing. The waste was ours, and the rule that fixes it is the
ladder's own charter: **it exists to find a filter the relay will accept, and a
credential refusal is not a complaint about the filter.** So `AliasProbe.Page`
carries `authRefused`, the walk stops on it, and `leaderPrint` does not try the
next rung. Measured end to end on the same three urls: **184s → 2-3s**, with
`nos.lol` unchanged at 2s.

**The stop lives in the WALK, not only in the ladder**, because the ladder is not
the only caller: `AliasFolding` dials every member of a group through
`fingerprint` with the leader's filter, and walks the leader a second time for the
reproducibility guard. Neither goes near `leaderPrint`, and without the
walk-level stop a refused member still pays the empty-page retry at
`FALLBACK_PROBE_PAGE` — the ask measured at 20,007ms. `a refused credential stops
the ladder at the first ask` and `a refused credential also ends the WALK, not
just the ladder` pin the two halves.

**THE MEASURED RELAY SHAPES, PINNED AS TESTS.** Everything above is prose until
a fake reproduces it, so each live shape has a deterministic test against
`AliasProbeTest.Fake` / `AliasFoldingTest.Upstreams`. Writing them found one real
bug and exercised one path that had never run:

| live host | shape | test |
|---|---|---|
| `groups.satsdisco.com` | refuses both general filters, serves 39000 | `a NIP-29 host folds on the one window a general filter cannot reach` |
| `top.testrelay.top` | bare filter → EOSE-empty, kinds serve | `a relay refusing bare filters is asked with kinds instead` |
| `filter.nostr.wine` | auth-refused, then silent | `a refused credential stops the ladder at the first ask` |
| `chorus.bonsai.com` | a page carrying a window AND a refusal | `a page carrying both a window and a refusal keeps the window` |
| `nwc.primal.net` | 100 urls, all answer, none serve | `a hundred unreadable urls collapse to one, and the dials are counted` |
| `haven.calva.dev` | `/chat` empty while the host serves | `one url that serves anything keeps the empty ones separate` |
| `buzz.relay.tools/echo` | silent beside a url that answers | `a url that never spoke keeps its whole group out of the fold` |

**The bug the tests found:** the walk returned on `Page.authRefused` BEFORE
ingesting the page, so `chorus.bonsai.com` — which serves 100 events and flags
the refusal on the same page — had its window thrown away and read as
unfingerprintable. The order is now window first, refusal second: a partial
window is still a window, and the flag only means there is no point asking for
MORE.

**The path that had never run:** the `foldUnreadableGroups` sweep adopts a
yardstick found past `YARDSTICK_ATTEMPTS`. A host whose fourth url is the only one
that answers used to be abandoned even though the sweep had just dialled it.
Taking it costs nothing — the dial already happened — and the group then folds on
a MEASUREMENT rather than on the shared name, which is the stronger verdict and
must win wherever it is available. `a window found past the third attempt is
adopted, not thrown away`.

**The cost, pinned rather than described:** 100 unreadable urls cost exactly
**300 asks** — three rungs each, no url asked twice. An EOSE-empty page cannot end
the ladder the way a credential refusal can, because `top.testrelay.top` proves a
host can answer a bare filter with nothing and still serve on kinds. So 3× is the
honest floor for this shape, and the assertion catches either regression: a
fourth rung appearing, or the sweep re-asking what the yardstick walk already
tried.

**One limit is documented rather than fixed.** The rule counts a credential
refusal and a clean empty EOSE as the same thing — "answered" — so a host whose
urls answer DIFFERENTLY still folds. Two endpoints that behave differently are
weak evidence of being one server, and demanding the same KIND of answer from
every url would be cheap and would shrink the false-fold surface. It is not done
because no host has been measured in that shape: every mixed-looking candidate
turned out uniform on a second look, and the census that suggested otherwise was
measuring our own rate limit. `urls that answered DIFFERENTLY are still folded
together` holds the current behaviour so the decision is visible; if it is ever
tightened, that test inverts.

**A HUNT FOR NEW RUNGS, BY READING WHAT RELAYS SAY WHEN THEY REFUSE.** The
kind-39000 rung was found in one CLOSED message, so `hunt` clustered the terminal
message of one bare ask across ~260 fresh urls (hosts already probed that session
excluded by name, one ask each, to avoid measuring our own rate limit again):

| what the relay said | urls | hosts |
|---|---|---|
| served events | 155 | 79 |
| `blocked: can't handle empty filters` | 58 | 29 |
| EOSE, zero events | 6 | 3 |
| credential refusals (auth / payment / member) | ~13 | 8 |
| `error: scraper, …pocket-db/src/lib.rs` | 2 | 1 |
| `blocked: please add kind 13194 or 23194 or 23195 or 23196` | 2 | 1 |

**No new rung is warranted, and the near-misses are worth writing down so nobody
re-hunts them.**

- **`can't handle empty filters` is still the big one** — 29 hosts, mostly haven
  instances — and rung 2 already exists for exactly it.
- **`top.testrelay.top` refuses a bare filter by answering EOSE WITH ZERO EVENTS**
  rather than a CLOSED. Same behaviour, no diagnostic. Rung 2 recovers it. The
  lesson is for censuses, not for the ladder: an "empty relay" bucket silently
  contains rung-2-recoverable hosts. A theory that our `until` anchor was
  silencing it was tested and is FALSE — `{kinds:[1], until}` and
  `{kinds:[1]}` both serve, and `nostr.oxtr.dev` serves at `until = now + 1 day`.
- **`nwclay.paywithflash.com` names the kinds it wants** (`13194/23194/23195/23196`,
  the NWC set) which looks like a free rung — and is not. Ask with those kinds and
  it answers `blocked: please add authors or #p`. There is no unscoped window at
  that host at all.
- **The pocket anti-scraper is a CLASS, not a quirk** — `chorus.bonsai.com` and
  `koru.bitcointxoko.org` both refuse `limit > 100` as scraping (leaking a cargo
  source path in the CLOSED). `FALLBACK_PROBE_PAGE` recovers them: koru serves 8
  events at limit=100 having refused 200. Existing machinery, no change needed.

**What the hunt DID find is more hazard for `foldUnreadableGroups`.** The rule
fires on "every url answered, none served", and that bucket now has named
members that are not minted-path duplicates:

- **relays with no unscoped window by design.** `nwclay.paywithflash.com` (NWC,
  demands kinds AND authors/`#p`) is the same shape as `filter.nostr.wine`'s
  per-npub paths — a PER-USER endpoint that can never answer a generic ask.
- **live, well-known relays that answer EOSE-empty to everything.**
  `relay.noswhere.com` advertises `nips=[1,11,50]` and returned zero events to a
  bare filter, `kinds:[1]`, `kinds:[0]`, and `search=bitcoin`, with and without
  `until`. `nwc.primal.net` the same. Nothing is wrong with these relays; we
  simply cannot fingerprint them.
- **uniform refusers.** `relay.getalby.com` answers `blocked: Request rejected` to
  every filter tried.

None of those would lose a mirrored stream if folded — we read nothing from them
either way — but each would be a signed public claim that two urls are one relay,
made on no measurement. Weigh that against the fan-out saving before leaving the
inverted default on.

**A cost census over 52 live urls, and the trap in running one.** With the
credential stop in place, `AuthRefusalProbe`'s census ranks what a single
`leaderPrint` costs per url. The SHAPES it reports are the useful part:

| shape | urls | median |
|---|---|---|
| a window came back | 35 | 1.1s |
| answered, served nothing | 3 | 1.7s |
| never spoke | 14 | ~20s |

**But do not quote the totals from that run, and do not read the silent column as
a property of those relays.** Probing the same hosts repeatedly from one IP all
session produced exactly what you would expect: `relay.rodbishop.nz` came back
`cannot:Server Misconfigured. Response: 429 Too Many Requests`, `relay.damus.io`
read as silent minutes after serving 500 events to a kinds filter, and
`chorus.bonsai.com` swung from "21s, served nothing" to "1.1s, 100 events"
between two runs. A census run warm measures OUR rate limit. Re-run it cold —
fresh IP, no prior sweep — before believing any number in it.

**One genuinely new shape did surface**, and the code already handles it:
`chorus.bonsai.com` refuses a 500-limit ask outright as anti-scraping
(`closed:error: scraper, …pocket-db/src/lib.rs:782:48` — it leaks a source path)
and then answers the `FALLBACK_PROBE_PAGE` retry with **100 events AND
`authRefused=true`** (*"At least one matching event requires AUTH"*). So a page
can carry a window and a refusal at once. `leaderPrint` tests the window first
and the refusal second, which is the right order: a partial window is still a
fingerprint, and the refusal only ends the walk when nothing came back with it.

**What this does NOT fix, and should not be confused with it:** a url that never
speaks at all. `buzz.relay.tools` still costs ~21s for two urls, because its
`/echo` path is genuinely SILENT — no frame ever arrives, so the idle window is
the only thing that can end the ask, and that is what the window is for. The host
correctly refuses to fold (not every url answered). Credential refusal is fast
now; silence costs what silence has always cost.

**FOLD UNLESS PROVEN DIFFERENT: a group nothing will serve from collapses onto
its survivor.** This INVERTS the oldest default in this component. Silence used
to decide nothing; a host whose every url answers and serves nothing now folds on
the shared DNS name alone — `AliasFolding.foldUnreadableGroups`,
`RelayAliases.foldUnreadable`, on by default and switchable off.

Three conditions, and each is load-bearing:

- **Every url must ANSWER.** An EOSE or a CLOSED is the relay being there; a null
  page is our own transport giving up. One silent url makes the group "we do not
  know", and folding it would publish our outage as a claim about their server.
  This is also what keeps the bound: a host that fails the yardstick walk by
  going SILENT still stops at `YARDSTICK_ATTEMPTS` and is never swept.
- **Nothing anywhere may serve a window** — including a THIN one. A url with
  content is distinguishable from an empty one, so the group is not "all alike",
  and sweeping it would undo the cheap exit that stops a thin yardstick dragging
  its group onto the wire.
- **The whole group is asked, not a sample of it.** "All of them answer, none of
  them serves" cannot be concluded from the three urls the yardstick walk tried,
  so the rest are swept concurrently first. A window turning up in that sweep is
  ADOPTED as the yardstick rather than discarded — it is a wider yardstick search
  that happened to run.

**What it decides, live.** `haven.calva.dev` is the control and the rule leaves
it alone: the bare url serves 500, `/inbox` scores **0.192** and is kept, and
`/chat` and `/private` come back empty and keep no verdict at all — because
SOMETHING on the host answered, the rule never fires. NIP-11 confirms they are
genuinely different relays ("calvadev's chat relay" against "calvadev's outbox
relay"), so this boundary is doing real work.

**THE CASE IT WAS BUILT FOR, AND THE SCALE OF IT: `nwc.primal.net` wears 100
urls and every one of them was being dialled.** Reported from the coverage card —
`100 url(s) -> 100 dialled`, every row NOT FOLDED — with paths in both pollution
shapes at once: minted words (`/echo`, `/marble`, `/victor`) and wallet
connection tokens (`/2tobu4855tuth8lr716v7hllkcssta`). The rule collapses them:

```
router: live nwc.primal.net served nothing at any of 6 url(s) and every one
  answered — folded onto wss://nwc.primal.net/ on the shared name, WITHOUT a measurement
  5 new alias(es) from 6 dial(s) in 6s, 0 sockets still held
```

**And nothing is lost, for a reason specific to NIP-47.** An NWC relay serves
wallet-connect traffic and nothing else, and of its four kinds only **13194 is
storable** — 23194/23195/23196 sit in the ephemeral range (20000-29999) and are
never persisted by anyone. So the entire mirrorable content of an NWC relay is a
set of replaceable info events, and `nwc.primal.net` serves none of those either
(EOSE-empty to `[13194]`, to all four kinds, to a bare filter, to `[1]`, and to
`search=`). There is no stream behind those 100 urls to lose. Measured across
three NWC hosts, the shapes differ and none of them is foldable any other way:
`nwc.nostr1.com` does serve 13194 (4 events) — and already folds at rung 1 —
while `nwclay.paywithflash.com` answers `blocked: please add authors or #p` to
every kind, so no unscoped window exists there at all.

`relay.noswhere.com` is the same conclusion by a different route: a search-only
relay (`nip50: ["ext include:spam", "query negate", "query exact-phrase-match"]`)
that returned zero events to seven different search forms — with kinds, without,
exact-phrase, `include:spam`, and on kind 0. Alive, well-known, and holding
nothing we can read.

**And it is demonstrably WRONG on `filter.nostr.wine`.** Its urls are
`/npub1…?broadcast=true` — a PER-USER filtered endpoint behind `auth_required`
and `payment_required`. Every one answers, none serves, so the rule folds four
users' feeds onto one url and signs it:

```
router: live filter.nostr.wine served nothing at any of 3 url(s) and every one
  answered — folded onto wss://filter.nostr.wine/ on the shared name, WITHOUT a measurement
```

Swept over 45 multi-url hosts taken from live relay lists, the rule fires on
three — and that is one of them. Treat a third of its firing population being
wrong as the number until someone re-measures it.

**Two things make it defensible anyway, and both should be understood before
touching it.** A url nothing can be read from is mirroring nothing, so a wrong
fold here costs no stream TODAY — only the day the relay starts answering us,
until the verdict expires. And the record says so in words rather than quoting a
number it does not have: `RelayVerdictRecord.publishUnreadable` writes *"nothing
readable at any of N url(s) on this host; folded on the shared name, not on a
measurement"*.

**AUTH RESCUES THE BENIGN CASES, WHICH CONCENTRATES THE RULE ON THE PATHOLOGICAL
ONES.** `support.flotilla.social` reads as all-empty to an unauthenticated sweep
and would fire the rule — but the router authenticates, gets 500 events on every
url, and folds it by MEASUREMENT at containment 1.000. So the population that
actually reaches this rule in production is smaller than an anonymous probe
suggests, and it is enriched for the hosts that refuse us for reasons no
credential fixes: payment walls, per-user endpoints. Do not size this rule with
an unauthenticated sweep.

**It WAS not cheap, and the reason turned out to be a bug in the ladder rather
than a property of the rule.** Three urls of `filter.nostr.wine` cost **184
seconds**; they now cost **3**. See the credential short-circuit below — the
sweep still scales with group size rather than stopping at three, but each url in
it is one ask instead of six.

**`ws://x` and `wss://x` are the one pair the urls themselves settle.** Every
other fold refuses to read anything off a url, and rightly — a path is routinely
a *different* endpoint, which is why `/inbox` must never fold on its spelling. A
scheme is not an endpoint; it is how we reach one. So `RelayAliases.schemeTwins`
folds a `ws://` url onto the `wss://` url of the same host and path when **both
answered this pass**, and it is not merely a shortcut for a fold containment
would have reached anyway — it decides the pairs containment *cannot*:

- **windows too thin.** A relay holding nine events hands both twins the same
  nine, and nine is under `minSample`, so nothing folded, nothing was cleared,
  and the group came back on every pass forever. Worse, such a host never even
  reached `learn`: a yardstick under `minSample` abandons its group by design.
  It still does — for the *group* — but its own scheme twin is now dialled and
  decided, one extra fingerprint, in the exit that used to write nothing at all.
- **only one twin has a verdict.** `toProbe` re-dials the secure twin of an
  unmeasured plain url even when that twin was cleared last month, precisely so
  there is a "both answered" to be had. Without it the plain url is compared to
  the group's *leader* — a genuinely different endpoint — disagrees with it
  correctly, and gets published as a relay in its own right.

Two guards keep it honest, and both are about not losing a relay. Silence is not
"it works": a url missing from the pass's prints was refused, silent, or never
asked (our own transport can decline it), so a `ws://` whose twin said nothing
stays exactly where it is. And the windows keep a veto **in the one direction
that can lose data** — everything the plain url served must already be on the
secure one, at `minOverlap`. Not `sameRelay`: no `minSample` floor (the pairing
is the argument; nine events are plenty to show they do not contradict it) and
not symmetric (a `ws://` serving 500 events beside a `wss://` serving nine is
refused, whatever the two urls are called).

The verdict is published through `RelayVerdictRecord.publishSecureTwin` rather
than `publish`, because the evidence is different in kind — *"same endpoint as
wss://x over TLS, both answered; 9 newest events here"*, not a containment. These
folds happen where the containment could not decide, so quoting one would offer
as the reason a number the verdict was never based on, in a signed month-long
claim about somebody else's server.

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
those is a correct conclusion. 1 host in 513 by count — but see the cost below, which is not proportional to
the count.

**A host that cannot be decided must not be re-probed at the front of every
pass.** The three exits that end a DIALLED group with no verdict — no url could
be a yardstick, nothing answered enough to compare against one, the yardstick not
reproducible — all write NOTHING down, on
purpose: each is a case where publishing would claim more than was measured. But
nothing written down means `RelayAliases.unresolved` hands the group straight
back next pass, and groups are probed WIDEST FIRST, which is exactly the shape
these hosts have. So an undecidable host was re-dialled first, every pass,
forever, spending a `probesPerCycle` budget that foldable hosts then never got.
Reported live as `relay.lightning.pub` wearing six unfolded paths while folding,
when finally measured, in **two seconds at containment 1.000** across all of
them. `AliasFolding.undecidable` is the fix: a 24h in-memory cooldown per host,
in memory and never signed, because "our pass could not measure this" is a fact
about us and not a claim about their server.

**A pass says which hosts it left unfolded and why.** Five things end a group
with nothing written down — out of probe budget, on the cooldown above, no url
that could be a yardstick, nothing to hold up against one, a host that cannot
repeat itself (`AliasFolding.Undecided`) — four of which recover on their own
and one of which never will. From outside the process all five look identical:
a url still being dialled beside eleven siblings that folded. Every investigation
of a specific host began by guessing which of the five it was, and the budget
exit left no trace at all. One line per pass, counted by reason with a few hosts
named as the lead:

```
router: outbox alias pass left 37 host(s) undecided — 11 out of probe budget (a.example, …); 9 cooling down from an earlier failed pass (…); 12 no url that could be a yardstick (…)
```

Do not read the named hosts as the whole set; the count is the fact and the
names are bounded on purpose.

Measured on a live router — Vespa, a real store seeded with 1,952 real relay
lists, 1,282 discovered urls — the first pass reported:

```
outbox measured 782 fingerprint(s) → 373 new alias(es), 1282 url(s) now fold onto 909 relay(s) in 4:32
outbox alias pass left 84 host(s) undecided — 7 declined by our own transport; 71 no url that
  could be a yardstick; 5 nothing to hold up against the yardstick; 1 a host that cannot repeat itself
```

**71 of 84 is "no yardstick"**, which is why that exit is the one worth widening
(see the yardstick search above) and why the budget exit — the one that used to
be invisible — turned out not to be the binding constraint at this scale. Do not
generalise the 84 to a production store without re-measuring; this corpus is an
order of magnitude narrower.

**The reproducibility bar gates the NEGATIVE claim only, and that asymmetry is
deliberate.** Noise in the yardstick is not symmetric between the fold's two
conclusions. A relay handing back a shuffled subset drives every containment
DOWN, so a sibling that still clears `minOverlap` did it in spite of the noise —
folding is the conservative read. The same noise pushes urls over the bar in the
other direction for free, which is how two paths of one server get signed as
separate relays for a month. Measured: `fiatjaf.com` self-scores 0.638 while its
two minted paths score 0.787 and 0.730 against it, and the pass publishes both
folds; `multiplexer.huszonegy.world` is self 0.594, siblings 0.622–0.870, four
folds. So a group that folds cleanly never pays for the second walk, and only a
group about to claim "these are different relays" does.

**A relay that cannot answer one filter the same way twice is removed from the
fan-out** — `RelayConsistency`, `ConsistencyPass`, published as a
`self-consistent` tag on the url's own NIP-66 30166 record and re-measured
monthly. Each url walked twice from one anchor, `RelaySelfConsistencyProbe`, and
**run twice**, which is where the actual finding came from:

| url | | 1min | 1hour | 1day | 7days |
|---|---|---|---|---|---|
| `nos.lol` | #1 / #2 | 1.000 / 1.000 | 1.000 / 1.000 | 1.000 / 1.000 | 1.000 / 1.000 |
| `nostr.oxtr.dev` | #1 / #2 | 1.000 / 1.000 | 1.000 / 1.000 | 1.000 / 1.000 | 1.000 / 1.000 |
| `relay.lightning.pub` | #1 / #2 | 1.000 / 1.000 | 1.000 / 1.000 | 1.000 / 1.000 | 1.000 / 1.000 |
| `multiplexer.huszonegy.world` | #1 / #2 | 0.446 / 0.782 | 0.712 / 0.908 | 0.770 / 0.916 | **0.964 / 0.654** |
| `fiatjaf.com` | #1 / #2 | 0.803 / 0.719 | 0.664 / 0.720 | 0.694 / 0.618 | 0.715 / 0.826 |

**A stable relay is 1.000 at every depth in every run.** That is the whole
safety argument: the good actors are nowhere near the 0.9 bar, so the gate
cannot cost them their place.

**One run would have shipped the wrong conclusion.** Run #1 alone read as
"huszonegy is a sharded backend that just needs time" — 0.446 climbing to 0.964
at a week. Run #2 scored the same relay at the same depth **0.654**. Its
disagreement with itself is not reproducible either, so there is nothing to wait
for. Re-run this probe more than once before moving the bar or the depth.

The seven-day anchor is not there to let a relay converge — it is there to
remove OUR anchor from the list of explanations, so a failure cannot be blamed
on new events, indexing lag, or replication.

**Consistency is not compliance, and the gate above cannot become one.** It
compares two answers to EACH OTHER — set containment over ids, and nothing in it
ever opens an event — so it catches the relay whose window is a fresh random
slice per REQ and is blind by construction to the relay whose window is the SAME
wrong slice every time. That second relay answers identically twice, scores
1.000, and was certified `prime`. The failure it produces downstream is the one
`RelayConsistency`'s own header describes for the first: bands and coverage
built over events the ask never wanted.

So the second question — **do the events match the filter that asked for
them** — is `RelayCompliance`, graded by `FitnessPass` and published as a
`compliant` tag beside `pageable`, with `noncompliant` as the refusal. What is
checked, and where the evidence comes from:

| check | evidence | where it comes from |
|---|---|---|
| `kinds` | events of a kind the filter did not name | the second page, which carries one where the ladder's bare rung cannot |
| `until` | events stamped above the cursor they were asked under | every page of the walk, against THAT page's own cursor |
| `limit` | events beyond what the page asked for | every page — published as a fact, never graded on |

Three things about the shape:

- **The tally is per PAGE, not per walk.** `until` steps down as a walk pages
  backwards, so an event above page four's cursor is the cursor being ignored
  even when it sits below the anchor. `FitnessPass` counted against the anchor
  alone before this and scored those relays clean.
- **One extra REQ per url per sweep** — `AliasProbe.pageBelow`, ten events,
  which is also the second page #187 needs. It carries the rung's own shape, so
  on a bare walk `kinds` goes unchecked; see the three rules below the table. Its budget is a quarter of the
  url's whole deadline, and when that clock fires the url is still graded on
  one page; the pass says so on its own line, because a check that silently
  stops checking looks exactly like a corpus that passed.
- **Over-serving the `limit` is deliberately not a refusal.** An over-served
  event matches the filter; it was simply not asked for yet. It costs bandwidth,
  which is why it is measured, and it does not make the answer wrong.

**The 300s slack applies to the ANCHOR and never to a cursor the relay itself
supplied.** An anchor is our clock against an author's `created_at`, so a
publisher running fast puts an honestly-served event above the line and the
slack absorbs it. Page two's cursor is one of the relay's OWN stamps minus one
— no second clock, nothing to absorb — and leaving the slack there is not
conservative but blind: twenty events at a busy relay span SECONDS, so a
cursor-ignoring relay re-serving them lands inside five minutes of the cursor
every time and scores as a walk that advanced. That is #187's failure surviving
the check written for it, and it is what `Compliance.of(slack = 0)` is for.

**The two bars are PROVISIONAL and the probe for them is written.** A relay is
refused at ≥3 off-filter events AND ≥10% of the answer — both, because a share
alone refuses a thin answer (one wrong out of two) and a count alone refuses a
firehose (three wrong out of five hundred). Neither number has been taken
against the network yet. `RelayComplianceProbe` prints exactly what each bar
would decide per url, asserts nothing, and the table above this one is the
standing warning about believing a single run of it:

```bash
./gradlew :monitor:test --tests '*RelayComplianceProbe*' -DcomplianceProbe=true --rerun -i
#   …or hosts of your own: -DcomplianceUrls='wss://relay.example,wss://other.example'
```

Record what it says here when it has been run twice, and move the bars in the
same commit as a `FITNESS_EPOCH` bump — which is what took every epoch-1 `prime`
back when this check shipped, since none of them was ever tested against it.

**One page is not a walk, and for a year the `pageable` tag said it was
(#187).** The pass sizes its walk at `FITNESS_TARGET` (20) and asks for that
many at once, so a relay serving a full page satisfies the target on the FIRST
`fetch` and `AliasProbe.walk` returns having never moved the cursor. The tag
then read `"20 events, all at or below the anchor"` — a statement about one
anchored page, published as a statement about paging.

Measured on `vespa-eventstore-staging`, one 11-minute window on a fresh pod:

| | |
|---|---|
| relays the mirror aborted with `End.UNPAGEABLE` | 137 |
| of those, found in the monitor's own 30166 records | 137 |
| graded `prime` | **137 (100%)** |
| tagged `pageable: true` | **137 (100%)** |

`nostr.wine`, `relay.primal.net`, `eden.nostr.land`, `nostr.mom`,
`relay.nostr.com` and `nostr.bitcoiner.social` are in that list, so this is not
a fringe of broken hobby relays. It is also the most expensive kind of bad
roster entry, because it does not fail — it answers, takes a visit, delivers
events, advances nothing, and leaves its relay unreconciled.

**THE MECHANISM IS NOT CONFIRMED, AND THE OBVIOUS ONE IS RULED OUT.** The
reading that produced the fix below — "honours `until` for the first ask, serves
the present ever after" — was checked against the relays the issue names and
does NOT hold. `RelayComplianceProbe` dialled eight of them in five ask shapes
each: the monitor's bare rung, the second page below it, the mirror's
`kinds=[0] since=…` and `kinds=[1] since=…`, and the OUTBOX shape with `authors`
bound to pubkeys the relay had just served. Every relay that answered advanced
the cursor or drained honestly, on every shape:

| url | page two | mirror `k0` | mirror `k1` | outbox `k0` (authors bound) |
|---|---|---|---|---|
| `relay.nostr.com` | advanced | advanced | advanced | drained |
| `relay.primal.net` | advanced | advanced | advanced | drained |
| `nostr.mom` | advanced | advanced | advanced | drained |
| `nostr.semisol.dev` | advanced | advanced | advanced | drained |
| `nostr.einundzwanzig.space` | advanced | advanced | advanced | drained |
| `relay.pleb.one` | advanced | advanced | advanced | advanced |
| `ec1.f7z.io` | advanced | advanced | advanced | drained |
| `relay.bchnostr.com` | advanced | advanced | advanced | drained |

(`nostr.wine` answered no window at all — it is auth-gated — and
`haven.dergigi.com` answered an empty one.)

**AND THE ABORT ONLY FIRES ON A WALK THAT DELIVERED NOTHING**, which reframes
what is being looked for. `VisitPool.refusedOutright` is `downloaded == 0 &&
end in {IDLE, CLOSED, AUTH_REQUIRED, CANNOT_CONNECT, UNPAGEABLE}` — so
`abortedUnpageable` counts a walk where NOT ONE EVENT MATCHED THE ASK, on any
page. An `UNPAGEABLE` that downloaded anything is recorded as a band, not
aborted. "Honours page one, stalls on page two" cannot produce the counter at
all: page one's events would already have been downloaded. The fault is at the
FIRST page, and it is closer to "the relay served something that was not asked
for" than to "the relay would not step its cursor" — which is
[RelayCompliance]'s subject, not this one's.

Six ask shapes were then run against the named relays, each through the code
that would see the fault, and NONE reproduces it:

| shape | what it tests | result |
|---|---|---|
| monitor bare rung + page two | our own check | advanced or drained, every relay |
| mirror `kinds=[0] since=…` | the aborting kind | advanced |
| mirror `kinds=[1] since=…` | a busy kind | advanced |
| outbox `kinds=[0] authors=…` | the replaceable current-version path | drained |
| older leg `since` AND `until` together | the only ask whose FIRST page carries a cursor | window honoured |
| `contentViaOutbox`'s real 141 kinds + one author, through quartz's own `fetchAllPages` | the production ask, the production pager, quartz's own `End` | **DRAINED, every relay** |

The last row is the one that matters: same kinds, same single-author split
(`RosterBuilder.asksOf` binds one author per filter, structurally), same
`fetchAllPages`, and the ending quartz itself reports. Twelve relays, all
DRAINED.

What a clean dial still is NOT: the pool visits at `visitConcurrency = 96` with
up to 500 live tails, over sockets shared between streams, under the
deployment's own NIP-42 identity, and its authors are 30382-paired providers
rather than pubkeys the relay was just seen to serve. So the remaining
hypotheses are about LOAD AND STATE rather than about a relay's cursor
handling, and none of them can be reached from outside the deployment.

Two things follow, and the second is the open one:

- **The second page is still the right check** and the argument for it needs no
  relay to misbehave: one page cannot be evidence about paging, and it was
  being published as if it were. The same goes for the empty-page claim below.
  Both are about what this pass may honestly SAY.
- **What the mirror is aborting on is still unexplained.** A clean single-socket
  dial is not the mirror's situation: its asks carry real author sets, many
  filters merged onto one REQ, and a socket shared with live tail subscriptions
  from other streams. `PagedFetchResult.End.UNPAGEABLE` is quartz's verdict, not
  ours, and reading its bytecode it has more than one path — one fires when a
  page's ids were ALL already seen, and one when a page ARRIVED but nothing on
  it matched the filter (the min-`created_at` ref is still `Long.MAX_VALUE`).
  The second is a walk that made no progress because the answer was not an
  answer, which is not a paging fault at all. **The next step is on the mirror's
  side, and only the deployment can take it** — which is what `RelayPages` is
  for, below.

**So the mirror now catches the page itself** — `RelayPages`, read by
`VisitAborts`. An abort line had two of the three things it needs: `asked` (what
we sent) and, through `RelayComplaints`, `said` (what the relay answered in
words). The third is what the relay answered in EVENTS, and no instrument in the
process could see it: quartz calls the pool's `onEvent` only for events matching
the filter, and an abort is `downloaded == 0` — so on exactly the walks worth
diagnosing, the pool's own hook never fires. The events crossed the socket and
were dropped by the match that produced the abort.

The sample is taken on the connection listener, the one seam upstream of that
match, and every part of the design is about not paying for it:

- **Armed, not always on.** This runs for every event of every socket of both
  planes — the mirror's ingest and the monitor's 20,000-url probe passes alike.
  Disarmed it costs one `isEmpty` per message; armed it costs a map lookup and,
  for at most five events per walk, a small append. The slot is taken before the
  ask and given back in a `finally`.
- **One sampler per relay.** Several streams walk one relay over one socket, so
  `arm` takes the slot with `putIfAbsent` and hands the loser nothing — a second
  sample would be the first walk's events, and a line attributing them would be
  worse than no line.
- **Subscription ids are reported, not filtered on.** The walk's own id is not
  knowable from a connection listener (`fetchAllPages` mints its own), and a
  page carrying another subscription's events is itself the answer: one id is
  the walk, several means the socket's other traffic landed inside the ask,
  which is a different fault with a different fix.
- **The sentence is comparisons, not counts.** "3 off-kind", "2 above the
  `until`" — because the question is never what the relay sent, it is which part
  of the ask went unhonoured. And a page that matched everything says so in
  those words, since that reading is not a relay misbehaving at all but our own
  side of the walk, and an operator has to be able to tell.
- **Nothing sampled prints nothing.** A socket that carried no event is not
  evidence, and "sent 0 events" would read as a finding.

One line, on the existing half-hourly per-(stream, relay, reason) gate, should
settle in a single production pass what six shapes of clean dial could not.

**AND THE 109 WERE PUT THROUGH THE REAL POOL.** #187 splits its relays into 28
that failed on BOTH outbox streams and 109 that failed on ONE, and that second
number is the sharpest fact in the issue: a relay refusing one stream and
serving the other is the same server on the same socket under the same pool and
the same quartz, so nothing about its cursor can differ between them. The ASK is
what differs — `contentViaOutbox` carries 141 kinds, `profileViaOutbox` carries
3. A fault that follows the STREAM is the filter's; one that follows the RELAY
is the relay's; and they want opposite fixes.

`AbortCensusLiveProbe` runs both streams over **all 137** through the real
`VisitPool`, the real client and a real Vespa, seeded first with 2,488 real
kind-10002 relay lists pulled off `search-staging.brainstorm.world` — the
deployment's own corpus, through its NIP-42 and its trust lens. Twenty minutes,
**1,065 visits**:

| | |
|---|---|
| `abortedUnpageable` | **0** |
| `abortedQuiet` | 417 |
| `abortedUnreachable` | 4 |
| relays that never aborted | 58 of 137 |
| aborted on BOTH streams | 77 |
| aborted on ONE stream | **2** — `nostr.bitcoiner.social`, `relay.nmail.li` |

**The fault does not follow the stream, and it is not `unpageable`.** The quiet
aborts split 71 / 71 between the 141-kind ask and the 3-kind one — identical
counts, so width discriminates nothing — and 77 of the 79 failing relays failed
on both. #187's "109 failed on ONE stream" did not reproduce: two did.

**What DID fail is a relay that connects and then never EOSEs**, which is
`PagedFetchResult.End.IDLE`, not the cursor. Every one of those lines carries no
page sample — correctly, since a silent relay sends nothing to sample and the
instrument says nothing rather than "sent 0 events". A relay serving the wrong
events would have shown its page there, which is the point of having it.

**A CORRECTION, because an earlier note here had it backwards.** The outbox
streams do NOT bind authors. Both `relaySource` blocks are a bare
`{"kinds":[30166],"#l":["prime"]}` with no `select`, so `bindings` is empty and
`RosterBuilder.asksOf` returns ONE unbound ask carrying the stream's whole
filter. The per-author split belongs to the `assertions` stream, whose select
binds `authors = 1` off a 30382 tag. So the census above sent exactly the ask
production sends, and there is no author gap to close.

**What is still missing is the IDENTITY.** This ran under a throwaway key, and
`RelayReachLiveProbe` already measured what that costs: sixteen of #185's fifty
"unreadable" relays served us once NIP-42 was answered, and the key relays
allowlist is the deployment's own. A run without it reads a relay's POLICY as
its behaviour, which is very likely what the 417 quiet aborts are. Re-run with
`-DabortCensusNsec=nsec1…` before concluding anything about a particular relay
on the list — that, and the deployment's 96-way concurrency with 500 live tails,
are the two differences left.

**The seam was verified against a real relay before being believed**, because
the unit suite structurally cannot: it builds the sample by hand and gives the
listener a client that never delivers a message, so it would be just as green if
quartz dispatched no event to a connection listener at all — and that dispatch
was inferred from bytecode. `RelayPagesLiveProbe` stages the aborting shape on
`nos.lol`: the walk ended DRAINED with `downloaded = 0` and its `onEvent` fired
zero times, while the sampler recorded three events under the tail's own
subscription id, flagged as above the `until`. That is both claims at once — the
events are there when every instrument downstream of the match has lost them,
and a foreign subscription is legible as foreign.

Two things that probe corrected on the way, and both are the same lesson:

- **A first cut armed the sampler for ~100ms** — the drain is instant — against
  a relay serving an event every few seconds, saw nothing, and read it as the
  seam being broken. The instrument was fine and the measurement was not.
- **The sentence hard-coded "quartz then counted as none"**, which is true where
  `VisitPool` calls it (a refusal is `downloaded == 0` by definition) and false
  the first time anything else does — the probe was told quartz had counted none
  of the 158 events it had just downloaded. `render` takes the count now.

With that said, the pass asks a second page at `until = <oldest event of page
one> - 1`, strictly below, and reads its three possible answers:

**Three rules the pre-merge audit put here, each of which was violated by the
first cut of this check and none of which a passing test caught:**

- **Page two is page two OF PAGE ONE.** It asks the shape the rung that answered
  used — null included. Substituting `kinds=[1]` on a bare walk so the tally
  could see `offKind` re-creates the exact unearned claim: a relay that answered
  a bare page one and holds no kind 1 below the cursor drains that page two
  honestly, and the drain reads as "the walk terminates" for a walk nobody made.
  What it costs is the `kinds` dimension on the relays that answer the bare rung
  — most of them — and that is not recoverable in one round trip. It is a fair
  price: a relay answering with events it was not asked for is answering ABOVE
  THE CURSOR too, which a bare page two sees perfectly well.
- **The verdict is handed over BEFORE any further dial.** The second page can
  change it, so it was tempting to wait — and waiting meant the per-url deadline
  firing during that ask left the url with no verdict at all, where the ladder
  had already proved one. Those urls count as `abandoned`, `abandoned` feeds the
  batch guard's blind share, and a slow enough batch would then refuse to
  publish any of its own verdicts. A paging check must not be able to cost a
  pass its output. A later refusal simply replaces the handover, exactly as the
  NEG-OPEN's fuller verdict already does.
- **The slack belongs to the ANCHOR alone.** Only page one's `until` is our
  clock against an author's stamp. Every page after it — inside a walk as much
  as in `pageBelow` — is asked below a timestamp the relay itself served, where
  five minutes of grace is not conservative but blind: a busy relay's page spans
  seconds, so a cursor-ignoring relay re-serving it lands inside the slack every
  time. `Compliance.of` takes the slack as a parameter for exactly this.

And one for the sampler: **`RelayPages` counts on arrival and keeps rows only
for display.** Tallied at render over the retained rows, a 158-event page whose
first five happened to match reported "all of them MATCHING the ask" — the
sharpest sentence the instrument has, the one pointing at OUR side of the walk —
about a page that was mostly the relay's fault.

| page two | what it proves | verdict |
|---|---|---|
| events at or below the cursor | the cursor advanced; a walk terminates | `pageable: true` |
| **empty** | a DRAIN — a cursor-ignoring relay would have served its newest events again | `pageable: true`, the strongest of the three |
| its newest events again | the cursor was ignored | **`unpageable`** |

It is the same REQ as the compliance check above, which is why the whole of
this costs one round trip: it carries a `kinds`, a `limit` and an `until`, so
one answer settles both questions. It is asked through the rung that answered,
because a group host holds no kind 1 and would drain a `kinds=[1]` ask honestly
— which this would then read as a walk that terminated.

**`pageable: true` on an empty first page is gone** — 26% of the 137, granted on
`"empty anchored page, honestly EOSEd"`. An honest EOSE proves the relay
answers; there was never anything there to page from. Those urls now carry NO
`pageable` tag and are counted on the pass's own line. They keep their grade:
absence of evidence is not a refusal here, for the reason the `silent` branch
spells out at length, and an empty anchored page is the honest answer of every
relay holding nothing recent.

**Most of a candidate set is never decided, and that is the normal state rather
than a fault.** A pass dials its whole set — the per-pass budget was dropped —
and on a discovered corpus it decides a few hundred urls out of several
thousand, because the rest cannot be measured at all. That ratio on its own
("595 of 8,172 checked for consistency") reads as a gate that is stuck, so the
urls with no verdict are counted by REASON — `ConsistencyPass.Unmeasured`, seven
of them, in two families:

| about us | about the far end |
|---|---|
| declined by our own transport | never answered a REQ |
| the probe failed mid-walk | answered one of the two asks, not both |
| | refused our auth |
| | answered, but served no filter we know |
| | too few events to judge on |

`report` publishes them as counts of URLS, so the candidate set divides exactly
once: `candidates = foldedAway + consistent + inconsistent + unmeasured`, and
`unmeasured` is the sum of those reasons. `ConsistencyReportTest` pins that
identity and one test per reason.

**The stats card draws the whole thing as a TREE** — `funnelOf` in
`/web/shared/sync.js`, one row per node, five levels deep:

```
every relay url this router knows of                                  17,584
├─ dropped before a pass could see it                                    832
│  ├─ excluded by config, or our own url                                   3
│  └─ known dead — one of our own signed `dead` verdicts               829
├─ known from our own records — no relay list names it now                 0
└─ in reach — the candidate set                                       16,752
   ├─ folded onto another url                                         11,429
   ├─ consistent                                                         583
   ├─ inconsistent — refused                                              12
   └─ no verdict                                                       4,728
      ├─ never answered a REQ                                          3,902
      │  ├─ the name does not resolve  on 1,502 host(s), largest 44    2,140
      │  ├─ it never answered in time  on 611 host(s), largest 61      1,204
      │  ├─ the connection was refused  on 388 host(s)                   402
      │  └─ the TLS handshake failed  on 121 host(s)                     156
      └─ refused our auth  on 4 host(s), largest 600                     826
```

**The mouth is `sourced + recordedOnly`, not one derivation's yield.** It was
`sourced` alone — every url the streams' relay lists named THIS round — under a
caption reading "every relay url this router knows of", and the two are not the
same corpus: a url leaves the relay lists for reasons of its own (the author who
listed it revised their 10002, a source was reconfigured, a stream was retired)
and every measurement this router ever took of it is still in the store, still
read by `AliasFolding.adoptWorld`. `StreamWorld.candidates` counts them —
`RelayDiscovery.recorded`, our own authors, on the verdict TTL, minus whatever
this round named — and the tree gets a branch for them beside `dropped` rather
than inside it, because nothing was decided against them: nobody asked. On a
deployment whose store holds records for five figures of urls while its current
relay lists name a couple of thousand, the card was drawing a tenth of its own
corpus and calling it everything.

The root is keyed `corpus`, NOT `sourced`, and that is the glossary's rule rather
than a preference: `sourced` is a published member meaning one of the two halves,
and a root row labelled "everything this router knows of" carrying that entry
would document the wrong number for the widest row on the card. Absent
`recordedOnly` — a router older than it — the tree is exactly what it was, with
no zero row claiming a corpus nobody measured.

It was an icicle first — one row per LEVEL, each a share of one width, a child
drawn under the parent it subdivides. That was correct, and it needed three
captions and a legend to say what indentation says for free: the nesting was
carried by horizontal offset, the one visual channel already spent on
proportion. Rendered on the real card it read as four unrelated bars. Every bar
is still a share of the ROOT, never of the parent, so a host under a reason
stays visibly a sliver of the corpus.

A node whose children do not sum to it gets an `unattributed` child in the fault
tone rather than a short bar — any arithmetic slip, and any reason list either
side truncated. **Absent is not zero**: a pass publishing none of the three
verdict members gets NO tree, because read as zeroes every url it checked lands
in `unattributed`. That one shipped, and a screenshot of the real card is what
caught it — 12,731 of the fold's 16,752 urls drawn as an arithmetic error on a
pass that was working perfectly.

**`never answered a REQ` splits again, on what the TRANSPORT said.** It is the
largest bucket on a discovered corpus and it covers a name that no longer
resolves, a refused connection, a failed TLS handshake and a window that lapsed
in silence — four findings with four responses, and only the last is worth
simply retrying. The evidence already existed: quartz hands us the socket
layer's own message as the url's terminal reason (`onCannotConnect` →
`"cannot:" + message`), and it was read once for a `startsWith` and dropped.
`Silence` classifies it, by substring, because the text is somebody else's
formatting and not a contract — quartz classifies the same strings the same way.
**What makes that honest is `Silence.UNKNOWN`**: text the table does not
recognise is counted as unrecognised and sampled to stderr, so the gap shows up
on the first pass after a deploy and the table is extended from real strings.

Those rows are published FLAT, each naming the reason it refines (`parent`), so
the list still sums to `unmeasured` — nesting on the wire would put the one
property the whole tree rests on at the mercy of a shape. The page nests them
and SYNTHESISES the parent from its children, because the parent has no urls of
its own.

**The hosts under a reason are published and NOT drawn as rows** — one row per
host is one row per SERVER on a corpus of two thousand of them, and the ranked
head (`undecided[].top`, capped at six) is short only because the router capped
it. What that ranking is FOR survives as two numbers on the reason's own row:
`on 1,502 host(s), largest 44`. That is the question `urls` and `hosts` raise
and cannot settle — 3,902 urls on 2,201 hosts with the largest at 61 is a dead
network spread thin, the same urls with the largest at 3,000 is three servers —
answered in a line rather than in forty rows. The names ride along on the row's
title, and the full ranking with its counts stays in the JSON.

The two probe passes' own lines carry ONE fact each — how far the pass got, and
how long it took. The verdicts were briefly on the gate's line too, because
nothing published them anywhere; they are the tree's business now, and a line
repeating what a chart six rows above it says is a line a reader has to
reconcile.

`Processors.MAX_UNDECIDED_REASONS` (8) is now the ONLY cap on that list, and
that is the point of the note. There used to be a second, `MAX_UNDECIDED_ROWS`
on the relay's re-read, and it sat at 6 against a gate that can reach 7 —
cutting below the writer is not bounding, it is dropping, and the dropped
reason's urls then surfaced as an arithmetic fault on a document that was
complete when it arrived. Two caps on one list is the shape of that bug; the
re-read is gone and so is the second cap.

Two things that partition made visible and then fixed. `dialled` was
`wanted.size`, so urls the transport declined were reported as dials that never
happened. And an auth refusal fell through to the kinds fallback anyway — two
more REQs into a wall we had already been shown, per url, per pass — because
`walkPair` flattened the refusal into "proved nothing" before the caller could
see it; see `AliasProbe.window`, which is `fingerprint` keeping that one bit.

**Why removal rather than a downgrade**, which is what an earlier draft of this
section argued for: a relay whose window is a different slice each time holds no
stable cursor, so its band never closes and every cycle re-downloads what the
last one already took. Measured on this mirror as millions of duplicated events
and cycles stretched from two hours to five. The claim being made is narrow and
is worded that way in the record — the relay cannot be synced against, which is
a property of the server, not of the operator — and it expires in a month, so a
server that is fixed rejoins on its own with nobody intervening.

**When a relay is tested again, and the trap that made half of them immortal.**
The verdict ages out after `RelayVerdictRecord.DEFAULT_TTL_SECONDS` (30 days) and
the monitor picks the lapse up on its next pass, so a re-measure lands within
`AliasMonitor.DEFAULT_INTERVAL_MS` (6h) of the month mark. Expiry is per url and
staggered by whenever each was first measured, so there is no day-30 herd.

What it could NOT be aged on, while a passive writer shared the record, was the
RECORD's `created_at` — which is what it was doing at first. Kind 30166 is
addressable and shared, and quartz's `RelayMonitor` rewrote the record for every
relay this client connected to on a 5-minute flush, carrying our tags forward.
So `created_at` tracked the last time we TALKED to a relay, not the last time we
MEASURED it — and the effect was exactly backwards. A REFUSED relay is never
dialled, so nothing refreshed its record and it expired on schedule; a KEPT
relay is dialled constantly, so its record never aged and its verdict was never
re-taken. **Measure once, trust forever — for precisely the population where
"was fine, now degraded" is the case worth catching.** The fold's `same-as` had
the identical hole: a folded url expires, the canonical it folded onto did not.

That writer is gone (above), so `created_at` is honest again and the `s` verdict
reads its freshness off it — the whole-record check date, which is the reading
the rest of the NIP-66 ecosystem applies and the one a config can express as
`since`. The per-tag stamps stay: they are public evidence, and the fold and the
stability gate still age on them through `RelayVerdictRecord.current`, which is
finer than the record clock when passes on different cadences share one record.

So both verdict tags carry the unix second they were measured, and
`RelayVerdictRecord.current` ages on that. A tag with NO stamp is stale, and the
fallback to the event's clock that used to cover those records is gone: it was
the same trap in a smaller costume, since the clock it fell back to is the one
the flush rewrites, so a pre-stamp verdict on any relay still being dialled
could never expire under any TTL. Refusing it costs one re-measure per url,
once. `RelayConsistencyTest` pins both directions by rewriting the record NOW
over a month-old verdict — the shape that flush produces — and over an unstamped
one, asserting each reads stale.

**Forcing a re-measure when the RULES change, not the relay: `FOLD_EPOCH`.** A
verdict means what the procedure that took it meant, and the fold's procedure
has changed repeatedly — comparing a host's urls to each other rather than only
to whichever one led, refusing to call a url distinct on a window too thin, the
reproducibility guard. A record signed before one of those is not a stale
reading of today's rule, it is a reading of a different rule, and waiting out
the TTL does not make it agree: it means a month of applying conclusions we
would no longer draw, with nothing in the store distinguishing them. So the last
element of both tags is the rules version, and `current` rejects anything that
is not the current one — which is exactly the "no verdict" state that makes
`unresolved` hand the group back and `measure` re-take it.

Bump `FOLD_EPOCH` (or `CONSISTENCY_EPOCH`, versioned separately because it is a
separate dial) **in the same commit as any change to what a fingerprint
concludes**, and never for logging, budget or ordering: the cost is a full
re-fingerprint of the store in ONE pass — there is no per-pass budget to spread
it over any more — `DEFAULT_CONCURRENCY` urls at a time, during which every
un-re-measured url is dialled unfolded. Nothing has to be deleted and no
operator has to intervene — `edit` overwrites the old tag with the new answer as
each group is decided.

Both expiry rules live in the tag's tail, so every other writer on that shared
address has to carry five elements forward, not just the tag's name.
`RelayVerdictRecordTest` asserts what `load` DECIDES after quartz's passive monitor
and our own stability pass have each rewritten the record — a writer that kept
the name and dropped the tail would leave a verdict that reads as stale forever,
re-fingerprinting the url every pass while the record on screen looked healthy.

**Reading the store back is `RelayAliases.replace`, not forget-then-adopt.** The
two-step version left a window — the length of a walk over five figures of urls
— in which every fold in the candidate set was missing, and this map is shared
by every stream and by the monitor's pass, all running concurrently. A stream
reading inside that window dials the duplicates for a whole cycle and nothing
ever says so, because the map is correct again by the time anyone looks.
`RelayConsistency.replace` is the same fix on the sibling verdict, made first;
the fold kept the racy shape until the audit that found this. Chains resolve
against the incoming map rather than the live one, so the result does not depend
on the order the store returned verdicts in, and a loop (A says B, B says A)
folds neither edge instead of whichever was read first.

Three things it does NOT do. It never removes a relay on silence, on a window
under `minSample`, or on a failed store read — only a positive measurement
counts, because a wrong exclusion is invisible while a wrong inclusion costs one
relay's duplicates until the next re-measure. And it does not detect the "feed
us events forever" attack at all: a relay returning a consistent 500 passes.
That one is novelty and drain (`PagedFetchResult.drained`, the visit's
activity clock, `LEG_QUIET_GIVE_UP_MS`), not identity.

**A replaceable event has one address and more than one writer, so writing it
is always an EDIT.** NIP-66's relay record is addressed by `d` = the relay url,
and quartz's monitor updates it passively every time a connection is opened —
so the fold and the monitor aim at the same slot. A writer that rebuilds the
record from its own tags deletes everyone else's, and nothing about the result
looks wrong: still signed, still a valid NIP-66 record, just saying less than it
did. Measured in `RelayVerdictRecordTest`, `[d, n, rtt-open]` became
`[d, same-as]`. `RelayVerdictRecord.edit` is the shape to copy — read what is
there, keep every tag this writer does not own, and stamp
`max(now, existing + 1)`, because a store enforcing replaceable semantics
REJECTS an edit that is not strictly newer and two writers inside one second are
ordinary. An edit lost that way reports success having done nothing.

Both quartz writers on that address — `RelayReachabilityStore` and
`RelayProber.toDiscoveryEventTemplate` — used to rebuild too, so their next
flush dropped our verdict tag. Fixed upstream in amethyst #3882 and #3883 and
taken here with the `4f41f16db5` pin; the local repair pass that used to restore
a clobbered verdict on the next fold is gone with it. `RelayVerdictRecordTest`
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
(`RelayVerdictRecord`) — the same monitor that already signs "I could not reach
this relay" saying the other thing a dial can prove:

```json
["same-as", "wss://nos.lol/",    "500 newest events, 498 shared with wss://nos.lol/",          "1776038400", "2"]
["same-as", "wss://nostr.ac/v1", "500 newest events, best 2 shared of 19 peer(s) on this host", "1776038400", "2"]
```

The last two elements are when it was MEASURED and which rules measured it. Both
exist to expire the verdict; see the re-measure section below for why neither is
optional.

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

**Members are compared to each other as well as to the leader, and skipping that
published a lie at scale.** "Not the leader" was being recorded as "its own
relay" — which on a host whose preferred url is a genuinely DIFFERENT endpoint
is a claim about nothing. `/inbox` is the shape: haven splits the inbox from the
public pool, NIP-17 made the split ordinary, and `/inbox` also sorts first under
`PREFERENCE`, so it leads its group and every minted path behind it disagrees
with it *correctly* and was signed as a separate relay for thirty days.

Measured live on `haven.calva.dev`: 7 urls, `/inbox` leading, all six minted
paths at **0.192** against it — and **1.000** against each other. The pass
published six distinct-relay verdicts and dialled seven urls for two endpoints.
`h.codingarena.top`, `relay.dergigi.com` and `relay.shawnyeager.com` are the
same shape in the same pass. After the cross-member pass the same group is
**5 folds, 2 kept** — `/inbox` and one pool url — which is the right answer.

The comparison costs no dial: every print is already in hand when `learn`
returns, so it is set intersections only. Against cluster HEADS rather than
every pair, so a host of genuinely distinct endpoints stays linear in endpoints
rather than quadratic in urls — and `lang.relays.land` (partitioned by language)
and `nostr.ac` (20 paths of different content) must still keep every url, which
is what `a host of genuinely distinct endpoints keeps every one of them` pins.
Because `sameRelay` is symmetric, every cleared url really has been held up
against the leader *and* every other head, so the evidence string can name the
count honestly again.

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

**`measure` groups the WORLD, not its candidate set** —
`RelayVerdictRecord.loadAll` reads every 30166 we still stand behind and
`AliasFolding.adoptWorld` unions the urls those verdicts are about into the set
being grouped. A duplicate is a property of a url next to another one, and the
candidate set is not the whole neighbourhood: a url's siblings drop out of it for
reasons that have nothing to do with the fold — held out as known dead by
`StreamWorld`, gone from the relay list that named them, discovered by a stream
since reconfigured. Grouped from candidates alone the newcomer is a group of ONE,
`unresolved` drops it for that, and it is dialled as its own relay for as long as
it is discovered while a signed record naming the very url it folds onto sits in
the store unread.

The urls this pulls in are free to carry: every one already holds a fold verdict,
so `toProbe` leaves them out of the dials and `learn` skips them for want of a
fingerprint. What they contribute is the one thing the group was missing — a
canonical for `leaderOf` to pick as the yardstick. **Verdicted urls only, and
that bound is load-bearing**: every url this router has written a record about
includes the corpse of every dead one, and pulling those in as group members
would put five figures of connect timeouts into a pass — `heldOutDead` undone
from the inside. `apply`'s `#d` read is unchanged and still runs in front of the
fan-out; only its "a set of one returns immediately" guard went, because reading
a stored verdict back for one url is an indexed query and a caller holding a
single url is exactly who needs to be told it is somebody else's second address.

**The card counts against what arrived UNDECIDED, not against the candidate
set.** `Work.newUrls` is the urls with no verdict when the pass began, read after
the adopt and before the first dial; `unmeasured` is that same population after,
so the pair is a fraction of one set rather than two numbers side by side, and
`probeProgress` draws it with a word — `… new relay(s) checked for aliases`.
Neither half of the older pair described the PASS. The line that prompted this
read `143 of 1,754 relay(s) checked for aliases in 11m`: the denominator was
every url the pass was handed, most of them carrying month-old verdicts nothing
was going to re-ask, and the numerator was every url holding a verdict at all —
including folds made weeks earlier, and in another process. Eleven minutes of
work moved neither. Absent on a pass that does not count it (the stability gate),
where the page falls back to `candidates - foldedAway`. A pass whose subject is
EMPTY says so in words (`PROBE_NONE`, "nothing new to check for aliases") rather
than `0 of 0`: that is not a rare state, it is the one both passes work towards
and hold for most of a monthly TTL, and two zeroes read as a broken pass. Caught
by rendering the real card against a live `/stats.json`, not by a unit test.

> **The engine this next stretch describes is DELETED**, and so is the one that
> outlived it. `DynamicSync`, `DeleteMissingSync`, `RelayRotation`,
> `CachedRelayList` and `LegProgress` went with the two-plane split;
> `StaticBackfill`, `PagingProgress`, `CycleTally` and every phase but
> `rotating` went when the declared-`urls` streams crossed onto the pool too.
> EVERY down stream rides `VisitPool` now (see the router intro above),
> retraction is `RetractionAudit`, and the loader refuses the era's knobs by
> name — `concurrency`, `recycleSeconds`, `authorsPerLeg`, `sync`,
> `SYNC_NEG_MIN_EVENTS`, an ungated scan — each with a migration note.
> The war stories are KEPT because their lessons transferred: the two gates
> became `visitConcurrency` against `tailBudget` (both per stream now, the
> second as `maxLiveConcurrency`); the leg give-up
> (`LEG_QUIET_GIVE_UP_MS`) bounds a visit's quiet ask sequence; the fold's
> `dropFolded` runs in `certifiedScan`; `refusedOutright` ends a visit as it
> ended a leg; the bands survive unchanged. `SharedIdSet` and `CycleTally` do
> NOT — they went with the second wave, so read every mention of them below as
> history too. Read what follows as the record of WHY those rules exist, not as
> a map of the current code.

**The fan-out no longer JOINS, and that is the change to understand before
touching the dynamic engine.** A dynamic stream used to launch every discovered relay,
await all of them, and only then start the next cycle. One relay could therefore
stop a mirror: measured, `fetchAllPages` against purplepag.es never returned at
all before the paging floor landed, and a 16,752-relay stream sat at "cycle in
progress" for the life of the process while every other relay in its list had
long since finished and nothing was due to dial any of them again.

It is a ROTATION now. The stream walks its relay list handing each url to a pool
of `concurrency` workers — `pool.acquire()` happens in the walk, not inside the
worker, which is what makes it a pool pulling from a queue rather than 16,752
coroutines contending for a permit — and a **pass ends when the last url has
been handed out, not when the last worker returns**. A slow relay costs one slot
and nothing else. Four consequences, each with its own home:

- **TWO GATES, and the wide one is not decorative.** `admission`
  (`concurrency * 16`, clamped to 128–512) bounds workers; `pool`
  (`concurrency`) bounds relays actually TRANSFERRING. The guards — strikes,
  the Tor check, the TCP pre-probe — run under the first and never the second,
  because a discovered relay list is mostly corpses and a slot spent proving one
  is dead is a slot no living relay can use. Measured live, with one gate doing
  both jobs: 2,692 urls off real NIP-65 lists, `concurrency = 8` returned **109
  relays in five minutes** — a two-hour pass — while the same list at
  `concurrency = 30` reached **2,349**. The pass was tracking the pool size, not
  the network. Split, the same stream ran ~9x faster and the pool filled with
  transfers instead of connect timeouts. The old code got this right by accident
  (the probe sat outside `gate.withPermit`); it also probed all 2,692 at once,
  which is an ulimit waiting to be found, hence the ceiling.
- **`in flight` and `transferring` are two numbers.** `RelayRotation.busyCount`
  is workers, `transferringCount` is sockets, and on a live fan-out they read
  128 and 8. Publishing the first as "syncing now" claimed a stream was syncing
  128 relays when it cannot sync more than 8.
- **Passes overlap, so a url must not be handed out twice.** `RelayRotation` is
  the busy set: `take` is the claim, `release` goes in a `finally`, and a url a
  worker still holds is passed over and counted as `busy` — a tally outcome of
  its own, because "still going from last time" and "never reached" are the same
  silence otherwise. It is per STREAM; `RelaySockets` is the wider,
  cross-stream socket refcount and stays what it was.

  **It is also the only thing that knows WHICH relays are running**, so the
  claim is stamped and carries the leg's own event counter (`held`, `leg`). The
  bare set published three counts and no url — see the `inFlight` note above.
- **The shared id set outlives the pass that built it.** `SharedIdSet` bounds
  the generations: an ask leases the set it started against, and a new one is
  installed only when nothing older is still being read (`mayInstall`). At most
  two are ever alive. A pass that arrives while a straggler holds the previous
  one reuses what it has — a diff against a slightly stale set costs some
  duplicate downloads that ingest drops, which is the cheap side against a third
  gigabyte-scale list on the heap.

  **The consequence is not obvious from the bound, and is pinned by a test: ONE
  long leg freezes the snapshot for its whole stream.** The straggler holds
  generation A, the next pass installs B and thereby RETIRES A, and nothing may
  be built over an occupied retirement slot — so a ten-hour leg buys every other
  relay on that stream exactly one refresh and then the same set for ten hours.
  Probed rather than assumed: `worthRebuilding` says yes at every hour and
  `mayInstall` says no at every hour but the first. It is logged with the
  offending urls NAMED — `busyUrls()` at the top of a pass is exactly the
  stragglers, since that pass has handed nothing out yet — because "reusing the
  id snapshot" repeating for hours is otherwise a symptom with no subject.

  **The pool is the stream's, not the pass's**, so a straggler does not merely
  get skipped by later passes — it is still holding its slot. A new pass gets
  `concurrency` minus the stragglers transferring, and `admission` minus the
  stragglers holding a worker at all. The number to watch is `concurrency`: at 8
  it takes eight wedged relays for a stream to stop downloading, while it goes
  on logging a pass a minute because the guards keep declining dead hosts and
  `done/total` keeps advancing. A pass that opens on a fully-held pool says so,
  with urls named.

  And a pass is **not** a walk over history: no shared cursor, no lockstep
  across relays, no snapshot advancing mid-pass. The set is built once before
  the walk and is static for it, and each relay's whole outstanding history —
  every leg `bands.legs()` hands back, past and present — is one worker's job.
- **The stream gate moved.** It used to wrap the whole fan-out; on a rotation
  that would be forever, since a stream that never finishes never releases and
  every other id-set stream would queue behind it for the life of the process. It now wraps the snapshot BUILD, so two full store walks
  are still never in flight at once. **What it no longer bounds is residency** —
  a negentropy dynamic stream's set is resident continuously rather than only
  during its cycle, so worst case is one per id-set stream plus one draining
  generation, where it used to be one across the process. The levers if that
  matters for a deployment are `sync = "fetch"` (which every wide stream in
  `router.conf.example` already uses — only `assertions`, at concurrency 8,
  holds a set at all) and narrowing the stream's filter.
- **"The cycle finished" stopped meaning "everything settled".** `pending`
  non-zero at the end of a pass is now the ordinary state rather than a sign the
  cycle was killed, and the phase line carries `running` — relays syncing right
  now, across every pass — beside `done/total`, which is only how far the WALK
  got. A rotation with a full pool and a finished walk was rendering as idle.

Workers are launched into the ENGINE's scope, not a per-pass `coroutineScope`,
so every worker body is wrapped: an escaping exception would cancel the scope and
take every stream, the ingest pipeline and the live tails with it. It used to be
caught one level up and lose only the cycle.

**Measured end to end against live relays**, one seed stream on three real
upstreams filling the store and two dynamic streams fanning out over what it
discovered — 18,687 urls on 2,494 hosts:

```
avatars pass 1 handed out 2692 relay(s) in 5:26; 2050977 event(s) so far
        (6276/s); 30 still running; …; next pass in 5s
avatars reusing the relay list from 5:42 ago — 2692 relay(s), rediscovering after 900s
avatars pass 2 — 2688 relay(s) to hand out, 4 still syncing from an earlier pass
```

Three claims in three lines: the pass ended with thirty relays still on sockets
and did not wait for them; the next one began five seconds later on the cached
list, paying no discovery and no fold; and it handed out 2,688 rather than 2,692
because four legs from pass 1 were still running, which is `busy`. At 18,687
urls the published partition closed exactly on all three streams —
`sum(outcomes) == urls.taken == 18687`, `accountedFor: true` — with `pending` at
12,748 mid-pass, which is what `pending` is for.

**One knob paced two different things, so a 6h refresh also meant 6h of
idling.** Everything between a dynamic stream waking up and its first downloaded
byte is derivation: the discovery scans, a normalisation pass over every url
they carry, `apply` (one `#d` query per 500 urls) and the exclude filter —
minutes on a full store, to produce a list differing from the last cycle's by a
handful of urls. The DOWNLOAD is what should repeat often; that is not.
`recycleSeconds` splits them: the list is held in memory
(`CachedRelayList`, a local in `loop`, per stream by construction) and the next
pass starts that many seconds after the previous walk ENDS — the stragglers of
that walk are still running through the gap, which is the point. `refreshSeconds`
keeps the meaning it always had — how often the sources are re-read — and is now
the cache's TTL. Unset, nothing is held and the loop is what it always was.

Three things about it are load-bearing:

- **The alias fold is the second expiry.** `AliasMonitor.generation()` counts
  verdicts learned this process; a list built at an older generation is thrown
  out. Without it a stream would go on dialling urls a pass has since folded —
  a socket, a band and a gate slot each, for events the survivor in the same
  list is already delivering — until the TTL happened to lapse. The generation
  is read BEFORE discovery runs, not after: a pass that publishes while
  discovery is walking has not been applied to its result.
- **Nothing about DIALLING is cached.** The NIP-66 known-dead set is re-read at
  the top of every cycle and `HostStrikes` is cycle-local, so a reused list
  skips a dead relay exactly as a fresh one does. The cache is a list of urls to
  consider, not a decision about any of them.
- **A cycle gets a FRESH `CycleTally`, with the same provenance numbers.** The
  outcome counters are mutated as urls settle, so handing one object to two
  cycles publishes the second's dispositions added to the first's against a
  `taken` that counts each url once — a partition that cannot close.
  `relayListAgeSec` is published beside them because `discovered` otherwise
  changes meaning silently: on a recycling stream that count can describe a
  store walk from five hours ago, and two identical documents would read as a
  network that stopped changing rather than one nobody re-read.

What it costs is a dial per relay per fan-out instead of per refresh period,
which is a rate against the whole discovered set — not free, and the reason it
is opt-in rather than defaulted.

**A rotation breaks one piece of arithmetic nothing warns about: per-pass work
is now per-`recycleSeconds`, not per-`refreshSeconds`.** The id snapshot is the
case that bit — one build per pass was one per six hours while the fan-out
joined, and became one per five seconds. `SharedIdSet.worthRebuilding` paces it
against its OWN cost (ten times the last build, one-minute floor), so the share
of a stream's time spent walking the store stays near a tenth whatever the
corpus costs. Measured live in exactly that regime — a negentropy stream whose
passes are seconds apart — **50 passes produced 5 builds**, against 50 before.
A WIDER window overrides the clock: the walk is narrowed to what the hungriest
relay needs, so a set built narrow and reused wide is a subset of what the diff
needs and the reconcile re-downloads events we hold. Anything else added to a
pass wants the same question asked of it.

**A fold has to take the earlier sync's state with it.** Nothing dials a folded
url again, so the bands it earned before the fold can never advance — but they
stay in `SYNC_STATE_FILE`, and that file is what `SyncCoverageReport` charts. The
symptom is a working fold that reads as one that never happened: `/stats.json`
listing twelve urls of one host as separately walked while exactly one of them is
being synced. `SyncBands.dropFolded`, called from `VisitPool.certifiedScan` as
the fold is applied to a scan's discovered universe, leaves them out of the
file. Three decisions in it, all of which have a
silent failure on the other side:

- **Dropped, not merged onto the survivor.** A band is a claim about a url we
  walked. A containment measurement is enough to stop dialling a duplicate —
  wrong, that costs a re-download — and not enough to close the survivor's legs
  over ground it was never walked for. What dropping costs is already being paid:
  the canonical was being walked in parallel all along, and ingest dedups.
- **Replaced each pass, not accumulated.** Verdicts carry a 30-day TTL and
  `RelayAliases.forget` drops them when the store stops standing behind one, at
  which point the url is back in the fan-out. A set that only grew would go on
  suppressing the bands it earns after that: dialled every cycle, written to no
  file, re-walked from nothing on every restart, with no error anywhere. The band
  stays in memory either way, so a url that comes back resumes rather than
  starting over.
- **Per stream, plus an explicit `keep`, and the sweep file is left alone.** A
  fold is applied to one stream's DISCOVERED set, so the stream name scopes it —
  except that the name does *not* separate declared urls from discovered ones:
  both may sit on ONE stream and both reach the roster under that name. A
  declared upstream the fan-out folds away is therefore still dialled, still
  recording, and would have had every one of those bands filtered back out — the
  relay syncing while the file says nothing and each restart re-walks its
  corpus. `dropFolded` takes the pinned set for exactly that.
- **Report what left the file, not what is folded.** The count is the urls this
  stream was actually holding a band for. Counting the verdict set instead made
  every restart log a mass deletion of thousands of urls whose state the previous
  process had already dropped — and mark the map dirty for it, rewriting a
  multi-megabyte file to produce the bytes it already had.

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

**The fingerprint's clock is an IDLE window that starts before the connect, so a
`.onion` was never foldable.** Quartz's `idleTimeoutMs` runs from the start of
the fetch — `IdleClock` is constructed with the walk and nothing has bumped it
yet — and the probe was handed `connectionTimeout`, the 20s that sizes a
clearnet TCP handshake. A hidden service is allowed
`SYNC_TOR_CONNECT_TIMEOUT_SECONDS` (90s) for the circuit and the rendezvous
ALONE, for exactly the reason that 20s is the wrong size for them. The two
disagreeing is not a slow probe, it is one that cannot finish: `fetchAll` returns
what it collected when the window lapses, and what it collected is nothing, which
reaches `RelayAliases` as an EMPTY window — not "measured and distinct" but **no
verdict at all**, so nothing folds, nothing is cleared, and every url on that
host is dialled again on every cycle for as long as the router runs. Symptom, off
a live coverage card: three urls of one onion host in *Running now* at once.
`probeIdleMs` gives a Tor-routed url the proxy's budget on top of the clearnet
window (summed, not maxed — one buys the connect, the other is the silence every
relay is allowed while answering) and `SYNC_TOR_ALL` carries it to clearnet urls
with everything else. What it costs is paid by hosts that never answer: a silent
leader is asked four times a pass (bare filter, then the kinds fallback, each
retrying once at the smaller page), so a dead onion group holds a permit for
minutes rather than seconds. WHOSE permit is the part of that which has since
been corrected: it was written when the gate was 16 undifferentiated permits and
Tor was "the handful of relays Tor reaches" — true of the population, and not of
the occupancy. See `DialGate` in the Tor section above for what that cost at
`dialConcurrency = 100` and for the split that replaced it.

**A relay that cannot reproduce its own answers made the fold a coin flip, and
signed the result for a month.** `fiatjaf.com` was the second host found in the
state `espelho.girino.org` is described in above, and measuring it settled that
the case is not rare enough to leave uncoded. What it does, asked directly:

| ask | answer |
|---|---|
| `{"limit":500,"until":anchor}` | **10** events, not newest-first, `created_at` spanning 2024–2026 in one page |
| the same ask again, same anchor, seconds later | 10 events again, **0 of them shared with the first** |
| `{"kinds":[1]}` / `[0]` / `[1,10002]` | 0 events, promptly EOSE'd |
| four bare asks, pooled | 40 events, **all kind 30023, all one pubkey** (fiatjaf's own) |

So it is a personal long-form relay serving an arbitrary ten of its own articles
per REQ whatever limit is asked, and `until` cursoring means nothing against it —
a paged walk ends on the stall heuristic at a different depth every time (120,
157, 168, 182, 190, 202 ids across six walks). The consequence for the fold is
the dangerous one: over a paged walk the url self-scores **0.694–0.720** while
its sibling paths score **0.592 and 0.775 against each other**, so the cross-url
number sits INSIDE the band the url scores against itself. Which side of
`minOverlap` a pass lands on is chance — and landing low publishes two urls of
one relay as separate relays for `DEFAULT_TTL_SECONDS`, during which `measured()`
answers true and nothing re-probes them. A duplicate pinned in the fan-out for
thirty days on evidence a re-run contradicts.

**What `AliasFoldLiveProbe` then said, running the REAL walk rather than a
reimplementation of it — and it corrects the paragraph above.** The numbers, one
line per comparison:

```
nos.lol      leader 500 ids   vs ITSELF 1.000    vs /cipher-zulu 0.998
fiatjaf.com  leader 196 ids   vs ITSELF 0.792    vs /ember 0.664   vs /xenon-lima 0.746
```

Two things follow. The self-overlap bar separates the two hosts cleanly with the
real walk — 1.000 against 0.792 — which is what it is for. But `fiatjaf.com`
FOLDS, and folds every time: five runs, five identical results, 2 aliases in ~11
seconds. The coin-flip framing came from a Python reimplementation whose stall
heuristic ended walks shallower than the real one; the real probe's margin over
`minOverlap` is 0.664 against 0.500, not a knife edge. The guard below therefore
never fires on this host — `result.distinct` is empty on every run — and it is
insurance against the pass that lands the other way, not the reason these two
urls fold. **Do not cite it as the fix for a host you have not run the probe
against.**

`RelayAliases.reproducible` is the guard: before a group publishes any NEGATIVE
verdict, the leader is walked a SECOND time from the same anchor through the same
filter, and unless it hands back `DEFAULT_MIN_SELF_OVERLAP` of its own window the
group is forgotten and nothing is published. Three things about it:

- **Paid only on the negative path.** A group that folded cleanly is making the
  safe claim and costs nothing extra; the second walk is one dial on groups that
  were about to call something a separate relay.
- **0.9 sits in an empty gap, not at a round number.** Stable relays self-score
  0.998 (nos.lol) and 1.000 (nostr.oxtr.dev); the two unmeasurable ones score
  0.435 and 0.694–0.720. Nothing has been measured in between.
- **It is not `minOverlap` and must not be merged with it.** That bar compares
  two DIFFERENT urls and is deliberately generous, because two dials seconds
  apart on a live relay legitimately disagree at the edges. One url against
  itself has no such excuse.

The host stays in the fan-out, unmeasured and re-probed. Dropping such a host
from the fan-out entirely is a separate policy question — note that
`router.conf.example` DOES ask for kind 30023, so this one is not pure cost, it
is a relay we cannot walk coherently whose events are carried elsewhere.

**Three more ways a url was permanently unmeasurable, all found auditing the
above and all the same silence from outside:**

- **AUTH — and read the A/B before repeating the claim this started as.** A relay
  gating reads behind NIP-42 answers the fingerprint's REQ with `CLOSED
  auth-required`, which `fetchAll` treats as terminal and returns EMPTY on while
  `RelayAuthenticator` — attached to the very same client — is still signing the
  challenge on that socket. `AliasProbe.over` now asks through
  `fetchAllWithHooks` with `pendingOnAuthRequired = true`. **But the claim that
  auth-gated relays were therefore unfoldable is false, and `AliasFoldLiveProbe`
  disproved it**: against `auth.nostr1.com` the leader comes back with a full
  500-id window either way — `true` in 0:21, `false` in 0:02, twice each. With
  the flag off the CLOSED ends page one at once and the empty-page retry at
  `FALLBACK_PROBE_PAGE` re-asks on a socket that has since authenticated, so a
  fallback meant for page-size refusals recovers an auth refusal by accident.
  The flag is kept as insurance rather than as the fix: that accident only covers
  the FIRST page of a walk, so an auth refusal on any later page truncates the
  window silently. Note the fold is built only when there is a signer, and so is
  the authenticator — a pass is never unauthenticated by configuration.
- **NULL AND EMPTY collapsed in the live wiring.** `AliasProbe` is written around
  "returns null when the url could not be asked at all — which is NOT the same as
  an empty page", and `over()` used `fetchAll`, which returns a list whatever
  happened. So a relay that never spoke arrived as one holding nothing, and the
  empty-page retry at `FALLBACK_PROBE_PAGE` was paid on every dead url — two idle
  windows instead of one, and at the Tor budget that is minutes. `doneOut` tells
  them apart now: EOSE *or* a CLOSED means the relay spoke (so the smaller-page
  retry a `blocked: limit too high` needs still happens), and only `cannot:` or a
  window that lapsed in silence is null.
- **A failed store read unfolded the fan-out, silently.** `AliasFolding.adopt`
  forgets every verdict before adopting what comes back — that is what makes the
  store authoritative and the 30-day TTL mean anything — on the documented
  promise that a failure arrives AS a failure. `RelayVerdictRecord.load` swallowed
  a failed chunk into an empty result instead, so one unlucky query silently
  unfolded up to 500 urls for that cycle. `load` throws now, and
  `AliasFoldingTest` pins it against a store that refuses every read.

**A fingerprint is a websocket, and quartz closes none of its own.** `fetchAll`
unsubscribes and leaves the connection in the pool; the client's keep-alive only
ever RECONNECTS. `RelaySockets` (the `AliasFolding.Sockets` implementation) is
the only thing in this repo that closes a dynamic relay's socket, and the fold
originally never called it — so a pass left one socket per url it measured, up
to `probesPerCycle`, against a dispatcher budget of 1024 for the whole process
and **20 per host**. The fold probes widest group first, which is precisely the
host wearing 55 urls. The refcount is claimed before the dial and released in a
`finally`, and it has to be SHARED — one count across every stream, probe pass
and pool visit — because that refcount is what stops a probe closing a socket
a transfer is still running on.

**And the release must NOT reach for `getOrCreateRelay`, which is the trap
inside the trap: that call is a CONSTRUCTOR.** The release path read
`client.getOrCreateRelay(url).disconnect()`, which looks like a no-op on a url
with no connection and is the opposite. `NostrClient` reconciles its pool
against the relays its live subscriptions want (`updatePool`, off a flow sampled
at 300ms), so a probed url normally leaves the pool moments after the fetch's own
`unsubscribe` — and a `getOrCreate` after that puts a fresh `BasicRelayClient`
back into it, subscribed to nothing, so nothing ever removes it again. The
60-second keep-alive (`reconnectIfNeedsTo`) then dials every disconnected relay
the pool holds, and `disconnect()` clears the backoff on its way out —
*"this is not an error, so prepare to reconnect as soon as requested"* — so the
backoff does not hold it back either.

Measured on staging: 105 threads in `okhttp3.internal.ws.RealWebSocket.loopReader`,
of which 5 were the static stream upstreams and about 100 were left over from
probe passes reporting `passesRun: 1` — i.e. FINISHED — the oldest silent for
over ninety minutes against a 20-second idle budget. **The leak was per RELEASE,
not per dial**, which is why it grew with passes that had already reported
themselves done. The fix is a pool-membership check first
(`client.availableRelaysFlow()`), and there is nothing to close when it fails:
a url quartz has already dropped was disconnected on its way out. What this
repo cannot do is REMOVE the entry — `RelayPool.removeRelay` exists and the pool
is private to `NostrClient` — so the remaining case is a url still in the pool
because our release beat the 300ms reconcile, which is the case where
disconnecting is exactly right. `RelaySocketsTest` pins it on pool membership,
because an entry in the pool is what the keep-alive walks.

**The other half of that question needs no probe, and answer it FIRST.** The
verdict is a signed kind-30166 addressed by the url, served by this relay:
`["REQ","v",{"kinds":[30166],"authors":["<this relay's pubkey>"],"#d":[<the urls>]}]`.
A `same-as` pointing elsewhere means the fold DID remove the url and what the
card showed was a leg outliving the pass that dialled it — `inFlight` spans
passes by design and nothing cancels a running leg when its url folds. A
`same-as` pointing at itself means measured and kept. No record means never
measured, which is when `AliasFoldOnionProbe` earns its run. Read the tag's last
two elements before concluding anything from it: a verdict whose rules version
is not the current `FOLD_EPOCH`, or whose measured-at is over a month old, is
one the router has already stopped acting on and is queued to re-take — the url
is being dialled BECAUSE the record is there to be replaced. The verdicts panel
on `/stats.html` draws exactly that as `expired`.

`maxRelaysPerList` (config, per stream) drops an event naming more relays than a
relay list plausibly holds — measured, 148 pubkeys published a kind 10002 of
100–10,591 entries. It **used to cost the source its tag projection**: a
per-event limit needs the event, and `distinctTagValues` returns values already
flattened across every event it matched. That was not a corner case — the NIP-65
select is exactly the shape the projection claimed, so before this was wired the
cap silently did nothing on the one stream it exists for (a live run discovered
222 urls from a seeded 200-entry list with the cap set). Now that every source
pages off the index it costs nothing at all: the paged walk is the only path
where the event is still whole, and it is the only path. Redundant default ports
(`:443` on `wss`, `:80` on `ws`) are folded by string in
`RelayDiscovery.normalize` rather than by probe — that one needs no evidence.

**THE TAG PROJECTION IS A CORPUS WALK, AND `RelayDiscovery` NEVER TAKES IT.**
`distinctTagValues` rides the store's `document/v1` visit carrying a selection
expression: the predicate runs per document with no index behind it, so it costs
the same whether the answer is 364 events or 364 million — and it costs that
ONCE PER TAG NAME, because the projection answers about one tag. Measured on
`vespa-eventstore-staging` (2026-09-02, #182), reading the 364 kind-10040
declarations in a 319,426,563-event corpus:

```
/search/         yql=select id from sources * where kind=10040     0.0058s
/document/v1/event/event/docid?selection=(event.kind==10040)      75.0260s
```

12,800x, and the monitor's 10040 source names 38 delegation tags, so one
derivation pass was 40 corpus walks — on the DOCUMENT API, which is where the
ingest dedup probe queues. `/search/` stayed fast throughout, which is why a
health-check query issued by hand answered instantly while the mirror was dead:
the shape that made #167 unreadable from outside.

Every source now takes one paged read off the search index, answering all of its
selects in that single walk. **There is no size rule and deliberately so.** The
two costs scale differently — paging is a function of the MATCH SET, the visit
of the CORPUS — so the match count at which the visit would win rises with the
store: ~100k events at 319M, ~33M at 10^11. Reaching it means parking the
document API for the ~6.5 hours a walk of 10^11 documents takes, which is not a
price any relay list is worth. A relay-list kind is measured in millions and
this corpus is heading for hundreds of billions; the gap only widens, so a rule
here could only ever answer one way. An earlier cut of this fix did carry one,
priced in a corpus-wide `count(Filter())` per pass — machinery to decide a case
that does not arise.

It also ended a trap nobody could see from the config: a BOUND select could
never take the projection (it hands back a set of values, and the pairing a
binding exists to keep is gone by then), so `authors = 1` was quietly the fast
path. The same 38 delegation tags appear twice in `router.conf.example` — bound
on the `assertions` stream, unbound under `monitor { sources }` — and only the
unbound copy was expensive.

**THE URL NORMALIZER IS MEMOIZED PER SPELLING** (`RelayUrlCache`). A relay-list
scan asks it the same few thousand strings once per author — a corpus with
19,844 known relay urls hands it those over millions of events — and each ask
was a trim, a whitespace scan, two prefix tests, a full parse through quartz's
`RelayUrlNormalizer`, an onion test, a loopback test and a port strip. **Quartz
has no cache of its own here**: `RelayUrlNormalizer` is a stateless companion of
pure functions, and the only interning it ships is `EventInterner`
(`ConcurrentHashMap<String, WeakReference<Event>>`, keyed by event id, for whole
events). This is that pattern pointed at urls. `allowOnion` is deliberately NOT
part of the key — it is a property of the deployment, not of the string, so the
entry carries whether the url is an onion and the gate is applied on the way
out; `RelayUrlCacheTest` pins that both orders of asking give different answers
from one cache. Bounded and dropped whole at the cap, because the keys come from
strangers: anyone's kind 10002 can name urls nobody has seen.

**WHAT A RELAY-LIST SCAN STILL OVER-READS, and what it is actually worth.** The
store's `EventYql.SUMMARY_FIELDS` is `id, pubkey, created_at, kind, tags,
content, sig, owner` on every recall. Discovery needs `id` and `created_at` (the
scan's cursor), `kind` (a select may name one), `pubkey` (the `EventPubkey`
binding slot) and `tags`. So `content`, `sig` and `owner` are dead on arrival —
about 200 bytes of a ~700-byte relay list, or **~30%** of the transfer and the
JSON decode. Not the several-fold win it looks like at first, because the cursor
and the binding slots need most of the rest.

Fixing it is a STORE change and cannot be done from here: a fourth
`document-summary` in `event.sd` beside `dedup` / `idtime` / `idtimetag`, plus a
recall path passing `presentation.summary`. The schema's own note says adding a
summary class is "a deploy-only change: no re-feed, no reindex" — but unlike
those three this one could not be attribute-only, since `tags` is summary-only
and still reads the disk summary store. Worth doing when the store is next
touched; not worth a pin on its own.

**RUN AGAINST REAL DATA, END TO END** (`RelayListLiveProbe`, 2026-09-02): the
364 kind-10040 declarations pulled off `wss://search-staging.brainstorm.world`
— the same 364 #182 counted — fed into a real Vespa in Docker and read back
through `router.conf.example`'s own `monitor { sources }` block, all 38
delegation tags:

```
discover    19 relay(s)      0.2242s   one indexed walk
projection  31 raw value(s)  4.9009s   38 corpus walks
```

**21.9x on a corpus of 364 events**, which is the size at which the visit looks
its BEST — its cost is the corpus, so a laptop-sized store is the one place it
is nearly free. The same comparison on the staging corpus was 12,800x. The two
paths named the same relays: nothing the scan found was missing from the
projection, and the 8 raw values the scan dropped are all loopback or `http://`
— `ws://127.0.0.1:7777`, `wss://localhost/relay`, and the `http://localhost:7778`
that `RelayDiscovery.normalize`'s KDoc already cited from a live 10040.

Pulling that data needs two things worth knowing. The relay speaks NIP-42 and
answers a plain REQ with `restricted: no kind 10040 for you here` — it serves
through a web of trust and a throwaway key has no provider list — so the probe
signs an auth event and then re-asks with the escape the relay's own notice
names, the NIP-50 `include:spam` token, which is what returns the corpus
unranked.

`RelayListReadCostBench` prices both walks against a real store and prints the
scan's ms/event, which is the number to watch as a relay-list kind grows: the
corpus walk is settled, but paging millions of whole events to read one tag off
each is the next thing that would want a store-side answer (a search that
projects `tags`, rather than a visit).

