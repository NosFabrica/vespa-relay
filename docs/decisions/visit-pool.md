# Visit pool decisions

The history behind `sync/.../VisitPool.kt`, moved out of the source so the
code reads on its own. One paragraph per decision; `git log -L` on the
function finds the commit.

**The unit of work is a (relay, stream) pair, not a relay.** When a visit
served every stream's asks in turn, a relay's slowest stream became
everyone's: fifty provider asks for `content` ran before `indexers` got its
one, both shared a revisit timer, and the status page could not say which
stream a relay was on. Bands are keyed by (stream, url, filter), so splitting
per stream also removed the only shared write.

**Worker count is the sum of the streams' dial widths.** The first 440-relay
run let the width equal the whole socket budget and 436 dials timed out inside
a minute: a thundering herd against its own connect timeout. A visit is
seconds long, so the width bounds the burst, not the throughput.

**Filter widths are shared with the pager.** A kind cap learned on a visit
but not on the audit's fallback REQs left every width-capped relay's audit
refused exactly as before (issue 185).

**The tail's identity includes the kind cap.** A cap is learned from a
refusal, which changes nothing on the roster, so a tail opened at full width
before the cap was learned kept a filter the relay refused, on a subscription
it had already closed, while reporting `tailed`.

**A refused walk with nothing delivered ends the visit.** Re-opening the same
conversation once per remaining leg cost an idle window of silence per leg;
one relay with hundreds of bound authors held a worker for five hours.

**Only a re-fetch pays a cap.** A catch-up runs inside a visit, legs one at a
time, so the dial width already bounds it. A re-fetch is due-gated,
independent of the visit rate, and can put every band on the walk at once.

**Audit dueness is checked before taking a permit.** Most asks are inside
their period, and a cap of four was being spent on four map lookups while the
one relay whose week was up went away with a `deferred`.

**The sweep's range is the whole filter, not the band's gap.** A relay that
back-fills behind a catch-up leaves the band claiming ground the store is
missing; a sweep narrowed to what the band does not cover can never find it.
Reconcile compares id sets, so a range that already agrees costs round trips
and no events.

**`rewalksCovered` judges against the leg's own kinds.** A band holds one span
per kind and quartz emits one leg per kind group. Comparing a kind-30023 leg
against edges folded from kind 1 filed ordinary catch-up as a re-walk, which
then took a `refetchConcurrency` permit and was skipped when the cap was full.

**One tail per stream, charged to that stream.** When one subscription carried
every wanting stream's filter and was charged to each, the budgets summed to
more than the sockets held, and eviction picked the globally weakest tail: a
low-volume stream lost every tail to a firehose relay whose own budget was
free.

**`earnTail` re-takes the permit rather than handing it over.** Another opener
of the same stream can win it in between; the loser gets one revisit delay.
Closing the race would mean `dropTail` returning the freed hold on a path four
other callers share.

**The in-flight and live lists are published whole.** Twenty rows against
128 workers showed a sixth of the answer with an `omitted` nobody reads.
Both lists are bounded by configuration (workers, live budgets), not by
discovery.

**`flushPhases` walks each collection once.** Gathering per stream on a
5,000-relay roster with three streams allocated ~15,000 throwaway keys and
~300,000 comparisons per flush, at 1 Hz through the boot storm.

**Audit progress reports the window's `since`.** Reporting `until` showed a
sweep with years left as `back to <today>` and only moved once a whole window
finished.

**Abort reasons are counted, not logged.** The one abort that had a log line
produced two lines against ~4,400 aborts in twenty minutes, which read as a
pool whose visits almost never failed. See `VisitAborts`.

**A walk stalled by our own ingest queue is not a relay refusing us.** quartz
drains each socket through one consumer coroutine that awaits every listener,
and `IngestPipeline.submit` suspends when the queue is full, so a parked hook
silences every subscription on that socket. The pager then ends `IDLE` or, on a
first page whose event went into the parked hook, `UNPAGEABLE` with nothing
downloaded, and both satisfied `refusedOutright`. On staging that was 90% of
76,485 aborts in eight hours, filed as `abortedQuiet` and `abortedUnpageable`
against relays the monitor had graded `prime` and which answered the same leg
by hand inside two seconds. Those two endings are now re-read at the instant
the walk returns against `IngestPipeline.parkedOn`, and filed as
`abortedBackpressured`, which never writes the relay's row. `CLOSED`,
`AUTH_REQUIRED` and `CANNOT_CONNECT` came through the same consumer, so it was
not parked when they were said. See `docs/router-internals.md`.

**A walk our queue stalled pauses instead of ending.** The pager's idle window
counts the time a hook of ours sits parked in the full queue as the relay's
silence, so a burst the store took longer than thirty seconds to absorb ended
the walk. A cut walk keeps a band only where it touches the one already held,
and a catch-up walks down from now toward that band, so the cut kept nothing
and the next visit downloaded the burst again from the top, refilling the queue
with duplicates. The walk now waits for the parked hook to clear and the queue
to fall to half (by count, and by bytes where `queueBytes` bounds them), then
re-asks from the oldest second it reached (`until`, inclusive, its re-served
events dropped rather than counted twice), and counts the walk once in
`walksPausedByIngest`. The resume is bounded both ways it could fail to help:
each resume must start older than the last, since a burst denser in one second
than half the queue, or a page of only implausible `created_at`s, would
otherwise re-ask the same ground forever; and every pause of one walk draws on
one `LEG_QUIET_GIVE_UP_MS` budget. A walk that runs out either way ends the
VISIT as `abortedBackpressured` — delivered events or not, and without waiting
for the cut's acks, which would wait on the same queue — so it releases its
permit and socket claim instead of parking once more per remaining chunk, leg
and band. Whether the stall was ours is decided when it is seen, before the
pause: by the time a pause gives up the hook may have got in, and the ending
read then was filed as the relay's.

The cause is in quartz's pager, whose idle clock ran while our hook was parked;
vitorpamplona/quartz#32 stops it while `onEvent` runs, and the quartz pin
carries it since c63c35ca76 (v2.4.0): a full queue no longer ends a page at
all, and this pause is the fallback for the stalls the pager still reports (a
socket a hook outside the walk is parked on).

**A walk waits for the store's verdicts, but not forever.** A band is recorded
only once the store has answered every event the walk handed to ingest
(`IngestAcks`), and that wait had no bound. On staging one batch pass sat on a
store query that never answered for 4 h 40 min (NosFabrica/vespa-relay#289):
nine visits whose walks were long over waited in it, each holding its slot and
socket claim, each row still reading `catching up (paging)` with a frozen
cursor, the same picture as a relay that had stopped answering. The quiet
give-up could not fire, since it is checked between asks. The wait is now bounded by
the same `LEG_QUIET_GIVE_UP_MS` silence a visit gives a relay, and the row reads
`walk read, waiting for the store's verdicts on its events` while it waits. A
walk cut there records no band and ends the visit as `abortedUnstored`, ours,
never on the relay's row; the revisit starts at the same ask rather than the
next, since the relay refused nothing. A late verdict is still counted and
changes nothing. The ingest pipeline bounds its own share of that wait too: a
drop-probe is cut at `PROBE_TIMEOUT_MS` (60 s, against a measured
`versions.pre` mean of 737 ms and max of 10.3 s) and the batch goes on
unprobed, since neither the store's query client nor Vespa's query profile
bounds a query. The bound is on the whole probe, not each query, because what
it protects is the pass. The time a visit spends waiting is taken off its quiet
clock, so the next ask does not read the store's silence as the relay's. A
retraction audit whose recovered events are unanswered deletes nothing.

**A visit is not dialled into a full queue.** The download would park its
first event, silence the socket, and come back `abortedBackpressured` an idle
window later, having cost the relay a handshake and a REQ for nothing — per
unit, per revisit, 96 at a time, for as long as the store is behind. Skipped
like a refused dial permit and counted as `visitsHeldByIngest`; open tails stay
open, since a tail that is not draining is honest backpressure.

**A cut visit resumes where it stopped.** Restarting every visit at the first
ask meant a unit cut at ask `k` never reached the asks after it. The next
visit starts at the ask that gave up, or just past the one that was refused.

**The tail offers an event only its author's asks.** Scope is re-derived per
event, and a discovery unit holds one ask per bound author, so a full scan
matched hundreds of filters that named someone else. Authors compare exactly,
so the index finds what the scan did.

**An eviction hands its permit over.** Releasing the evicted tail's permit and
then asking for one let another worker's `trySpare` take it in between, so the
eviction cost a tail and the evicting unit still had none.

**A band's drain settles it at the band's own floor.** `drainSettlesThePast`
compared every chunk with the stream filter's floor, and a younger re-fetch
band's legs stop at its window edge, so no younger band could ever record
`complete`: its bottom was re-walked on every visit and the prime-relays table
held every banded pair at `paging`. The walk now passes the ask bounded by the
band's older edge. Nothing below that edge is ever the band's to owe, because
the edge only rises with `now`; the oldest band's edge is the filter's own, so
an unbanded stream is unchanged.

**A visit turned away before it dialled retries within seconds.** The workers
are the streams' visit shares summed and draw from one queue, so a worker that
draws a unit whose stream's share is full is routine, and so is a full ingest
queue. Both returned, and the unit then waited out a revisit timer sized for a
relay that had just been visited: five minutes at no yield. Live, a roster
admitting 76 units at once was refused 103 visits in its first ten minutes, and
49 of the 76 ended the run with no band. Nothing was dialled, so nothing is owed
a wait: the unit retries after `TURNED_AWAY_RETRY_MS`, and a timer standing from
an earlier visit is disarmed so it cannot hold the unit to the longer one. Each
try counts in `deferred` or `visitsHeldByIngest`, so both now count tries rather
than units. The next live run with both fixes ended with 13 of 65 units
unbanded.

**An evicted unit's prompt visit does not evict in turn.** Eviction requeues the
loser at once so it catches up on what its tail would have carried, and that
visit ended in `openTail`, where `earnTail` compared yields again. A visit's own
catch-up adds to its relay's yield, re-delivered events included, so between two
low-yield relays the one just visited always won: live, the pool counted 279
evictions in ten minutes, nearly all of them three quiet relays trading one
tail. The prompt visit now takes only a spare permit; its next ordinary visit
may evict again, so a pair can still trade a tail, at the revisit cadence rather
than back to back. The next live run counted 25 evictions.

**The tails and the status projection live beside the pool, not in it.**
`VisitPool.kt` had grown past 1,300 lines holding three jobs: the visit
itself, the live tails with their yield-scored eviction and permit hand-over,
and everything the status page reads. `TailBook` now owns the tails, their
yields and the eviction marks; `PoolStatus` owns the in-flight and live rows,
the per-relay table, each stream's limits, schedule and phase, and the
progress counters, reading the pool's state and writing none of it. The pool
keeps the visit, the roster and the workers.

**The band bounds the legs, never the key.** Coverage is quartz's, keyed by
`(relay, filter)`, so a filter carrying the band's window took a new key every
time the window moved with `now`: every band was re-keyed daily, its coverage
orphaned and its whole span re-paged from scratch, for every relay, three
bands over, against a `refetchThePastSeconds` meant to re-page that ground
monthly. The key is the band-qualified stream and the stream's own filter,
both of which stand still, and the window is intersected into the legs after
they come back, so a day costs the day the window gained at the top.

**Retraction audits are counted apart from sweeps.** They ride the sweeps'
clock and pool, but summed into `negentropyRuns` they made seven finished
sweeps read as thirty-seven; `retractionRuns` carries them.

**A band is recorded only once the store has answered every event the walk read.** A walk handed
its events to ingest's queue and recorded its band as soon as the relay had sent the page, so for as
long as the events sat in the queue the band claimed ground the store did not hold. A process that
stopped then lost them for good: the band said covered, and nothing walked it again. Each walk now
tracks its submissions (`IngestAcks`) and waits for their outcomes before `record`, `recordCut` or
`recordAudit`; the retraction audit does the same before its `record`. A walk with any event the
store failed to take records nothing and counts in `bandsWithheld`, and the next visit walks it
again. The wait is one ingest round trip per chunk, since a chunk records once at its end, not per
page. `VisitPoolAckGateTest` pins both directions, and fails on the old order.

**A drained walk of a closed window records the window, events or none (issue 291).** Quartz's
`SyncCoverage` records only kinds that produced events, and claims up to the newest one. On a
past-only stream (`backfillMutesReports`, kinds 1984 + 10000, an `until` in the past) both failed:
a relay holding none of the kinds drained an empty page and recorded nothing, so it stayed
`neverRun` and was walked from the floor on every visit, and a relay that did hold some kept a
newer leg `(newest, until]` it could never fill, so 72% of the stream's visits went to relays
already marked complete. `withDrainedWindow` now hands quartz the evidence the walk holds: every
kind the chunk named, over the whole chunk, complete when the walk reached the floor. A chunk that
reaches the filter's own past `until` claims up to the moment it was asked, because the filter
matches nothing above its bound and quartz's newer leg is inclusive at `max`. An open chunk (no
`until`) is unchanged: its forward edge stays the newest event, because late arrivals land below
the present. Quartz itself is not changed, so this needs no bump. `ClosedWindowBandTest` pins it.

**A past-only ask holds no live tail.** A tail opens at `now - TAIL_OVERLAP_SECONDS`, so an ask
whose `until` is below that is an inverted window that matches nothing. With
`maxLiveConcurrency = 1` the stream's one live permit sat on such a tail; one visit was measured
in `draining queued heals, then the tail` for 508 s. `TailBook.open` drops these asks, and opens
no tail when nothing is left.

**A visit that completes a band says so once.** Aborts have a log line and successes had none, so a
stream's progress could not be read from the log. `router: visit … settled [...]` prints when a
band first turns complete, so it fires at most once per band rather than once per visit.

**An ask a relay refuses as policy is unreadable, not owed (issue 291, Q3).** Of the issue's failing
relays, many refuse the ask as policy, and their answer never changes: `restricted: this relay only
serves kinds …`, `blocked: kinds 10000 not allowed`, `invalid-filter: only wallet kinds are
allowed`, `blocked: … must have 'h', 'e' or 'a' tag`, or a NIP-42 gate that will not take our
identity. Each revisit asked again, logged an abort, and left the pair in `neverRun`, so a past-only
stream's counter had a floor it could not get below. `StandingRefusal` reads the ending and the
relay's sentence: NIP-01's refusal prefixes and a "kind not allowed" phrase count; anything about
our load (`rate-limited:`, `blocked: already 50 subscriptions`, "too many") never does, and width
refusals still go to `FilterWidths` first. Two such refusals make the ask unreadable. Other endings
neither add a strike nor reset the count, because mostro-p2p.tech was measured alternating its
`kind not allowed` CLOSED with silence; only a clean read clears it. While the verdict stands, the
pool skips the ask (no dial, and no tail carries it), and the schedule counts it under `cannotRun`,
the column `CannotReconcile` already uses. The per-relay row keeps the relay's sentence after the
abort itself is cleared. The verdict is persisted, so a restart does not spend two more refusals to
learn it again. It lapses after a day, one REQ against a policy that rarely moves, and then the
next standing refusal restores it, since the strikes are kept until a clean read.
`UnreadableAskTest` pins it, and fails without the skip.
