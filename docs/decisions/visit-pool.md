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

**A visit is not dialled into a full queue.** The download would park its
first event, silence the socket, and come back `abortedBackpressured` an idle
window later, having cost the relay a handshake and a REQ for nothing — per
unit, per revisit, 96 at a time, for as long as the store is behind. Skipped
like a refused dial permit and counted as `visitsHeldByIngest`; open tails stay
open, since a tail that is not draining is honest backpressure.
