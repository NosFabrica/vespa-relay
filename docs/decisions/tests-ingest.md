# Ingest test decisions

The history behind the ingest, refused-id, serving-pressure and stats-snapshot
tests, moved out of the sources so the tests read on their own. One paragraph
per decision; `git log -L` on the test finds the commit.

**An event the store holds is dropped before it is verified.** Schnorr
verification is the most expensive thing ingest does per event, about 48µs,
and a mirror is offered the same event once per relay that holds it. The id
probe and the version probe run ahead of `verify()`; the other half of the
bargain, pinned beside it, is that nothing unverified is ever written.

**The fast path reports the refusals it makes.** `dropSuperseded` keeps a
stale replaceable away from the store, which is where the `replaced` verdict
used to come from. Without a report from the fast path the refused-id filter
and the healer went quiet in proportion to how well the optimisation worked;
a backfill running at 94% replaced fed them almost nothing.

**A wedge is every worker held, whatever the queue depth.** In issue 167 all
eight ingest workers suspended inside store calls that never answered: the
queue sat at 8206/8192, the health line called it backpressure, and a thread
dump showed every ingest thread parked in `LinkedBlockingQueue.take`, since a
suspended coroutine has no frame. The first test asserted that an unfull
queue is never a wedge, which encoded how 167 presented rather than what a
wedge is; behind a slow upstream every worker can be held with the queue
nearly empty.

**An open-ended refused-id lookup costs the epochs on disk.** The first
version counted from epoch 0 to `epochOf(now)` and probed each index: at a
one-day epoch that is over 20,000 map misses per id, paid once per id in
`diff.needIds`, thousands of times per relay per sweep. Measured at 0.57ms per
call against 0.0007ms for a narrow window. `since = null` is the ordinary
shape of a deleteMissing ask, so this was the case that actually ran.

**Test ids for the cuckoo filter are real hashes.** The filter slices its
bucket out of the id's first 16 hex characters and its fingerprint out of the
next 8, with no hashing of its own. A counter formatted as `"%064x"` is all
zeros across both slices, so every such id lands in one bucket with one
fingerprint and they are all hits on each other.

**An oversized cuckoo capacity clamps.** Before the clamp `bucketsFor`
promised 2^28 buckets, a 4 GiB table, and `open` raised "Size exceeds
Integer.MAX_VALUE" from inside the ingest path: a capacity chosen by an
operator decided whether the router ran at all. An operator asking for more
than one partition can give gets the biggest partition plus the SEALED
warning when it fills.

**Empty kind-0 content is not a parse failure.** "Expected start of the
object '{', but had 'EOF' instead" was the second-largest class in a live
parse audit, 3,783 of 77,753 reports. quartz cdef4e9658 reads an empty
content as an empty profile, which is what it always meant.

**A stats staleness notice is cleared only by the tier that left it.** Charts
had been failing for hours beside counters forty seconds old, publishing every
minute, and every one of those publishes cleared the notice that said so. The
page then drew a document indistinguishable from a healthy one.

**A healthy read on the production corpus is about 400ms.** That is a normal
read against 52M documents, the baseline the serving-pressure tests feed
before a slow read; a threshold near it would throttle the mirror on ordinary
traffic.

**The ingest batch is capped at `capacity / workers`.** The pipeline bounds a
batch to its share of a queue sized for memory, so at eight workers a batch
cannot exceed 2048 however high `SYNC_INGEST_BATCH` is set. The cost bench's
shape sweep carries a one-worker row for that reason: with one writer mutex
per commit, a lock hold is worth `batch x (1 - dropRate)` survivors.
