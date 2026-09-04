# Ingest decisions

The history behind `peers/.../ingest/` (`IngestPipeline`, `BisectingInsert`,
`ProbeGate`, `ParseAudit`, and the `refused/` filters), moved out of the source
so the code reads on its own. One paragraph per decision; `git log -L` on the
function finds the commit.

**`submit` suspends; it never blocks.** It used to call `trySendBlocking`
because quartz's subscription callbacks were not suspending. Those callbacks
run on the shared coroutine pool, and so does the store the drain must reach,
so a parked producer held a thread the drain needed. Measured twice, ~13
minutes after each start: all 64 shared workers parked under `trySendBlocking`,
the process silent at 2% CPU with Vespa idle. Giving the workers their own
thread pool was the first attempt and did not hold on its own, since the
store's loop body reaches for `Dispatchers.IO` regardless.

**`queued` is incremented before the send and taken back on failure.** A
post-send increment lost the race with a worker that took the event and
decremented first, and the health line read `ingest queue -1/4096`. The
rollback is in a `finally` because `send` also throws `CancellationException`
on shutdown.

**`submitted` counts arrivals, `accepted + rejected` counts the drain.**
Staging sat with the queue pinned at 16,400 of 16,400, `bottleneck: ingest`
and `0 ev/s`, and every reading was consistent both with a store that had
stopped answering and with producers that had stopped producing, because the
rate counted what came out of a batch. Nothing counted at the entrance.

**Queue depth is bounded at both ends, apart from the batch.** The capacity
was `batch * 4` with only a floor, so raising the batch to 20,000 sized the
queue at 80,000 events and the heap went over. `MAX_INBOUND_QUEUE` is 16k,
a few hundred MB at Nostr's event sizes.

**The batch cap is a cost, and it is left in place.** `batchSize` is capped to
the queue's fair share per worker (2048 at eight workers, whatever
`SYNC_INGEST_BATCH` says). The original argument was fairness: a wider batch
lets one worker take everything. That is true and it does not matter, because
the store takes one writer mutex for the whole of `commit`, so commits never
run in parallel. `IngestCostBench`'s shape sweep on identical 100k work:
`8 x 1024` (a real batch of 512) ran at 6,685 ev/s with 99.1s of aggregate
`lock.ingest.wait` to perform 0.2s of writing; `2 x 8192` ran at 60,492 ev/s.
The cap stays because every other writer (verdict edits, the healer, the
sweep) queues on that mutex, `RelayVerdictRecord.EDIT_DEADLINE_MS` already
exists for that wait, and nothing has measured how a wider batch lengthens it.
The lever is `SYNC_INGEST_CONCURRENCY=2 SYNC_INGEST_BATCH=8192`, and `start`
says so when the cap bites.

**Duplicates are dropped before the signature check.** A schnorr verify costs
~48µs/event isolated and 70-95µs in situ, because the router shares cores with
the engine it feeds, and a mirror paid it once per copy: a popular event held
by 40 discovered relays was verified 40 times to be stored once. Measured
against a real Vespa (`IngestCostBench`, 20k-event batches) a batch of
duplicates went from 56µs/event to 21, with the `verify` stage disappearing
from the stage line. The cost is that junk colliding with our corpus no longer
shows as `bad signature`; that is junk this relay was never going to store.

**The id probe is gated on batch width and on its own hit rate.** The per-id
price falls with width as the round trip amortises (23µs/id over 4k ids,
11µs over 20k) while a verify is flat, so at a single chunk's width the probe
is a wash; `PROBE_MIN_VERIFIABLE = 128` is where the wash sits. A dropped
duplicate saves ~33-44µs and the probe costs 11-23µs per id, so the gate stops
paying below roughly a third (`minHitRate = 0.35`). For the version probe:
29µs/event against the ~123µs a dropped stale replaceable saves (158 to ~35),
so break-even is ~20%.

**`ProbeGate` measures the drop rate instead of being told the phase.** The
alternative was to thread a catch-up-versus-live flag through seven submit
sites and probe only while backfilling. Measuring needs no flag, is right
about legs nobody classified (a negentropy reconcile delivers only events we
lack, so both probes are dead weight there), and cannot be wrong about a
stream whose behaviour changed since it was labelled. The counters halve above
a million judged so a backfill's 94% cannot keep a probe on through the quiet
months after it.

**Superseded replaceables are dropped before verification too.** A newer
generation of a profile is a different id, so the id probe calls it new, it
pays full verification, and only then does the store reject it as `replaced`:
158µs/event measured, of which `versions` was 38% and `verify` 32%. Under the
outbox model different relays hold different generations of the same address
permanently, and negentropy cannot converge them away because it reconciles
ids while a replaceable's identity is its address. One production backfill ran
at 94% replaced-or-duplicate. Addressables stay the store's business: their
version query is an (authors x d-tags) cross product, silently truncated where
hits are capped, and a truncated answer here is a dropped event.

**`dropSuperseded` reports its drops to the refusal sink, after `verifyId`.**
The refused-id filter and the healer are fed by exactly one signal, a store
refusal, and the fast path exists to stop the store producing it for the
commonest case; reporting only from `insertIsolating` left both configured,
reporting zero, doing nothing. Nothing on this path is verified yet, so an
unchecked id would be attacker-chosen: forge a kind 0 for any pubkey with an
old `created_at`, stamp it with the id of an event this relay should never
fetch, and two of those suppress that id permanently. `verifyId` closes it for
~1.5µs against the 48-95µs a signature costs, and the signature is not needed
for this class because supersession is decided inside the hashed content.

**The bisecting insert has a fixed write budget.** A bulk write fails as a
unit, so one bad event cost the whole batch: 999 good events lost per bad one
at the default size, with no retry. Isolating k bad events out of n costs
about `2·k·log2(n)` writes, so a budget of 64 covers three in a 1000-event
batch, past the rate seen in practice. What it really bounds is the store-wide
case (a full disk, a dead engine), where every half fails all the way down and
isolation would turn one failed write into ~2n at the worst moment.

**`lostToStore` is its own counter and `rejectReasons` is capped.** A schema
drift once lost 2.3M events while every status line read healthy, because the
loss blended into the duplicates. The same run showed that store throws embed
per-event content (a Vespa 400 quotes the document), so an uncapped reason map
would have retained 2.3M distinct strings during the one incident where heap
was already the thing to protect; `poisonSeen` is capped for the same reason.

**`wedged` is defined by the workers, not by the queue.** Issue 167 presented
as a full queue with every ingest thread apparently idle, because a suspended
worker has no frame in a thread dump; `busySince`, `inBatch` and
`oldestBatchMs` exist to settle that from outside the process. The predicate
first also required the queue to be at its ceiling, which sent a wedge behind
a slow upstream (every worker held, queue part full) to `mixed`, rendered as
"keeping up". Nothing here ends a wedge: cutting a pass discards a batch of
good events that nothing re-offers before the next full resync.

**`WEDGE_AFTER_MS` is ten minutes, and the first number was wrong.** Two
minutes was derived from `IngestCostBench` on a ~500k-document corpus whose
slowest throughput was ~2,400 ev/s, a 17x margin. On a ~200M-document store
with 67 concurrent visits, ingest oscillated between ~11,400 ev/s and 136; at
that floor two workers on 8192-event batches take 120 seconds, the threshold
exactly, and `oldestBatchSec` was seen at 43 in a healthy sample. Dedup cost
scales with the index, so the bench could not have found it. A false `wedged`
retires the word as fast as a router that cried "keeping up" through an outage.

**Store calls are booked round the whole probe, not per chunk.** What an
operator needs from `oldestBatchSec` at 794 is the call the worker is
suspended in, and the worker is suspended inside a fan-out the store owns.
The write is booked apart from the two probes because they fail for unrelated
reasons: a probe waits on the query path, the write on the writer mutex.

**The per-batch origin map is built only when the sink reads it.** The sink is
inert unless `SYNC_REFUSED_DIR` is set and the pipeline is shared by every
stream, so an unconditional map made every deployment allocate and hash one
entry per event for a lookup that never happened; `RefusalSink.tracksOrigins`
is the switch.

**`rejectionReasons` publishes counts, not prose.** A mirror rejects most of
what arrives because it is offered each event once per relay holding it: 7.9M
rejected against 524k accepted on the run this came from. The split between
"already have this", "a newer version exists" and a bad signature is what makes
that readable, and it existed only inside a log string.

**Two cuckoo filters per epoch, and the candidate gate is the safety property.**
A first refusal only makes an id a candidate; it is downloaded again and must
be refused a second time before it is suppressed, so a `needIds` id that
reappears because a fetch died mid-transfer can never suppress anything. A
false positive in the candidate filter does not cost a download: it promotes
the id on its first refusal instead of its second, which is still an id the
store refused. At ε ≈ 1.9e-9 that fast-track is expected ~0.2 times per 100M
refusals.

**A cuckoo filter, not a Bloom filter, with a 32-bit fingerprint.** A Bloom
filter sized for 50M holding 500M keeps answering with a double-digit
false-positive rate, discarding a sixth of everything and logging nothing;
the cuckoo filter refuses the insert once the relocation chain gives up near
95% load, and its accuracy is load-independent (`ε ≈ 2b/2^f`). With `b = 4`
and 32 bits, `ε ≈ 1.9e-9`: a 100M-event backfill loses ~0.2 events. A false
positive is silent, permanent loss, which is why the fingerprint is not the 8
or 16 bits of a textbook. `MAX_KICKS = 500` is the figure from the paper.

**`contains` is lock-free.** The sync process is the sole writer, `add` is
synchronized because relocation moves other fingerprints, and the worst a read
racing a relocation can see is a fingerprint mid-move, which reports absent and
costs one re-download. A failed relocation chain likewise leaves one entry
homeless: a possible false negative, never a false positive.

**`MAX_BUCKETS` is `1 shl 26`, clamped rather than thrown.** At 4 slots x 4
bytes that is 1 GiB plus the header; the next power of two is 2 GiB + 32, past
`Int.MAX_VALUE`, where `FileChannel.map` throws and the slot offsets overflow.
The cap used to be `1 shl 28`, which turned a large
`SYNC_REFUSED_EPOCH_CAPACITY` into a crash on the first refusal. An oversized
capacity now gets the biggest partition we can build plus the SEALED warning.

**The seal warning quotes the real ceiling, and the default capacity is a
measured size.** Bucket counts round up to a power of two, so a partition asked
for 8M ids seals near 15.9M; quoting the request told an operator to raise a
number that was never binding. That rounding is also why 8M per epoch is 64 MiB
per table and 128 MiB per epoch with the candidate filter beside it (8.4 B/id,
not the ~4.3 B/id the structure costs at a lucky bucket count), in page cache
rather than heap; an earlier figure understated it by two.

**Epochs are partitioned by `created_at` and reopened at construction.** The
set being remembered grows forever while any fixed structure does not, and an
epoch entirely below the lowest `since` any stream asks for can be dropped
exactly, where a timer-based rotation is a lossy guess. Epochs are opened
lazily by `record`, so after a restart every lookup missed until a fresh
refusal reopened the partition; the partitions on disk are adopted in `init`.

**`suppressedInWindow` walks the epochs that exist.** An open-ended window
(`since = null`, the ordinary `deleteMissing` case) starts at epoch 0, so
counting from `lo` to `hi` probed every epoch index since 1970 per id: 0.57 ms
per call against 0.0007 ms, paid once per id in `diff.needIds`, thousands of
ids per relay per sweep.

**The health line prints the candidate count.** Suppression only begins on an
id's second refusal, so a healthy gate spends its first cycles with nothing
suppressed, indistinguishable from an inert mechanism. A live three-cycle run
ended at 23 candidates and 0 suppressing, where the line alone could not tell
the two apart.

**Suppression is off without `SYNC_REFUSED_DIR`.** The whole mechanism is
opt-in, and a router not given somewhere to keep its filters behaves as it did
before they existed rather than keeping a heap-only set nobody asked for.
Setting the capacity too low is loud (the epoch seals and says so), not silent.
Operator sweeps (`SWEEP_ORPHAN_SCORES_ON_START`, by-kind reclaim) never reach
the refusal path; they are designed to be re-downloaded.

**The parse audit swallows the quartz report it attributes.** One log line per
malformed profile across a multi-year backfill is what buried the real logs;
the report file is the output. The flusher is joined on close, not merely
interrupted, because a flusher caught mid-`writeReport` shares the temp path
and the loser's half-written file could be renamed over the report.

**Parked producers are counted in `submit`, per relay, only while the send
suspends.** The count first lived in the visit pool as a wrapper around its
three hooks, which missed the two producers outside it on the same shared
sockets (`RetractionAudit`, the monitor's `StreamWorld`) and counted the fast
path too, so a tail's hook merely passing through at the instant a genuinely
silent relay's walk gave up read as a stall of ours. `trySend` first, then the
count around `send`, keyed by `origin.url`; `parkedOn` and `isFull` are what
the pool reads.
