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

**Only a verified event shadows another inside a batch.** Winners were first
picked before verification, by id and by address: a forged kind 0 with a later
`created_at` and a junk signature beat the genuine one, which was dropped and
reported `replaced` to the refusal sink, and then the forgery failed its
check, so the address lost its real version and the real id was on its way to
suppression. A bad-signature copy arriving first likewise shadowed a good copy
of the same id from another relay. Now each id's copies and each address's
versions (newest first) are one contest, verified in that order until one
passes; the rest drop unverified. The common case still costs one
verification per contest, and the store-version probe still runs first, since
the version it compares against is already verified.

**A group of copies is placed by a copy whose id hashes its content.** Copies
were grouped by their claimed id and the group placed by its first copy's
kind, author and stamp, none of them checked. A forged copy carrying a genuine
note's id while claiming an old kind 0 by an author with a stored profile took
the note down with it: 130 such copies ahead of 130 genuine notes stored none
and reported all 130 `replaced`, on the way to suppressing them. The leader is
now the first copy passing `verifyId`, and the copies ahead of it count as bad
signatures. Copies that share a checked id share their content, so the leader
speaks for the group, and its signature check skips the hash it already paid:
the common case costs what it did. A trusted stream's copy leads unhashed and
is hashed only before it is reported.

**A batch pass that throws loses its batch, not its worker.** The worker loop
was guarded only by its caller, so a `StackOverflowError` from the parse audit
on deeply nested content ended a worker for the life of the process, and every
batch after it waited on the survivors. The pass now catches `Exception` and
`StackOverflowError`, counts what it had not yet tallied as an `ingest fault`
rejection, and continues; any other `Error` still ends the worker loudly.
Refusal bookkeeping (the sink, the outcome loop) is caught on its own, because
a throw there used to read as a failed write and rewrite an accepted batch.
An outcome loop that dies partway (only an `Error` gets past the sink's guard)
books the outcomes it had not reached as an `ingest fault`, so
`accepted + rejected` still meets `submitted`.

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

**Only a stamp a walk can ask for opens an epoch.** `retireBelow` once had no
production caller, so epochs only accumulated, and any validly signed event
with an attacker-chosen `created_at` opened a fresh pair of tables. An epoch
now opens only for a stamp between the floor (the lowest `since` any stream
asks for, never above quartz's `PLAUSIBLE_FLOOR`) and a day past now, with
`MAX_EPOCHS` as a backstop for a short `SYNC_REFUSED_EPOCH_SECONDS`; anything
else is `OUT_OF_RANGE` and costs at most one re-download. The window only
widens upward while the process runs, so retirement happens once, at boot,
over what is on disk.

**Only the floor deletes an epoch at boot.** Boot also retired every epoch
above a day past now, files and all, so a container started before NTP set its
clock deleted current epochs and their suppressions. An epoch ahead of the
clock is now left on disk unopened, and the first refusal that reaches it once
the clock catches up adopts its tables. Skipping rather than loading keeps an
epoch a forged stamp opened before the bound out of memory and out of
`MAX_EPOCHS`, at the cost of re-downloads until that first refusal.

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

**`dropSuperseded` steps aside for a batch carrying a deletion or a vanish,
and reads before the writer lock.** On the store's replay path an event's
fate depends on its position among the others in the batch, so deciding a
winner here for a batch that also carries a kind 5 or kind 62 could leave an
address empty; the whole batch goes to the store as it is. The check is on
the kind, not the type, because an Event that skipped quartz's factory is a
plain Event. The comparison itself is NIP-01's own, the rule the store's
version stage applies, but it runs before the writer lock: a NIP-09 deletion
of the newer version landing inside that window drops an event the store
would have taken, until the next full resync re-offers it. Accepted, because
the window is one batch wide and the loss is recoverable.

**A probe that throws teaches its gate nothing.** The dedup and version
probes used to catch `Throwable`, answer "nothing held", and record that
answer to their `ProbeGate`, so a store briefly refusing queries read as a 0%
hit rate and dropped the probe to one batch in 32 long after the store
recovered. A failure now skips the gate, is counted beside it on the
`ingest probes` stats line (`id N failed`), and is logged at most once a
minute. An `Error` is no longer the probe's to swallow. A probe that keeps
failing pauses (`ProbeTrouble`: 5s, doubling to 5 minutes, reset by an answer)
rather than costing every batch a round trip to a store that is already
struggling, which is exactly when probes fail.

**`VESPA_DEDUP_CHUNK` is read with the rest of the ingest sizing.** It was
read in `IngestPipeline`'s companion initializer, so a bad value surfaced as an
`ExceptionInInitializerError` on the first batch rather than a boot refusal,
and a test could not set it. It now rides `IngestTuning` from
`RouterConfigLoader.fromEnv`.

**`ParseAudit` keeps at most 256 distinct findings.** Its key is quartz's
message normalized, and quartz's templates quote the offending field's value,
so a stream of profiles each with a different malformed value minted one
finding per event and the map grew for the life of the process. Past the cap
a report is counted in one `overflow` finding without samples, and every
message is cut at 200 characters. `close()` hands `Log.sink` back to the sink
it wrapped, so a closed audit stops swallowing quartz's log.

**`RefusedIds.record` decides on what the add reports.** It checked the
candidate table and then added to one table or the other with nothing held
between, so two relays refusing one id at once both found no candidate, both
added it, and the id stayed a candidate with `candidatesAdded` counted twice.
The cuckoo `add` is already atomic per table and says `PRESENT` when it lost
such a race; that answer now promotes the refusal to a suppression.
`retireBelow` is private, since only the boot calls it, and
`CuckooFilter.close` says it only flushes: a `MappedByteBuffer` cannot be
unmapped, and the filters are mmap'd in place, so `writeDurably` has no part
in them.

**The relay's publishes take the mirror's pipeline.** The relay wrote through
quartz's `IngestQueue`, one writer committing one batch of up to 64 at a time
with every signature checked, while the mirror's `IngestPipeline` had grown the
store's costs into it: several workers, so one plans while another commits;
the id and version probes, so a resend or a stale replaceable never costs a
signature check or a write; and the bisecting insert. The two diverged because
quartz's `LiveEventStore` took the concrete `IngestQueue`, and the OKs and live
fanout ride on it. Quartz's `EventIngest` is the seam now, the pipeline
implements it, and `RELAY_INGEST=queue` keeps the old writer for A/B runs. On
the first 100k of relay.ditto.pub's kind 3s ([ingest-benchmark.md](../ingest-benchmark.md)),
one publisher went from 427 ev/s to 620, with 21% less engine CPU per event.

**Each publish hears exactly one outcome, in the store's words.** A drop the
pipeline decides before the store (stored id, stale version, bad signature) is
answered with the `RejectionReason` the store itself would have given, so a
publisher cannot tell which side decided. A second copy of an event in one
batch follows the copy that is written: `duplicate:` once that one is accepted,
its own failure otherwise, never an `OK true` for an event that was not stored.
A pass that throws answers whatever it had not answered as `Failed`, in a
`finally`, which is what makes "exactly once" hold through a fault.

**A resend is acknowledged without its signature being checked.** The id probe
drops what the store holds before verification, so a forged copy of a stored
event's id hears `OK true duplicate:`. That is a true statement about the id,
and the alternative is a schnorr check per resend on the relay's hottest path:
resends are the common case for a publisher whose OK was late.
The same holds inside one batch with no probe at all: a forged copy that
arrives after the genuine one follows it and hears `duplicate:`.

**What else a publisher can see differently from the queue**, all of it the
pipeline's batching rather than a different rule, and none of it changing what
is stored:

- An older replaceable version in the same batch as a newer one is answered
  `OK false replaced:` and not fanned out; the queue wrote both in turn and
  answered both `OK true`.
- Two workers commit in parallel batches, so one connection's EVENT and the
  kind 5 deleting it can commit in either order: the EVENT may hear
  `blocked: deleted` where the queue accepted it first. The store ends the same.
- An expired event the sweeper has not reached yet, resent in a probed batch,
  hears `OK true duplicate:` (the id is held) where the store would refuse it.
- A publish that races shutdown hears `OK false error: relay shutting down`, as
  quartz's queue answers it.

**The queue in front of the workers is one batch per worker deep for the relay**
(`IngestTuning.queue`), not the mirror's four batches with a 4,096 floor. Every
waiting publish is a parsed event in the heap, up to the relay's 256 KiB frame
limit each, and a publisher's own window already buffers what it sends; the
mirror's depth rides out download bursts the relay does not have. At the
defaults that holds 2,000 waiting plus 2,000 in two batches, against 6,096.

## One writer: the sync forwards to the relay

**The relay is the only writer, and the sync reaches it as a client.** With both processes
writing one index, every admission check the store makes (dedup, supersession, the NIP-09 and
NIP-62 guards) is query-then-write inside one process's writer lock, and nothing orders the
relay's writes against the sync's: a replaceable published to the relay and the same address
mirrored by the sync can both pass their version check, and a kind 5 stored by one can miss
the event the other is admitting (vespa-eventstore `docs/multi-node-consistency.md`, "Multiple
WRITERS are the threat"). The sync also paid an ingest stack of
its own (feed client, batch handler, trust drainer, keying migration) that the relay already
runs. So the sync keeps everything that only reads (the dedup and version probes, the band
clocks, the audits) and sends its writes over a plain NIP-01 socket (`RelayWriter`). A Nostr
EVENT, rather than a private RPC, so the relay's rules, signature check and live fan-out apply
to mirrored events exactly as to anyone's publish, with no second path to keep in step.

**The mirror lane is chosen by who signed in, not by a port or a header.** `MirrorLane`
tags an event when its connection authenticated (NIP-42) as the relay's own key, which only a
process holding `RELAY_NSEC` can do; the tagger runs last in the policy stack, so a refused
event leaves no tag. Tagged events go to a pipeline of their own, so a backfill filling its
queue never makes a client's publish wait. Both processes need the same `RELAY_NSEC`, and the
sync boot reads the relay's NIP-11 `self` and stops on a mismatch: with the wrong key every
write would still be stored, through the publishers' lane, and nothing would say so.

**The AUTH is signed for `RELAY_URL`, whatever the socket dials.** Compose dials
`ws://relay:7777`, and the relay's NIP-42 check reads the AUTH's `relay` tag against its own
addresses. Rewriting the tag on the signing side (`signedFor`) keeps an internal address a sync
setting only. The socket is held open with Quartz's `keepConnected` (amethyst #4591, which replaced a
subscription that matched no event): Quartz drops a relay from its pool once nothing wants it, and
each redial is a new challenge whose first events could beat the AUTH. Those are still written, on the publishers' lane under the same rules, and
counted as `unsignedBatches`.

**OKs read back as the store's own outcomes.** The relay answers a store rejection in the
store's words and a duplicate `OK true duplicate:`, so `outcomeOf` maps them back and the
pipeline, the refusal filter and the band gate read a forwarded write as they read a local one.
What may succeed later (`error:`, a `rate-limited:` the stream already waited out,
`auth-required:`, no answer) is `Failed`, never `Rejected`: a `Rejected` id can end up
suppressed, and a `Failed` one only withholds the band, so the walk retries it.

**Deletes go as a signed ephemeral request** (`MirrorDelete`, kind 29391), because the
retraction audit's delete-by-id has no Nostr command: it removes what an upstream stopped
serving, without the authors' kind 5. The relay answers it in its event-command hook, ahead of
the policies, and only for its own key, a fresh `created_at`, and explicit ids (at most 1,000),
so a request can never stand for the corpus. Ephemeral, so nothing stores it.

**Not `WriterTopology.SINGLE_WRITER`.** With one writer the store's guard-owner cache would be
exact, but the store measured what it buys (vespa-eventstore `benchmark/README.md` §2bb): no
difference on `batchInsert`, −4.5% on per-event inserts, and a one-time exhaustive guard-set
scan before the first insert that grows with the corpus, which at 211M events is a stall, not a
saving. The correctness the single writer brings (one lock ordering every write) comes from the
forwarding alone, so the relay stays on `SHARED_STRICT`.

**A delete counts only when the hook answers it.** A relay that runs no `MirrorDelete` hook (no
`RELAY_NSEC`, or a build before the hook) takes the request for an ordinary ephemeral event and
answers `OK true`, having deleted nothing; the audit used to log the delete as done. The hook now
answers `mirror-delete: N id(s) purged` and the writer accepts nothing else, and the sync refuses
to boot against a relay whose NIP-11 states no `self`.

**The relay's final policy refusals suppress the id, forwarded only.** The refusal filter counted
only the store's permanent answers, so an event over the relay's tag or size limit, or outside its
allow lists, was fetched and forwarded again on every re-fetch and reconcile (1 in 200,000 of the
production slice, all of a write-restricted relay's foreign events). In relay mode those count too
(`PermanentRefusals.isRelayPolicy`), never as a repair; a bad signature, a clock check and
unstorable text stay retryable. Suppression itself stays opt-in (`SYNC_REFUSED_DIR`).

**One back-off, not two.** Forwarding, the sync's ingest no longer yields to `SYNC_PRESSURE_URL`:
the relay's mirror lane yields to the same client reads, measured in-process, and both pausing put
the back-off twice on every batch.

**The relay checks the signatures, the sync only chooses between copies.** Forwarding, the sync
checked every signature and the relay's ingest checked it again (`RelaySession` submits each EVENT
with `skipVerify = false`, on either lane), so a fresh backfill paid each schnorr check twice. Now an
event that nothing else in its batch contests (one version, one copy) goes to the relay with only
its id checked against its content, and the relay's `invalid: bad signature or id` is what the
pipeline hears, tallies as a bad signature and leaves retryable. A contest, two copies of one id or
two versions of one replaceable address, still verifies here: only a verified candidate may beat
another, and the relay cannot choose for us, since it sees only the copy we send. A forged copy
ahead of the genuine one would otherwise be forwarded, refused, and take the genuine copy (answered
as its duplicate) or the older genuine version (answered `replaced:`, and suppressed) with it.
`StoreWriter.checksSignatures` carries the switch; the local writer says no, since the store holds
unsigned rumors by design. Local mode is unchanged.

**Forwarding is the only mode.** `SYNC_INGEST=local` stayed the default while relay mode proved
itself, which left the race above open on every deployment that did not opt in. It is gone: the
sync always opens its store read-only and writes through `RelayWriter`, `RELAY_NSEC` is required
there, and a deployment still setting `SYNC_INGEST=local` stops at boot with the reason rather
than quietly changing who writes. What only local mode needed went with it: the sync's own strict
`VESPA_FEED_BATCH` read, its feed compression and batching in compose (it sends no documents; its
get by id and visits still read through `VESPA_FEED_URLS`), and the switch that let its ingest
yield to `/pressure`. `StoreWriter.local` stays, because the relay's own pipelines write through
it. Measured before the switch, on 200k production events, forwarding cost 20-26% of a fresh
backfill's throughput, almost all of it the mirror lane's small batches (about 43-71 events per
store write against about 1,000 locally); filling them is the open lever.
