# Proposal: events we will never store, re-downloaded forever

**Status:** the shape is decided — **both** Fix 2 (push the update back to the
source) and Fix 3a (a tombstone for when that push is refused). Nothing is
implemented yet; the sizing, the rejection classification and the epoch
partitioning are still open. Fix 1 and Fix 3b remain as written: one free, one
an upgrade nobody has justified yet.

## The loop

Negentropy reconciles **id sets**. Our side of that set is whatever
`snapshotIdsForNegentropy` returns, which is whatever is *stored*
(`StoreWindowIndex.entriesFor`, `NegentropyPager.kt:95`).

The store refuses several classes of event at insert and will refuse them
identically forever — `REPLACED`, `DELETED`, `VANISHED`, `EXPIRED`
(vespa-eventstore `ingest/BulkRecordInsert.kt`). Those ids are therefore
permanently *absent* from our id set. The upstream has them. So every reconcile
computes the same non-empty difference, transfers the same events, the ingest
pipeline verifies them, and the store rejects them again
(`IngestPipeline.kt:250`). Next cycle, the same. Nothing anywhere carries the
memory that the answer was already decided.

The dominant case is replaceable and addressable kinds, and the shape of it is
narrower than an earlier draft of this document claimed — the correction matters
because it sizes everything downstream.

**A NIP-01-compliant relay holds only the current version per (kind, pubkey, d).**
strfry's `writeEvents` deletes the loser outright, as read in
[Prior art](#prior-art-what-strfry-does). So a compliant upstream is not offering
us the whole history of an author's profile; it is offering us **its** current
version, which is stale only because we already hold a newer one from somewhere
else. The loop is therefore **one event per (relay, address) per cycle**, not one
per historical version — bounded by relays × addresses, not by relays ×
addresses × edits. Deep version histories only exist at relays that archive on
purpose.

That is still enormous across a 16k-relay fan-out and millions of addresses, and
it still never converges. And the *tombstone* set grows faster than the per-cycle
loop does, because churn keeps minting new stale ids: relay A's v3 becomes v4
next month, v3 is gone from A, and we have already recorded it. **Kind 3 remains
the worst case** — a follow list is rewritten on every follow and unfollow — but
by churn rate rather than by any relay hoarding versions.

Two consequences worth carrying forward: Fix 2 is worth more than first stated,
because pushing our winner at a compliant relay resolves that pair permanently;
and the filter needs to hold distinct stale ids *in circulation over time*, which
is far less than "every version ever published".

**This is the same shape as the NIP-09 case** already written up in
[router.md](../router.md#mirroring-the-deletions-themselves): *"it is the stored
tombstone that the re-download is checked against and rejected by."* That
sentence describes the storage side working. It is also an admission that the
**download** side does not — the tombstone stops the write, it never stopped the
transfer. Replacement doesn't even have the storage-side memory: there is no
tombstone, the older version is simply swept
(`BulkRecordInsert.kt`, `scheduleRemove`).

And it falsifies a claim the router docs currently make:

> Leave [`since`] unset and every cycle reconciles the filter's whole history —
> cheap enough over negentropy, which diffs against what we already hold.
> — [router.md](../router.md)

Cheap only if the diff converges to empty. For replaceable kinds it converges to
"that relay's current version, wherever it differs from ours", which is a floor,
not a transient.

## What this costs today — unknown, and that is the first thing to fix

The sample log in router.md shows `402,113 rejected [duplicate: already have
this event x401980]`. That is **not** this bug — a duplicate *is* in our id set,
so negentropy never asks for it; that line is a `fetch`-mode fan-out and the doc
is right to call it the system working.

The number that matters is the `REPLACED` tally, and specifically the *repeat*
`REPLACED` tally, and neither is broken out anywhere. Everything below is sized
against a number nobody has. See [Step 0](#step-0-measure-first) — it is cheap,
and it is the only honest gate on the rest.

## Prior art: what strfry does

Read from the source at `hoytech/strfry@9acdaeb`, because strfry is the reference
NIP-77 implementation and Doug Hoyte wrote negentropy itself. **It has the same
bug and does not address it**, which is worth knowing before designing around an
assumption that upstream will.

- **A superseded replaceable event leaves no trace.** In `writeEvents`
  (`src/events.cpp`) an incoming older version gets
  `EventWriteStatus::Replaced` and is dropped with nothing written. When the
  incoming version wins instead, the stored loser goes onto `levIdsToDelete`, and
  the deletion loop calls `updateNegentropy(…, false)` — **actively removing the
  id from the negentropy tree** — then `deleteEventBasic`. Nothing anywhere
  records that the id was ever seen. Next sync, the peer offers it again.
- **Its NIP-09 tombstone has exactly the limitation this proposal describes for
  ours.** `Event__deletion` is not a side table: `golpe.yaml` defines it as an
  index derived from stored kind-5 events, keyed `eventId + pubkey`. `writeEvents`
  checks it and sets `EventWriteStatus::Deleted`. It blocks the **write** and
  cannot touch the download — and being an index over `e` tags it carries no
  `created_at` for the target, so it could never be placed in a negentropy tree.
  That is candidate 4 above, confirmed from an independent implementation.
- **`Event__replaceDeletion` is an address-level watermark** —
  `sha256(a-tag) + created_at`, blocking any addressable event at or before that
  time. The watermark design considered and rejected here for the download side,
  used by its author exactly where it works (refusing writes) and nowhere else.
- **`cmd_sync`'s dedup is per-run and in-memory.** `seenHave` / `seenNeed` are
  `flat_hash_set`s that dedup within one invocation and are forgotten at exit.

**Why it hurts them less: strfry's router is not a negentropy router.**
`src/apps/mesh/cmd_router.cpp` is REQ-stream based — `dir` up/down/both over live
subscriptions, no negentropy anywhere in it. Negentropy lives only in the
one-shot `strfry sync`. So strfry never runs repeated reconciles against
thousands of relays on a cycle, which is precisely the workload that turns this
from a wart into a floor. The bug is shared; the exposure is ours.

**One finding that supports Fix 2.** strfry *does* honour replacement on write —
the winner deletes the stored loser and updates the negentropy tree — so pushing
our newer version at a strfry upstream genuinely heals it. Given strfry's share
of the network, Fix 2's convergence assumption holds for a large slice of it, and
the "archival relays keep both copies" caveat is narrower than stated above.

**One thing worth envying, and it is a different problem.** strfry keeps
**persistent, incrementally-maintained negentropy BTrees** per configured filter
(`NegentropyFilter` config → `negentropy::storage::BTreeLMDB`, updated inside
`neFilterCache.ctx` on every write *and* every delete). When a sync's filter
minus `since`/`until` matches a configured one, it reconciles straight off that
tree with `SubRange` applying the time bounds; otherwise it scans into a
`storageVector`. That is the server side of the behaviour AGENTS.md already
documents from the client side — "a shape that stays byte-identical rides their
index". It answers our *snapshot cost* problem, the one `NegentropyPager` and the
stream semaphore exist to manage, and it answers nothing in this document. Worth
its own proposal.

## Three fixes, and they compose

Fix 1 stops asking. Fix 2 removes the events being offered. Fix 3 remembers our
refusal when neither of the first two can reach. They apply to different
populations of upstream and are worth having together.

### Fix 1 — stop asking the question (free, no new state)

A negentropy reconcile over the *full history* of kinds 0/3/10002 asks for set
equality on a corpus where exactly one version per author is wanted. AGENTS.md
picks `negentropy` for those kinds because "the same event lives on many
relays" — true of the *current* version, and precisely false of the history.

The lever already exists: the stream's `since`. Set it to how far back a
genuinely new version could plausibly be published — weeks, not years — and let
the sync bands own the deep history as a one-time walk instead of a per-cycle
one. This does not fix the general case (an old event genuinely new to us still
has to arrive once), and it interacts with the known multi-kind band bug in
AGENTS.md, but it removes most of the re-transfer for zero new state.

**Do this first. It may be enough**, and Step 0's numbers will say.

#### `latestOnly` — the same idea for `fetch` mode

`since` narrows the *window*. A `fetch`-mode stream can be narrowed on the axis
that actually matters instead, with a per-stream flag: for replaceable and
addressable kinds, **decompose the ask into one filter per address with
`limit: 1`**, batched many-to-a-REQ.

`{"kinds":[0],"authors":["<one>"],"limit":1}` returns that author's newest and
structurally cannot return a superseded one — on a compliant relay *or* an
archival one, with no protocol extension and no cooperation required. It is the
only NIP-01 construction that expresses "latest per address"; a bare `limit` on a
multi-author filter is newest-first *globally* and gives no such guarantee.

Costs, all of them real:

- **Filter count scales with the author set.** NIP-11 advertises `max_filters`
  and relays enforce it, so the batching has to respect it and fall back. The
  fan-out already chops author sets with `authorsPerLeg`, so the shape is not new.
- **It does not apply to regular kinds**, which have no "latest per address" to
  ask for. The flag must be a no-op outside replaceable/addressable, not an error.
- **It should record no sync band.** A band claims a `created_at` range was
  walked; a `limit: 1` per-address ask claims something entirely different, and
  writing one would tell the next cycle a range is covered when it was never
  swept. Either skip the band or give this ask its own coverage record.

Where it applies it is strictly better than a tombstone, for the same reason
Fix 2 is: nothing is transferred and nothing has to be remembered.

### Fix 2 — heal the upstream: push the update back to the source

Instead of remembering that we refused an old version, **send the upstream the
thing that supersedes it**. A relay that implements NIP-01's replaceable rule
drops its old copy on accepting ours, the sets converge, and the difference is
gone — for us, and for every other mirror that would have paid the same
transfer. O(1) state instead of a gigabyte, and it fixes the cause rather than
the symptom. strfry honours replacement on write
([Prior art](#prior-art-what-strfry-does)), so this converges against a large
slice of the network.

**And it is the only fix that survives our own state loss.** A tombstone is
private memory, and it is exactly as durable as one disk: rotate an epoch, retire
a generation, wipe `/var/lib/vespa-relay`, redeploy onto a fresh volume, or
invalidate a band and re-walk from scratch, and every id it was suppressing comes
straight back. A healed source stays healed through all of that — the stale event
no longer exists to be offered. It also fixes the pair for every other mirror on
the network, which no amount of local memory can do. **Fix 3 is a cache; Fix 2 is
a repair.**

**Four triggers, and they are not equally sensitive:**

| we hold | we push | author asked for this? |
|---|---|---|
| a newer **replaceable** event (0, 3, 10002, …) | the newer version | no — inferred from the outbox model |
| a newer **addressable** event (30xxx) | the newer version | no — same |
| a **kind 5** deletion whose target they still serve | the kind 5 | **yes**, explicitly |
| a **kind 62** carrying `relay = ALL_RELAYS` | the kind 62 | **yes**, to every relay by name |

The right-hand column looked like the policy question, until the trigger
settled it. **The heal can only fire at a relay that already holds the author's
data** — the trigger is the store rejecting *that relay's own stale copy*, so by
construction the push never introduces an author to a new relay. It changes the
**version** a relay serves, never the **distribution set**. The consent that
placed the data there has already been exercised; we are synchronising state the
relay already carries, in the direction the author moved it. And for the two
retraction kinds it is stronger still: a kind 5 and an `ALL_RELAYS` kind 62 are
requests the author already addressed to every relay, and a mirror that holds
one and declines to propagate it is withholding a deletion the author asked for.

The amplitude guard and the consent argument are therefore **the same
mechanism**: triggering off the `REPLACED`/`DELETED`/`VANISHED` rejection — and
never off `diff.haveIds` — is simultaneously what stops the healer seeding our
corpus into peers and what guarantees every push lands where the author's data
already lives.

One edge needs an explicit guard: **a relay the author vanished from.** If we
hold a kind 62 scoped to relay X and relay X is (non-compliantly) still serving
that author's old events, the heal for that pair is the **kind 62 itself,
pushed to X** — which the scoping rule already permits — and never the author's
content, which would re-establish them on a relay they asked to leave. So:
content pushes are blocked for any (author, relay) pair a stored vanish covers,
and the retraction push takes over. `shouldVanishFrom` is the existing decision
point.

**A relay-scoped kind 62 must never be propagated.** NIP-62's `relay` tag is
either `ALL_RELAYS` or one specific url — `RequestToVanishEvent.shouldVanishFrom`
is where the store already makes that distinction. Pushing a vanish request
scoped to relay A onto relay B erases the author from a relay they never asked
to leave. This is the sharpest edge in the whole proposal: it is data destruction
on someone else's server, executed on our inference. Gate it on the tag, test it,
and make the test the kind that fails loudly.

**The trigger is the store's refusal, not the reconcile's diff.** An earlier
revision proposed triggering heals off `diff.haveIds` — ours that they lack — on
the grounds that for replaceable kinds "they lack our id" and "they hold a stale
version" are nearly the same statement. **They are not, and the gap is the whole
amplitude question**: `haveIds` also contains every address the relay *never had
at all*, so pushing it would seed our corpus into every peer — a full `dir = up`
sync smuggled in through the healer, on streams that were configured read-only.
Absence is `UpstreamPush`'s job, behind its own explicit setting; the healer's
job is *staleness*, and the one proof of staleness is the store rejecting their
copy as `REPLACED` after we downloaded it.

That trigger turns out cheaper than the diff route anyway, in every mode at once:

- **It is mode-independent.** The `REPLACED` rejection fires identically whether
  the stale copy arrived by negentropy, fetch, or deleteMissing — one trigger,
  three paths, nothing per-mode.
- **The rejected event carries its own address.** `Rejected(REPLACED)` does not
  name the winner and does not need to: the loser's (kind, pubkey, d) *is* the
  winner's address, read straight off the rejected body. The healer resolves the
  current winner from the store at drain time — which is also what makes it
  correct against races: whatever is current at drain (a newer version still, or
  a kind 5 that arrived in between) is what gets pushed. No store change needed.
- **Churn keeps it fed under suppression.** A suppressed id is never downloaded
  again, so it can never re-trigger — but each *new* stale version (their v4
  when we hold v5) is downloaded once, rejected once, and triggers the heal for
  its relay before its id ever reaches the filter. Heal-then-suppress is the
  natural order per id, enforced by the gate below.

Same shape for the retraction kinds: the store rejecting their copy as `DELETED`
or `VANISHED` proves the relay still serves what our stored tombstone retracts,
and the healer resolves that tombstone from the store at drain time.

#### Where it stops working

Four ways the upstream does not drop its copy:

1. **We cannot write.** The dominant case. The dynamic fan-out is ~16k
   discovered relays — auth-required, paid, whitelisted, rate-limited. We have
   write access to almost none of them.
2. **The relay archives deliberately.** It accepts our newer version and keeps
   both. A real population, and not misbehaviour — but a smaller one than it
   looks: strfry, read below, does honour replacement and does heal.
3. **Replacement is asynchronous.** It accepts, deletes eventually, and the next
   reconcile still sees the old id.
4. **We cannot tell 1–3 apart** without waiting a cycle to see whether the id
   comes back.

**The asymmetry is the thing to weigh.** When Fix 3 fails it has cost memory.
When Fix 2 fails it has cost *the same download plus an upload, every cycle* —
strictly worse than doing nothing. So it needs an observation loop and a
give-up rule: mark a relay write-closed after N unaccepted pushes, the way
`HostStrikes` already records per-host facts.

It also cannot touch `EXPIRED` at all. No message says "this NIP-40 event has
expired", so an upstream serving one will serve it forever.

#### It is a policy change, not a performance change

Today exactly one place in the router writes: `UpstreamPush`, behind an explicit
`dir = up` on an explicitly listed relay. `down` streams never write, and the
parser *enforces* that `relaySource` streams are down-only. This would make
every down stream an unsolicited writer to every discovered relay, triggered by
their data, with no configured amplitude — one popular author's profile edit
becomes 16k publishes.

The consent half of that worry is resolved by the trigger (see above): every
push targets a relay already hosting the author's data, so no one's reach is
expanded. What remains is operational — write amplitude toward servers we
otherwise only read from — and that is what the switches govern: **per-stream,
distinct from `dir = up`** (the relay list is not the same list), rolled out
opt-in, with defaults that can honestly sit at on once the acceptance numbers
from the static upstreams are in.

#### The push must be off the hot path

**Non-negotiable, and it shapes the design.** The hot path is reconcile →
download → verify → `batchInsert`, and it is already the thing that competes
with client reads: `IngestPipeline.loop()` yields on `ServingPressure` for
exactly that reason. Publishing inline would block the sweep on a publish plus
an `OK` round trip, serialise the walk behind `UpstreamPush`'s 40 ms pacing, and
add write traffic to a pipeline that is already backpressured. A fix for slowness
that makes the system slower is not a fix.

So the reconcile does the cheapest possible thing and hands off:

- **Enqueue an address, never an event.** On identifying a candidate the sweep
  appends `(relay, kind, pubkey, d)` — or `(relay, eventId)` for a kind 5 / kind
  62 — to a bounded set. No store query, no serialisation, no allocation worth
  measuring. Resolving the winner from the store happens in the healer.
- **Coalesce, and drop on overflow.** A `Set` keyed on the address means a
  popular profile enqueued five thousand times is one entry. When the set is
  full, **drop and log** — never backpressure. Losing a heal costs nothing: the
  next cycle re-enqueues it. This is the opposite of `IngestPipeline.submit`,
  which suspends rather than drop, and deliberately so: an event dropped there is
  data lost, a heal dropped here is a retry.
- **Drain per relay, after its sync, before the socket closes.** A `relaySource`
  stream drops each relay's socket as soon as the sync returns — "these streams
  have no live tail" — so a fully detached healer would have to re-dial thousands
  of relays just to push. Draining that relay's queue at the end of its own sync
  keeps the connection, bounds the work to one relay's worth, and is still off
  the reconcile's critical path. Only a relay whose sync died mid-way needs the
  detached re-dial path.
- **Yield to `ServingPressure` like ingest does.** Same
  `backoffMs()?.takeIf { it > 0 }?.let { delay(it) }`, same reason. Healing is
  the lowest-priority work in the process and should say so in code.

#### The `OK` response is the signal, and it is better than waiting

The original plan was to infer a failed heal by watching whether the id came back
next cycle. It still needs that (below), but NIP-01 gives a **synchronous,
per-event, machine-readable** answer first, and its prefix decides whether a
tombstone is earned:

| `OK false` reason | tombstone? | why |
|---|---|---|
| `auth-required:` / `restricted:` / `blocked:` | **yes, immediately** | policy. Paid relay, allow list, block list. It will refuse the next one identically. |
| `rate-limited:` | **no** | transient, and the one that would do real damage — a momentary blip would permanently blind us to a relay that was going to heal. |
| `error:` | **no** | transient by definition. |
| `invalid:` / `pow:` | **no** | our event or our effort, not their policy. Fixing it is a different bug. |
| `duplicate:` | **no** | they already have our winner. Whether they *also* kept the loser is the next-cycle question, not this one. |
| no `OK` at all (timeout) | **not before N** | ambiguous. Count strikes per relay, the way `HostStrikes` already does, and only then treat the relay as write-closed. |

**Two tiers, because `OK true` is not proof of healing.** An accept means the
relay took our event, not that it dropped the loser — an archival relay does the
first and not the second. So:

1. **`OK false`, permanent class → tombstone now.** Certain, immediate, free.
2. **`OK true`, and the id is offered again next cycle → tombstone then.** This
   relay accepts writes and keeps both versions. Only the observation loop can
   see it, which is why 3a must not blind itself — see the note under Fix 3.

**Two structures, two jobs.** Do not conflate them:

- **A per-relay write-capability bit** (plus the reason and a strike count).
  Bounds the *push* cost: one `auth-required` means never spend another publish
  on that relay. Tiny — one row per relay, and `HostStrikes` is the shape to
  copy.
- **The per-id filter.** Bounds the *download* cost, and is still needed at a
  write-closed relay, because being unable to heal it does not stop it offering
  us the same ids forever.

Global per id, not per relay: "nobody should send us this" is one fact, and a
per-relay filter would multiply the storage by the fan-out.

### Fix 3 — remember what we refused

The fallback for the population Fix 2 cannot reach: relays we cannot write to,
and relays that provably will not heal.

Two halves:

- **(A)** record every event the store rejected as *permanently unstorable*;
- **(B)** consult that record on the **down** path so the event is not fetched
  again.

Half A is the same question either way — see the table below. Half B has two
implementations, and they sit at **different points in the flow**, which is what
makes them so different in cost:

|  | 3a — suppress the fetch | 3b — claim the id |
|---|---|---|
| structure | approximate membership filter (cuckoo — see below) | exact `(created_at, id)` set |
| where it hooks | after the reconcile names `needIds`, before the REQ that fetches them | inside the local id set the reconcile compares |
| stops | the **event bodies** | the difference itself |
| still pays | bisection round trips + the 32-byte ids | nothing |
| size | ~4–6 B/id | ~40 B/id |
| risk | silent false-positive loss | none |
| touches the id set | **no** | yes — and that is where all of Half B's hazards come from |

**3a gets most of the win for a tenth of the cost, and none of the danger.**
Take it first; 3b is an upgrade, not the plan.

#### 3a — the membership filter, in detail

The hook point is the thing to see. A reconcile produces the diff *before*
anything is downloaded — `DeleteMissingSync.kt:126` shows the shape:
`diff.needIds` in hand, then `fetchAll(Filter(ids = chunk))` to get the bodies.
Filtering `needIds` through a membership test there needs no enumeration and no
ordering, which is exactly what an approximate membership filter can do.

What that saves: a kind-0 profile is several hundred bytes to a couple of KB; its
id is 32. So suppressing the fetch avoids **~95% of the transfer** while the
reconcile still pays to identify the ids. It also avoids everything downstream of
the download, which is not a rounding error — `verify()` per event is the ingest
pipeline's dominant CPU cost, and ingest yields to `ServingPressure`, so wasted
ingest is paid for in client search latency.

**Concrete properties that make this small:**

- **No hashing.** An event id is already a 32-byte cryptographic hash, uniformly
  distributed. The bucket index and the fingerprint are disjoint bit-fields
  sliced straight out of it. There is no hash function to choose, tune, or get
  wrong.
- **A fixed-size mmap'd file per epoch.** No sort-merge, no LSM, no eviction
  policy, no ordering invariant, no atomic-rename dance. A corrupt file costs
  re-downloads, not correctness — checksum it and rebuild on mismatch.
- **One writer — and under the final gate, only the sync process is one.** An
  earlier revision had both processes writing, to catch the supersessions the
  RELAY performs when its own clients publish (the hole that killed the `:sync`
  state file for 3b). The gate makes that moot: a row requires refusals observed
  on the *sync* path — a refused push, or the store rejecting an upstream's copy
  — and when a client publishes v4 to the relay, the superseded v3 reaches the
  router the same way everything else does: an upstream offers it, the store
  refuses it, the gate counts it. The relay process writes nothing and reads
  nothing, and the two-writer-files design (and its cross-process questions)
  collapses to one writer per epoch file, with a striped lock across the ingest
  workers inside that one process.
- **Sizing.** ~4–5 B/id at the epsilons that matter (table below) — call it 10×
  smaller than 3b, before doubling for the candidate filter.

**The one real danger: a false positive is silent, permanent data loss.** We skip
an event we actually wanted, nothing logs it, and the same id hits the same bits
next cycle, so it never self-corrects. Three things follow, and the third is
mandatory:

1. **Size `p` against the backfill, not the steady state.** Expected loss per
   cycle is `|genuinely wanted| × p`. At p=10⁻⁹, a 100M-event backfill loses
   ~0.1 events. At p=10⁻², it loses a million.
2. **Partition, do not rotate on a timer** — see
   [The set grows forever](#the-set-grows-forever-and-neither-filter-does)
   below, which is the constraint that actually shapes this.
3. **Count the inserts and fail open past the design capacity.** This is
   non-negotiable. A Bloom filter sized for 50M holding 500M does not degrade
   gracefully — p goes from 10⁻⁹ to double digits and it starts discarding a
   sixth of everything, invisibly. Tracking `n` and disabling the filter once it
   is over budget turns the catastrophe into "the router got slower", which is a
   thing an operator can see and fix.

#### The set grows forever, and neither filter does

Neither structure grows. A cuckoo filter is a fixed table — `m` buckets × `b`
slots × `f` bits, allocated at construction — exactly as a Bloom filter is a
fixed bit array. Both must be sized for `n` up front. What differs is only what
happens when you exceed it, and that difference is sharper than it first looks:

- **Bloom's accuracy degrades with load.** `p` rises smoothly and silently as
  `n` grows past the design point.
- **Cuckoo's accuracy is load-independent.** `p ≈ 2b/2^f` is fixed by the
  fingerprint width, not by how full the table is. A half-full cuckoo filter and
  a 94%-full one answer equally well. What degrades is the *insert success
  probability*, and then it stops outright.

So over-provisioning a cuckoo filter costs space and nothing else, and
under-provisioning it stops loudly. That is the whole argument from the previous
section, now with the mechanism attached.

**But the set being remembered grows monotonically and without bound**, and that
is the thing to design around. Every superseded version ever published, forever;
nothing ever leaves it on its own. **Kind 3 is the pathological case** — a follow
list is rewritten on every single follow and unfollow, so one active account can
hold hundreds or thousands of superseded versions where kind 0 holds a handful.
Across a broad fan-out that is not a large set, it is an enormous one.

An unbounded set in a fixed structure means the structure is always a temporary
answer. Two ways out:

**Chained generations (scalable Bloom / chained cuckoo).** Add a new, larger
filter whenever the current one refuses an insert; look up by checking every
generation in turn. Grows without knowing `n`, which is exactly our situation,
and insert-failure is the trigger — the property that made cuckoo attractive.
Cost: lookups touch every generation, and space still grows forever.

**Partition by `created_at` epoch — recommended.** One filter per slice of Nostr
history (a quarter, say), each sized for that slice:

- **Growth is bounded to ~4 new filters a year**, each sized for a period whose
  refusal volume is roughly knowable from the last one.
- **Retirement becomes correct rather than lossy.** An epoch entirely below the
  lowest `since` any stream uses can be dropped outright: nothing will ever
  generate a `needId` in that range, so the filter is never consulted there. This
  is the time-floor bound under
  [Bounding it](#bounding-it--what-applies-to-which-variant),
  applied at the granularity that makes it exact instead of heuristic. A
  timer-based rotation, by contrast, forgets true positives and re-pays them.
- **The epoch is already known at the hook point.** `NegentropyPager.sweep` walks
  one `created_at` window at a time (`w: LongRange`), so every `needId` it sees
  came from a window with known bounds, and the reconcile call can close over it.
  `DeleteMissingSync`'s ask carries `since`/`until` for the same reason. No extra
  quartz surface is needed beyond the `wantId` predicate itself.

The one wrinkle: an operator widening a stream's `since` back into a dropped
epoch re-pays that epoch's refusals once. Same semantics as editing a filter and
invalidating its sync band, and acceptable for the same reason.

**This is also the strongest argument that Fix 1 is load-bearing rather than a
warm-up.** Every year of history a stream stops asking for is a year of epochs
that can be dropped. Narrowing `since` does not merely reduce the transfer — it
bounds the memory this whole approach needs.

#### Bloom or cuckoo?

Close, and worth deciding deliberately rather than by habit. Amortised bits per
id, at the two false-positive rates that matter here:

| ε | Bloom `1.44·log₂(1/ε)` | cuckoo (b=4, α=0.95) | binary fuse |
|---|---|---|---|
| 10⁻⁶ | 28.7 b (3.6 B) | 24.2 b (3.0 B) | 22 b (2.8 B) |
| 10⁻⁹ | 43.1 b (5.4 B) | 34.6 b (4.3 B) | 32.4 b (4.1 B) |

Cuckoo beats Bloom on space below about ε = 3%, so both our targets are in its
favour — by ~20%, which is real but is not the reason to pick it.

**The reason to pick it is that it cannot saturate silently.** Point 3 above is
the scariest property of the Bloom design: a filter sized for 50M holding 500M
keeps answering, with a false-positive rate in the double digits, discarding a
sixth of everything and logging nothing. A cuckoo filter **fails the insert** at
~95% load, after its relocation chain gives up. The mandatory counter-and-fail-
open becomes intrinsic to the structure instead of a discipline we have to
remember — and better, **insert failure is an exact rotation trigger**, so the
chained-generation scheme above stops needing a tuned threshold. The structure
tells you when a generation is done.

**Its headline feature is useless here, though.** Cuckoo filters support
deletion; Bloom filters do not, and that looked like the answer to "the winner
was deleted, so its losers are wanted again". It is not. **Deleting requires the
item** — you need the id to compute its fingerprint and buckets. When a winning
event is removed we have no way to enumerate the superseded ids it should
release, because not storing them is the entire point of using a filter. The
only world where the deletion works is one where we already kept the ids, and
that world is 3b, which does not need a filter. Epoch partitioning remains the answer
either way.

Deletion does buy one modest thing: an operator affordance. "Why don't you have
event X?" is answerable with a one-line un-suppress instead of "rotate the
filter or wait".

**What cuckoo costs is the concurrency story**, and that was the best property of
the Bloom design. Bloom bit-sets are idempotent and monotonic, so both processes
can write one mmap with no locking. A cuckoo insert *relocates existing
fingerprints*, so concurrent writers corrupt each other, and a reader can miss an
entry mid-relocation. Two consequences:

- **Cuckoo filters do not merge.** Two Bloom filters union with a bitwise OR;
  two cuckoo tables cannot be combined at all without reinserting the original
  items, which we do not have.
- **The lock-free shared file is gone** — which is what made the relay's own
  refusals recordable.

**The fix that kept cuckoo viable was one file per writer — and the gate then
removes the second writer entirely** (see the single-writer bullet under 3a):
only the sync process ever inserts, so cuckoo's concurrency cost falls away with
no cross-process coordination left to design.

**Binary fuse filters** are ~6% smaller again and are the space-optimal answer,
but they are **immutable** — built once from the complete key set, no incremental
insert. Making that work means buffering the generation's raw ids to freeze it
later, which is a real design and buys 6%. Not worth it.

**Verdict: cuckoo, single-writer per-epoch files** — the
epoch design needs a signal for "this partition is full", and insert failure is
that signal exactly. If the design stays one flat filter, take Bloom for the lock-free
shared file and keep the counter discipline.

**What it needs from quartz.** On the main sweep path the id-to-event fetch
happens *inside* `negentropySync` — the signature `WindowSync.reconcile` mirrors
(`localIndex`, `targetWindow`, `onProgress`, `onUnreconcilableWindow`,
`onEvent`) has no hook between naming an id and fetching it. So this wants one
upstream parameter: a `wantId: (String) -> Boolean` predicate consulted before
the REQ. That is a much smaller upstream ask than anything else in this document,
and AGENTS.md is explicit that reconcile behaviour is fixed upstream rather than
forked here. `DeleteMissingSync` can do it locally today with no quartz change at
all, which makes it the natural place to prove the idea.

**What 3a does not do.** It does not make the cycle *converge* — the reconcile
still bisects down to isolate those ids on every pass, so the protocol chatter
and the id transfer remain, and they grow with the size of the permanent
difference. Only 3b removes that, by making the sets actually match. Measure the
residual after 3a before deciding 3b is worth a store release.

#### How it composes with Fix 2

Tightly, and this is now the design rather than an option. **A candidate earns a
row only once a push has been refused** — by `OK false` in the permanent class,
immediately, or by the id being re-offered after an `OK true`, one cycle later.
Relays that heal never cost a single entry. That is a far stronger bound than any
heuristic under
[Bounding it](#bounding-it--what-applies-to-which-variant), and it is what makes
the filter's size a function of *how much of the network refuses our writes*
rather than of how much of the network has stale copies.

The dependency runs one way: Fix 3 is useful without Fix 2, and Fix 2's
`OK false` tier stands alone — but its archival tier (accepted, not dropped) is
observable only through Fix 3's re-offer detection.

**And here 3a and 3b differ in a way that is easy to miss.** 3a suppresses the
*fetch*, not the *observation* — the id still appears in `needIds` before the
predicate drops it, so "this relay offered it again" is still visible and Fix 2's
healing check keeps working. **3b destroys that signal.** Once the id is in our
claimed local set the reconcile reports no difference at all, so there is no
longer any way to tell whether a relay healed or is still hoarding. Shipping 3b
therefore blinds Fix 2's observation loop, and anything built on it — the
write-closed strike rule, the "populate only for relays that failed to heal"
bound — has to be settled *before* 3b lands, or it cannot be measured afterwards.

#### Every sync mode, and which mechanism covers it

The *fix* must work in `negentropy`, `fetch` and `auto`, static and dynamic. It
does — but **not by the same mechanism in each**, and the honest split is that
Fix 2 covers every mode while the filter only earns its place where there is a
hook before the body arrives.

| path | mechanism | why |
|---|---|---|
| **negentropy** (`NegentropyPager`, `StaticBackfill`, `DynamicSync`) | Fix 2, **plus** the filter on `needIds` before the REQ | there is a hook before the body, and it is worth ~95% of the transfer |
| **deleteMissing** (`DeleteMissingSync`) | same — the filter on `diff.needIds` before `fetchAll` | same hook, same saving |
| **fetch** (`fetchAllPages`) | **Fix 2 and `latestOnly` only. No filter.** | a REQ never names an id before it sends the body, so there is no hook that saves anything worth the complexity |

**The fetch path does not get a suppression hook, and does not need one.** The
bytes are already spent by the time an `onEvent` predicate could fire, so all a
filter could save there is `verify()` and a store round trip. The way to fix
`fetch` is to **heal the source**: once the upstream holds our version instead of
its stale one, the next fetch — including a from-scratch one after a band is
invalidated — brings back the current event, which the store rejects as an
ordinary `DUPLICATE`. That is the baseline cost of running a fetch-mode mirror at
all, and router.md is right to call it the system working.

Dropping that hook takes three things off the table with it: the `onEvent`
predicate, the trap of having to run `SyncCoverage.observe` before the drop (skip
it and the leg loses its per-kind evidence, quartz records no band, and the
stream re-walks the relay every cycle — costing more than the filter saved), and
a third tally for events that are neither accepted nor rejected. **The filter is
a negentropy-path mechanism.** Fetch is Fix 2's job.

**Fix 2's trigger is the same in every mode** — the store's `REPLACED` /
`DELETED` / `VANISHED` rejection after the stale copy arrives, with the address
read off the rejected body (see the trigger section under Fix 2, including why
`diff.haveIds` must *not* drive content pushes). The download it rides on is one
the mirror was already paying: suppression only begins after the gate has seen
refusals, so every (relay, address) pair gets its heal attempt before its id can
reach the filter.

**Static versus dynamic** changes the economics, not the mechanism. The per-id
filter is global and behaves identically in both. The per-relay write bit is one
row per relay — 16k rows is nothing. What differs is that a dynamic stream drops
each socket as its sync returns and we can write to almost none of those relays,
so on the fan-out Fix 2 is a one-time probe per relay with a low expected
acceptance rate, and the filter carries the recurring load.

#### Which means the gate needs a third path

Gating a row on a *refused push* leaves a hole exactly where the problem is
largest: the rollout ships the healer on static upstreams only, so on the dynamic
fan-out no push is attempted, nothing is refused, and nothing would ever earn a
row. Three ways in, then:

1. **Push refused, permanent class** → row immediately.
2. **Push accepted, id offered again next cycle** → row then. The relay archives.
3. **No push attempted** — healing off for the stream, or the relay already
   marked write-closed — **and the id is offered again** → row then.

Path 3 is what covers the fan-out, and it costs one more structure. A filter
answers membership, not counts, so "twice" needs **two filters per epoch**: a
**candidate** filter and a **suppress** filter. Both cuckoo, both per-epoch,
both membership-only — no counters anywhere.

**A "sighting" is a store refusal, never a `needIds` appearance.** An id can
reappear in `needIds` for innocent reasons — the fetch died mid-download, the
relay dropped the socket — and inserting on mere appearance would let two
transient failures suppress a wanted event forever. So: the FIRST refusal
(`REPLACED`/`DELETED`/`VANISHED`/`EXPIRED`, after a completed download) inserts
into the candidate filter; on a later cycle an id that hits the candidate filter
is **downloaded once more anyway**, and only a SECOND refusal inserts it into
the suppress filter. That one extra download per id buys the property that makes
the epsilon arithmetic safe: a candidate-filter false positive costs one wasted
download, never a suppression — only the suppress filter's own epsilon can lose
data, and every id in it is backed by two independent refusals. It also settles
`EXPIRED`, which no push can fix: two refusals, row, done.

The candidate filter is Step 0's repeat-detector. The plan built it to measure
and then threw it away; now it stays, and the storage estimate roughly doubles.

## Half A — what earns a row, and what must never

Two gates now, in series. The table below says which store refusals make an event
a **candidate**; the `OK` classification under Fix 2 says when a candidate
becomes a **row**. An event has to pass both — be permanently unstorable *and*
have resisted a heal.

This table is the actual content of the proposal. Getting a line wrong here
either wastes a gigabyte or loses data permanently.

| insert outcome | record? | why |
|---|---|---|
| `REPLACED` | **yes** | the case. Deterministic for as long as the newer version stands. |
| `DELETED` / `VANISHED` | **yes** | already guarded at insert by a stored tombstone; this stops the *download* too, which the tombstone never did. |
| `EXPIRED` | **yes**, but see the tension below | it will never become storable again. |
| `DUPLICATE` | **no** | it is already in our id set. The reconcile never asked for it. A row here is pure growth for zero saving. |
| bad signature (`IngestPipeline`, pre-store) | **no** | an id is the hash of the *content*, not of the signature. The same id can arrive correctly signed from a different relay. A row here would make one relay's corruption permanent. |
| `InsertOutcome.Failed` | **never** | `IngestPipeline.kt:255` already says it: the event was good, the failure is the store's, and it is lost. A row here converts a transient store fault into permanent, silent data loss — the exact failure mode `lostToStore` exists to make loud. |
| swept / pruned by an operator | **no** | `SWEEP_ORPHAN_SCORES_ON_START` and by-kind reclaim are *designed* to be re-downloaded ([configuration.md](../configuration.md), AGENTS.md). A row here silently converts "reclaimed space" into "never again". |

The invariant that falls out: **only insert-time rejections, and only the ones
whose verdict is a property of the event rather than of the store's health.**

### The `EXPIRED` tension is worth understanding before touching anything

The store already excludes expired events from the snapshot, and its comment
reasons in exactly the opposite direction:

> Exclude already-expired events (NIP-40), exactly as query/count do — otherwise
> a peer keeps trying to reconcile events we refuse to serve.
> — `NostrSemanticsStore.snapshotIdsForNegentropy`

That is correct for the **serving/up** direction: don't advertise an id you will
not hand over. It is exactly wrong for the **down** direction: don't ask for an
id you will not keep. One set cannot be right for both, which is the strongest
argument that the merge belongs on the down-path local index and *not* inside
the store's snapshot.

## Half B — where the merge goes, and the three places it must not

**This whole section is 3b's problem, and 3a does not have it.** A membership
test applied to `needIds` never enters the local id set, so none of the sites
below can see it and none of the hazards arise. That is most of why 3a is worth
taking first. Read on when the exact set is on the table.

Five call sites hold a local id set, and they mean different things:

| site | merge? | consequence of getting it wrong |
|---|---|---|
| `StoreWindowIndex` (`NegentropyPager.kt:88`) | **yes** | the paged sweep. Merge into both `count()` and `entriesFor()` — `count()` sizes the window. |
| `Snapshots.snapshotIdsReporting` → `StaticBackfill.kt:583`, `DynamicSync.kt:594` | **yes** | one materialised list reused across the fan-out. Cheapest place to do it and the biggest win: the dynamic stream shares one snapshot across thousands of relays. |
| `UpstreamPush.kt:70` | **no** | the local set there is *what we can hand over*. A claimed id we cannot materialise makes `store.query(Filter(ids=…))` return nothing and publishes nothing — wasted round trips, and a lie to the peer. |
| `DeleteMissingSync.kt:103` | **absolutely not** | the local set there is *what the upstream's silence retracts*. Claiming ids we don't hold feeds phantom entries into the delete side of the diff. This is the one place in the router where a wrong set destroys data. |
| relay's `NostrRelayServer.kt:194` (serving negentropy to clients) | **no** | same as `UpstreamPush` — see the `EXPIRED` tension above. |

So Half B is a **decorator around the local index**, not a store change:
`NegentropyLocalIndex` and the snapshot helper get wrapped; nothing else sees it.

### The idea to reject explicitly

*"Just fabricate a local entry so the sets match."* No. Negentropy is symmetric.
An id in our set that we cannot produce is a promise to the peer, and the two
rows above are what collects on it. Only real ids we really refused.

## Where the tombstone lives

**For 3a the answer is short**: fixed-size mmap'd cuckoo tables on the shared
`/var/lib/vespa-relay` mount, partitioned by `created_at` epoch, written by the
sync process alone — see 3a above for why the gate leaves it the only writer,
and why per-epoch rather than one flat table. The rest of this section is about **3b**,
where the `created_at` has to be exact and the storage question gets hard.

Two constraints decide this, and both point the same way.

**The `created_at` must be exact, and it is available only at the moment of the
decision.** Negentropy is a set of `(created_at, id)` pairs sorted by time; a
pair filed under the wrong timestamp lands in a different bucket than the peer's
copy, so the range fingerprints disagree, the session bisects down, and the peer
concludes *both* that we lack the event (it sends it anyway) and that we hold
something it lacks (we start claiming an id we cannot produce — the
`UpstreamPush` / `DeleteMissingSync` hazard from Half B). A wrong timestamp is
worse than no row. So the timestamp cannot be derived after the fact; it has to
be captured where the refusal happens.

**Every refusal happens inside the store, and in BOTH processes.** `REPLACED` is
decided in `BulkRecordInsert`'s supersession stage; `DELETED` / `VANISHED` in
`Deletions.isDeleted` / `isVanished`; the sweep of already-stored victims in
`Deletions.applyDeletion`, which resolves the doomed docs and therefore holds
their `created_at`. The relay process makes all of these too — a client
publishing a new profile supersedes an old version right there.

### The four candidate homes

**1. A file in `:sync`, beside `SyncBands` and `SweepState`.** What the first
draft of this proposal assumed. Ships without a store release, matches an
existing precedent and lifecycle.

It has a hole that only shows up on the second reading: it can only record what
the *sync* process refuses. A profile a client pushes to the **relay** supersedes
an old version whose id the router will then never learn — and the router is
precisely who will be offered that id, by every upstream, forever. For a relay
with its own users that is not an edge case. Same argument as `SHARED_STRICT` in
AGENTS.md: per-process memory of a store-wide decision is wrong here, and for
the same reason.

Keep it as a throwaway prototype for [Step 0](#step-0-measure-first). Do not
ship it as the destination.

**2. A third Vespa document type — recommended.** There is already precedent:
`reputation.sd` sits beside `event.sd` as a non-event doc type, and
`SchemaDeploy` redeploys the bundled package on every boot in both processes, so
adding a schema costs no migration step.

```
schema refused {
    document refused {
        field id         type string { indexing: attribute | summary
                                       attribute: fast-search
                                       match { cased } dictionary { hash } }
        field created_at type long   { indexing: attribute | summary
                                       attribute: fast-search }
        field kind       type int    { indexing: attribute | summary }
        field reason     type int    { indexing: attribute | summary }
    }
}
```

Four properties that the file cannot match:

- **Idempotent by construction.** The Vespa document id *is* the event id, so
  re-refusing the same event is an overwriting put. No dedup structure, no
  growth from repeats, no "have I already recorded this" lookup on the ingest
  hot path.
- **It costs no router heap.** The mmap'd file below was competing for the
  gigabytes `NegentropyPager`'s header already names as the router's scarcest
  resource (14.9M ids for one stream, three concurrent heading for ~9 GiB).
  This lives on the disk already sized for the corpus.
- **The read path nearly exists.** `snapshotIdsForNegentropy` already walks
  `event` via `visitIds` with a `created_at` fieldSet and a page loop, and
  already carries the cross-source dedup set for multi-filter snapshots. A
  parallel visit over `refused` on the same range, unioned, is a close copy of
  code that is there.
- **Pruning is declarative.** The time-floor bound under
  [Bounding it](#bounding-it--what-applies-to-which-variant)
  becomes a Vespa garbage-collection selection on the doc type instead of a
  hand-rolled eviction policy with its own bugs.

Declare it a **regular** doc type, not `global` like `reputation`. Global
replicates to every content node and is held in memory — right at roster scale
(10^5..10^6 pubkeys), wrong for a set that is a multiple of the event corpus.

**3. A `superseded` array on the surviving event doc — rejected, but it is the
idea that looks best on a whiteboard.** Self-cleaning (the rows go when the
winner goes), naturally per-address, no new doc type. It breaks on the read
path: a superseded version's `created_at` is not the winner's, so enumeration by
window would need `superseded_at` as an `array<long>` matched by range — which
recalls the whole winner doc if *any* element is in range, then filters
client-side. Every window over-fetches, and one address's pairs scatter across
many windows. It also only covers `REPLACED`: a NIP-09-deleted regular event has
no surviving doc to hang anything on.

Worth stealing one property from it, though: **deleting the winner should
release its rows.** Under design 2 that has to be done on purpose — see the
first open question.

**4. "The kind 5 already is the tombstone" — does not work, and the reason is
the useful part** (and strfry reaches the same place — see
[Prior art](#prior-art-what-strfry-does))**.** For `DELETED` the target ids are already stored, in the kind
5's `e` tags, at zero extra cost. But NIP-09 records no `created_at` for its
targets, and per the first constraint above a proxy (the kind 5's own timestamp)
is actively harmful rather than approximate. Nothing derivable substitutes for
capturing the real one.

### If it does have to be a file first

Packed, sorted by `created_at`: parallel `long[]` plus a raw 32-B-per-id
`byte[]` — raw, not hex — for **40 B/entry**, in a `MappedByteBuffer` under the
shared `/var/lib/vespa-relay` mount so it costs page cache rather than heap. A
range query is two binary searches, which is exactly the shape
`entriesFor(window)` wants. Append into a small on-heap buffer, sort-merge on
`SweepState`'s cadence. **Binary, not JSON** — `SyncBands` and `SweepState` are
pretty-printed because a human reads them; 10M hex ids pretty-printed is a
~700 MB file nobody will ever open.

## Bounding it — what applies to which variant

The two variants bound themselves by different means, and the epoch partitioning
under 3a supersedes most of what this section originally said for 3b.

**Both:**

- **Time floor.** A reconcile only ever asks inside a leg, and legs are bounded
  below by the stream's `since` or `SyncCoverage.PLAUSIBLE_FLOOR`. Anything below
  the lowest floor any configured stream uses can be dropped: nothing will ask.
  Under 3a this is not a heuristic but the retirement rule itself — a whole epoch
  goes at once, exactly.
- **Only what a reconcile could re-ask.** A `fetch`-mode stream has no id set, so
  entries born from paged ingest buy nothing. Tag the submission with its source
  and record only reconcile-fed rejections.

**3a only:**

- **Capacity is per epoch, and insert failure is the signal.** No global ceiling
  to tune and no eviction policy to get wrong.
- **Nothing else is even possible, which is a simplification rather than a
  limit.** A filter stores no keys, so per-address caps, LRU, age-weighting and
  every other per-entry policy are structurally unavailable. There is nothing to
  tune and therefore nothing to tune wrongly.

**3b only** — an exact set has per-entry metadata, so it needs the policies a
filter cannot have and cannot get wrong:

- **Per-address version cap.** Keep the K most recent superseded versions per
  (kind, pubkey, d). Wrong in principle — an upstream may hold version 1 of 500 —
  but the distribution is heavily skewed to recent versions and the cost of being
  wrong is one re-download, not incorrectness.
- **A hard cap with a *logged* eviction rule**, evicting oldest `created_at`
  first (consistent with the floor rule). Log it: a table silently sitting at its
  ceiling is a fix that quietly stopped working.
- **Do not evict by LRU.** A row that is doing its job is *never touched* — it
  prevents the ask. LRU would evict precisely the entries that work. This is the
  trap most caches walk into here.

## Step 0: measure first

Nothing above should be built on an estimate.

1. Break `REPLACED` out of `IngestPipeline.rejectReasons` (`IngestPipeline.kt:132`)
   onto the health line as its own per-stream number.
2. Add the 3a filter over rejected ids and count **repeat** rejections per
   cycle. Repeats are the entire prize; a first-time `REPLACED` is not
   recoverable by any of this.

Two numbers — *"replaced x N, of which N′ we had already rejected in an earlier
cycle"* — decide whether anything past step 3 is worth building, and how big to
size each epoch's filter.

**Measure BEFORE Fix 1, not after.** Narrowing a stream's `since` reduces the
`REPLACED` count by reducing what is asked for, so a baseline taken afterwards
would read as "the problem is small" when the truth is "we stopped looking".
Take one cycle of numbers first, apply Fix 1, take another. The delta is what
Fix 1 bought; the remainder is what Fix 3 has to justify itself against.

**Partition the measurement filter by epoch from the start**, even though a flat
one would count repeats just as well. Retrofitting partitioning changes the
per-epoch sizing, so a flat measurement produces a number that does not transfer
to the design it is meant to size.

**The instrument and the fix are the same object.** The filter that counts
repeats *is* 3a; the only difference is whether its answer is consulted before
the fetch or merely tallied. So step 1 should build it at the size 3a will need
rather than as a throwaway — measure with `p` already low, and it graduates into
the fix by wiring one predicate.

(An earlier draft of this document dismissed a Bloom filter as saving "not a
single byte of bandwidth". That was reasoning about a lookup on the *ingest*
path, after the download had already happened. Moving the lookup to `needIds`,
before the fetch, is what turns it from a CPU saving into the main fix.)

## Rollout

1. **Step 0's instrumentation alone.** One cycle, unchanged config — the
   baseline, for the reason above.
2. **Fix 1** — `since` on the replaceable-kind streams, and `latestOnly` on the
   `fetch`-mode ones. One more cycle. The delta against step 1 is the whole of
   what the remaining work has to beat, and on the fetch path it is the *only*
   thing that saves bandwidth at all.
3. **Fix 2's healer, on the static upstreams only** — the `urls` in
   `router.conf`, where we have a relationship and plausibly write access.
   Per-stream opt-in, default off, and **two switches**: one for the retraction
   kinds (5 and `ALL_RELAYS` 62), one for the content kinds. Build the bounded
   coalescing queue, the per-relay post-sync drain, the `ServingPressure` yield
   and the `OK` classifier in this step — they are the whole mechanism, and a
   bounded blast radius is the place to learn the real acceptance rate.
   The relay-scoped kind-62 guard ships here, with its test.
4. **Fix 3a**, on the negentropy paths only — per-epoch cuckoo filters,
   **candidate and suppress**, written by the sync process alone, fed by the
   three-path gate rather than by every store refusal. Wire them into
   `DeleteMissingSync`'s `diff.needIds` first, which needs no upstream change and
   proves the suppression on one path; then the `wantId` predicate in quartz for
   the main sweep, closing over the window's bounds to pick the epoch. **Nothing
   on the fetch path** — see the mode table; fetch is healed, not filtered. Log the
   suppression count and each epoch's load from day one — a filter doing nothing
   and a filter eating everything look identical without it. Decide up front what
   an insert failure does: open a new generation for that epoch, or stop
   suppressing. Silently continuing is the one unacceptable answer.
5. Measure the **residual**: with the bodies suppressed, how much is the
   un-converged reconcile still costing in round trips and id transfer? That
   number, and nothing else, justifies step 6.
6. If it does: **Fix 3b**, the `refused` doc type
   in **vespa-eventstore**, written at each decision point, populated only for
   relays that failed to heal. Read back through an opt-in argument —
   `snapshotIdsForNegentropy(…, includeRefused = false)` — never a blanket
   change, because a blanket change breaks `UpstreamPush` and `DeleteMissingSync`
   in the two ways tabulated in Half B, and contradicts the store's own
   `EXPIRED` reasoning for the serving direction. Budget a store release, a pin
   bump, and the JitPack lexicographic trap AGENTS.md warns about; the schema
   itself needs no migration step, since `SchemaDeploy` runs every boot.
   **Settle Fix 2's strike rule before this lands** — 3b removes the id from the
   diff entirely, so after it ships there is no longer any way to observe whether
   a relay healed.
7. Chart the suppression count, the epoch count and each epoch's load on the
   Sync coverage card, beside the bands and sweeps.

8. **Extend Fix 2 to the dynamic fan-out** — a position this document reversed.
   The write-closed strike rule makes the cost **one-time per relay**, not
   per-cycle: probe until N refusals, mark the relay, never spend another publish
   on it. Sixteen thousand relays at three probes each is ~48k publishes *ever*,
   spread across cycles and paced, and every relay that does accept is healed
   permanently — including for every other mirror. Identification costs nothing
   extra here either: the trigger is the same `REPLACED` rejection the mirror was
   already producing. What remains against it is the **policy** objection, not the
   cost one: unsolicited writes to relays we only ever read from. That argues for
   keeping the retraction switch and the content switch separate and letting the
   content one default off, not for staying out of the fan-out.

Every step carries its slice of the [Test plan](#test-plan), and the **[SAFETY]**
cases are merge blockers rather than follow-ups: each guards an outcome that is
silent, permanent, or on somebody else's server.

## Test plan

Specifications, not code — nothing here is implemented yet. Names follow the
house style, where the name states the property and, where one exists, the bug
it guards. Everything lands in `:sync` unless marked. Per AGENTS.md: **assert the
property, not the implementation** — several of these describe behaviour that
must survive a rewrite of the mechanism underneath them.

Eleven of these are load-bearing. They are marked **[SAFETY]**, and each one
guards an outcome that is silent, permanent, or on someone else's server. If the
budget runs out, these are the ones that ship.

### `LatestOnlyAskTest` — Fix 1's flag

- `latestOnly decomposes a replaceable ask into one limit-1 filter per address`
- `latestOnly keys addressable filters on the d tag, so two d values are two asks`
- `latestOnly is a no-op for regular kinds rather than an error` — a flag that
  throws on kind 1 makes a mixed-kind stream unconfigurable.
- `latestOnly batches within the relay's advertised max_filters and splits beyond it`
- `latestOnly records no sync band, because limit-1 per address walked no range`
  — a band here would tell the next cycle a range is covered that was never swept.

### `HealTriggerTest` — what becomes a push candidate

- `a REPLACED rejection enqueues a push for the rejected event's address`
- `an addressable rejection enqueues on kind pubkey and d`
- **[SAFETY]** `an address the relay never offered is never pushed` — the healer
  repairs staleness; seeding absence is dir = up's job, behind its own setting.
  Feed the healer a haveIds-shaped diff and assert nothing is enqueued from it:
  this pins the trigger to the store's rejection, not the reconcile's diff.
- `a kind 5 whose target the relay still serves enqueues the kind 5, not the target`
- `a kind 62 tagged ALL_RELAYS enqueues a push`
- **[SAFETY]** `a kind 62 scoped to another relay is never pushed anywhere` — the
  sharpest edge in the design: propagating it erases an author from a relay they
  never asked to leave. Data destruction on someone else's server, executed on
  our inference. Assert against a `relay` tag naming a third-party url *and*
  against a malformed tag, which must also not push.
- `a kind 62 scoped to this relay is pushed to this relay only`
- **[SAFETY]** `an author's content is never pushed to a relay a stored vanish covers`
  — the one case where "the relay already has their data" is not consent: the
  author asked to leave, and the relay kept serving them anyway. The heal for
  that pair is the kind 62, never the content that would re-establish them.
- `with the content switch off, replaceable and addressable enqueue nothing while retractions still do`
- `with both switches off, nothing is ever enqueued` — the
  configured-but-inert trap AGENTS.md names; assert the queue stays empty rather
  than that some flag was read.

### `HealQueueTest` — the off-hot-path contract

- `the reconcile enqueues an address and never resolves the event` — assert the
  store sees **zero** queries during the sweep. This is the whole point of the
  deferral and the easiest thing to regress.
- `a popular address enqueued many times occupies one slot`
- **[SAFETY]** `a full queue drops and logs rather than suspending the producer`
  — deliberately the inverse of `IngestPipeline.submit`, and the test must state
  why: a dropped event there is data lost, a dropped heal here is a retry. A
  future reader "fixing the inconsistency" would stall the sweep behind the
  healer.
- `the queue drains at the end of that relay's sync, before the socket is released`
- `a relay whose sync threw still has its queue drained or discarded, never leaked`
- `the healer yields while ServingPressure reports backoff`
- `the winner is resolved at drain time, so a version superseded after enqueue pushes the newest, and a target deleted after enqueue pushes the kind 5`

### `OkClassifierTest` — the rejection taxonomy

One case per row of the table under Fix 2, plus:

- **[SAFETY]** `rate-limited earns no tombstone and no write-closed mark` — a
  momentary blip would otherwise permanently blind us to a relay that was about
  to heal. Same assertion for `error:`.
- `auth-required, restricted and blocked each earn a tombstone and close the relay for writes`
- `invalid and pow earn nothing, because they are our event and not their policy`
- `duplicate earns nothing, because holding our winner is not the same as dropping the loser`
- `an unrecognised reason prefix earns nothing` — unknown means unknown; the
  same conservatism `Unreachability.proves()` already applies to NIP-66 claims.
- `silence earns a strike and only closes the relay after the threshold`
- `OK true earns no tombstone on its own`

### `RefusedIdsTest` — the filter

- `an inserted id is reported present`
- **[SAFETY]** `an id that was never inserted is never reported present` — a
  false *negative* would suppress an event we wanted, silently and permanently.
  Structural for cuckoo, but a relocation bug breaks it, so pin it: insert a
  large population, then assert every non-member of a disjoint population is
  absent.
- `the measured false-positive rate stays within the configured epsilon at design load`
- **[SAFETY]** `a filter at capacity fails the insert rather than continuing to answer`
  — an over-filled filter discards a fraction of everything and logs nothing.
  Assert the failure surfaces, and that the configured response (new generation,
  or stop suppressing) actually happens.
- `the table round-trips through save and load unchanged`
- `two writer files are both consulted, and an id in either is present`
- `a lookup window spanning an epoch boundary consults both epochs` — insertion
  keys on the event's exact `created_at`; lookup keys on the sweep window, and
  windows do not respect quarter boundaries. Get this wrong and suppression
  silently stops working near every boundary.
- `retiring an epoch below the floor un-suppresses the ids it held`

### `RefusedIdsGateTest` — the three paths in

- `a refused push in the permanent class earns a row immediately`
- `an accepted push whose id is offered again earns a row on the second sighting`
- `an id refused twice with no push attempted earns a row` — the fan-out path.
- `one refusal alone never earns a row` — candidate and suppress are distinct.
- **[SAFETY]** `a needIds appearance without a completed download is never a sighting`
  — a fetch that died mid-transfer must count for nothing, or two transient
  failures suppress a wanted event forever.
- `a candidate hit is re-downloaded, and only a second refusal promotes it` —
  the property that keeps a candidate-filter false positive at the cost of one
  download instead of a permanent suppression.
- **[SAFETY]** `an InsertOutcome.Failed never becomes a candidate` — the event
  was good, the failure was the store's, and a row would convert a transient
  fault into permanent silent loss. `lostToStore` exists to make that loud;
  this test keeps it loud.
- **[SAFETY]** `a bad signature never becomes a candidate` — an id is the hash of
  the content, not the signature, so the same id can arrive correctly signed from
  another relay. One relay's corruption must not be permanent.
- `REPLACED, DELETED, VANISHED and EXPIRED each become candidates`
- `a DUPLICATE never becomes a candidate, because it is already in our id set`
- `an operator sweep and a deleteMissing retraction never become candidates` —
  both are designed to be re-downloaded.

### `SuppressionScopeTest` — where it must not reach

- **[SAFETY]** `DeleteMissingSync's local set never contains a suppressed id` —
  the one place in the router where a wrong set destroys data: phantom entries
  feed the delete side of the diff. Assert on the set handed to
  `negentropyReconcileIds`, not on the outcome.
- `UpstreamPush's local set never contains a suppressed id`
- `the relay's snapshotIdsForNegentropy is unaffected` (`:relay`) — we must not
  advertise ids we cannot serve.
- `a fetch-mode stream submits every event it receives` — fetch is healed, not
  filtered; assert no suppression hook exists on that path at all.
- `suppression removes the id from the fetch, not from the diff` — the id must
  still be observable in `needIds` afterwards, or Fix 2's healing check goes
  blind and the third gate path stops working.

### `HealAndSuppressLoopTest` — end to end, against relay personalities

One fake upstream per behaviour, two cycles each, asserting what crosses the
wire on cycle 2 rather than what any component believed:

| personality | cycle 1 | cycle 2 must |
|---|---|---|
| **compliant** (replaces on write, accepts) | offers stale, we push, it replaces | transfer nothing — the diff is empty and no row was created |
| **write-closed** (`OK false auth-required`) | offers stale, push refused | not transfer the body; row created on cycle 1 |
| **archival** (accepts, keeps both) | offers stale, push accepted | not transfer the body; row created on cycle 2, not 1 |
| **rate-limiting** (`OK false rate-limited`) | offers stale, push refused | **retry the push** — no row, relay not closed |
| **silent** (no `OK`) | offers stale, push unanswered | strike recorded, still under threshold, push retried |
| **no-push stream** (healing off) | offers stale twice, both downloaded and refused | row created on the second refusal |

### What must keep passing

`NegentropyPagerTest`'s cursor and window-ordering assertions, `SyncBandsTest`,
`SweepStateTest` and `DeleteMissingCascadeTest` are the blast radius. The band
and cursor files are pinned by tests for a reason; nothing here changes their
on-disk shape, and if a diff makes one of them fail, that is the finding.

## Open questions, worked through

Each carries a resolution or a recommendation; the two marked **decide** are
policy calls that should be confirmed before their step ships.

**1. Resurrection — resolved by structure, worth stating as policy.** A replaced
version whose winner is later deleted is *not* re-fetched: a filter cannot
enumerate a winner's losers (not storing them is the point), so 3a decides this
de facto, and it matches replaceable semantics — deleting v4 does not reinstate
v3 anywhere else either, and the author can always republish. Adopting 3a *is*
adopting no-resurrection; the escape hatches are epoch rotation and the manual
un-suppress. Only 3b could implement the other answer (rows carrying the winning
address, swept by address on winner removal) — if that ever matters, it is one
more argument in 3b's step-6 case, not a change to 3a.

**2. Filter edits — rows survive, with a documented escape hatch.** Bands are
keyed by the filter because a band answers "what did this ask cover", and
editing the ask invalidates the answer. A refusal answers "what did the store
decide about this id", and the ask has no bearing on it. So the filters survive
every filter edit, and the operator documentation states the real reset lever:
delete the filter files to force a total re-offer. One file pair per epoch makes
that a targeted or total wipe, both safe — the cost is re-downloads, never
correctness.

**3. Ordering — resolved, from quartz's own contract.** `NegentropyLocalIndex`
(read at amethyst HEAD; the KDoc is the contract) says it outright: *"The
`(created_at, id)` pairs inside [window]. **Order does not matter.**"* — and the
list overloads sort internally (`of()` wraps entries in a `SortedListIndex` via
`sortedBy { createdAt }`). So a 3b decorator may append its rows unsorted. The
same KDoc adds two constraints the decorator must honour instead: both methods
may be called **concurrently** (`reconcileConcurrency > 1`) and **repeatedly for
the same window** (an overflowing window is re-asked as halves), so the merge
must be cheap, side-effect free, and thread-safe. Verify against the pinned
quartz commit at implementation time; the contract predates this proposal.

**4. Content-push consent — resolved: the trigger is the consent.** A heal can
only fire at a relay that offered us its own stale copy, so the target already
hosts the author's data and the push changes the version, never the
distribution set (argued in full under Fix 2's trigger table). Two designs were
considered and rejected on the way here. *Default-off-until-revisited* left the
largest population unhealed for a worry the trigger already answers.
*Outbox-relays-only* — restricting content pushes to relays in the author's
current kind 10002 — sounded principled and aims at exactly the wrong set: the
relays most likely to serve a stale profile are the ones the author **left**,
which are precisely the relays no longer in their 10002. An outbox-only healer
would never heal the worst offenders. The one restriction that survives is the
vanish guard: no content push to any (author, relay) pair a stored relay-scoped
kind 62 covers — the heal there is the kind 62 itself.

**5. Global versus per-relay — resolved: global.** The filter records one fact —
"we will refuse this id, whoever sends it" — and that fact is not per-relay. A
relay that heals stops offering the id, so a global row costs it nothing; a
relay that cannot heal is exactly who the row is for. Per-relay filters would
multiply storage by the 16k fan-out for no correctness gain, and the per-relay
dimension that IS needed (write capability, strikes) already lives in its own
small table.

**6. Silence — resolved: a strike rule with `Unreachability`'s conservatism.**
An unanswered `EVENT` never tombstones an id and never closes a relay on its
own. A publish counts as *unanswered* only when it was actually written to an
open socket and the connection then stayed open long enough for an `OK` to have
arrived — sized by the slowest single answer, not by the queue, per the NIP-45
trap in AGENTS.md (a 15s deadline once scored 91 "timeouts" of which zero were
refusals). Unanswered publishes earn strikes; write-closed requires the
threshold to accumulate across **at least two separate connections**, so one bad
session cannot close a relay. And write-closed gates only the *push* — the
filter still suppresses that relay's re-offers, which is why the two structures
stay separate.
