# Proposal: events we will never store, re-downloaded forever

**Status:** proposal, nothing implemented. Written to be argued with.

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

The dominant case is replaceable and addressable kinds. An author with five
years of kind-0 edits has one live version here and *n−1* superseded ones that
every negentropy-mode upstream will offer us on every cycle, forever. The set of
events in this state only grows, and it grows fastest on exactly the kinds
`sync = "negentropy"` was chosen for (0, 3, 10002).

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
"every superseded version the upstream holds", which is a floor, not a
transient.

## What this costs today — unknown, and that is the first thing to fix

The sample log in router.md shows `402,113 rejected [duplicate: already have
this event x401980]`. That is **not** this bug — a duplicate *is* in our id set,
so negentropy never asks for it; that line is a `fetch`-mode fan-out and the doc
is right to call it the system working.

The number that matters is the `REPLACED` tally, and specifically the *repeat*
`REPLACED` tally, and neither is broken out anywhere. Everything below is sized
against a number nobody has. See [Step 0](#step-0-measure-first) — it is cheap,
and it is the only honest gate on the rest.

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

### Fix 2 — heal the upstream: push the winner back

Instead of remembering that we refused an old version, **send the upstream the
newer one**. A relay that implements NIP-01's replaceable rule drops its old
copy on accepting ours, the sets converge, and the difference is gone — for us,
and for every other mirror that would have paid the same transfer. O(1) state
instead of a gigabyte, and it fixes the cause rather than the symptom.

The same move covers retractions, and there it is worth more than the
bandwidth: an upstream still serving an event our stored kind 5 deletes gets the
**kind 5**, and one still serving a vanished author's history gets the
**kind 62**. Deletion propagation is a known-weak part of Nostr; this is a
mirror in a position to repair it.

**It is cheaper to build than it looks.** It does not need the ingest-rejection
path at all — a reconcile already computes *both* halves of the diff in one
round trip. `DeleteMissingSync.kt:115` reads `diff.haveIds` (ours they lack)
beside `diff.needIds`; the ordinary down path simply discards that half. For
replaceable kinds, "they lack this id of ours" and "they hold a stale version"
are nearly the same statement, because there is one live version per address.
Going through the diff also sidesteps a store gap: `Rejected(REPLACED)` does not
say which event *won*, so the rejection path would need a query per address or a
store change to report the winner.

#### Where it stops working

Four ways the upstream does not drop its copy:

1. **We cannot write.** The dominant case. The dynamic fan-out is ~16k
   discovered relays — auth-required, paid, whitelisted, rate-limited. We have
   write access to almost none of them.
2. **The relay archives deliberately.** It accepts our newer version and keeps
   both. A real population, and not misbehaviour.
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

There is a decent argument that it is welcome (the event is author-signed, and
the relay already demonstrably hosts that author's data), but republishing
people's events to relays they did not choose is a values call, not only an
engineering one. **Per-stream opt-in, default off**, and it should be a distinct
setting from `dir = up` — the relay list is not the same list.

#### Two constraints to design in from the start

- **Push per address, not per rejected event.** `UpstreamPush` paces at 40 ms
  between publishes; 400k rejections would be 4.4 hours of pure pacing. Dedupe
  on `(relay, kind, pubkey, d)` once per cycle.
- **Convergence is only observable next cycle.** Which is exactly what Step 0's
  repeat-detector measures — see the composition note under Fix 3.

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
  distributed. Slice `k` disjoint bit-fields straight out of it — eight 32-bit
  indices are free, addressing up to a 512 MB filter. There is no hash function
  to choose, tune, or get wrong.
- **A fixed-size mmap'd file.** No sort-merge, no LSM, no eviction policy, no
  ordering invariant, no atomic-rename dance. Set bits in a bit array. A corrupt
  file costs downloads, not correctness — checksum it and rebuild on mismatch.
- **Concurrent writers are safe without locking, and this is the good part.**
  Setting a bit is idempotent and monotonic. Two processes mmap'ing the same file
  can both write with no coordination; the only race is a lost byte-level
  read-modify-write, and a lost bit-set is a *missed suppression* — it degrades
  to the status quo, never to a wrong answer. **That dissolves the objection that
  killed the state file for 3b**: the relay process can record its own refusals
  into the same file, so a client publishing a new profile no longer leaves the
  router blind to the id it superseded.
- **Sizing.** ~3.6 B/id at p=10⁻⁶, ~5.4 B/id at p=10⁻⁹ — call it 10× smaller
  than 3b. Use a *blocked* layout so a lookup touches one or two cache lines
  rather than `k` random pages in a half-gigabyte mapping.

**The one real danger: a false positive is silent, permanent data loss.** We skip
an event we actually wanted, nothing logs it, and the same id hits the same bits
next cycle, so it never self-corrects. Three things follow, and the third is
mandatory:

1. **Size `p` against the backfill, not the steady state.** Expected loss per
   cycle is `|genuinely wanted| × p`. At p=10⁻⁹, a 100M-event backfill loses
   ~0.1 events. At p=10⁻², it loses a million.
2. **Rotate rather than delete.** Bloom filters cannot remove entries, which
   matters for "the winner was deleted, so its losers are wanted again". Two
   generations, retire the older one on a slow cadence: a false positive gets a
   second chance when the generation that produced it retires, at the price of
   re-paying the true positives once per rotation. That is a knob, and a
   quarterly rotation still avoids ~99% of the cycles.
3. **Count the inserts and fail open past the design capacity.** This is
   non-negotiable. A Bloom filter sized for 50M holding 500M does not degrade
   gracefully — p goes from 10⁻⁹ to double digits and it starts discarding a
   sixth of everything, invisibly. Tracking `n` and disabling the filter once it
   is over budget turns the catastrophe into "the router got slower", which is a
   thing an operator can see and fix.

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
generational scheme in point 2 stops needing a tuned threshold. The structure
tells you when the generation is done.

**Its headline feature is useless here, though.** Cuckoo filters support
deletion; Bloom filters do not, and that looked like the answer to "the winner
was deleted, so its losers are wanted again". It is not. **Deleting requires the
item** — you need the id to compute its fingerprint and buckets. When a winning
event is removed we have no way to enumerate the superseded ids it should
release, because not storing them is the entire point of using a filter. The
only world where the deletion works is one where we already kept the ids, and
that world is 3b, which does not need a filter. Rotation remains the answer
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

**The fix keeps cuckoo viable: one file per writer, and the reader queries both.**
Merging is not actually required — checking two tables in turn is two bucket
pairs, still a handful of cache lines. Each process is then a single writer to
its own file with no cross-process lock, and only the in-process ingest workers
need a plain striped lock. The sync process reads both; the relay writes its own
and reads nothing.

**Binary fuse filters** are ~6% smaller again and are the space-optimal answer,
but they are **immutable** — built once from the complete key set, no incremental
insert. Making that work means buffering the generation's raw ids to freeze it
later, which is a real design and buys 6%. Not worth it.

**Verdict: cuckoo, with per-writer files** — provided the generational rotation
in point 2 is adopted, because insert-failure-as-rotation-trigger is most of the
value. If the design stays single-generation, take Bloom for the lock-free
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

Cleanly, and in one direction. **Step 0's repeat-detector is exactly the
instrument that tells you whether a push worked**: we pushed the winner, and
either the old id stopped being offered (healed — no row needed, ever) or it
came back (this relay will not heal — earn the row). So the table only grows for
relays that have demonstrated they need it, which is the strongest bound
available and strictly better than any of the heuristics under
[Bounding it](#bounding-it--the-part-that-decides-whether-this-is-safe-to-ship).

The dependency runs one way: Fix 3 is useful without Fix 2, but Fix 2 needs
Fix 3's instrumentation to know whether it is working at all.

## Half A — what earns a row, and what must never

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

**For 3a the answer is short**: one fixed-size mmap'd bit array on the shared
`/var/lib/vespa-relay` mount, written by both processes without locking, for the
reasons under 3a above. The rest of this section is about **3b**, where the
`created_at` has to be exact and the storage question gets hard.

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
  [Bounding it](#bounding-it--the-part-that-decides-whether-this-is-safe-to-ship)
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
the useful part.** For `DELETED` the target ids are already stored, in the kind
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

## Bounding it — the part that decides whether this is safe to ship

- **Time floor.** A reconcile only ever asks inside a leg, and legs are bounded
  below by the stream's `since` or `SyncCoverage.PLAUSIBLE_FLOOR`. Rows below the
  lowest floor any configured stream uses can be dropped: nothing will ask.
- **Only what a reconcile could re-ask.** A `fetch`-mode stream has no id set,
  so rows born from paged ingest buy nothing. Tag the submission with its source
  and record only reconcile-fed rejections.
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
cycle"* — decide whether anything past step 2 is worth building, and how big to
size the filter that step 3 ships.

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

1. **Fix 1** (`since` on the replaceable-kind streams) plus **Step 0**'s
   instrumentation. One cycle of data.
2. **Fix 2 on the static upstreams only** — the `urls` in `router.conf`, where we
   have a relationship and plausibly write access. Per-stream opt-in, default
   off, deduped per address per cycle, with a write-closed strike rule. This is
   the population where it converges, and it is a bounded blast radius to learn
   the acceptance rate in.
3. **Fix 3a** — wire the Step 0 filter into `DeleteMissingSync`'s `needIds`
   first, which needs no upstream change and proves the suppression on one path.
   Then the `wantId` predicate in quartz for the main sweep. Ship it with the
   insert counter and the fail-open ceiling from day one, and log the suppression
   count per cycle — a filter silently doing nothing and a filter silently
   eating everything look identical without it.
4. Measure the **residual**: with the bodies suppressed, how much is the
   un-converged reconcile still costing in round trips and id transfer? That
   number, and nothing else, justifies step 5.
5. If it does: **Fix 3b**, the `refused` doc type
   in **vespa-eventstore**, written at each decision point, populated only for
   relays that failed to heal. Read back through an opt-in argument —
   `snapshotIdsForNegentropy(…, includeRefused = false)` — never a blanket
   change, because a blanket change breaks `UpstreamPush` and `DeleteMissingSync`
   in the two ways tabulated in Half B, and contradicts the store's own
   `EXPIRED` reasoning for the serving direction. Budget a store release, a pin
   bump, and the JitPack lexicographic trap AGENTS.md warns about; the schema
   itself needs no migration step, since `SchemaDeploy` runs every boot.
6. Chart the suppression count and the filter's fill ratio on the Sync coverage
   card, beside the bands and sweeps.
7. Extend Fix 2 to the dynamic fan-out only if step 2's acceptance rate is high
   enough to be worth 16k unsolicited publishes. It probably is not.

## Open questions

- **A replaced version can become wanted again** if the newer one is later
  deleted (NIP-09 on the newer event, a `deleteMissing` retraction, a kind-62
  vanish). Under a permanent row we would never re-fetch it. I think that is
  correct Nostr semantics — a superseded version is not owed resurrection — but
  it is a behaviour change and should be stated, not discovered. Candidate 3
  gets the other answer for free by hanging the rows on the winner; under the
  recommended candidate 2, releasing them means carrying the winning address on
  each row and sweeping by address when a winner is removed. Decide which
  behaviour is wanted *before* choosing whether to carry that field.
- **Does the row survive a filter edit?** Sync bands are deliberately keyed by
  the whole filter so an edit forces a re-walk. These rows are keyed by *id* and
  would survive it. That is probably right (the store's verdict didn't change),
  but it means "edit the filter to force a re-walk" no longer re-offers this
  class of event. Worth a deliberate answer.
- **Ordering.** `entriesFor` merges two sorted sequences; confirm quartz does not
  require global sort order beyond what it already sorts internally.
- **Does Fix 2 need the author's consent?** We would be republishing someone's
  event to a relay they did not choose. The mitigating fact is that the target
  relay already hosts an older version from the same author, so it has already
  accepted their data — but "already has an old copy" is not the same as "wants
  the new one", and a vanish request (kind 62) is precisely an author saying
  otherwise. The kind-62 push and the profile push may not deserve the same
  default.
- **Does 3a want to be per-relay?** One global filter says "nobody should send us
  this". A per-relay filter would let a relay that healed drop out of
  suppression, and keeps one relay's junk from suppressing another's. It also
  multiplies the storage by the fan-out, which is unaffordable at 16k relays.
  Global is almost certainly right; worth stating rather than assuming.
- **What does a push cost against a relay that ignores it?** An unanswered
  `EVENT` is cheap per message, but the strike rule needs a definition of
  "unaccepted" that works when a relay simply never sends `OK`. Reuse
  `Unreachability`'s reasoning about what a silence may be published as.
