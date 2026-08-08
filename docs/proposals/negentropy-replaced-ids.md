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

- **(A)** record `(created_at, id)` for every event the store rejected as
  *permanently unstorable*;
- **(B)** merge that set into the local side of **down**-direction reconciles,
  and only those.

Once our side claims the id, the difference is empty and it never crosses the
wire again. Zero bytes — not "cheaper bytes".

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

## Representation and size

This is where the proposal lives or dies. Per entry: a 32-byte id and a
timestamp.

- **`HashMap<String, Long>`** — ~150–200 B/entry with the hex string and boxing.
  10M entries ≈ 2 GB heap. Not viable in a process that already serialises
  snapshots behind a semaphore for exactly this reason (`NegentropyPager`'s
  header: 14.9M ids for one stream, three concurrent streams heading for ~9 GiB).
- **Packed, sorted by `created_at`** — parallel `long[]` and a raw 32-B-per-id
  `byte[]` (raw, not hex). **40 B/entry**: 10M ≈ 400 MB, 50M ≈ 2 GB. A range
  query is two binary searches, which is *exactly* the shape `entriesFor(window)`
  wants, and every window is already time-bounded.
- **The same layout in a `MappedByteBuffer`** under the shared
  `/var/lib/vespa-relay` mount — OS page cache instead of heap, survives restart
  for free, and the relay could chart it the way it already reads the band and
  sweep files for the Sync coverage card (`SyncCoverageReport`).

Recommend the mmap'd packed layout. Writes are append-only into a small on-heap
buffer, sort-merged into the packed region by a flusher on `SweepState`'s
cadence.

**Persist it as binary, not JSON.** `SweepState` and `SyncBands` are
pretty-printed because a human is meant to read them; 10M hex ids in pretty JSON
is a ~700 MB file nobody will ever open.

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
2. Add a Bloom filter over rejected ids — ~12 MB buys 100M bits at ~1% false
   positive — and count **repeat** rejections per cycle. Repeats are the entire
   prize; a first-time `REPLACED` is not recoverable by any of this.

Two numbers — *"replaced x N, of which N′ we had already rejected in an earlier
cycle"* — decide whether the table is worth a gigabyte or is a rounding error.

That same filter is a usable **tier 0** on its own: a positive lookup skips
`verify()` and the store round trip. It cannot feed negentropy — it cannot
enumerate, and `entriesFor` must — so it saves CPU and ingest pressure but not a
single byte of bandwidth. Forty lines, and it is the whole ingest-side saving.

## Rollout

1. **Fix 1** (`since` on the replaceable-kind streams) plus **Step 0**'s
   instrumentation. One cycle of data.
2. **Fix 2 on the static upstreams only** — the `urls` in `router.conf`, where we
   have a relationship and plausibly write access. Per-stream opt-in, default
   off, deduped per address per cycle, with a write-closed strike rule. This is
   the population where it converges, and it is a bounded blast radius to learn
   the acceptance rate in.
3. If the repeat count *after* step 2 still justifies it: a `ReplacedIds` table
   in `:sync`, packed and mmap'd behind `SYNC_REPLACED_IDS_FILE`, **off when
   unset**, merged only at `StoreWindowIndex` and `Snapshots`, and populated only
   for relays that failed to heal.
4. Chart it on the Sync coverage card, beside the bands and sweeps it resembles.
5. Extend Fix 2 to the dynamic fan-out only if step 2's acceptance rate is high
   enough to be worth 16k unsolicited publishes. It probably is not.
6. **Only then** consider moving the table into vespa-eventstore. It belongs there —
   the store is what decided `REPLACED`, and the relay serving negentropy to
   *downstream clients* has the identical bug mirrored (a client re-offers its
   old profile version forever). But it must arrive as an opt-in argument
   (`snapshotIdsForNegentropy(…, includeRefused = false)`), never a blanket
   change: a blanket change breaks `UpstreamPush` and `DeleteMissingSync` in the
   two ways tabulated above. And it costs a store release plus a pin bump, with
   the JitPack lexicographic trap AGENTS.md warns about.

## Open questions

- **A replaced version can become wanted again** if the newer one is later
  deleted (NIP-09 on the newer event, a `deleteMissing` retraction, a kind-62
  vanish). Under a permanent row we would never re-fetch it. I think that is
  correct Nostr semantics — a superseded version is not owed resurrection — but
  it is a behaviour change and should be stated, not discovered.
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
- **What does a push cost against a relay that ignores it?** An unanswered
  `EVENT` is cheap per message, but the strike rule needs a definition of
  "unaccepted" that works when a relay simply never sends `OK`. Reuse
  `Unreachability`'s reasoning about what a silence may be published as.
