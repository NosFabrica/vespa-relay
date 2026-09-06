# Sync engine decisions

The history behind `sync/.../{SyncEngine,SyncBands,NegentropyPager,
SweepState,RosterBuilder,RetractionAudit}.kt`, moved out of the source so the
code reads on its own. One paragraph per decision; `git log -L` on the
function finds the commit.

## SyncEngine

**A monitor-only node boots the whole plumbing.** Behind the "nothing to
mirror" return the monitor started nothing and said nothing. Three pieces
were missing and each failed silently: ingest was never started, so a probe's
first submit parked on a bounded channel nobody drained; `peers.connect()`
was never called, and `NostrClient.subscribe` guards its send behind the
`isActive` flag only `connect()` sets, so every probe REQ was recorded and
none sent and the only grade still reachable was `dead` off the raw TCP
pre-probe; and the progress document was never published, so the rows moved
in memory and reached no page.

**The progress document is published on its own loop.** It used to ride the
per-stream phase report, which is skipped for a push-only router, so a
perfectly healthy mirror with no down stream rendered as a stopped one. The
document describes the process as much as its streams.

**`wedged` is a third answer beside `ingest`.** A queue at its ceiling
because ingest is writing as fast as it can and a queue at its ceiling
because ingest has stopped look the same from the depth. Issue #167 was a
whole investigation because the line said "downloads are backpressured"
while three events landed in nine minutes. `IngestPipeline.wedged` (a worker
inside one batch pass for minutes) separates them, and `bottleneckOf` is read
once so the log line and the document cannot disagree about the same instant.

**The health line reports both queue rates.** `rate` is what came out of a
batch and `arriving` what went in. Staging showed a full queue at `0 ev/s`
with thousands still arriving, which is a store that stopped answering, and
the line could only say the rate, which reads the same as a fan-out gone
quiet.

**`stageSplit` is one `IngestStats.snapshot()` read, replacing the `dump()`
parser.** `stageMs` used to come from parsing `dump()`'s string beside a
second `snapshot()` read for `stageDetail`. Two reads of a live counter
describe two instants, so a row's `ms` could disagree with the `calls`
published next to it, and the parser lost precision through `%.2fs`: a stage
under 5 ms rounded to nothing. `snapshot()` is the structured read the parser
stood in for, so both members come off one map at one instant in exact
nanoseconds; `IngestStageReadTest` pins both. The sort is done here, not by
the store, because the order is this page's presentation choice.

**Fatal errors are counted.** Four OOMs once passed unnoticed while the
phases still read healthy. `watchForFatals` is a named function because both
boot paths need it and the monitor-only one was running without it.

**`streamGate` bounds the id-set build, never a run.** A stream's negentropy
snapshot was measured at 14.9M ids, and three concurrent streams headed for
~9 GiB, so builds are serialised. The pool is a rotation with no join, so
holding the permit for a run would hold it for the life of the process.

**Filter widths are one instance for the pool and the pager.** See
visit-pool.md (issue 185); `ClientWindowSync` takes the instance with no
default so the sharing cannot regress by a call site compiling.

**The heal row's `queued` is the live depth.** It published `enqueued`, a
lifetime counter, under the same name the ingest row uses for a depth, on
the row directly below it.

**There is no `reachability` processor.** It reported a passive NIP-66
watcher that no longer exists; the fitness pass publishes a count per
verdict and `heldOutDead` sits on the alias source's row.

**`close` orders cursors, workers, listeners, transport.** Bands and sweep
cursors flush first so a killed visit keeps its ground; the scope is
cancelled before the client closes so visits do not count their own deaths
as aborts; the complaint and page listeners come off before `peers.close()`
or they leak on the closing pool.

## SyncBands

**The audit clock is the router's, not quartz's `fullAt`.** `Band.widen`
keeps the old `fullAt` on every non-stale merge (upstream defines it as when
the last from-nothing pass finished). Read as "last verified" it froze: once a
band aged past `negentropySyncThePastSeconds` every visit's audit was due
again, and one relay took 13 full history sweeps in 40 minutes. `verifiedAt`
advances on every `reconciledThrough` record and is persisted beside the band.

**`claimAudit` goes through `auditDueAt`.** The clock chain was spelled twice,
once per audit path, so the gate an audit ran on and the number the status
page certified it by were two expressions that could disagree; the panel
written to prove the schedule is obeyed would then be the thing that lied.

**Attempts are spaced independently of completion.** An audit that cannot
complete (negentropy refused, sweep interrupted) advances no clock, so
without the attempt stamp it retried on every visit at the cost of a full id
snapshot each time. A quarter of the period, floored at 15 minutes so a flaky
relay is not re-swept on the revisit floor and capped at six hours so a
weekly audit still retries within a shift.

**The re-fetch period is per stream, with no default anywhere.** Re-reading a
relay's whole history is the most expensive thing the router does on a
schedule (the content mirror is ~130 kinds against every certified relay),
and it was running on quartz's week in every deployment that had never heard
of the knob, beside reconciles covering the same ground for the difference
alone. The three env names that used to carry it are refused by name rather
than ignored, because ignoring one silently takes a deployment's schedule
away on upgrade.

**Per-stream periods are fixed at construction.** A `SyncCoverage` is built
on a stream's first band and carries its period for the process's life, so a
period registered later would be silently ignored and the symptom, a stream
re-walking on the wrong clock, is invisible for a week.

**`dropFolded` returns what changed, and replaces the set.** The fold is
re-applied from the store on the first cycle after boot, so counting the
verdict set reads as a mass deletion every boot and rewrites a multi-megabyte
file to produce the bytes it had. On the production corpus that was 5,514
urls' worth of bands charted on `/stats.json` as walked while no sync
existed for them. The set replaces rather than accumulates because a verdict
carries a TTL; accumulating kept suppressing the bands a url earned after its
verdict expired. Bands are dropped from the file, not moved onto the
canonical, because the fold's evidence is a containment measurement over one
window, enough to stop dialling a duplicate and not enough to hand its claim
to a url whose own legs would then close over ground it never walked. `keep`
exists because `urls` and `relaySource` may name one stream, so a configured
upstream the fan-out folded away was dialled and recorded under the stream
name while every band was filtered out of the file.

**Flat pre-stream keys are pruned on load.** They were held aside for the
first stream to claim them, but a claim needs a live stream to ask and the
keys still there are precisely the ones no stream asks about. On staging
four days after the format nested, 2,624 of 2,628 top-level keys were flat
(2.5MB of a 13.8MB file), none written since 90 minutes after the build
landed, and every one a subpath alias the fold had removed from the fan-out.
`SyncCoverageReport` charted them as three unnamed frozen groups.

**A corrupt band file starts fresh and reports no prune.** Exiting costs the
mirror. The prune count from a parse that stopped part-way is not a fact
about the file, and reporting it would let the boot rewrite the damaged file
before anyone looked at it.

**`flooredForPaging` is what ends a paged walk against purplepag.es.**
`fetchAllPages` cursors newest-first by `until = oldest created_at seen`.
purplepag.es holds twelve kind-10002 events stamped `created_at = 0` and
treats `until <= 0` as no `until`, answering its five hundred newest events;
none match client-side, quartz steps to `until = -1`, and it asks again, ~5.5
pages a second for the life of the process, never recording a band. quartz
now floors its own cursor (amethyst#3889, pin `a5507f9a`) and calls a relay
answering above the boundary `UNPAGEABLE`, which settles nothing and re-walks
1.49M events on the next boot; flooring the REQ makes the relay drain at real
data so the leg closes. Known hole: a config writing `since = 0` walks
unfloored; the right fix is the config loader normalising it to absent, not a
clamp here, because `drainSettlesThePast` compares the leg's floor against
the filter's.

**`drainSettlesThePast` compares floors, not equality.** `SyncCoverage.legs`
hands the older leg the filter's own `since` (null for every configured
stream) while the sweep fallback pages `outstanding()`, which materialises
that null as `PLAUSIBLE_FLOOR`. An equality test made the drain unreachable
on the whole NIP-77-less backfill path while looking correct at both call
sites. For a bounded filter `SyncCoverage.windows` re-opens the older leg
even after a drain; closing that needs `complete` to carry the floor it was
earned at, an upstream change.

**The band arithmetic is quartz's.** ~150 lines of it lived here as a fork
and missed two upstream fixes (`coveringWindow` letting a relay that needs
nothing widen the shared snapshot; `legs` not re-owing an older leg when the
floor dropped) while this file's comments described them as solved. The key
split at the first space was upstream's too (amethyst#3877).

## NegentropyPager

**`WindowSync.page` says whether it was refused as its own fact.** It
returned an `Int`, and a refusal (a capped filter width, an auth wall, a
policy `CLOSED`) delivers zero exactly as an empty window does, so the sweep
marked the window complete and the cursor claimed ground nothing had read,
durably, through the band recorded on top of it. Returning quartz's
`PagedFetchResult` was the first attempt and wrong: `refusedOutright` is
`downloaded == 0 && ...`, and a walk over several width chunks let one chunk
that delivered mask a later one that was turned away.

**`complete` is `refusedWindows == 0`, not an empty stack.** A sweep that
walked every window and was refused on one reached the end and said true,
so the band settled over ground no REQ had been served for and every later
audit was told there was nothing to find. The stack being empty means the
windows were visited, never that they were answered.

**The NEG-OPEN is not chunked by width; its fallback REQs are.** A reconcile
compares an id set, so splitting its filter by kinds is N reconciles and N id
snapshots where the REQ path costs N round trips. A width-refused NEG-OPEN
throws `UNAVAILABLE` and lands on the chunked page path either way. Chunking
the catch-up and not the sweep left every width-capped relay's audit refused
as before.

**`onWindow` is announced after the bisection, not before.** The first window
off the stack is the whole leg, a decade wide on a deep history; announcing
it reported a floor the sweep had not reached and then walked the cursor
upwards as each bisection narrowed it.

**The per-kind reconcile's unreconcilable hook pages.** It was `{ }`: a
(kind, second) denser than the peer's cap at the peer while thin here (a spam
burst never mirrored, the thing an audit exists to find) was neither
reconciled nor paged, and the surrounding window then completed and was
claimed with the slice unreachable on every later audit.

**`sayRefused` reads complaints from the window's own ask time.** It passed
`0`, which reads any sentence the relay has ever said and printed an
hours-old unrelated refusal as this window's cause. Awaited, because the
refusal reaches the caller before the sentence reaches the recorder.

**`PrimedIndex` hands quartz the count already taken.** quartz counts the
window it is given before deciding whether to sub-split, the same window
the pager just counted to size the checkpoint; at tens of thousands of
windows the second query was worth fifteen lines.

**The pager's window boundary is a timestamp decided by a count.** A NEG-OPEN
is all-or-nothing at both ends: our id snapshot must fit in memory, and a relay
refuses outright past its `max_sync_events`. So a window is sized from two
sources, our own `NegentropyLocalIndex.count` before the round trip and the
peer's cap from its refusal (`NegentropySyncResult.peerCap`) or, failing that,
a window quartz had to split. quartz owns everything inside one call; the pager
owns what survives a call: the cursor, the per-peer size, and the newest-first
order every push onto the stack keeps.

## SweepState

**A window merges into the claim only when it touches it.** Blind min/max
widening broke on one path: a resumed sweep pushes the slice above the old
claim, the first window it completes up there bisects down from the new
ceiling, and merging it absorbed the un-compared gap between the old `upTo`
and the window's floor. Interrupted a second time, the next resume skipped
the gap and `finish` recorded a history audit as verified with a hole in it.
A disjoint window forfeits its progress on the next resume, which negentropy
makes cheap, and still moves the liveness stamp.

**The stream is part of the cursor's identity.** Two streams asking one
relay the same filter start at different moments and stop at different
depths; sharing the cursor let a stream inherit ground it had not compared.
The cost is at most one window walked twice.

**Pre-stream cursors are a migration shim, drained by first ask.** The flat
key never said which stream wrote it. `claim` runs from `advance` too, so it
merges rather than puts, or a window just finished on another coroutine is
dropped. Delete the map, `claim`, and the flat branches in `load` and
`snapshot` together.

**The stale-after knob keeps a default; the re-fetch period does not.** Both
read the same env var once. This one decides whether an interrupted sweep
resumes or restarts, work already scheduled and at most one sweep's worth;
the other schedules a whole history download and may not do so unasked.

## RosterBuilder

**Asks and their identity are one value.** `asks` and `wants` were two maps
with the same `url → stream → ...` shape kept in step by convention, and two
adjacent numbers on the status card were counted off different maps. The
identity is the filter's JSON because quartz's `Filter` compares by
reference: comparing asks requeued the whole roster every tick, comparing
nothing left a new ask waiting out the tailed revisit for its first catch-up,
and the old `ask !in wanting` linear scan matched nothing.

**One identity set per (url, stream), not per url.** One set per url made
every stream's business every other's: a scan pairing relay R with a new
provider for `indexers` moved the url's set, so `content`'s tail on R saw its
want list change and re-opened its subscription for a change it had no part
in.

**The roster nests by (url, stream).** With url → asks, every reader filtered
a list to find its own stream's; the tail listener did it per event, so a
relay three streams want walked all three streams' asks three times per
message delivered.

**Shared authors are read off every ask, not the scan branch.** An
author-bound stream filter fanned to N relays by a verdict source ran its
retraction audits with an empty shared set, and one relay's answer could
retract what its siblings still served.

**The gate is cached on the per-source clock.** Re-derived every roster tick
it ignored its own `refreshSeconds`: an extra indexed query a minute for a
30166 gate, a corpus walk a minute for one pointed at a curated 10002 list.

**One ask per bound author, no knob.** A `30382:rank` tag pairs one provider
with one relay, so a per-author split keeps each `(relay, provider)` band
valid forever: a new provider naming the relay is a new band beside the old
ones, never an invalidation. This is `authorsPerLeg = 1` made structural.

**A cancelled rebuild is not a store that could not answer.** Swallowed, it
printed the could-not-read line during every ordinary stop, a wolf cry
against the one line that matters when the store really cannot.

## RetractionAudit

**The audit does not page.** The legacy `DeleteMissingSync` paged the
cascaded kinds and any ask whose reconcile failed, because in the cycle model
that call was the ask's only visit. In the pool the catch-up has already
paged the ask, so a failed reconcile decides nothing.

**No cascade onto kind 0 and 10002.** A wholly retracted service's profile
and relay list went with its scores, on the reasoning that a key signing
nothing is kept alive by our copy alone. A provider relay serves no kind 0
or 10002 (measured over 12 (service, relay) pairs), so every copy came from
the profile streams, which re-mirrored them over a live tail right after the
cascade deleted them.

**The clock, the reconcile and the delete read one owned projection.**
Reading the full ask's filter for the clock found a key nothing stamps and
fell back to a band `fullAt` no reconcile moves, a schedule permanently in
arrears while the audits ran perfectly well. `AuditClock` distinguishes
never-compared (due) from no-owned-kind (not scheduled), because counted as
due the latter is a backlog that can never drain.

**No size guard on the delete.** A provider that retracts a subject usually
does so because the subject turned out to be a scammer, exactly the score
that must not survive; the completed reconcile is what makes an empty answer
trustworthy.
