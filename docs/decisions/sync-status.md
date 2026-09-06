# Sync status decisions

The history behind `sync/.../SyncMain.kt`, `status/SyncProgress.kt`,
`status/StreamPhases.kt`, `status/SyncStatus.kt`, `status/StatusRollup.kt`,
`status/GaugeSeries.kt`, `status/SyncCoverageReport.kt` and
`status/RelayStatusReport.kt`, moved out of the source so the code reads on its
own. One paragraph per decision; `git log -L` on the function finds the commit.

**The mirror serves its own status page.** The mirror used to write three JSON
files to a shared volume that the serving relay read back, re-parsed against an
allowlist and re-narrated, about 2,500 lines whose only job was to re-derive
what the writer already knew. A file cannot say whether the process writing it
is alive, so the document carried a `writtenAt` heartbeat and the reader turned
it into a `staleForSec`; an HTTP request answers that by whether it answers.
`SyncProgressReport`, some 700 lines re-copying our own members, went with the
boundary that justified it; `SYNC_PROGRESS_FILE` is refused rather than ignored
so a deployment expecting the relay's card finds out at boot.

**The status rollup runs on its own daemon thread.** A rollup sharing
`Dispatchers.IO` with ingest queues behind whatever is saturating it, so the
moment an operator opens the page is the moment it stops refreshing. The pass
is wrapped in `runCatching` because `scheduleAtFixedRate` cancels the schedule
on the first exception, which would silently end the feature for the life of
the process.

**The status sites bind before `engine.start()`.** On staging (2026-09-04)
`main` parked for the life of the process behind a store walk inside `start()`,
`:7778` and `:7779` never bound, and `/sync/` and `/monitor/` answered 502 while
the log kept printing visits. Everything the pages read exists once the engine
is constructed, and a page that says "starting" is the state they are for.

**The schema deploy is retried on boot.** Compose starts both processes
together, and two deploys can race the same config server session; on a fresh
Vespa the loser has nothing serving to fall back to and crash-loops through its
first boot. Both processes deploy because this is the one whose writes a drifted
schema silently discards: 2.3M events were lost in one run while every status
line read healthy.

**The gauge series lives in the served document.** `StatsSnapshot` already
merges each tier into the document it serves and persists it to the stats file,
so a series kept there is carried across rollups by the merge and across
restarts by the file, with no ring buffer or second lifetime. It is bounded by
count rather than age because the bound has to hold whatever the rollup
interval is set to, and sixty samples cost about 1.2KB. Per-stream and
per-processor series were left out: the alias fold runs on a six-hour clock, so
an hour of samples would not contain one of its passes.

**One band snapshot per status tick.** The band map was measured at 13.7MB and
213ms to parse, and both the coverage fold and the per-relay table walk it, so
`SyncStatus.publish` builds it once and hands it to both reports.

**The band file nests stream → filter → relay.** The flat `"<relay> <filter>"`
format wrote one byte-identical filter into 4,000 keys, so the coverage parse
happened 4,000 times; nested, it happens once per distinct filter by
construction. The flat keys survive only as a migration shim: `SyncBands`
prunes them on load after 2,623 folded aliases nothing dialled were charted as
three unnamed groups frozen at `reconciled=0`, while `SweepState` still claims a
flat cursor lazily, so dropping those would chart a mid-sweep relay as
un-walked.

**A group's filter is what its legs agree on.** An author-narrowed stream
reaches the file as thousands of distinct filters under one stream name, and
publishing one of them as the group's filter lied about the other 3,999.
Members a narrow varies are dropped even when the legs agree, because a
discovery filter's `authors` runs to thousands of keys on every stats poll.

**`hosts` is published beside `relays`, not instead of it.** One server answers
on every path, so a relay list can mint urls without limit; measured, 3,272
urls on 850 hosts. The gap is the disclosure.

**Coverage bands are merged per relay, never overwritten.** A relay reached by
several legs of one author-narrowed stream landed once per leg, and taking the
last one charted whichever author the file happened to write last as the
relay's whole coverage.

**`parse` catches `Exception`, not `Throwable`.** The band file scales as
relays × filter size and once exhausted the heap; `runCatching` turned the
`OutOfMemoryError` into "no sync state in this document", a quiet lie on the
one page built to report it.

**The per-relay table joins on the unit's owed asks, not on the bands it
holds.** A `contentViaOutbox` unit owes one ask per bound author, and the
first version called a unit owing forty asks complete when its one drained
band was settled. `RosterBuilder.UnitAsks.identity` is already the filter JSON
the band snapshot is keyed under, so the join is exact and free;
`RelayStatusReportTest` pins the case. Asking `SyncBands` per ask instead would
serialise a 141-kind filter per row per poll past quartz's fingerprint cache.

**Urls join verbatim.** Both sides write the pool's own normalized string, so
canonicalising would be about 10,000 re-parses per tick to produce the strings
we started with, and a canonical form that is not the snapshot's key would join
nothing.

**A row carries two axes, and the fault rule is stale and not tailed.** The
first table sorted by `syncStatus` alone, so a `complete` pair nine days cold
sat green at the bottom under 1,200 healthy paging rows, and a mirror nine days
cold read as 1,452 relays finished. A tailed pair is excluded whatever its age
because its present arrives live.

**Statuses are published as rows, not members.** `complete` is already a
band's member and `counted` the root's sentence, so four status members would
have put one word on two quantities, the overload `StatusVocabularyTest`
exists to catch.

**The fold indexes units by position under a nested stream → relay map.** A
flat `"$stream $relay"` key was a string concatenation per band, about 10,000
per tick, and the stream level lets a bands file's renamed or retired streams
be skipped whole rather than walked per filter and relay.

**`MAX_ROWS` is 1,000, worst first.** The table this replaced was 10,462 rows
behind a filter box nobody could read; a four-stream router on a 700-relay
roster is 2,800 units. One named relay is a question for the document via
`jq`, not the page.

**`STUCK_LEG_SECONDS` is ten minutes.** The slowest healthy leg measured was
the full `indexers` walk on purplepag.es, 1,490,010 events in about 10.8
minutes, and directory.yabu.me serves a 1.2M-event backlog below its floor. The
stuck line names the url because container logs rotate inside the hour and a
leg held since the small hours left no trace by the time anyone looked.

**`StreamPhases` has one phase.** It carried a fan-out's whole vocabulary
(discovering, snapshotting, fetching, syncing, holding, idle, failed) because a
stream's engine was a pass over its relay list. Every stream rides the visit
pool now, so a stream is `Rotating` from its first visit and what moves is the
numbers; what each relay is doing is the in-flight list. `names()` replaced
three identical one-call-site methods that each re-did `register` and the map
lookup.

**`arrivingPerSec` is published beside `eventsPerSec`.** With the queue full
and the drain at zero (staging, 2026-09) the page read "0 events/s into the
store", which is true of a store that stopped answering and of a fan-out gone
quiet alike; only the arrival side tells which.

**`stageMs` is cumulative.** `IngestStats.statusLine` is destructive, storing
a per-stage high-water mark and returning the delta, so a second caller would
halve the operator's log line. Its absence is why #167 was diagnosed by
inference: `bottleneck` and `oldestBatchSec` could not say whether a batch was
in `dedup`, `write` or `lock.ingest.wait`, which have different remedies.

**`fatals` is always published, including zero.** An `OutOfMemoryError` kills
whichever thread allocates next and is caught by nobody, so the router carries
on looking merely quiet; four of them once passed unnoticed. A member that
appears only on damage cannot be told from a router too old to say, and the
rest of the document follows the same rule.

**`sweeping` counts readable cursors, not `marks.size`.** `widest` keeps a
cursor with a missing edge rather than poisoning the pair, and the row emits
`sweep` only when both edges read, so the count and the rows disagreed by one
on a state file the router did not write. The page's url filter, which
restates the count off the rows, is what made the gap visible.

**`RELAY_NSEC` unset is said in the boot log.** `PeerClient` attaches the
NIP-42 responder only with a signer, so an anonymous deployment printed nothing
and "they turn our key down" and "we never answer" were the same log.

**`stages` is one list, not a total list beside a detail list.** The rows came
off one `IngestStats.snapshot()` at one instant, and two lists invited a row
whose `ms` and `calls` were read seconds apart. The present-tense `lockHeldBy`
sits above the cumulative stages because it is the row that answers a stall,
and stages are rows rather than a member per stage because a dynamic member
name is one the glossary can never define.

**The coverage card's denominator is relays a stream has touched, not relays
it names.** A dynamic stream has no configured list, so the only honest count
is the urls that reached the band file or a sweep cursor.
