# Relay maintenance decisions

The history behind `relay/.../maintenance/` (`StatsRollup`, `RelayProfile`,
`MirrorReport`, `FtsReindex`, `TrustReconcile`, `OrphanScoreSweep`,
`ExpirationSweeper`) and the relay's live stats page, moved out of the source so the code
reads on its own. One paragraph per decision; `git log -L` on the function
finds the commit.

## Stats

**The page counts the store over the relay's websocket; nothing rolls it up.**
The charts tier walked the corpus behind the server with Vespa groupings: 135s
every six hours on the live deployment (470M events), so every number on the
page was hours old by design, the newest-event tile was the age of the rollup
rather than of the store, and the distinct-authors-per-bucket series was the
shape that OOMKilled the engine twice. Measured over the public socket on the
same store (anonymous, `include:spam`): a COUNT over the last minute, hour or
day answers in 10–60ms, a per-kind day in ~10ms (fifty of them in 440ms), a
30-day window in ~1s, the one-per-author totals in 56ms (10040) to 1.3s
(kind 0), while the whole store took ~9s and kind 30382 alone 2.3s. So the page
asks only bounded windows and one-per-author kinds unprompted, each on its own
cadence by how fast its answer moves, and the whole-store count waits for a
press. The cost moved from a periodic walk nobody might be reading to asks made
only while somebody is: about 380 on a page load and under one a second after,
none in a hidden tab.

**What did not survive the move.** Distinct authors per bucket, the all-time
per-kind histogram with its spans, the hour-of-day shape, the NIP-65 relay
distribution and the zap services: none is a COUNT over a window, and each was a
corpus walk. The replaceable kinds stand in for authors where the protocol
allows it — a count of kind 0 is a count of pubkeys with a profile — and are
labelled as exactly that.

**A live tail would not have done it.** A subscription on the relay sees only
events published to the relay process; the mirror writes into Vespa from the
sync process, so on the live deployment a `limit: 0` REQ stayed silent for 40s
while the store took 12 events a second. Rates come from COUNTs over
`created_at` windows instead, which also count mirrored history arriving late
— which is why a closed bucket is re-asked, rarely, rather than frozen.

**The kinds table enumerates from the store's own answers.** The page it once
replaced (`kind_stats.html`) asked one COUNT per kind it knew to name, so an
unregistered kind was invisible. The live table counts the router's
`sync.mirrors.kinds` plus every kind among the newest 100 events, so a kind
nobody named shows up as soon as it arrives.

**Every window is bounded above at now.** `created_at` is author-signed; the
store holds notes dated 2100, which would count in every window reaching the
present and open a bar decades out.

**`SCHEMA_VERSION` is bumped only for released fields.** Version 3 is the
corpus sections leaving with the rollup; version 2 was `pubkeys` to
`authors.pubkeys`, `corpus.kinds` dropped, `tookMs` to `tiers.<name>.tookMs`.

**What the grouping rollup taught, for anyone bringing one back.** A `count()`
walks every document it matches, and a `kind` filter bounds the group set, not
the walk. `grouping.defaultMaxGroups`/`defaultMaxHits` must travel as -1 or a
pipeline with no `max()` returns a plausible top ten. Aggregations must run
under `unranked`, since a recency profile's match phase under-counted by 10x.
A nested grouping and a flat one answer in the same shape. `time.date()` does
not zero-pad. A failed rollup must mark the document stale by tier. The code is
in the history before this change (`StatsYql`, `StatsVespa`).

## MirrorReport

**The manifest's kind set is published so a coverage comparison can mean
something.** Counting our events for an author against their own relay's count
is a filtered mirror over an unfiltered total: measured 31,118 here of 89,485
there (35%) on a mirror missing nothing it was asked to hold. The gap was kinds
3, 4, 5, 6, 7 and 1059, two of them encrypted DMs and gift wraps the mirror
must never hold.

**A stream with no kind bound publishes `allKinds: true` and no `kinds` list.**
A union over only the streams that name kinds is a smaller set than the truth,
and a client scoping a COUNT to it under-counts the very denominator this fixes.
A present but unreadable `kinds` member contributes nothing rather than
widening the set to everything.

**Every member is read through `as? JsonPrimitive`, never `jsonPrimitive`.**
The accessor throws on an object or an array, and the manifest is another
process's file; one `"name": {}` would reach `StatsRollup`'s per-section catch
and drop the whole `sync` section, coverage included.

## RelayProfile

**The NIP-11 fields are blanked, not nulled, when the doc no longer carries
them.** Quartz reads null as "leave whatever is there" and blank as "delete
it"; a description the operator removed from the doc must not survive in the
profile the doc is the source of. Everything the writer does not own (a
`nip05`, a `lud16`) is carried forward by `updateFromPast`.

**NIP-39 `i` tags are put back exactly as found.** `MetadataEvent.updateFromPast`
drops every `i` tag and re-adds what `IdentityClaimTag.parse` returns, which is
lossy twice: a claim with no proof (`["i", "github:alice"]`) disappears, and an
identity with a second colon (`matrix:@alice:example.org`) is split on the first
and reassembled as `matrix:@alice`. Both are silent, both are signed, and both
survive every later boot because the damaged tag is what the next edit reads.
The initializer runs last, so restoring the held tags there is the whole fix.

**`created_at` is `max(now, held + 1)`.** A store enforcing replaceable
semantics refuses an edit that is not strictly newer, and a clock that has not
moved since the last write, or went backwards across a restart, is ordinary.
Silently losing the write is how a boot reports success having published
nothing.

**A failed read is retried, never taken as an empty store.** Both kinds are
replaceable and decided from what the store holds; a cold Vespa read as "nothing
stored" would replace a profile carrying a `nip05` with the two fields this
file knows about. Bounded at ten minutes, because a wrong url must not retry
for the life of the process.

## FtsReindex

**The walk repairs fed columns only, never derived ones.** `search_text_gram`
(store e3be81564d, `indexing: input search_text | ... | index`) is produced by
Vespa at index time, so deploying it left the column empty on every stored
event. The store's drift check compares the search columns and near arrays,
finds both identical, re-puts nothing, and the run reports success having fixed
nothing; whole-word search keeps working the whole time, so nobody notices.
Such a column needs a Vespa REINDEX on the config server (a POST alone leaves
the job `pending` forever; a redeploy dispatches it) or a full re-feed. See
docs/migrations.md.

**The cursor is persisted per page, by temp file and move.** An in-memory
cursor once threw away 12M events on a failed page. A bare `writeText`
truncated mid-kill leaves a corrupt token that fails every resume until someone
deletes it by hand. On a resumed run no denominator is printed: `total` counts
only the remainder, and total/corpus would report 3% at completion.

**`runCatching` around a page must rethrow cancellation.** A shutdown mid-page
arrived in the failure branch and printed a page failure and a retry for a page
that had not failed.

## TrustReconcile

**Zero providers is retried like a failure.** The call once "succeeded" one
second after start against a content node still loading 24M events, and the
projection stayed empty for hours. Zero is indistinguishable from cold, so it is
only accepted once the ten-minute wait budget is spent; a real failure past the
budget serves unranked results rather than holding the relay off its port.

**The first failure prints its stack, whichever attempt it is on.** A success on
attempt 1 followed by a throw on attempt 2 deserves the same diagnostic, or the
loop retries silently for ten minutes; a deterministic bug and a cold engine
look identical from one message.

**The reconcile's progress callback is wired, throttled once for the whole walk.** `reconcileTrust` always took an `onProgress` and the reconciler emits a real denominator with it, but every caller passed the argument out, so an hour-long walk printed one line at its start and one at its end. Two phases are reported as two things: screening moves `inspected` toward `total`, re-deriving holds it there and moves `rebuilt` and the card count, because a percentage in the second phase reads as a motionless 100%. The reporter is built once outside the retry loop; built inside it, every attempt restarted the 30s window and a reconcile that retried for half an hour printed nothing. Retries stay audible for the same reason: a loop that prints only its first failure cannot be told from one that stopped failing.

## OrphanScoreSweep

**The sweep log prints counts and three example ids.** Printing the full orphan
list once produced a 38,920-character log line. A deletion is not a tombstone,
so the by-kind 30382 stream re-downloads what the sweep frees unless its filter
is narrowed in the same change.
