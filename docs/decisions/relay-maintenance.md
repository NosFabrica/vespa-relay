# Relay maintenance decisions

The history behind `relay/.../maintenance/` (`StatsRollup`, `StatsYql`,
`StatsVespa`, `RelayProfile`, `MirrorReport`, `FtsReindex`, `TrustReconcile`,
`OrphanScoreSweep`, `ExpirationSweeper`), moved out of the source so the code
reads on its own. One paragraph per decision; `git log -L` on the function
finds the commit.

## Stats

**The stats document has two cadences, split by measured cost.** Every number
was recomputed on one timer, so a total Vespa answers in milliseconds was
fifteen minutes stale because a distinct-authors-per-month series over four
years of corpus ran beside it. The tier is the section, not the query: a
section carries one `generatedAt`, and `pubkeys` moved out of `corpus` into its
own `authors` section rather than sit under a timestamp wrong for half the
members. The tiers are not serialised against each other; a mutex would make
the fast cadence as slow as the slow one.

**The per-kind table has no author count.** `all(group(kind) each(all(group(pubkey)
output(count()))))` partitions the whole corpus into a pubkey set per kind. At
91.5M events (kind 1 alone 39.7M) it drove proton to allocate 2 GiB
`PartialResult` buffers per match thread and OOMKilled the engine at a 46Gi
limit, then again at 64Gi (measured 2026-08-08). A distinct count over a
high-cardinality field is the group set; a cheaper query does not exist, and
the column would need a counter maintained on write or a sketch.

**Zaps are on the slow cadence despite being counts.** A `kind` filter bounds
the group set, not the walk: `group(pubkey)` over millions of kind-9735
receipts returns a handful of LNURL services and touches every document to
find them. Sats are not published at all; the amount is in `bolt11` and
`description`, multi-character tag names `tag_index` cannot address.

**The monthly series is anchored to January 2023 and asked a year at a time.**
A rolling 24-month window slid the corpus's early years off the left edge.
Anchored, `distinctAuthorsBy(MONTH)` over the whole span would grow a pubkey
set per month forever with no request-path cap (`StatsVespa` sets no read
timeout on purpose), so `monthSlicesFrom` cuts at calendar years: a month
bucket must fall entirely inside one slice because distinct authors do not sum
across a split, and years divide months exactly.

**The newest event is asked over a two-day window, and carried forward.**
Freshness is what the per-minute cadence is for, but the honest whole-corpus
answer is the kinds histogram's spans. `recentNewest` asks the same
`spanBy(kind)` with a window on it; a quiet window falls back to the previous
document's `corpus.newestEvent` or per-kind spans, and the maximum is taken so
the number only moves forward. A timestamp is exactly as true as when it was
taken, which a count would not be.

**Every window is bounded above at now.** `created_at` is author-signed; the
reference dashboard reported a `last_seen` of 4130944797 (the year 2100), which
opens a day bucket 74 years out and squashes every real bar. Vespa also answers
no bare aggregate: `all(output(max(created_at)))` fails with HTTP 500 and a
null-pointer message, so the corpus-wide newest is derived from per-kind spans
(`NO_BARE_AGGREGATES`).

**Section status counts successes, not `data` emptiness.** The status was
`data.isEmpty() -> "failed"`, and every section writes its own metadata
(`asOf`, `windowDays`) before a query returns, so `failed` was unreachable and a
section whose every query errored reported `partial`.

**Per-query `queryMs` is published in the document, failures included.** It is
the only evidence for which queries belong on the per-minute cadence; a corpus
twice the size or a kind filter that stops being selective moves a query
between tiers, and an operator re-tiers from this rather than guessing.

**A failed year in a series contributes nothing, not even its zero fill.** Zero
bars for a year that could not be read is a claim about the corpus; absent bars
are a gap beside a `partial` badge naming the year. The pubkeys column is all or
nothing across slices because a point without `pubkeys` charts exactly like a
real zero. An empty authors map is likewise not zero-filled: `bucketed` drops
groups it cannot read, and zero-filling made `stats.html` print "No publishing
pubkeys in this window." beside a chart full of bars.

**Relay distribution sums per canonical url.** The `tag_index` grouping returns
one row per distinct string, and `wss://nos.lol` and `wss://nos.lol/` are two
strings for one relay; `toMap()` kept whichever came last and threw the other's
count away.

**The per-kind daily series is one nested query.** It was one aggregation per
kind, eight sequential queries for eight sparklines; `nested("kind", DAY)`
answers kind -> day -> count in a single 310ms response on Vespa 8.733.

**The kinds table names kinds from Quartz's registry, and lists every kind.**
The web UI's `kinds.js` holds about 117 renderer badges; `KindNames` holds 291
protocol kinds, and naming from the renderer table left 180 rows reading
"kind N". The table replaced `kind_stats.html`, which asked one NIP-45 COUNT
per kind it already knew to name, so an unregistered kind was invisible; a
top-N would reintroduce that blind spot.

**`SCHEMA_VERSION` is bumped only for released fields.** Before the endpoint
shipped, `retention`, `newUsers` and `kinds.shown` each left and each bumped
the number, which would have landed the first public version at 4. Version 2 is
the first post-release change: `pubkeys` to `authors.pubkeys`, `corpus.kinds`
dropped, `tookMs` to `tiers.<name>.tookMs`.

**A failed rollup marks the document stale, named by tier.** A stats service
failing all night otherwise serves a page indistinguishable from a healthy one.
With two cadences one half can fail for hours while the other publishes, and an
unattributed notice on a document whose counters are ten seconds old reads as a
page-wide outage.

## StatsYql

**Aggregations run under the `unranked` profile.** The recency profiles' match
phase caps the match set on a large corpus, and a ranked grouping under-counted
by 10x or more (`EventYql.buildCount`). Attribute `order by` trips the same
phase, so no query here sorts.

**`grouping.defaultMaxGroups` and `defaultMaxHits` travel as -1 per query.**
Without them a pipeline with no `max()` returns ten groups and no error, which
for a kind histogram is a plausible-looking top ten. `grouping.globalMaxGroups`
cannot be sent per request (Vespa 400s it) and lives at -1 in the application
package's `search/query-profiles/default.xml`; a deployment that replaces that
profile breaks the store's own queries too.

**`distinctAuthorsBy` answers in exactly the shape of `countsBy`.** Vespa
collapses the inner list's aggregate onto the outer group, so both come back as
one leaf per value carrying one `count()`. Verified on Vespa 8.733: over the
same corpus `countsBy("kind")` reported 79 for kind 0 and `distinctAuthorsBy`
35, from byte-identical structures. No reader can tell them apart; only calling
the right function keeps events out of a column labelled users.

**`time.date` values go through `isoDay`.** Vespa 8.733 does not zero-pad:
documents nine months apart group as `"2025-1-5"` and `"2025-10-9"`. Unpadded
values mis-sort only where the digit count differs in the same position, so a
month can look fine and the next one interleaves; every bar keeps its height
and only the axis is wrong, which reads as noisy data.

**Weeks are `(created_at + 259200) / 604800`.** Grouping expressions take
arithmetic (renders as `div(add(created_at, 259200), 604800)` on 8.733), which
sidesteps the padding problem entirely. Epoch second 0 is a Thursday; the shift
is the distance back to Monday 1969-12-29. Shifting forward would give negative
bucket indices for 1970-1974.

**Group readers search for the shallowest `grouplist:`.** The `group:root:N`
wrapper's depth has changed between Vespa versions, and a reader that counts
levels breaks on an upgrade with an empty chart rather than an error.
`distinctCountOf` reads the collapsed count first and walks the nested list
only as a fallback, because an unconditional descent finds nothing on the shape
this deployment returns.

**The queries are not built on the store's `EventYql`.** Its builder is private
and its pipelines a fixed set, so every new chart would cost a store release
plus a JitPack pin bump. The proven shapes (store 95ded7b3de kept `buildCount`
and `buildDistinctAuthors`) are copied verbatim; the duplication is the WHERE
clause.

## StatsVespa

**Degradation is judged by `coverage` at 100 with no `degraded` block, not by
`full`.** `full` is `docs == active`, an exact equality; `coverage` rounds
`docs/targetActive`. A node a hair short of its target is `full: false` at 100%
(refusing it refuses every query while the node settles); a node holding
documents not yet active anywhere is `full: true` at any percentage. Mirrors
`SearchCoverage.undegraded` in the store. At 100% at most 0.5% of the target
went unsearched, which is the accuracy the page can claim.

**Vespa's response body goes to the log, never into the published message.**
`/stats.json` is public and a failed section carries the exception's message;
the body names content nodes, container hostnames and internal ports, all of
which rode out on the first failed aggregation. The YQL stays in the message
because it is our own and is the whole diagnostic value.

**No client-side read timeout.** The bundled query profile puts Vespa's own
deadline at its maximum with soft timeout off so a slow aggregation finishes
rather than returning a quiet half-answer; a timeout here would reintroduce the
truncation as a retry that keeps failing on a corpus that has simply grown.

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

## OrphanScoreSweep

**The sweep log prints counts and three example ids.** Printing the full orphan
list once produced a 38,920-character log line. A deletion is not a tombstone,
so the by-kind 30382 stream re-downloads what the sweep frees unless its filter
is narrowed in the same change.
