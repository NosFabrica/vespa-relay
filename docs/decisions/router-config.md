# Router config decisions

The history behind `peers/.../config/RouterConfig.kt`, `RouterConfigLoader.kt`
and `RelaySourceConfig.kt`, moved out of the source so the code reads on its
own. One paragraph per decision; `git log -L` on the declaration finds the
commit.

**Workloads are bounded per stream, and nowhere else.** `refetchConcurrency`,
`negentropyConcurrency`, `maxLiveConcurrency` and `visitConcurrency` are each
stream's share of the pool's jobs; what every stream may take between them is
the sum, written where the stream that pays it is configured. A router-wide
ceiling over the top would be a second number kept in step by hand, and its
failure (a stream inside its own share, refused by a limit named nowhere near
it) is the one these exist to make legible. Streams are not peers: a content
mirror over ~130 kinds and a thirty-relay index stream share one pool, and
without a share the first one's audits occupied every worker the second needed.

**Top-level `visitConcurrency` and `tailBudget` are refused, not dropped.** When
the two socket numbers moved inside the streams, accepting the old spelling and
ignoring it would have left a deployment believing it had bounded its dials.
The pool's worker count is the sum of the streams' `visitConcurrency`, and a
stream that sets none contributes `UNCAPPED_STREAM_VISITS` (128, the number the
router-wide setting defaulted to, so a single-stream deployment that configures
nothing runs the pool it always did).

**A null `maxLiveConcurrency` is 600, not uncapped.** The three in-visit jobs
are bounded by `visitConcurrency` even where no share is set, but a tail is
taken between visits and released only when the roster drops the relay, so an
unbounded live gate is one socket per relay on the roster, with every new
connect on the process queued behind them. 600 was sized to the measured prime
population (~600 responsive hosts after folding) so every certified relay is
effectively always connected.

**`liveBudget` is the one expression for the default.** The gate that enforces
the budget and the boot warning that adds the budgets up each resolved the
default on their own, and drifted: the warning went on quoting 600 per uncapped
stream after the gate had stopped applying it, so a deployment was told it
would hold 600 tails while the pool held one per relay on the roster.

**Workload caps floor at 1 rather than refusing 0.** Zero is an off switch
wearing a tuning knob's name; a stream silently doing no catch-up because
someone typed 0 is the worst reading of the feature. An operator who wants a
job off says so by not configuring the work. The monitor's `dialConcurrency`
and `fastLaneSeconds` follow the same rule, except that `fastLaneSeconds = 0`
is the documented off switch for the lane.

**The monitor's dial width is 128, up from 16.** The 16 came with a note calling
the probe work "a side quest" that must stay below the fan-out's concurrency,
true while the fold shared its sockets with the streams and a relic after the
split. Nothing certifies until the passes finish, and a mostly-dead corpus
costs timeouts, not bandwidth: a 929-url sweep at 16 spent half an hour in the
fitness dials alone, nearly all of it waiting.

**A relay measured as not answering NEG-OPEN is never audited.** A failed
negentropy audit advances no clock, so an unanswerable one was retried every
six hours forever. The monitor signs the `nip77` verdict onto the same 30166
record the roster admits the relay by, and relays without it have their past
re-checked by `refetchThePastSeconds` instead. The knob was `verifySeconds`,
then `auditSeconds`; neither said which of the two re-checks it scheduled, and
both older spellings still parse with a nudge.

**`refetchThePastSeconds` has no default and no environment fallback.** Re-reading
a whole history is the most expensive scheduled thing the router does, and it
was running on quartz's week in every deployment that had never heard of the
knob. One period cannot be right for a 130-kind content mirror and a
five-relay bootstrap at once, so `SYNC_REFETCH_THE_PAST_SECONDS` and its older
names are refused at boot (see `SyncBands.refuseRemovedEnv`), and the loader
warns when a stream's re-fetch period is at or below its audit period, since
the re-fetch then pages the history the reconcile was about to compare.

**`deleteMissing` needs a scan whose every select binds `authors`.** The only
retraction one relay can prove is "this relay no longer serves this provider",
so the licence is per (relay, provider). An unbound ask would reconcile every
author's owned records against a single relay and delete whatever it happens
not to hold. The check is by binding rather than by shape, so it also catches a
kind-30166 verdict source, whose `d`-tag select binds nothing.

**An ungated stream is warned about, not refused.** It used to be a parse error
unless every source was a verdict query. Stating that rule meant this module
deciding which tag, and which value in it, constitutes a vouching, which is the
operator's choice and another monitor's spelling; a filter that gates and a
filter that scans are indistinguishable from the loader. The config is the
authority and the boot line names the stream.

**A relay source is one kind of thing.** A filter for kind 30166 used to parse
into a separate `VerdictSource` read back through a verified path, while
everything else was a scan; the two differed in which questions they could
answer and in almost nothing else. Once the verified read had nothing private
left (no rules epoch, no tag-stamp freshness), a verdict query became a scan
whose select is the `d` tag, removing a type, a read path and a rule set. The
`certified { }` block and `resultsFilteredBy` went with it; `gatedBy` is the
stream-level replacement.

**`maxAgeSeconds` is unbounded by default and inferred from nothing.** A NIP-65
relay list is timeless (replaceable, newest version is the truth); a monitor's
verdict is a measurement that goes stale. Which of those a filter asks for is
the operator's knowledge, so `DEFAULT_MAX_AGE_SECONDS` is a documented number
to reach for, not one the loader applies by tag name. It bounds the event's
own `created_at`, which for a monitor record is the last re-check; before the
passive NIP-66 writer went away, a 30166's `created_at` tracked the last time
we opened a socket to the relay, and the bound meant nothing.

**`maxRelaysPerList` is free now and still null by default.** A per-event limit
needs the event, and it used to cost the source its tag projection, which
handed back values already flattened across every event it matched. Every
source now pages off the search index, the one path where the event is still
whole. It stays null because a cap set too low reads from outside exactly like
a store that holds nothing; duplicate urls are `RelayAliases`' job either way.

**Bindings are read per tag occurrence, never as independent sets.** Collecting
the `authors` slot and the relay slot independently and combining them produced
the cross product: 5,928 asks standing in for the 256 (relay, author) pairs
that existed.

**Default ports are stripped here, not left to `RelayAliases`.** `wss://relay/`
and `wss://relay:443/` are one url written two ways, and the normalizer keeps
both: on this store 861 discovered urls carried a redundant default port and
362 of them duplicated a portless url already in the set, each costing a dial,
a cursor band and a set of NIP-66 records. Dropping 443 on `wss` and 80 on `ws`
needs no evidence; the alias fold is for urls only measurement can prove equal.

**Negative `since`, `until` and `limit` are refused at parse time.** Measured
across the five `indexers`: strfry kills the subscription (`CLOSED: bad req`),
three answer a NOTICE and never EOSE so every page burns an idle timeout, and
purplepag.es silently drops the bound and serves its newest page. A negative
`limit` is quieter and worse: quartz drops the filter before the first REQ, so
the stream reports LIMIT_REACHED having downloaded nothing, every cycle. None
of those failures name the config that caused them.

**`since = 0` is normalised to null; `until = 0` is not.** Two readers treat
`since != null` as "bounded": `flooredForPaging` passes such a filter through
unfloored, which ended the leg unpageable against purplepag.es with no coverage
recorded and a re-walk every boot, and the relay-source `narrowed` guard
counted it as narrowing, buying a regular kind an unbounded scan. Normalising
here rather than clamping in `flooredForPaging` matters because
`drainSettlesThePast` compares the leg's floor against the filter's, and a leg
clamped above the floor its filter asked for could never settle history.
`until = 0` is a real, near-empty bound, not the absence of one.

**An inverted window is refused rather than walked.** The relay EOSEs an empty
page, the walk reports DRAINED, `drainSettlesThePast` compares the leg's floor
against the filter's (the same value) and returns true, and the band records a
settled past from a window that could never return an event.
`PagingProgress.begin` already refused the shape, so such a leg was also
invisible to the progress line.

**`limit = 0` stays legal.** It is the NIP-01 idiom for "no stored events, just
the live tail", and the down tail reuses the same filter overriding `since`
but not `limit`. On the paged path quartz drops the filter before the first
REQ and reports LIMIT_REACHED, which is not DRAINED, so it claims no coverage;
"downloaded 0 history" is the truth for a stream configured not to want any.
`limit` is read with `getInt`, not `getLong().toInt()`, because HOCON
range-checks the int and Long would truncate an out-of-range value into a
plausible one.

**A `filter { }` block holds hex, and bech32 is refused rather than decoded.**
The fields are NIP-01's: the spec says 64-character lowercase hex and that is
what goes on the wire, so a config can be copied out of a REQ and pasted back
into one. Silently decoding an `npub1` would make the block "mostly NIP-01",
the worst of both. Bech32 belongs to the settings that are ours to define,
where the checksum is a free guard on a typed value. An `nsec1` is called out
by name because it is a private key in a file people commit.
