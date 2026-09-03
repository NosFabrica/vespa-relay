/*
 * Copyright (c) 2026 NosFabrica
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.nosfabrica.vespa.relay.monitor

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllWithHooks
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * One relay's newest [target] events, as the set of ids in them — the
 * measurement [RelayAliases] folds on.
 *
 * **Paged, because a single REQ measures the relay's limit rather than the
 * relay.** Every relay caps a REQ somewhere and almost none of them say where:
 * measured across 60 live hosts, half of those advertising `max_limit` say 500,
 * others say 100, 1024, 2100, 10000 or nothing at all, and one advertises 0. A
 * one-shot ask therefore returns "min(what we wanted, whatever this relay
 * allows)" — so the same fingerprint is a different depth at every host, and at
 * a host that caps hard it is too shallow to mean anything. Worse, a relay that
 * ENFORCES its cap refuses outright rather than truncating (purplepag.es
 * answers `{"limit": 1000}` with `CLOSED blocked: limit too high: 1000 (max
 * 500)`), which reaches this layer as silence and takes that host out of the
 * fold entirely.
 *
 * So this walks instead: ask a page, take the oldest `created_at` it returned
 * as the next `until`, ask again, until [target] ids are in hand. The relay's
 * own cap becomes the page size rather than the ceiling, and the depth is ours
 * to choose.
 *
 * **The walk is ANCHORED, because "the newest N" is a moving window.** On a
 * firehose, a thousand events span minutes, so two walks taken minutes apart
 * cover different ranges and their overlap collapses even though the relay is
 * the same — measured live, `wss://nos.lol` against `wss://nos.lol/cipher-zulu`
 * scored 0.41 unanchored and would have escaped the fold, on precisely the busy
 * relay where the duplicate costs most. Every url of a group is therefore given
 * one shared `anchor` to start from, so both walks read the same range of the
 * timeline however far apart they actually ran.
 *
 * A bare `{"limit": n, "until": …}` filter on purpose: no kinds, no authors.
 * The question is "what is at the end of THIS url", and any narrowing invites
 * the two dials to disagree for a reason that has nothing to do with whether
 * they are the same server.
 *
 * Everything it downloads is handed to [onEvent] before the ids are counted.
 * The probe is a sync that also identifies: on a url that turns out to be
 * distinct the window was worth having anyway, and on one that turns out to be
 * a duplicate the store drops it as already-held. Nothing is fetched twice to
 * pay for the verdict.
 */
class AliasProbe(
    /**
     * One page, as a function, so the walk below can be tested without a relay.
     * Takes the url, the page size, and the exclusive-to-us `until` cursor
     * (null for the newest page). See [Page] for the three answers it can give —
     * a window, an empty relay, and a url that never spoke are all different
     * things here, and so is a relay that refused our credentials.
     */
    private val fetch: suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> Page,
    /** How many ids make a fingerprint. The walk stops here. */
    private val target: Int = RelayAliases.DEFAULT_PROBE_TARGET,
    /** How many events one REQ asks for. Trimmed to [fallbackPage] if the first page is refused. */
    private val page: Int = RelayAliases.DEFAULT_PROBE_PAGE,
    /**
     * The humbler page size, tried once when the first ask comes back empty.
     * A refusal and an empty relay are indistinguishable here, so this is
     * unconditional rather than parsed out of a CLOSED message.
     */
    private val fallbackPage: Int = RelayAliases.FALLBACK_PROBE_PAGE,
    /**
     * A hard stop on the walk. Nothing should reach it — [target] / [page]
     * pages plus a few for boundary re-reads — but a relay that answers every
     * `until` with the same events would otherwise page forever.
     */
    private val maxPages: Int = DEFAULT_MAX_PAGES,
    /**
     * How long ONE ask of this url may sit silent — the transport's own window,
     * carried here so [deadlineMs] can be derived from it rather than guessed
     * beside it.
     *
     * Per url, not per process, for the reason `probeIdleMs` gives: the router
     * dials over two transports and a hidden service is allowed a circuit on
     * top of the clearnet budget. A pass that sized its wall clock from a
     * constant would cut every `.onion` it measured.
     *
     * Defaulted so a test with a synthetic [fetch] need not name a budget for a
     * transport it does not have. [over] passes the real one.
     */
    private val idleMs: (NormalizedRelayUrl) -> Long = { DEFAULT_IDLE_MS },
) {
    /**
     * THE WALL CLOCK ONE URL'S WORK GETS — the bound a probe pass puts around
     * a job, and the one thing the job's own timeouts do not add up to.
     *
     * ## Why an idle window is not a bound
     *
     * Every outbound call a probe makes is bounded — the TCP pre-probe at 5s,
     * the NIP-11 document at 10s, each rung of the ask ladder at [idleMs], one
     * NEG-OPEN at 10s — and a pass built out of them still hung for 74 minutes
     * on one url of 12,374, holding the whole fitness pass, the roster and
     * three dynamic streams behind it. The reason is in quartz's own header for
     * `fetchAllWithHooks`: `idleTimeoutMs` is *an idle window, not a hard cap*,
     * "there is no wall-clock ceiling parameter … a hard deadline composes at
     * the call site". Two paths inside that loop are outside the window by
     * construction and both are reachable from here:
     *
     *  - a relay that never stops sending. The window is armed only when both
     *    channels are dry, so a relay feeding faster than we drain leaves it
     *    disarmed forever, and the only thing that ends the fetch is the
     *    caller's cancellation.
     *  - the suspending `onEvent` hook, deliberately run outside the timeout
     *    scope so a stalled write is not cancelled mid-event. This router hands
     *    those events to ingest, whose queue is bounded and therefore blocks.
     *
     * So this is the call-site deadline quartz asks for, and it belongs to the
     * probe rather than to each pass because it has to be derived from the very
     * window it is bounding: sized from a constant it would cut every `.onion`
     * measured, which is exactly the failure `probeIdleMs` exists to prevent.
     *
     * ## What a pass does when it fires, and why that is not a verdict
     *
     * NOTHING IS PUBLISHED ABOUT A URL THIS CUT — nothing this cut DECIDED, at
     * least. A deadline says our instrument gave up, not that the relay is
     * slow, dead or unreadable — the same rule
     * [ConsistencyPass.Unmeasured.FAILED] already carries for a probe that
     * threw. The url is counted, named, and measured again next pass. That is
     * what makes the number below safe to set close: the cost of cutting a
     * relay that would have answered is one more pass, and the cost of not
     * cutting one is the whole mirror.
     *
     * **The converse is the half that was missing.** A verdict the dial had
     * ALREADY EARNED before the clock fired is not our timeout talking — the
     * relay answered, and our giving up one step later does not un-answer it.
     * A pass that threw those away would re-grade the corpus at a cadence set
     * by how long each url's job ran, which is the slowest relays last and,
     * past a point, never; see [FitnessPass]'s cut-late branch for what it
     * does with one instead. The rule is the walk's own, one level up: keep
     * what was proved, publish nothing that was not.
     *
     * That was also the first theory for #172 and it was WRONG — the cause was
     * the write loop, and [FitnessBudgetLiveProbe] is the measurement that
     * separated them: against the nine relays the issue names, a whole url job
     * runs 1.1-11.9s against a 240s budget. Recorded because "the deadline is a
     * backstop and not the fix for a specific relay" now has a second corpus
     * behind it.
     */
    fun deadlineMs(url: NormalizedRelayUrl): Long = WINDOWS_PER_URL * idleMs(url)

    /**
     * One page, and the one thing about it that ends a walk early.
     *
     * [events] keeps the distinction the rest of this class is built on — null
     * is a url that never spoke, an empty list is one that answered with
     * nothing. [authRefused] is the third state, and it exists because the
     * first two cannot express it: a relay that turned our credentials down
     * ANSWERED, so it is not silence, and it will not serve, so it is not an
     * empty relay either.
     */
    data class Page(
        /** The events, or null when the url could not be asked at all. */
        val events: List<Event>?,
        /**
         * The relay refused our credentials — NIP-42 came back rejected, or it
         * went on demanding auth we cannot satisfy.
         *
         * **Terminal for the whole ladder, not just this page.** Measured on
         * `filter.nostr.wine`: the first ask is answered in 1.6s with exactly
         * this, and every ask after it on that connection is answered with
         * NOTHING — no CLOSED, no EOSE, no reason at all — so each one waits out
         * the full idle window. Six asks down the ladder cost 61s per url where
         * one costs 1.6. Asking again is not merely wasteful, it is asking a
         * question already answered: a credential refusal is not a complaint
         * about the FILTER, so no filter can fix it.
         */
        val authRefused: Boolean = false,
        /**
         * What the transport SAID when it gave up — quartz's terminal reason for
         * this url, `"cannot:"` and then whatever the socket layer reported.
         *
         * Only meaningful when [events] is null. It is the only evidence that
         * separates a name that does not resolve from a refused connection, a
         * failed TLS handshake and a window that lapsed, and it was read once
         * for a `startsWith` and dropped. Kept RAW: classifying somebody else's
         * text is a judgement, and [Silence] makes it in one place.
         */
        val reason: String? = null,
    )

    /**
     * The leader's fingerprint AND the filter that produced it — the two things
     * the rest of its group needs.
     *
     * A bare filter is tried first, because it asks the purest form of the
     * question. When the relay refuses one, [FALLBACK_KINDS] is tried instead
     * and reported back, so every sibling asks the SAME thing: two urls
     * fingerprinted through different filters are not comparable, and a fold
     * decided on that comparison would be worthless.
     */
    suspend fun leaderPrint(
        url: NormalizedRelayUrl,
        anchor: Long,
        onEvent: suspend (Event) -> Unit,
    ): Attempt {
        val bare = walk(url, anchor, null, onEvent)
        if (!bare.ids.isNullOrEmpty()) return Attempt(Leader(bare.ids, null), spoke = true)
        // THE LADDER IS FOR FILTERS THE RELAY WILL NOT ACCEPT, and a refused
        // credential is not one of those. See [Page.authRefused] — this is the
        // difference between 1.6s and 61s at a paid relay.
        if (bare.authRefused) return Attempt(null, spoke = true)
        // Measured: 46 of 229 hosts in a full-corpus sweep answered a bare
        // filter with `CLOSED blocked: can't handle empty filters`, taking 892
        // urls out of the fold — by far its largest blind spot, and every one
        // of the twelve retried answered a kinds filter perfectly well.
        val general = walk(url, anchor, FALLBACK_KINDS, onEvent)
        if (!general.ids.isNullOrEmpty()) return Attempt(Leader(general.ids, FALLBACK_KINDS), spoke = true)
        if (general.authRefused) return Attempt(null, spoke = true)
        // A RELAY THAT REFUSED IS NOT A RELAY THAT SAID NOTHING, and only the
        // first is worth a third ask.
        //
        // Null is our transport giving up; an EMPTY set is the relay answering
        // — an EOSE with nothing in it, or a CLOSED. [fingerprint] keeps those
        // apart precisely so this decision can be made, and it is what keeps
        // this rung free where it would cost the most: a dead url, or a hidden
        // service whose circuit never built, returns null on BOTH rungs above
        // and is not asked again. Against the onion window that matters, since
        // these attempts are sequential and each is minutes — see
        // [AliasFolding.YARDSTICK_ATTEMPTS], which multiplies them by three.
        //
        // One answer is enough to earn the ask, hence `&&` rather than `||`: a
        // url whose bare walk was cut short by our own transport while its
        // kinds walk came back refused is a live server declining the SHAPE of
        // the question, which is the NIP-29 signature — see
        // [RelayAliases.GROUP_METADATA_KINDS]. Demanding that BOTH rungs be
        // refused would drop such a host on our own blip, and the cost of the
        // looser rule is one dial at a url that has already answered once.
        if (bare.ids == null && general.ids == null) return Attempt(null, spoke = false)
        val groups = walk(url, anchor, RelayAliases.GROUP_METADATA_KINDS, onEvent)
        if (!groups.ids.isNullOrEmpty()) return Attempt(Leader(groups.ids, RelayAliases.GROUP_METADATA_KINDS), spoke = true)
        // Every filter refused, and the relay was there for all of them.
        return Attempt(null, spoke = true)
    }

    /**
     * What one url gave up, in the two facts a pass needs to tell apart.
     *
     * **A window and an ANSWER are different things, and the second one used to
     * be thrown away here.** `leaderPrint` returned a nullable [Leader], which
     * flattened "the relay refused every filter I know" into "the relay was not
     * there" — and those support opposite conclusions about a host wearing
     * several urls. A relay that answered every rung with nothing has told us it
     * is reachable and that our instrument does not work on it; a url that never
     * spoke has told us nothing about anything. See
     * [AliasFolding.foldUnreadableGroups], which can only be safe because these
     * are kept apart.
     */
    data class Attempt(
        /** The window and the filter that produced it, or null when none was had. */
        val leader: Leader?,
        /** Did the relay ANSWER at all — an EOSE or a CLOSED, rather than silence? */
        val spoke: Boolean,
    )

    /** What a group's leader answered, and what it had to be asked to get it. */
    data class Leader(
        val ids: Set<String>,
        val kinds: List<Int>?,
    )

    /**
     * The ids at [url], or null when it could not be asked at all. Null and
     * empty are different answers and must stay that way: empty is a relay that
     * holds nothing (and is therefore no evidence of anything), null is a relay
     * that never spoke, and [RelayAliases] folds on neither.
     *
     * A short walk is a fine answer. A relay holding 300 events returns 300,
     * and that is still far more than the fold's minimum sample — the target is
     * a ceiling on effort, not a requirement.
     */
    suspend fun fingerprint(
        url: NormalizedRelayUrl,
        /**
         * Where the walk starts, as a `created_at`. Every url in one group is
         * given the SAME anchor, which is what makes their fingerprints
         * comparable — see the drift note on the class.
         *
         * Null walks from now, which is only correct for a lone measurement.
         */
        anchor: Long? = null,
        /**
         * The kinds to ask for, or null for a bare filter. Set by the group's
         * leader — see [leaderPrint] — and never chosen per url.
         */
        kinds: List<Int>? = null,
        onEvent: suspend (Event) -> Unit,
    ): Set<String>? = window(url, anchor, kinds, onEvent).ids

    /**
     * The same walk as [fingerprint], keeping the ONE fact that call throws
     * away: whether the relay turned our credentials down.
     *
     * [fingerprint] answers `Set<String>?`, in which an auth refusal and an
     * empty relay and a relay whose window was simply thin are one value. That
     * is enough for the fold, which only ever asks "can this be compared" — and
     * it is not enough for a pass that has to say WHY a url could not be
     * measured, because "it refused our auth" is the one reason with an
     * action attached to it and the one that must not cost a second ask. See
     * [ConsistencyPass.Unmeasured].
     */
    suspend fun window(
        url: NormalizedRelayUrl,
        anchor: Long? = null,
        kinds: List<Int>? = null,
        onEvent: suspend (Event) -> Unit,
    ): Window = walk(url, anchor, kinds, onEvent)

    /**
     * What one url's window turned out to be, and whether the ladder may go on.
     *
     * [ids] keeps the distinction the whole class is built on — null is a url
     * that never spoke, empty is one that answered with nothing.
     */
    data class Window(
        val ids: Set<String>?,
        val authRefused: Boolean = false,
        /** Why [ids] is null — see [Page.reason]. Null on a walk that reached the relay. */
        val reason: String? = null,
        /**
         * How long the FIRST page took, ask to answer — NIP-66's `rtt-read`.
         *
         * The first page and not the walk, because a walk is however many
         * round trips this relay's cap makes necessary: a host capping at 10
         * pages twice for a 20-event target, and timing the whole thing would
         * publish our target divided by their cap as their latency. One page
         * is one REQ and its EOSE, which is what a read round trip is.
         *
         * Null when the walk never got a page at all — a relay that did not
         * answer has no read latency, and a zero would say it answered
         * instantly.
         */
        val firstPageMs: Long? = null,
        /**
         * What the relay DID with the filter it was handed — see [Compliance].
         *
         * Zeroed on a walk that got no page, which is the same "no evidence"
         * a null [ids] already says. A caller must read [seen][Compliance.seen]
         * before believing a zero: nothing off-filter and nothing at all look
         * identical in the counters and are opposite findings.
         */
        val compliance: Compliance = Compliance(),
    )

    /**
     * DID THE ANSWER MATCH THE ASK — the counters, and nothing that judges them.
     *
     * ## Why this is not the same question as consistency
     *
     * [RelayConsistency] asks one filter twice and compares the two answers to
     * EACH OTHER. That catches a relay whose answer is a fresh random slice
     * every time, and it is blind by construction to the one next door: a relay
     * that answers every REQ with the same wrong events agrees with itself
     * perfectly, scores 1.000 containment, and is certified. Nothing in that
     * comparison ever reads a field of an event, so nothing in it can notice
     * that the events are not what was asked for.
     *
     * This is the other half. Every check here is one event against the filter
     * that fetched it, so it needs no second walk and no second relay — the
     * evidence is already in hand on any ask the pass was going to make anyway.
     *
     * ## What is counted, and against what
     *
     * Against THE PAGE'S OWN filter, never the walk's first one. `until` steps
     * down as a walk pages backwards, so an event above page four's cursor is
     * the cursor being ignored even when it sits below the anchor the walk
     * started at — and a tally kept against the anchor alone would score that
     * relay clean.
     *
     * [offKind] is the check that needs an ask shaped for it: a bare
     * `{"limit": n, "until": …}` constrains no kind, so nothing it returns can
     * be off-kind and a zero here means only that the question was not put. That
     * is what [kindsAsked] exists to say, and what [FitnessPass]'s narrow ask
     * exists to make true for the relays that answer the bare rung.
     */
    data class Compliance(
        /** Events the answer carried, off-filter ones included. The denominator. */
        val seen: Int = 0,
        /** …of which carried a kind the filter did not ask for. Only meaningful when [kindsAsked]. */
        val offKind: Int = 0,
        /** …of which were stamped above the `until` they were asked under, past [WINDOW_SLACK_SECONDS]. */
        val offWindow: Int = 0,
        /**
         * Events served BEYOND the `limit` asked for, summed over pages.
         *
         * Counted apart from [offFilter] and deliberately not a per-event fault:
         * an over-served event is a real event that really matches, so the
         * answer is not WRONG, it is merely larger than the ask. It costs
         * bandwidth and it says something about the relay, which is why it is
         * measured; it does not poison a walk the way a wrong kind does, which
         * is why [RelayCompliance] does not refuse on it.
         */
        val overLimit: Int = 0,
        /**
         * DISTINCT events failing at least one per-event check — the numerator.
         *
         * Not [offKind] + [offWindow]: one event can be both, and a share built
         * by adding the two would climb past 1.0 on exactly the relay this is
         * for, the one answering a narrow ask with its newest firehose.
         */
        val offFilter: Int = 0,
        /** Was a `kinds` actually asked? A zero [offKind] means nothing without it. */
        val kindsAsked: Boolean = false,
    ) {
        /** Two tallies of the same relay — pages of one walk, or a walk and the narrow ask beside it. */
        operator fun plus(other: Compliance) =
            Compliance(
                seen = seen + other.seen,
                offKind = offKind + other.offKind,
                offWindow = offWindow + other.offWindow,
                overLimit = overLimit + other.overLimit,
                offFilter = offFilter + other.offFilter,
                kindsAsked = kindsAsked || other.kindsAsked,
            )

        companion object {
            /** One page against the filter that asked for it. */
            fun of(
                events: List<Event>,
                limit: Int,
                until: Long?,
                kinds: List<Int>?,
            ): Compliance {
                var offKind = 0
                var offWindow = 0
                var offFilter = 0
                for (event in events) {
                    val wrongKind = kinds != null && event.kind !in kinds
                    val aboveWindow = until != null && event.createdAt > until + WINDOW_SLACK_SECONDS
                    if (wrongKind) offKind++
                    if (aboveWindow) offWindow++
                    if (wrongKind || aboveWindow) offFilter++
                }
                return Compliance(
                    seen = events.size,
                    offKind = offKind,
                    offWindow = offWindow,
                    overLimit = (events.size - limit).coerceAtLeast(0),
                    offFilter = offFilter,
                    kindsAsked = kinds != null,
                )
            }
        }
    }

    /**
     * ONE ASK WHOSE WHOLE PURPOSE IS TO BE CHECKABLE — a narrow filter, a small
     * page, and every event it returns compared against it.
     *
     * The ladder's first rung is a BARE filter on purpose ([leaderPrint]), so on
     * the relays that answer it — most of them — `kinds` is never put to the
     * relay at all and [Compliance.offKind] cannot be anything but zero. This is
     * the one extra round trip that closes that: `kinds` and `until` and a
     * `limit`, all three checkable, at [COMPLIANCE_LIMIT] events.
     *
     * One page, never a walk: the question is whether the answer matches the
     * ask, and a second page answers it no better than the first while costing
     * a second round trip at every relay in the corpus.
     *
     * A relay that holds no [FALLBACK_KINDS] answers empty, which proves
     * nothing and is graded as nothing — see [RelayCompliance.Verdict.UNMEASURABLE].
     * Everything it does return goes to [onEvent] like any other window: this is
     * a sync that also checks, same bargain as the rest of the class.
     */
    suspend fun complianceAsk(
        url: NormalizedRelayUrl,
        anchor: Long?,
        onEvent: suspend (Event) -> Unit,
    ): Compliance {
        val events = fetch(url, COMPLIANCE_LIMIT, anchor, FALLBACK_KINDS).events ?: return Compliance()
        for (event in events) onEvent(event)
        return Compliance.of(events, COMPLIANCE_LIMIT, anchor, FALLBACK_KINDS)
    }

    private suspend fun walk(
        url: NormalizedRelayUrl,
        anchor: Long?,
        kinds: List<Int>?,
        onEvent: suspend (Event) -> Unit,
    ): Window {
        // id -> created_at, because the fingerprint is "the newest [target]",
        // and only the timestamp can say which those are. A page may arrive in
        // any order, and two urls on the same host can page at different sizes
        // (one relay caps at 500, another at 100) — trimmed by insertion order
        // they would be compared at different depths, which is exactly the
        // skew paging exists to remove.
        val ids = HashMap<String, Long>()
        var until: Long? = anchor
        var size = page
        var spoke = false
        var stalls = 0
        // Set once, by the first ask that comes back — see [Window.firstPageMs].
        var firstPageMs: Long? = null
        // What every page of this walk did with the filter that asked for it.
        // Accumulated rather than taken from the last page: a relay that serves
        // one honest page and then stops honouring the cursor has done both
        // things, and only the sum says so.
        var compliance = Compliance()

        // Every exit from this walk goes through here, so the measurement
        // cannot be dropped by whichever of the seven returns is taken.
        fun done(
            found: Set<String>?,
            authRefused: Boolean = false,
            reason: String? = null,
        ) = Window(found, authRefused, reason, firstPageMs, compliance)

        repeat(maxPages) {
            if (ids.size >= target) return done(newest(ids))
            // A FULL page every time, never trimmed to what is still missing.
            // `until` is inclusive, so each page re-reads its boundary and
            // yields one fewer new id than it returned; trimming the last ask
            // to the exact remainder therefore guarantees falling short of the
            // target by that boundary, and then asking for 1, and then for 1
            // again. Over-fetching by at most a page costs nothing — the
            // events go to ingest either way.
            val startedNs = System.nanoTime()
            val page = fetch(url, size, until, kinds)
            // The relay ANSWERED — an empty page and a CLOSED are answers, and
            // both are round trips. Only a transport that never returned one
            // leaves this null.
            if (firstPageMs == null && (page.events != null || page.authRefused)) {
                firstPageMs = (System.nanoTime() - startedNs) / 1_000_000
            }
            val events = page.events
            if (events == null) {
                // A refusal with no page at all is still the relay ANSWERING,
                // so it ends the walk here rather than reading as silence.
                if (page.authRefused) return done(newest(ids), authRefused = true)
                // Mid-walk the transport gave up. Keep what the walk already
                // proved rather than throwing it away — but a walk that never
                // got a single page has nothing to stand behind.
                return if (spoke) done(newest(ids)) else done(null, reason = page.reason)
            }
            spoke = true
            if (events.isEmpty()) {
                // The first empty page is ambiguous: an empty relay, or one
                // refusing this page size. Drop to the smaller ask once and
                // let the next page decide which it was.
                // Nothing came with the refusal, so there is nothing to keep
                // and no smaller page worth trying.
                if (page.authRefused) return done(newest(ids), authRefused = true)
                if (ids.isEmpty() && size > fallbackPage) {
                    size = fallbackPage
                    return@repeat
                }
                return done(newest(ids))
            }
            // AGAINST THIS PAGE'S OWN FILTER — `size`, `until` and `kinds` as
            // they stand on this iteration, not as the walk began. See
            // [Compliance]. Tallied before the events are folded into `ids`,
            // because a duplicate id collapses there and an off-filter event
            // the relay sent twice was sent twice.
            compliance += Compliance.of(events, size, until, kinds)
            val before = ids.size
            for (event in events) {
                onEvent(event)
                ids[event.id] = event.createdAt
            }
            // THE WINDOW COMES FIRST, and the order is load-bearing. A page can
            // carry events AND a refusal at once: `chorus.bonsai.com` refuses a
            // 500-limit ask as anti-scraping and answers the smaller retry with
            // 100 events plus `auth-required: At least one matching event
            // requires AUTH`. Returning on the flag before ingesting them threw
            // away a fingerprint the relay had actually served — a partial
            // window is still a window, and the refusal only means there is no
            // point asking for MORE.
            if (page.authRefused) return done(newest(ids), authRefused = true)
            // `until` is INCLUSIVE, so the next page re-sees everything sharing
            // the oldest timestamp — harmless for a set, except that a page
            // which is entirely one timestamp cannot move the cursor at all.
            // A page that added nothing new is exactly that case: step strictly
            // below it. The same boundary problem RelayDiscovery.scan solves,
            // and cheaper here because duplicates simply collapse.
            val oldest = events.minOf { it.createdAt }
            if (ids.size == before) {
                until = oldest - 1
                // Two stalled pages in a row is a relay that is not walking
                // backwards for us. Take what we have.
                if (++stalls >= MAX_STALLS) return done(newest(ids))
            } else {
                until = oldest
                stalls = 0
            }
        }
        return done(newest(ids))
    }

    /** The [target] newest ids of a walk that may have overshot by up to a page. */
    private fun newest(ids: Map<String, Long>): Set<String> =
        if (ids.size <= target) {
            ids.keys.toSet()
        } else {
            ids.entries
                .sortedByDescending { it.value }
                .take(target)
                .mapTo(HashSet()) { it.key }
        }

    companion object {
        /**
         * Ceiling on pages per url. Generous on purpose: at the default page
         * size a walk is two or three, but a relay that caps at 100 needs
         * eleven to reach the same depth and must not be cut off at a shallower
         * fingerprint than everyone else — that is the whole point of paging.
         * This exists only so a misbehaving relay cannot spin.
         */
        const val DEFAULT_MAX_PAGES = 32

        /**
         * Idle windows one url's job may legitimately spend, and the whole
         * sizing of [deadlineMs].
         *
         * Counted from what the job actually asks for rather than from
         * [DEFAULT_MAX_PAGES], which is a spin guard and not a budget. The
         * fitness pass is the widest of the three: three rungs, each a walk
         * that reaches its target in one page at the default size and is
         * retried once at the smaller one, plus a NEG-OPEN. Call it eight asks,
         * and the three fixed steps beside them (5s pre-probe, 10s document,
         * 10s NEG-OPEN) fit inside the slack of the remaining four.
         *
         * At the default `connectionTimeout = 20` that is four minutes, against
         * a job whose own bounds sum to about ninety seconds. A relay answering
         * anything at all never approaches it; the hang this exists for sat at
         * 74 minutes. Through Tor it scales with the circuit budget, which is
         * the point of deriving it per url.
         */
        const val WINDOWS_PER_URL = 12

        /**
         * The window assumed when nobody named one — a test's synthetic
         * transport, and nothing the router builds. [over] always passes the
         * real one. Quartz's own `fetchAllWithHooks` default, for the same
         * reason: it is the number that applies when no caller has an opinion.
         */
        const val DEFAULT_IDLE_MS = 8_000L

        /**
         * How far behind the clock an anchor sits: one minute.
         *
         * A shared anchor already stops the window sliding, but anchoring it at
         * `now` still straddles the moment the relay is in the middle of. An
         * event is not visible to a REQ the instant its `created_at` passes —
         * it has to arrive, verify, and be indexed — so an event stamped just
         * under `now` can be missed by a walk that runs immediately and found
         * by one that runs two minutes later, from the SAME anchor. That is the
         * anchor failing to do the one thing it is for.
         *
         * A minute back, the window is settled: everything at or below it has
         * long since landed, so every walk of a group sees the same events
         * however staggered the dials are. It also absorbs the ordinary case of
         * a publisher whose clock runs slightly fast, whose events would
         * otherwise drop in and out of the top of the window.
         *
         * Costs nothing — a fingerprint is an identity, and a minute-old
         * identity is the same identity.
         */
        const val ANCHOR_LAG_SECONDS = 60L

        /**
         * The newest `created_at` a fingerprint will look at, given the current
         * clock — the same value for every url of a group.
         */
        fun settledAnchor(now: Long): Long = now - ANCHOR_LAG_SECONDS

        /** Consecutive pages that add nothing before the walk gives up. */
        private const val MAX_STALLS = 2

        /**
         * How far above an `until` an event may be stamped before it counts as
         * the cursor being ignored: FIVE MINUTES.
         *
         * A compliant relay owes us `created_at <= until` exactly, so in
         * principle the slack is zero. It is not zero because the number being
         * compared is not ours on both sides: the cursor is our clock and the
         * stamp is the AUTHOR's, and a relay that accepted an event from a
         * publisher running a few minutes fast is serving it honestly at a
         * `created_at` we would score above the line. Five minutes is wide
         * enough to cover the ordinary fast clock and far too narrow to hide
         * the failure this is for, where the answer is the relay's newest
         * events and the gap is the whole anchor lag — a minute for the fold,
         * SEVEN DAYS for the stability gate.
         *
         * Was [FitnessPass]'s own `ANCHOR_SLACK_SECONDS` and moved here with
         * the check, so the one number is not two numbers kept in step by hand.
         */
        const val WINDOW_SLACK_SECONDS = 300L

        /**
         * The live wiring: one paged REQ per ask, over the router's own client.
         *
         * [idleMs] is asked PER URL rather than fixed for the process, because
         * the router dials over two transports and their budgets are not the
         * same number. Quartz's window is measured from the start of the fetch,
         * so it covers the connect: a hidden service given the clearnet
         * handshake budget comes back empty while its circuit is still being
         * built, and an empty window is a url that can never fold. See
         * `probeIdleMs`, which is what the engine passes here.
         *
         * **`fetchAllWithHooks` rather than `fetchAll`, for the one thing this
         * class is built on that the plain call cannot express.**
         *
         * **NIP-42 is no longer one of them.** This used to pass
         * `pendingOnAuthRequired = true` here, with an A/B against
         * `auth.nostr1.com` arguing it was worth ~19s per auth-gated leader.
         * Quartz reworked the whole path (amethyst #3905/#3906) and both halves
         * of that argument are gone:
         *
         *  - **The flag's correct value is computable, so it is derived.** The
         *    default is now `hasAuthResponder()` — true exactly when something
         *    will answer a challenge. With no responder, `true` and `false`
         *    produce identical outcomes (`awaitAuthOutcome` returns
         *    `NO_RESPONDER` without waiting); with one, waiting is the entire
         *    point. This router always attaches a `RelayAuthenticator` when it
         *    has a signer, and it only builds a probe when it has one, so the
         *    derived answer is the answer we were hardcoding. Passing it
         *    explicitly now only creates a way to be wrong later.
         *  - **The wait is bounded by the AUTH, not by the idle window.** A
         *    short grace for a responder to pick the challenge up
         *    (`DEFAULT_AUTH_GRACE_MS`, 1s), then for it to settle, capped by our
         *    own `idleTimeoutMs`. The guarantee upstream ships with it is that
         *    an auth-gated relay costs at most what a silent one already cost —
         *    which is what retires the 19s figure rather than merely improving
         *    it.
         *
         * The same rework plumbed this into `fetchAllPages` and `fetchAll`,
         * where it matters far more than it ever did here: those are the
         * router's actual sync paths, and an auth-gated relay used to read as an
         * EMPTY one on both. See AGENTS.md.
         *
         * `doneOut`, because NULL AND EMPTY ARE DIFFERENT ANSWERS and the plain
         * call flattens them: it returns a list, so a relay that never spoke
         * arrives here as one that holds nothing. That is the distinction
         * [fingerprint] is written around, and losing it cost a second window on
         * every dead url — the empty-page retry at [RelayAliases.FALLBACK_PROBE_PAGE]
         * fired for hosts that had not answered at all. A relay that reached
         * EOSE *or refused with a CLOSED* did speak: its empty list is an
         * answer, and the smaller-page retry that a `blocked: limit too high`
         * needs still happens. Only `cannot:` and a window that lapsed in
         * silence are null.
         */
        fun over(
            client: NostrClient,
            target: Int,
            /**
             * How big an ask each page is — and it is SIZED FROM THE TARGET by
             * default, which it was not.
             *
             * [over] used to leave this at the fold's [RelayAliases.DEFAULT_PROBE_PAGE]
             * (500) for every caller, so the fitness pass — whose whole target
             * is twenty events ([FitnessPass.FITNESS_TARGET]) — asked each relay
             * for five hundred and threw 96% of them at the ingest queue on the
             * way to a verdict it had after twenty. Measured here: one sweep
             * over 923 urls grew a 20,347-event store to 369,210. On a corpus
             * of 20,000 urls that is millions of events per sweep, every sweep,
             * downloaded to answer "does it answer" — and every one of them
             * makes the NEXT sweep's reads slower, including the projection the
             * candidate derivation is built on.
             *
             * A page smaller than the target would page needlessly, so the
             * default is the target itself: one round trip, nothing spare.
             */
            page: Int = maxOf(target, MIN_PAGE),
            idleMs: (NormalizedRelayUrl) -> Long,
        ): AliasProbe =
            AliasProbe(
                fetch = { url, size, until, kinds ->
                    val result =
                        client.fetchAllWithHooks(
                            filters = mapOf(url to listOf(Filter(limit = size, until = until, kinds = kinds))),
                            idleTimeoutMs = idleMs(url),
                        ) { _, _ -> true }
                    // "Spoke" is any terminal state that came from the relay.
                    // `cannot:` is our own transport failing and is exactly the
                    // case null exists for.
                    //
                    // NOT `result.anyRelayServed`, which is the obvious-looking
                    // swap and is narrower: it is EOSE alone, while this has to
                    // count a relay that answered with a CLOSED as having
                    // spoken. `blocked: limit too high` is an ANSWER — it is
                    // what the fallback-page retry in [fingerprint] exists to
                    // react to — and reading it as silence would return null,
                    // skip the retry, and take every relay capping under
                    // [RelayAliases.DEFAULT_PROBE_PAGE] out of the fold.
                    val spoke = result.doneReasons.values.any { !it.startsWith(CANNOT_CONNECT) }
                    Page(
                        events = if (spoke) result.events.map { it.second } else null,
                        // One entry: `filters` names one relay.
                        reason = result.doneReasons[url],
                        // Quartz derives this from the same `doneReasons` map —
                        // the reason string starts with `auth-refused` — so it
                        // costs nothing to read and it is the only warning we
                        // get before five asks into a wall. See [Page.authRefused].
                        authRefused = url in result.authRefused,
                    )
                },
                target = target,
                page = page,
                // Never above the page itself: the humbler retry exists for a
                // relay that refuses the first ask as too large, and a fallback
                // BIGGER than what was just refused is not a humbler ask.
                fallbackPage = minOf(RelayAliases.FALLBACK_PROBE_PAGE, page),
                // The same lambda the fetch above is given, held so
                // [deadlineMs] is a multiple of the window it bounds rather
                // than a second number kept in step with it by hand.
                idleMs = idleMs,
            )

        /**
         * The smallest page [over] will ask for, whatever the target.
         *
         * A one-event ask reads as a probe rather than as a read to some
         * relays, and a page this small pays a round trip per event on any
         * caller whose target is larger than it looks. Ten is enough to be an
         * ordinary REQ.
         */
        const val MIN_PAGE = 10

        /**
         * How many events the narrow ask asks for — see [complianceAsk].
         *
         * [MIN_PAGE], because this is the one ask in the class whose size is a
         * fact being TESTED rather than a depth being reached: a relay serving
         * more than this asked for is over-serving, and the smallest ordinary
         * REQ is the cheapest way to find that out. A corpus-wide sweep pays it
         * once per url, so the number is also the per-sweep cost of the whole
         * check: ten events a relay.
         */
        const val COMPLIANCE_LIMIT = MIN_PAGE

        /** Quartz's prefix for a terminal reason that is our connect failing, not the relay answering. */
        private const val CANNOT_CONNECT = "cannot:"

        /**
         * What to ask a relay that will not take a bare filter. Kind 1 because
         * it is the one kind every general relay holds; a relay that has none
         * of it answers empty, which is the same "no evidence" this treats any
         * short window as.
         */
        val FALLBACK_KINDS = listOf(1)
    }
}
