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
 * One relay's newest [target] events, as the set of ids in them: the
 * measurement [RelayAliases] folds on.
 *
 * Paged, because a single REQ measures the relay's limit rather than the
 * relay: each page's oldest `created_at` becomes the next `until`, so the
 * relay's cap sets the page size and the depth is ours. Every url of a group
 * walks from one shared `anchor`, because "the newest N" is a moving window
 * and two walks minutes apart on a firehose would not overlap.
 *
 * Null and empty are different answers throughout: null is a url that never
 * spoke, empty is a relay that answered with nothing. Everything downloaded
 * goes to [onEvent] before the ids are counted, so a probe is also a sync.
 */
class AliasProbe(
    /**
     * One page, as a function so the walk can be tested without a relay:
     * url, page size, the inclusive `until` cursor (null for the newest page),
     * and the kinds (null for a bare filter).
     */
    private val fetch: suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> Page,
    /** How many ids make a fingerprint. The walk stops here. */
    private val target: Int = RelayAliases.DEFAULT_PROBE_TARGET,
    /** How many events one REQ asks for. Trimmed to [fallbackPage] if the first page is refused. */
    private val page: Int = RelayAliases.DEFAULT_PROBE_PAGE,
    /** The humbler page size, tried once when the first ask comes back empty. */
    private val fallbackPage: Int = RelayAliases.FALLBACK_PROBE_PAGE,
    /** A hard stop on the walk, so a relay answering every `until` with the same events cannot page forever. */
    private val maxPages: Int = DEFAULT_MAX_PAGES,
    /**
     * How long one ask of this url may sit silent: the transport's own window,
     * per url because a hidden service gets a circuit budget on top of the
     * clearnet one. [deadlineMs] is derived from it.
     */
    private val idleMs: (NormalizedRelayUrl) -> Long = { DEFAULT_IDLE_MS },
) {
    /**
     * The wall clock one url's work gets. Quartz's idle window is not a hard
     * cap: a relay that never stops sending never arms it, and the `onEvent`
     * hook runs outside it, so a pass needs a deadline of its own. A url cut
     * by it is measured again next pass and nothing is published about it.
     */
    fun deadlineMs(url: NormalizedRelayUrl): Long = WINDOWS_PER_URL * idleMs(url)

    /** One page, and the one thing about it that ends a walk early. */
    data class Page(
        /** The events, or null when the url could not be asked at all. */
        val events: List<Event>?,
        /**
         * The relay refused our credentials. Terminal for the whole ladder: a
         * credential refusal is not a complaint about the filter, so no filter
         * can fix it, and further asks on that connection sit silent.
         */
        val authRefused: Boolean = false,
        /**
         * Quartz's terminal reason for this url when [events] is null. Kept
         * raw; [Silence] classifies it in one place.
         */
        val reason: String? = null,
    )

    /**
     * The leader's fingerprint and the filter that produced it, so every
     * sibling asks the same thing: two urls fingerprinted through different
     * filters are not comparable.
     *
     * The ladder is bare filter, then [FALLBACK_KINDS], then
     * [RelayAliases.GROUP_METADATA_KINDS], and a rung is only earned by a
     * refusal: a url that never spoke on both first rungs is not asked again.
     */
    suspend fun leaderPrint(
        url: NormalizedRelayUrl,
        anchor: Long,
        onEvent: suspend (Event) -> Unit,
    ): Attempt {
        val bare = walk(url, anchor, null, onEvent)
        if (!bare.ids.isNullOrEmpty()) return Attempt(Leader(bare.ids, null), spoke = true)
        // A refused credential is not a refused filter. See [Page.authRefused].
        if (bare.authRefused) return Attempt(null, spoke = true)
        val general = walk(url, anchor, FALLBACK_KINDS, onEvent)
        if (!general.ids.isNullOrEmpty()) return Attempt(Leader(general.ids, FALLBACK_KINDS), spoke = true)
        if (general.authRefused) return Attempt(null, spoke = true)
        // `&&` rather than `||`: one answer is enough to earn the third rung.
        // A kinds walk refused beside a bare walk our transport cut is a live
        // server declining the shape of the question, the NIP-29 signature.
        if (bare.ids == null && general.ids == null) return Attempt(null, spoke = false)
        val groups = walk(url, anchor, RelayAliases.GROUP_METADATA_KINDS, onEvent)
        if (!groups.ids.isNullOrEmpty()) return Attempt(Leader(groups.ids, RelayAliases.GROUP_METADATA_KINDS), spoke = true)
        return Attempt(null, spoke = true)
    }

    /**
     * What one url gave up: a window, and whether the relay answered at all.
     * A relay that refused every filter is reachable and unreadable; a url
     * that never spoke says nothing. See [AliasFolding.foldUnreadableGroups].
     */
    data class Attempt(
        /** The window and the filter that produced it, or null when none was had. */
        val leader: Leader?,
        /** Did the relay answer at all, with an EOSE or a CLOSED, rather than silence? */
        val spoke: Boolean,
    )

    /** What a group's leader answered, and what it had to be asked to get it. */
    data class Leader(
        val ids: Set<String>,
        val kinds: List<Int>?,
    )

    /**
     * The ids at [url], or null when it could not be asked at all. A short
     * walk is a fine answer: the target is a ceiling on effort.
     */
    suspend fun fingerprint(
        url: NormalizedRelayUrl,
        /** Where the walk starts. Every url in a group shares one; null walks from now. */
        anchor: Long? = null,
        /** The kinds to ask for, or null for a bare filter. Set by the group's leader, never per url. */
        kinds: List<Int>? = null,
        onEvent: suspend (Event) -> Unit,
    ): Set<String>? = window(url, anchor, kinds, onEvent).ids

    /**
     * The same walk as [fingerprint], keeping what that call flattens: whether
     * the relay refused our credentials, and what the pages did with the
     * filter. See [ConsistencyPass.Unmeasured].
     */
    suspend fun window(
        url: NormalizedRelayUrl,
        anchor: Long? = null,
        kinds: List<Int>? = null,
        onEvent: suspend (Event) -> Unit,
    ): Window = walk(url, anchor, kinds, onEvent)

    /** What one url's window turned out to be, and whether the ladder may go on. */
    data class Window(
        /** Null is a url that never spoke, empty is one that answered with nothing. */
        val ids: Set<String>?,
        val authRefused: Boolean = false,
        /** Why [ids] is null. See [Page.reason]. */
        val reason: String? = null,
        /**
         * How long the first page took, ask to answer: NIP-66's `rtt-read`.
         * One page and not the walk, because a walk is however many round
         * trips the relay's cap makes necessary. Null when no page came back.
         */
        val firstPageMs: Long? = null,
        /**
         * What the relay did with the filter it was handed. Read
         * [Compliance.seen] before believing a zero: nothing off-filter and
         * nothing at all look identical in the counters.
         */
        val compliance: Compliance = Compliance(),
        /**
         * The oldest `created_at` among the kept ids, where a second page
         * would be anchored. Of the kept ids, not everything walked, so a
         * trimmed overshoot cannot step past the window. Null when nothing
         * came back.
         */
        val oldestAt: Long? = null,
    )

    /**
     * Did the answer match the ask: the counters, and nothing that judges them.
     *
     * [RelayConsistency] compares two answers to each other and is blind to a
     * relay that answers every REQ with the same wrong events. This checks
     * each event against the page's own filter, so it needs no second walk.
     * Against the page's filter, not the walk's first: `until` steps down as
     * the walk pages, and a tally against the anchor alone would score a
     * cursor-ignoring relay clean.
     */
    data class Compliance(
        /** Events the answer carried, off-filter ones included. The denominator. */
        val seen: Int = 0,
        /** Of which carried a kind the filter did not ask for. Only meaningful when [kindsAsked]. */
        val offKind: Int = 0,
        /** Of which were stamped above the `until` they were asked under, past [WINDOW_SLACK_SECONDS]. */
        val offWindow: Int = 0,
        /**
         * Events served beyond the `limit` asked for, summed over pages. Not a
         * per-event fault: an over-served event still matches, so
         * [RelayCompliance] does not refuse on it.
         */
        val overLimit: Int = 0,
        /**
         * Distinct events failing at least one per-event check: the numerator.
         * Not [offKind] + [offWindow], since one event can be both.
         */
        val offFilter: Int = 0,
        /** Was a `kinds` actually asked? A zero [offKind] means nothing without it. */
        val kindsAsked: Boolean = false,
    ) {
        /** Two tallies of the same relay: pages of one walk, or a walk and the second page below it. */
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
                /**
                 * How far above [until] an event may be stamped before it
                 * counts. [WINDOW_SLACK_SECONDS] where the cursor is our clock
                 * against the author's; zero where the cursor is one of the
                 * relay's own stamps, or a re-served page lands inside it.
                 */
                slack: Long = WINDOW_SLACK_SECONDS,
            ): Compliance {
                var offKind = 0
                var offWindow = 0
                var offFilter = 0
                for (event in events) {
                    val wrongKind = kinds != null && event.kind !in kinds
                    val aboveWindow = until != null && event.createdAt > until + slack
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
     * The second page: one ask strictly below where the first one ended, and
     * the only evidence that a relay can be walked. A first page that fills
     * the target never moves the cursor, so it proves nothing about paging.
     *
     * Three answers: events at or below [until] (the cursor advanced), an
     * empty page (a drain, the strongest answer), or the newest events again
     * ([Verdict.UNPAGEABLE]). Null is the relay not answering at all.
     *
     * [kinds] is passed through exactly as the rung that answered used it,
     * null included: page two must be page two of page one, or a drain proves
     * nothing. [Compliance.kindsAsked] stops the resulting zero reading as a
     * clean sheet.
     */
    suspend fun pageBelow(
        url: NormalizedRelayUrl,
        until: Long,
        /** The shape the rung that answered used, unchanged; null for the bare rung. */
        kinds: List<Int>?,
        onEvent: suspend (Event) -> Unit,
    ): Compliance? {
        val events = fetch(url, COMPLIANCE_LIMIT, until, kinds).events ?: return null
        for (event in events) onEvent(event)
        // No slack: this cursor is one of the relay's own timestamps minus one.
        return Compliance.of(events, COMPLIANCE_LIMIT, until, kinds, slack = 0)
    }

    private suspend fun walk(
        url: NormalizedRelayUrl,
        anchor: Long?,
        kinds: List<Int>?,
        onEvent: suspend (Event) -> Unit,
    ): Window {
        // id -> created_at, because only the timestamp can say which ids are
        // the newest [target] when two urls page at different sizes.
        val ids = HashMap<String, Long>()
        var until: Long? = anchor
        var size = page
        var spoke = false
        var stalls = 0
        var firstPageMs: Long? = null
        // Accumulated over pages: a relay that serves one honest page and then
        // stops honouring the cursor has done both things.
        var compliance = Compliance()
        var pagesAsked = 0

        // Every exit goes through here, so no return can drop the measurement.
        fun done(
            found: Set<String>?,
            authRefused: Boolean = false,
            reason: String? = null,
        ) = Window(
            found,
            authRefused,
            reason,
            firstPageMs,
            compliance,
            found?.let { kept -> ids.entries.filter { it.key in kept }.minOfOrNull { it.value } },
        )

        repeat(maxPages) {
            if (ids.size >= target) return done(newest(ids))
            // A full page every time, never trimmed to what is missing: `until`
            // is inclusive, so each page re-reads its boundary, and a trimmed
            // last ask always falls short by it.
            val startedNs = System.nanoTime()
            val page = fetch(url, size, until, kinds)
            // An empty page and a CLOSED are answers; only a transport that
            // never returned one leaves this null.
            if (firstPageMs == null && (page.events != null || page.authRefused)) {
                firstPageMs = (System.nanoTime() - startedNs) / 1_000_000
            }
            val events = page.events
            if (events == null) {
                if (page.authRefused) return done(newest(ids), authRefused = true)
                // Mid-walk the transport gave up: keep what the walk proved,
                // unless it never got a page to stand behind.
                return if (spoke) done(newest(ids)) else done(null, reason = page.reason)
            }
            spoke = true
            if (events.isEmpty()) {
                if (page.authRefused) return done(newest(ids), authRefused = true)
                // The first empty page is ambiguous: an empty relay, or one
                // refusing this page size. Drop to the smaller ask once.
                if (ids.isEmpty() && size > fallbackPage) {
                    size = fallbackPage
                    return@repeat
                }
                return done(newest(ids))
            }
            // Against this page's own filter, tallied before the events are
            // folded into `ids` so a duplicate the relay sent twice counts
            // twice. Only page one's `until` is the caller's anchor; every
            // later page is asked below a timestamp the relay itself served.
            compliance += Compliance.of(events, size, until, kinds, slack = if (pagesAsked == 0) WINDOW_SLACK_SECONDS else 0)
            pagesAsked++
            val before = ids.size
            for (event in events) {
                onEvent(event)
                ids[event.id] = event.createdAt
            }
            // The window is ingested before the flag is checked: a page can
            // carry events and a refusal at once, and a partial window is
            // still a window.
            if (page.authRefused) return done(newest(ids), authRefused = true)
            // `until` is inclusive, so a page that is entirely one timestamp
            // cannot move the cursor; a page that added nothing steps strictly
            // below it instead.
            val oldest = events.minOf { it.createdAt }
            if (ids.size == before) {
                until = oldest - 1
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
         * Ceiling on pages per url. Generous: a relay capping at 100 needs
         * eleven pages to reach the same depth as one serving 500, and must
         * not be cut off shallower. Only a spin guard.
         */
        const val DEFAULT_MAX_PAGES = 32

        /**
         * Idle windows one url's job may spend: the sizing of [deadlineMs].
         * Counted from what the widest pass asks (three rungs, each retried
         * once, plus a NEG-OPEN), not from [DEFAULT_MAX_PAGES].
         */
        const val WINDOWS_PER_URL = 12

        /** The window assumed when nobody named one: a test's synthetic transport. [over] always passes the real one. */
        const val DEFAULT_IDLE_MS = 8_000L

        /**
         * How far behind the clock an anchor sits. An event is not visible the
         * instant its `created_at` passes, so an anchor at `now` straddles the
         * moment each relay is still writing; a minute back, every walk of a
         * group sees the same events however staggered the dials are.
         */
        const val ANCHOR_LAG_SECONDS = 60L

        /** The newest `created_at` a fingerprint will look at: the same value for every url of a group. */
        fun settledAnchor(now: Long): Long = now - ANCHOR_LAG_SECONDS

        /** Consecutive pages that add nothing before the walk gives up. */
        private const val MAX_STALLS = 2

        /**
         * How far above an `until` an event may be stamped before it counts as
         * the cursor being ignored. The cursor is our clock and the stamp is
         * the author's, so a publisher running a few minutes fast is served
         * honestly above the line; the failure this is for sits a whole anchor
         * lag above it.
         */
        const val WINDOW_SLACK_SECONDS = 300L

        /**
         * The live wiring: one paged REQ per ask, over the router's own client.
         *
         * `fetchAllWithHooks` for `doneOut`: the plain call returns a list, so
         * a relay that never spoke would arrive as one that holds nothing.
         * NIP-42 waiting is left at quartz's derived default, which is true
         * exactly when a responder is attached.
         */
        fun over(
            client: NostrClient,
            target: Int,
            /**
             * How big an ask each page is. Sized from the target, so a pass
             * whose target is twenty events does not ask each relay for five
             * hundred and throw the rest at ingest.
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
                    // Any terminal state from the relay is "spoke", a CLOSED
                    // included: `blocked: limit too high` is what the
                    // fallback-page retry exists for. Not `anyRelayServed`,
                    // which is EOSE alone.
                    val spoke = result.doneReasons.values.any { !it.startsWith(CANNOT_CONNECT) }
                    Page(
                        events = if (spoke) result.events.map { it.second } else null,
                        // One entry: `filters` names one relay.
                        reason = result.doneReasons[url],
                        authRefused = url in result.authRefused,
                    )
                },
                target = target,
                page = page,
                // Never above the page itself: a fallback bigger than what was
                // just refused is not a humbler ask.
                fallbackPage = minOf(RelayAliases.FALLBACK_PROBE_PAGE, page),
                idleMs = idleMs,
            )

        /**
         * The smallest page [over] will ask for. A one-event ask reads as a
         * probe to some relays; ten is an ordinary REQ.
         */
        const val MIN_PAGE = 10

        /**
         * How many events the second page asks for. [MIN_PAGE], because this
         * size is a fact being tested rather than a depth being reached, and
         * "did the cursor move" is answered by the first event.
         */
        const val COMPLIANCE_LIMIT = MIN_PAGE

        /** Quartz's prefix for a terminal reason that is our connect failing, not the relay answering. */
        private const val CANNOT_CONNECT = "cannot:"

        /** What to ask a relay that will not take a bare filter: the one kind every general relay holds. */
        val FALLBACK_KINDS = listOf(1)
    }
}
