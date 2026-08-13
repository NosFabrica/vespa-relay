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
package com.nosfabrica.vespa.relay.router.discovery

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
     * (null for the newest page). Returns null when the url could not be asked
     * at all — which is NOT the same as an empty page.
     */
    private val fetch: suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> List<Event>?,
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
) {
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
    ): Leader? {
        fingerprint(url, anchor, null, onEvent)?.takeIf { it.isNotEmpty() }?.let { return Leader(it, null) }
        // Measured: 46 of 229 hosts in a full-corpus sweep answered a bare
        // filter with `CLOSED blocked: can't handle empty filters`, taking 892
        // urls out of the fold — by far its largest blind spot, and every one
        // of the twelve retried answered a kinds filter perfectly well.
        return fingerprint(url, anchor, FALLBACK_KINDS, onEvent)?.takeIf { it.isNotEmpty() }?.let { Leader(it, FALLBACK_KINDS) }
    }

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
    ): Set<String>? {
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

        repeat(maxPages) {
            if (ids.size >= target) return newest(ids)
            // A FULL page every time, never trimmed to what is still missing.
            // `until` is inclusive, so each page re-reads its boundary and
            // yields one fewer new id than it returned; trimming the last ask
            // to the exact remainder therefore guarantees falling short of the
            // target by that boundary, and then asking for 1, and then for 1
            // again. Over-fetching by at most a page costs nothing — the
            // events go to ingest either way.
            val events = fetch(url, size, until, kinds)
            if (events == null) {
                // Mid-walk the transport gave up. Keep what the walk already
                // proved rather than throwing it away — but a walk that never
                // got a single page has nothing to stand behind.
                return if (spoke) newest(ids) else null
            }
            spoke = true
            if (events.isEmpty()) {
                // The first empty page is ambiguous: an empty relay, or one
                // refusing this page size. Drop to the smaller ask once and
                // let the next page decide which it was.
                if (ids.isEmpty() && size > fallbackPage) {
                    size = fallbackPage
                    return@repeat
                }
                return newest(ids)
            }
            val before = ids.size
            for (event in events) {
                onEvent(event)
                ids[event.id] = event.createdAt
            }
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
                if (++stalls >= MAX_STALLS) return newest(ids)
            } else {
                until = oldest
                stalls = 0
            }
        }
        return newest(ids)
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
            idleMs: (NormalizedRelayUrl) -> Long,
        ): AliasProbe =
            AliasProbe(
                fetch = { url, size, until, kinds ->
                    val done = HashMap<NormalizedRelayUrl, String>()
                    val events =
                        client.fetchAllWithHooks(
                            filters = mapOf(url to listOf(Filter(limit = size, until = until, kinds = kinds))),
                            idleTimeoutMs = idleMs(url),
                            doneOut = done,
                        ) { _, _ -> true }
                    // "Spoke" is any terminal state that came from the relay.
                    // `cannot:` is our own transport failing and is exactly the
                    // case null exists for.
                    if (done.values.any { !it.startsWith(CANNOT_CONNECT) }) events.map { it.second } else null
                },
                target = target,
            )

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
