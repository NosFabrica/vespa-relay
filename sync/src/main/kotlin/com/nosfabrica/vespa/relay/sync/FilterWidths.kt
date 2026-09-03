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
package com.nosfabrica.vespa.relay.sync

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * HOW MANY KINDS EACH RELAY WILL ACCEPT IN ONE FILTER, learned from the relay's
 * own refusal.
 *
 * `contentViaOutbox` asks for 139 kinds in one filter, and a relay that caps
 * filter width does not trim the ask — it rejects the whole REQ:
 *
 * ```
 * 63x  ERROR: bad req: filter validation failed: too many kinds in filter: 139
 *  9x  error: too many kinds in filter
 *  7x  invalid: too many kinds (max 100)
 * ```
 *
 * Nothing was delivered, so the walk is [refusedOutright] and the visit ends —
 * and since the next visit re-asks the same 139 kinds, such a relay could never
 * complete a single ask, however many times it was visited. Nine relays on one
 * 90-minute window of `vespa-eventstore-staging`, and not a line anywhere said
 * so.
 *
 * ## Why a constant would not do
 *
 * The three messages above are three relays with three different limits — one
 * of them says `max 100` and the others say nothing at all — so a static cap is
 * either above somebody's limit (still refused) or below everybody's (every
 * stream pays extra round trips forever, on the thousands of relays that never
 * had a limit). The cap has to be the RELAY's, which means it has to come from
 * the refusal. That is also how Amethyst's own client handles an over-wide ask:
 * split on the refusal, not on a guess.
 *
 * ## What is learned, and how
 *
 * A refusal naming a number BELOW what we asked is the relay stating its limit
 * and is adopted verbatim. A refusal with no such number — including one that
 * merely quotes our own 139 back at us — is a limit we cannot read, so the ask
 * HALVES and the next refusal halves again: three round trips to get under a
 * limit of 20 from 139, once, for the life of the process. Both directions only
 * ever narrow ([learn] refuses a cap that is not strictly narrower than the ask
 * that was refused), which is what makes the loop terminate.
 *
 * ## What it deliberately does NOT do
 *
 * **No persistence.** A cap is one refused ask to re-learn and the file it
 * would live in is one more thing to migrate, so it dies with the process.
 *
 * **No NIP-11.** `limitation` has no field for this — quartz's `LIMITS` message
 * and `Nip11RelayInformation` both carry `max_filters` and neither carries a
 * kind width — so there is nothing to read ahead of the refusal even for a
 * relay that documents itself.
 */
internal class FilterWidths {
    private val caps = ConcurrentHashMap<NormalizedRelayUrl, Int>()

    /** How many relays this process has learned a width for — published as `narrowedRelays`. */
    val narrowed: Int get() = caps.size

    /** The width [url] is known to accept, or null while nothing has refused us. */
    fun capFor(url: NormalizedRelayUrl): Int? = caps[url]

    /**
     * Read a refusal. Returns true when a NEW, strictly narrower cap was
     * learned — which is the caller's signal that the same leg is worth
     * re-walking rather than abandoning.
     *
     * False for everything else, deliberately including a repeat of a cap we
     * already hold: a relay that refuses at a width it already told us about is
     * refusing for a different reason, and re-walking it would be a loop.
     */
    fun learn(
        url: NormalizedRelayUrl,
        said: String?,
        kindsAsked: Int,
    ): Boolean {
        val cap = said?.let { capFrom(it, kindsAsked) } ?: return false
        // Strictly narrower than the ask that was refused, or the next walk
        // sends the same REQ and meets the same refusal.
        if (cap >= kindsAsked) return false
        val was = caps[url]
        if (was != null && was <= cap) return false
        caps[url] = cap
        return true
    }

    /**
     * [filter] as the REQs this relay will actually take: itself when the relay
     * has never refused us or the ask already fits, else one filter per chunk
     * of kinds, everything else about the ask unchanged.
     *
     * Splitting by KINDS and by nothing else is what keeps this free of
     * consequences downstream: the band is per kind ([SyncCoverage.Band]), so a
     * chunk records exactly the kinds it walked and the chunks of one leg widen
     * one band between them. A split on authors or on time would not have that
     * property.
     */
    fun chunk(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): List<Filter> {
        val cap = caps[url] ?: return listOf(filter)
        val kinds = filter.kinds
        if (kinds == null || kinds.size <= cap) return listOf(filter)
        return kinds.chunked(cap).map { filter.copy(kinds = it) }
    }

    /** …and the same over a whole subscription's filters — the live tail's REQ. */
    fun chunkAll(
        url: NormalizedRelayUrl,
        filters: List<Filter>,
    ): List<Filter> = if (caps[url] == null) filters else filters.flatMap { chunk(url, it) }

    companion object {
        /**
         * THE CAP THIS REFUSAL ASKS FOR, or null when it is not about filter
         * width at all.
         *
         * Pure arithmetic over the relay's own sentence, so the three shapes
         * measured on staging can be pinned as a table rather than argued
         * about. Two rules:
         *
         *  - **A number below what we asked is the relay's limit.** `max 100`
         *    against 139 kinds is a statement, and the smallest such number in
         *    the sentence is the safe reading of it.
         *  - **Anything else halves.** `too many kinds in filter: 139` quotes
         *    OUR width back — it is the offence, not the limit — and a relay
         *    that names no number at all has told us only that this was too
         *    many. Both are the same fact: narrower, by an amount we have to
         *    find.
         *
         * The phrase gate is deliberately narrow. Every refusal this router has
         * ever measured on width says `too many kinds`, and widening the match
         * to `too many` or to a bare `kinds` would let a rate limit or an
         * allowlist refusal chunk an ask that was never too wide — which costs
         * round trips on relays that are refusing us for good.
         */
        internal fun capFrom(
            said: String,
            kindsAsked: Int,
        ): Int? {
            if (!TOO_MANY_KINDS.containsMatchIn(said)) return null
            val stated =
                NUMBER
                    .findAll(said)
                    .mapNotNull { it.value.toIntOrNull() }
                    .filter { it in MIN_CAP until kindsAsked }
                    .minOrNull()
            return (stated ?: (kindsAsked / 2)).coerceAtLeast(MIN_CAP)
        }

        /** One kind per REQ is the narrowest an ask can get and still be the ask. */
        const val MIN_CAP = 1

        private val TOO_MANY_KINDS = Regex("too many kinds", RegexOption.IGNORE_CASE)
        private val NUMBER = Regex("\\d+")
    }
}
