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
 * How many kinds each relay accepts in one filter, learned from the relay's
 * own refusal.
 *
 * A relay that caps filter width rejects the whole REQ, so a visit that
 * re-asks the same width can never complete. A refusal naming a number below
 * the ask is adopted as the cap; one naming no such number halves the ask.
 * Both only ever narrow, which is what makes the loop terminate. Caps die
 * with the process: one refused ask re-learns them, and NIP-11 has no field
 * to read them from ahead of time.
 */
internal class FilterWidths {
    private val caps = ConcurrentHashMap<NormalizedRelayUrl, Int>()

    /** How many relays have a learned width; published as `narrowedRelays`. */
    val narrowed: Int get() = caps.size

    /** The width [url] is known to accept, or null while nothing has refused us. */
    fun capFor(url: NormalizedRelayUrl): Int? = caps[url]

    /**
     * Reads a refusal. True only when a new, strictly narrower cap was learned,
     * which is the caller's signal to re-walk the same leg. A repeat of a cap
     * already held is a refusal for some other reason, and re-walking would loop.
     */
    fun learn(
        url: NormalizedRelayUrl,
        said: String?,
        kindsAsked: Int,
    ): Boolean {
        val cap = said?.let { capFrom(it, kindsAsked) } ?: return false
        // Strictly narrower than the refused ask, or the next walk sends the same REQ.
        if (cap >= kindsAsked) return false
        val was = caps[url]
        if (was != null && was <= cap) return false
        caps[url] = cap
        return true
    }

    /**
     * [filter] as the REQs this relay will take: itself when it fits, else one
     * filter per chunk of kinds. Split by kinds and nothing else: the band is
     * per kind, so the chunks of one leg widen one band between them.
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

    /** The same over a whole subscription's filters, for the live tail's REQ. */
    fun chunkAll(
        url: NormalizedRelayUrl,
        filters: List<Filter>,
    ): List<Filter> = if (caps[url] == null) filters else filters.flatMap { chunk(url, it) }

    companion object {
        /**
         * The cap this refusal asks for, or null when it is not about filter
         * width. A number below what we asked is the relay's stated limit;
         * anything else, including our own width quoted back, halves. The
         * phrase gate is narrow on purpose: a wider match would let a rate
         * limit or an allowlist refusal chunk an ask that was never too wide.
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

        /** One kind per REQ is the narrowest an ask can get. */
        const val MIN_CAP = 1

        private val TOO_MANY_KINDS = Regex("too many kinds", RegexOption.IGNORE_CASE)
        private val NUMBER = Regex("\\d+")
    }
}
