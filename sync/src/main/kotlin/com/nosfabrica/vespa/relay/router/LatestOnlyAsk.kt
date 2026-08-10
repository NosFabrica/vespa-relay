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
package com.nosfabrica.vespa.relay.router

import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

/**
 * `latestOnly`: ask a `fetch`-mode stream for the CURRENT version of each
 * replaceable address instead of everything in a time range.
 *
 * The fetch path has no hook between an id being named and its body arriving —
 * a REQ simply streams events — so a suppression filter can save it nothing on
 * the wire. The only thing that saves the transfer there is not asking, and
 * `{"kinds":[0],"authors":["<one>"],"limit":1}` is the one NIP-01 construction
 * that means "latest for this address". A bare `limit` on a multi-author filter
 * is newest-first *globally* and guarantees nothing per author, which is the
 * mistake this exists to avoid.
 *
 * Three things it deliberately does NOT do:
 *
 *  - **Regular kinds pass through untouched.** They have no "latest per
 *    address" to ask for, so the flag is a no-op on them rather than an error —
 *    a mixed-kind stream must stay configurable.
 *  - **Addressable kinds pass through too.** One `limit: 1` per (kind, author)
 *    would return one arbitrary `d` out of many, silently dropping the rest.
 *    Asking per `d` needs the `d` values, which are only knowable from a corpus
 *    we may not have yet, so the honest answer over the wire is the plain ask.
 *  - **A filter with no `authors` passes through.** There is nothing to
 *    decompose on.
 *
 * The caller must record no ordinary sync band for a decomposed ask: a band
 * claims a `created_at` range was walked, and `limit: 1` per address walked no
 * range at all.
 */
internal object LatestOnlyAsk {
    /**
     * Split [filter] into REQ-sized batches. Returns one list per REQ, each no
     * longer than [maxFiltersPerReq] — NIP-11 advertises `max_filters` and
     * relays enforce it, so a stream with thousands of authors becomes many
     * REQs rather than one refusal.
     *
     * A filter this cannot improve on comes back as a single batch holding it
     * unchanged, so callers need no special case.
     */
    fun decompose(
        filter: Filter,
        maxFiltersPerReq: Int = DEFAULT_MAX_FILTERS,
    ): List<List<Filter>> {
        val authors = filter.authors
        val kinds = filter.kinds
        if (authors.isNullOrEmpty() || kinds.isNullOrEmpty()) return listOf(listOf(filter))

        val perAddress = kinds.filter { it.isReplaceable() && !it.isAddressable() }
        val passThrough = kinds.filterNot { it.isReplaceable() && !it.isAddressable() }
        if (perAddress.isEmpty()) return listOf(listOf(filter))

        val decomposed =
            perAddress.flatMap { kind ->
                authors.map { author ->
                    filter.copy(
                        kinds = listOf(kind),
                        authors = listOf(author),
                        limit = 1,
                    )
                }
            }
        val rest = if (passThrough.isEmpty()) emptyList() else listOf(filter.copy(kinds = passThrough))

        val batchSize = maxFiltersPerReq.coerceAtLeast(1)
        return (decomposed + rest).chunked(batchSize)
    }

    /** True when [decompose] would actually change the ask. */
    fun applies(filter: Filter): Boolean {
        val authors = filter.authors
        val kinds = filter.kinds
        if (authors.isNullOrEmpty() || kinds.isNullOrEmpty()) return false
        return kinds.any { it.isReplaceable() && !it.isAddressable() }
    }

    /**
     * A conservative stand-in for NIP-11's `max_filters`. Relays that publish a
     * smaller number should have it read and used; this is the floor to fall
     * back to, low enough that the common implementations accept it.
     */
    const val DEFAULT_MAX_FILTERS = 20
}
