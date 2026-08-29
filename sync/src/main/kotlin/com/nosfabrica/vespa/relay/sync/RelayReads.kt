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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * THE THREE THINGS THE VISIT POOL ASKS OF A RELAY, and the seam that makes the
 * pool's scheduling testable without a network.
 *
 * `fetchAllPages` is a quartz EXTENSION on `NostrClient`, so a pool that calls
 * it directly cannot be driven by a stand-in: the only way to answer it is to
 * be a real client with a real socket. That is why the pool's own behaviour —
 * which units run together, which are excluded, what a refusal ends, when a
 * tail opens and whose budget it costs — had no test at all, and every change
 * to it was verified by compiling and by reading. Two defects in one
 * afternoon's work said what that was worth.
 *
 * DELIBERATELY THREE METHODS. This is not a client abstraction and must not
 * grow into one: it is the list of things `VisitPool` does to a relay, and the
 * day it needs a fourth is a day to ask whether the pool should be doing it.
 *
 * The url is a parameter rather than a property because one instance serves
 * every relay, exactly as the client does.
 */
internal interface RelayReads {
    /**
     * Walk [filter] newest-first, paging until the relay drains, refuses, or
     * goes quiet for [idleTimeoutMs] — quartz's own endings, believed. See
     * `VisitPool.refusedOutright` for which of them ends a visit.
     */
    suspend fun page(
        url: NormalizedRelayUrl,
        filter: Filter,
        idleTimeoutMs: Long,
        onEvent: suspend (Event) -> Unit,
    ): PagedFetchResult

    /**
     * Hold a live subscription on [url] under [subId], delivering only that
     * relay's events.
     *
     * The relay check is HERE rather than at every call site: quartz reports a
     * subscription's events with the relay they came from, one listener can be
     * fed by several, and a pool that forgot the check would ingest another
     * relay's events under this unit's trust and scope.
     */
    suspend fun tail(
        subId: String,
        url: NormalizedRelayUrl,
        filters: List<Filter>,
        onEvent: suspend (Event) -> Unit,
    )

    /** …and let it go. Idempotent, because every caller of it is racing another. */
    fun untail(subId: String)
}

/** The real one: quartz, unchanged, with the relay check folded in. */
internal class ClientRelayReads(
    private val client: NostrClient,
) : RelayReads {
    override suspend fun page(
        url: NormalizedRelayUrl,
        filter: Filter,
        idleTimeoutMs: Long,
        onEvent: suspend (Event) -> Unit,
    ): PagedFetchResult = client.fetchAllPages(url, listOf(filter), idleTimeoutMs, onEvent = onEvent)

    override suspend fun tail(
        subId: String,
        url: NormalizedRelayUrl,
        filters: List<Filter>,
        onEvent: suspend (Event) -> Unit,
    ) {
        client.subscribe(
            subId,
            mapOf(url to filters),
            object : SubscriptionListener {
                override suspend fun onEvent(
                    event: Event,
                    isLive: Boolean,
                    relay: NormalizedRelayUrl,
                    forFilters: List<Filter>?,
                ) {
                    if (relay != url) return
                    onEvent(event)
                }
            },
        )
    }

    override fun untail(subId: String) {
        runCatching { client.unsubscribe(subId) }
    }
}
