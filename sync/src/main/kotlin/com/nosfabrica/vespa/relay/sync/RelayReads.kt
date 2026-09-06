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
 * The three things the visit pool asks of a relay, and the seam that lets the pool's
 * scheduling be tested without a network. Three methods, not a client abstraction.
 */
internal interface RelayReads {
    /**
     * Walks [filter] newest-first until the relay drains, refuses, or goes quiet for
     * [idleTimeoutMs].
     */
    suspend fun page(
        url: NormalizedRelayUrl,
        filter: Filter,
        idleTimeoutMs: Long,
        onEvent: suspend (Event) -> Unit,
    ): PagedFetchResult

    /**
     * Holds a live subscription on [url] under [subId], delivering only that relay's events:
     * one listener can be fed by several relays.
     */
    suspend fun tail(
        subId: String,
        url: NormalizedRelayUrl,
        filters: List<Filter>,
        onEvent: suspend (Event) -> Unit,
    )

    /** Drops the subscription. Idempotent, because every caller is racing another. */
    fun untail(subId: String)
}

/** Quartz, unchanged, with the relay check folded in. */
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
