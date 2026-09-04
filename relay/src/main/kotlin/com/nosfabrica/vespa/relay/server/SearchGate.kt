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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * How many ranked reads one connection may have running at once; the rest
 * queue in arrival order.
 *
 * A ranked read scores every posting a common word matches, split across all
 * the engine's match threads, so two on one socket share rather than overlap
 * and both finish later. Per connection, not global: a client only ever waits
 * behind itself, and a second socket is a second lane. What queues is the
 * engine work, from the store call to EOSE; a REQ then parks at its live tail
 * without a permit.
 *
 * Ranked is decided by the parser the lens gate and the store use: terms, a
 * phrase, or a `sort:`. Plain recall passes straight through. A lane is
 * created on a connection's first ranked read and dropped on disconnect.
 */
class SearchGate(
    /** Ranked reads in flight per connection. 0 turns the gate off. */
    val permits: Int,
) : RelayServerListener {
    private val lanes = ConcurrentHashMap<Long, Semaphore>()

    /** Whether any of [filters] would be held by this gate. */
    fun gates(filters: List<Filter>): Boolean = permits > 0 && filters.any(Filter::isRankedRead)

    /**
     * Runs [block] under the connection's lane when [filters] rank. The permit
     * is released at the first of the wrapped [onEose] firing or [block]
     * leaving, so a read that throws before its EOSE cannot keep the lane.
     */
    suspend fun <T> through(
        ctx: RequestContext,
        filters: List<Filter>,
        onEose: () -> Unit,
        block: suspend (onEose: () -> Unit) -> T,
    ): T {
        if (!gates(filters)) return block(onEose)
        val lane = lanes.computeIfAbsent(ctx.connectionId) { Semaphore(permits) }
        lane.acquire()
        val held = AtomicBoolean(true)
        val release = { if (held.compareAndSet(true, false)) lane.release() }
        try {
            return block {
                release()
                onEose()
            }
        } finally {
            release()
        }
    }

    /** [through] for a prompt read (COUNT), whose answer is its return. */
    suspend fun <T> throughPrompt(
        ctx: RequestContext,
        filters: List<Filter>,
        block: suspend () -> T,
    ): T = through(ctx, filters, {}) { block() }

    /** How many connections currently hold a lane. */
    val lanesOpen: Int get() = lanes.size

    override fun onDisconnect(connectionId: Long) {
        lanes.remove(connectionId)
    }

    companion object {
        /** One: a page never benefits from two ranked reads on one socket. Raise it for a client that prefers interleaved to queued. */
        const val DEFAULT_PERMITS = 1
    }

    /** This gate's lifecycle hook composed in front of [other], so the server installs one listener. */
    fun listening(other: RelayServerListener): RelayServerListener =
        object : RelayServerListener {
            override fun onConnect(connectionId: Long) = other.onConnect(connectionId)

            override fun onDisconnect(connectionId: Long) {
                this@SearchGate.onDisconnect(connectionId)
                other.onDisconnect(connectionId)
            }
        }
}

/**
 * Whether this filter ranks: terms, a phrase, or a `sort:`. These are the
 * shapes the store's `FilterMapping` sends to a relevance profile.
 */
internal fun Filter.isRankedRead(): Boolean {
    val parsed = SearchQuery.parse(search?.takeIf { it.isNotBlank() } ?: return false)
    return parsed.hasText || parsed.extension("sort") != null
}
