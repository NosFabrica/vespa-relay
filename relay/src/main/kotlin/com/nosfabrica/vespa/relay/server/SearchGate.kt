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

import com.nosfabrica.vespa.eventstore.ReadShape
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * How many ranked reads one connection may have running at once; the rest queue in arrival order.
 * Per connection, not global: a client only ever waits behind itself. The permit covers the store
 * call to EOSE, not the REQ's live tail. Plain recall passes straight through.
 */
class SearchGate(
    /** Ranked reads in flight per connection. 0 turns the gate off. */
    val permits: Int,
) {
    /** A connection's queue, and how many reads hold or wait on it; it exists only while one does. */
    private class Lane(
        permits: Int,
    ) {
        val slots = Semaphore(permits)
        var users = 0
    }

    private val lanes = ConcurrentHashMap<Long, Lane>()

    /** Whether any of [filters] would be held by this gate: one the store ranks by relevance ([ReadShape.ranked]). */
    fun gates(filters: List<Filter>): Boolean = permits > 0 && filters.any { ReadShape.of(it).ranked }

    /**
     * Runs [block] under the connection's lane when [filters] rank. The permit is released at the
     * first of the wrapped [onEose] firing or [block] leaving.
     */
    suspend fun <T> through(
        ctx: RequestContext,
        filters: List<Filter>,
        onEose: () -> Unit,
        block: suspend (onEose: () -> Unit) -> T,
    ): T {
        if (!gates(filters)) return block(onEose)
        val id = ctx.connectionId
        // Joined and left under the map's own lock, so a lane is dropped by its last reader and never resurrected empty.
        val lane = lanes.compute(id) { _, l -> (l ?: Lane(permits)).also { it.users++ } }!!
        val leave = { lanes.computeIfPresent(id) { _, l -> l.takeIf { --it.users > 0 } } }
        try {
            lane.slots.acquire()
        } catch (e: Throwable) {
            leave()
            throw e
        }
        val held = AtomicBoolean(true)
        val release = {
            if (held.compareAndSet(true, false)) {
                lane.slots.release()
                leave()
            }
        }
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

    /** How many connections have a ranked read running or queued. */
    val lanesOpen: Int get() = lanes.size

    companion object {
        /** A page never benefits from two ranked reads on one socket. */
        const val DEFAULT_PERMITS = 1
    }
}
