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
 * HOW MANY RANKED READS ONE CONNECTION MAY HAVE RUNNING AT ONCE — the rest
 * queue, in arrival order, behind the ones already in the engine.
 *
 * A ranked read is the one shape this store cannot make cheap: a NIP-50 search
 * for a common word matches millions of postings and scores every one of them
 * (store `benchmark/README.md`, "Where a common word's seconds go"), and the
 * engine splits that work across every match thread it has. Two such reads on
 * the same connection therefore do not overlap, they SHARE — and the sharing
 * is what a client that fans out pays for. Measured against staging
 * (2026-09-03, `bitcoin`, kind 1, one anonymous socket each): one search
 * answered in 3.8s, three at once in 5.0s each, six at once in 6.8s each. The
 * search page was the client that fanned out: it sent one ranked read per
 * keystroke of `bitcoin` plus the submit plus the pager's preload — nine — and
 * the first page landed at 9.1s, on a relay that answers that query alone in
 * 3.7s. The page no longer does that (web/app.js runs one type-ahead at a
 * time and reuses it on Enter), and this gate is the relay's own half: no
 * client, ours or anybody's, can stack ranked reads on one socket and make
 * every one of them slow.
 *
 * PER CONNECTION, not global. A global cap would put one reader's slow word
 * in front of everybody else's fast one, which is a worse page than the one
 * this fixes; per connection, a client only ever waits behind itself, and a
 * client that opens two sockets has asked for two lanes and gets them. What
 * queues is the ENGINE WORK: the REQ is accepted, its subscription exists,
 * and its replay starts the moment the connection's previous ranked replay
 * reaches EOSE — the permit is held from the store call to EOSE, which is the
 * span `ServingPressure` already measures as the read, and NOT to the end of
 * the call, since a REQ parks at its live tail until the client closes it and
 * a permit held that long would let one open search block the connection
 * forever.
 *
 * WHAT COUNTS AS RANKED is decided by the same parser the lens gate and the
 * store use (quartz's `SearchQuery`): a filter with terms or a quoted phrase,
 * or one asking for a `sort:` — a `sort:rank` with no terms is a trust-ordered
 * match-all, which ranks the whole corpus. `include:spam` alone, an
 * `observer:` alone, and every plain NIP-01 recall pass straight through:
 * those are the shapes the store answers in milliseconds and the router makes
 * thousands of. COUNTs are gated on the same terms, because a searching COUNT
 * runs the same match set (measured there at 15x the search it summarizes).
 *
 * A connection's lane is created on its first ranked read and dropped when the
 * connection goes — the listener half of this class — so a relay serving
 * mostly plain reads keeps no state for them at all.
 */
class SearchGate(
    /** Ranked reads in flight per connection. 0 turns the gate off. */
    val permits: Int,
) : RelayServerListener {
    private val lanes = ConcurrentHashMap<Long, Semaphore>()

    /** Whether any of [filters] would be held by this gate. */
    fun gates(filters: List<Filter>): Boolean = permits > 0 && filters.any(Filter::isRankedRead)

    /**
     * Run [block] under the connection's lane when [filters] rank, else
     * straight away. The permit is released at the FIRST of: the wrapped
     * [onEose] firing, or [block] leaving — however it leaves — so a read that
     * throws or is cancelled before its EOSE cannot keep the lane.
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

    /** [through] for a prompt read (COUNT), whose answer IS its return. */
    suspend fun <T> throughPrompt(
        ctx: RequestContext,
        filters: List<Filter>,
        block: suspend () -> T,
    ): T = through(ctx, filters, {}) { block() }

    /** How many connections currently hold a lane — a test's and a status line's view of the map. */
    val lanesOpen: Int get() = lanes.size

    override fun onDisconnect(connectionId: Long) {
        lanes.remove(connectionId)
    }

    companion object {
        /**
         * ONE. A page never benefits from two ranked reads on one socket — they
         * share the engine's match threads and both finish later (the
         * measurement above) — and a client that wants a second lane opens a
         * second socket. Raise it for a client that legitimately fans a search
         * out over several REQs and prefers them interleaved to queued.
         */
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
 * Does this filter rank — terms, a quoted phrase, or a `sort:` order? The
 * store's `FilterMapping` sends exactly these shapes to a relevance profile;
 * everything else is plain recall. Parsed by quartz's own `SearchQuery`, the
 * parser the lens gate and the store already share, so the three cannot read
 * one token three ways.
 */
internal fun Filter.isRankedRead(): Boolean {
    val parsed = SearchQuery.parse(search?.takeIf { it.isNotBlank() } ?: return false)
    return parsed.hasText || parsed.extension("sort") != null
}
