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

/**
 * How many HTTP reads may run at once, per client address and in all. Refuses rather than queues:
 * a queued request holds a socket and a deadline for nothing, and a refusal tells the client to back off.
 */
class HttpReadGate(
    /** Reads one client address may run at once. 0 lifts the per-client cap. */
    val perClient: Int,
    /** Reads the whole relay may run at once. 0 lifts the total cap. */
    val total: Int,
) {
    /** Why [through] did not run a read. */
    enum class Refusal { CLIENT_BUSY, RELAY_BUSY }

    private val byClient = HashMap<String, Int>()
    private var running = 0

    /** Runs [block] when [client] has a slot, otherwise answers [refused] with why it has none. */
    suspend fun <T> through(
        client: String,
        refused: suspend (Refusal) -> T,
        block: suspend () -> T,
    ): T {
        enter(client)?.let { return refused(it) }
        try {
            return block()
        } finally {
            leave(client)
        }
    }

    /** Reads running right now. */
    val inFlight: Int
        @Synchronized get() = running

    @Synchronized
    private fun enter(client: String): Refusal? {
        val mine = byClient[client] ?: 0
        if (perClient > 0 && mine >= perClient) return Refusal.CLIENT_BUSY
        if (total > 0 && running >= total) return Refusal.RELAY_BUSY
        byClient[client] = mine + 1
        running++
        return null
    }

    @Synchronized
    private fun leave(client: String) {
        running--
        val left = (byClient[client] ?: 1) - 1
        if (left <= 0) byClient.remove(client) else byClient[client] = left
    }

    companion object {
        const val DEFAULT_PER_CLIENT = 2
        const val DEFAULT_TOTAL = 64
    }
}
