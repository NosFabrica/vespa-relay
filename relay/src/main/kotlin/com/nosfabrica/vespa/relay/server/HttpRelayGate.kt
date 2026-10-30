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
 * How many HTTP commands may run at once across the relay. Refuses rather than queues: a queued
 * request holds a socket and a deadline for nothing, and a refusal tells the client to back off.
 */
class HttpRelayGate(
    /** Commands the whole relay may run at once. 0 lifts the cap. */
    val total: Int,
) {
    private var running = 0

    /** Runs [block] when a slot is free, otherwise answers [refused]. */
    suspend fun <T> through(
        refused: suspend () -> T,
        block: suspend () -> T,
    ): T {
        if (!enter()) return refused()
        try {
            return block()
        } finally {
            leave()
        }
    }

    /** Commands running right now. */
    val inFlight: Int
        @Synchronized get() = running

    @Synchronized
    private fun enter(): Boolean {
        if (total > 0 && running >= total) return false
        running++
        return true
    }

    @Synchronized
    private fun leave() {
        running--
    }

    companion object {
        const val DEFAULT_TOTAL = 64
    }
}
