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
package com.nosfabrica.vespa.relay.ingest

/**
 * One drop-probe's failures: counted, logged at most once per [logEveryMs], and backed off so a probe
 * the store keeps refusing stops costing every batch a round trip. Its gate learns nothing from these.
 */
internal class ProbeTrouble(
    private val name: String,
    private val firstPauseMs: Long = 5_000,
    private val maxPauseMs: Long = 300_000,
    private val logEveryMs: Long = 60_000,
    private val now: () -> Long = System::currentTimeMillis,
) {
    private var total = 0L
    private var pauseMs = 0L
    private var pausedUntil = 0L
    private var loggedAt = Long.MIN_VALUE / 2

    /** Whether the probe may run now; false while a failure's pause lasts. */
    @Synchronized
    fun ready(): Boolean = now() >= pausedUntil

    @Synchronized
    fun failures(): Long = total

    @Synchronized
    fun succeeded() {
        pauseMs = 0
    }

    /** Counts [e], doubles the pause, and logs when the last line is old enough. */
    @Synchronized
    fun failed(e: Exception) {
        total++
        pauseMs = if (pauseMs == 0L) firstPauseMs else minOf(pauseMs * 2, maxPauseMs)
        val at = now()
        pausedUntil = at + pauseMs
        if (at - loggedAt >= logEveryMs) {
            loggedAt = at
            System.err.println(
                "router: ingest $name probe FAILED ($total so far) — ${e.javaClass.simpleName}: ${e.message?.take(200)}; " +
                    "batches go to the store unprobed for ${pauseMs / 1000}s",
            )
        }
    }
}
