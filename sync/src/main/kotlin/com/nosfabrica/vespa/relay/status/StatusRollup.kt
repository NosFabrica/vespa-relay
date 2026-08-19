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
package com.nosfabrica.vespa.relay.status

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Rebuild a status document on a timer — one instance per plane.
 *
 * Its own single daemon thread rather than a coroutine on the engine's scope,
 * and the reason is the one this whole split is about: the page has to keep
 * answering when the mirror is in trouble. A rollup sharing `Dispatchers.IO`
 * with ingest queues behind whatever is saturating it, so the one moment an
 * operator opens the page is the moment it stops refreshing.
 *
 * A pass that throws is caught and logged rather than allowed to kill the
 * timer: `scheduleAtFixedRate` cancels the schedule on the first exception, so
 * one bad tick would silently end the feature for the life of the process.
 */
internal class StatusRollup(
    /** What this timer is for, in the log line a failed pass prints. */
    private val name: String,
    private val everySeconds: Long,
    private val publish: () -> Unit,
) : AutoCloseable {
    private val timer =
        Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "$name-status").apply { isDaemon = true }
        }

    fun start(): StatusRollup {
        timer.scheduleAtFixedRate({
            runCatching { publish() }
                .onFailure { System.err.println("router: $name status rollup failed: ${it.message}") }
        }, everySeconds, everySeconds, TimeUnit.SECONDS)
        return this
    }

    override fun close() {
        timer.shutdownNow()
    }
}
