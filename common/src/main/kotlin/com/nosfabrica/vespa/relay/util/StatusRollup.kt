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
package com.nosfabrica.vespa.relay.util

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Rebuilds a status document on a timer, one instance per plane. Its own daemon thread, so
 * the page keeps refreshing while the mirror's dispatchers are saturated.
 */
class StatusRollup(
    /** Names the thread and the log line a failed pass prints. */
    private val name: String,
    private val everySeconds: Long,
    private val publish: () -> Unit,
    /** The owning process's log prefix: `router` for the sync, `monitor` for the monitor. */
    private val logPrefix: String = "router",
) : AutoCloseable {
    private val timer =
        Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "$name-status").apply { isDaemon = true }
        }

    fun start(): StatusRollup {
        timer.scheduleAtFixedRate({
            // `scheduleAtFixedRate` cancels the schedule on the first exception.
            runCatching { publish() }
                .onFailure { System.err.println("$logPrefix: $name status rollup failed: ${it.message}") }
        }, everySeconds, everySeconds, TimeUnit.SECONDS)
        return this
    }

    override fun close() {
        timer.shutdownNow()
    }
}
