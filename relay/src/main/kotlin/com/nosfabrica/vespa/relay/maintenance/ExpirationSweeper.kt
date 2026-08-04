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
package com.nosfabrica.vespa.relay.maintenance

import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext

/**
 * Periodically prunes NIP-40 expired events from the [store] — the store
 * exposes `deleteExpiredEvents()` but schedules nothing itself. A non-positive
 * interval disables the sweeper.
 */
class ExpirationSweeper(
    private val store: IEventStore,
    private val intervalSeconds: Long,
    parentContext: CoroutineContext = SupervisorJob() + Dispatchers.IO,
) : AutoCloseable {
    private val scope = CoroutineScope(parentContext)

    fun start(): ExpirationSweeper {
        if (intervalSeconds <= 0) return this
        scope.launch {
            while (isActive) {
                sweepOnce()
                delay(intervalSeconds * 1_000)
            }
        }
        return this
    }

    /** One sweep; failures are swallowed so a transient error can't kill the loop. */
    suspend fun sweepOnce() {
        runCatching { store.deleteExpiredEvents() }
            .onFailure { System.err.println("expiration sweep failed: ${it.message}") }
    }

    override fun close() {
        scope.cancel()
    }
}
