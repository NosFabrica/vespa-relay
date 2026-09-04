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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import kotlinx.coroutines.delay

/**
 * Reconcile the trust projection, waiting out a cold engine. A failure and a
 * zero-provider answer are both read as "not answering yet" and retried; zero
 * is only accepted once the wait budget is spent. Bounded so a failure that is
 * not warm-up cannot hold the relay off its port.
 */
suspend fun reconcileTrustWithRetry(store: VespaEventStore) {
    var waited = 0L
    var attempt = 0
    var printedFirstFailure = false
    while (true) {
        attempt++
        val result = runCatching { store.reconcileTrust() }
        result.onSuccess { r ->
            when {
                r.services == 0 -> {
                    if (waited >= TRUST_RECONCILE_MAX_WAIT_MS) {
                        println(
                            "trust: still no provider lists after ${waited / 1000}s — nothing to project. " +
                                "A fresh relay: ranking stays empty until a kind 10040 arrives and this runs again.",
                        )
                        return
                    }
                    if (attempt == 1) {
                        println("trust: no provider lists yet; waiting for the engine to serve its corpus before ranking is usable")
                    }
                    delay(TRUST_RECONCILE_RETRY_MS)
                    waited += TRUST_RECONCILE_RETRY_MS
                    return@onSuccess
                }

                r.isClean() -> {
                    println("trust: ${r.services} service(s) checked, projection consistent")
                    return
                }

                else -> {
                    println("trust: re-derived ${r.rebuilt.size} of ${r.services} service(s) whose scores were unprojected")
                    return
                }
            }
        }
        if (result.isSuccess) continue
        val cause = result.exceptionOrNull()
        if (waited >= TRUST_RECONCILE_MAX_WAIT_MS) {
            System.err.println(
                "trust: reconcile still failing after ${waited / 1000}s; " +
                    "serving with the projection as-is — ranked searches may return nothing until it runs clean",
            )
            cause?.printStackTrace()
            return
        }
        if (!printedFirstFailure) {
            // The first failure on any attempt gets the stack: a bug and a cold
            // engine read the same from one message.
            printedFirstFailure = true
            println("trust: engine not answering yet (${cause?.message?.take(80)}); waiting for it before ranking is usable")
            cause?.printStackTrace()
        }
        delay(TRUST_RECONCILE_RETRY_MS)
        waited += TRUST_RECONCILE_RETRY_MS
    }
}

private const val TRUST_RECONCILE_RETRY_MS = 5_000L
private const val TRUST_RECONCILE_MAX_WAIT_MS = 10 * 60 * 1000L
