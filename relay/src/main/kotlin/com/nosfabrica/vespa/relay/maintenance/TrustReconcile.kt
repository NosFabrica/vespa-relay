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
 * Reconcile the trust projection, waiting out an engine that is not answering
 * yet. A cold Vespa serves queries only once the content node has loaded the
 * index — minutes on a large corpus — so a failure is treated as "not yet" and
 * retried rather than as an answer.
 *
 * Zero providers is retried too: the call once "succeeded" one second after
 * start against a content node still loading 24M events, and the projection
 * stayed empty for hours. Zero is indistinguishable from cold, so it is only
 * accepted once the wait budget is spent.
 *
 * Bounded, because a failure that is NOT warm-up (a wrong url, a dead cluster)
 * must not hold the relay off its port forever — serving unranked results
 * beats serving nothing.
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
            // The whole stack, once — this is the only place it can be printed.
            cause?.printStackTrace()
            return
        }
        if (!printedFirstFailure) {
            // On the FIRST failure, whichever attempt that is — a success on
            // attempt 1 followed by a throw on attempt 2 deserves the same
            // diagnostic, or the loop retries silently for ten minutes. A
            // deterministic bug and a cold engine look identical from one
            // message, hence the stack.
            printedFirstFailure = true
            println("trust: engine not answering yet (${cause?.message?.take(80)}); waiting for it before ranking is usable")
            cause?.printStackTrace()
        }
        delay(TRUST_RECONCILE_RETRY_MS)
        waited += TRUST_RECONCILE_RETRY_MS
    }
}

// The engine is being waited ON, not polled at: a cold content node takes
// minutes, and each attempt is a real query.
private const val TRUST_RECONCILE_RETRY_MS = 5_000L
private const val TRUST_RECONCILE_MAX_WAIT_MS = 10 * 60 * 1000L
