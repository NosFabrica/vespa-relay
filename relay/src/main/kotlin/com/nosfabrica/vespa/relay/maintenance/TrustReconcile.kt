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
import java.util.concurrent.atomic.AtomicLong

/**
 * Reconcile the trust projection, waiting out a cold engine. A failure and a
 * zero-provider answer are both read as "not answering yet" and retried; zero
 * is only accepted once the wait budget is spent. Bounded so a failure that is
 * not warm-up cannot hold the relay off its port.
 */
suspend fun reconcileTrustWithRetry(store: VespaEventStore) {
    var waited = 0L
    var attempt = 0
    // ONE instance for the whole retry sequence. Built inside the loop — as it
    // was — every attempt got a fresh throttle starting at zero, so a walk that
    // threw and retried inside the window printed nothing at all, and a
    // reconcile retrying forever was indistinguishable from one running
    // cleanly. That is the failure this line was added to prevent.
    val progress = reconcileProgress()
    val reportFailure = reconcileFailures()
    while (true) {
        attempt++
        val result = runCatching { store.reconcileTrust(onProgress = progress) }
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
        // The first failure gets the stack — a bug and a cold engine read the
        // same from one message. The REST used to be silent, which is how a
        // reconcile that retried for half an hour looked like one that was
        // working: the only line on the log was the optimistic one it printed
        // before its first attempt.
        if (reportFailure(attempt, cause)) cause?.printStackTrace()
        delay(TRUST_RECONCILE_RETRY_MS)
        waited += TRUST_RECONCILE_RETRY_MS
    }
}

private const val TRUST_RECONCILE_RETRY_MS = 5_000L
private const val TRUST_RECONCILE_MAX_WAIT_MS = 10 * 60 * 1000L

/**
 * THE RECONCILE'S OWN PROGRESS, WHICH NOTHING WAS LISTENING TO.
 *
 * [VespaEventStore.reconcileTrust] has always taken an `onProgress`, and
 * [com.nosfabrica.vespa.eventstore.trust.TrustReconciler.reconcile] emits a
 * real DENOMINATOR with it — the one thing an operator needs to turn "it is
 * still going" into "it has 30 minutes left". Every caller here passed the
 * argument out, so a walk that takes the better part of an hour over a real
 * corpus printed one line when it started and one when it ended. Asked how
 * long a live reconcile had to run, the best available answer was to sample
 * documents and infer the fraction from how many still carried a stale
 * `max_rank`.
 *
 * TWO PHASES, REPORTED AS TWO THINGS. The reconciler screens every service
 * first, moving `inspected` toward `total`; then it re-derives the ones that
 * came back unprojected, holding `inspected` at `total` and moving `rebuilt`
 * and the card count instead. Rendering the second phase as a percentage
 * would print a motionless 100%, which is how the first phase's completion
 * and the second phase's start look identical.
 *
 * Throttled, because the walk calls this per page.
 */
internal fun reconcileProgress(
    everyMillis: Long = PROGRESS_EVERY_MS,
    now: () -> Long = System::currentTimeMillis,
    emit: (String) -> Unit = ::println,
): (Int, Int, Int, Int) -> Unit {
    val startedAt = now()
    val lastAt = AtomicLong(startedAt)
    return { inspected, total, rebuilt, applied ->
        val t = now()
        val prev = lastAt.get()
        if (t - prev >= everyMillis && lastAt.compareAndSet(prev, t)) {
            emit(reconcileProgressLine(inspected, total, rebuilt, applied, t - startedAt))
        }
    }
}

/** The line itself, separated so its arithmetic can be asserted without a store. */
internal fun reconcileProgressLine(
    inspected: Int,
    total: Int,
    rebuilt: Int,
    applied: Int,
    elapsedMs: Long,
): String {
    val secs = (elapsedMs / 1000).coerceAtLeast(1)
    if (rebuilt > 0 || (total > 0 && inspected >= total)) {
        // Past screening: the denominator no longer moves, so quoting one
        // would be a percentage frozen at 100 for the whole expensive half.
        val cards = if (applied > 0) ", $applied card(s) applied" else ""
        return "trust: reconcile re-deriving $rebuilt service(s) of $total$cards (${secs}s elapsed)"
    }
    if (total <= 0) return "trust: reconcile screening $inspected service(s), total not known yet (${secs}s elapsed)"
    val pct = (inspected.toLong() * 100 / total).coerceAtMost(100)
    val rate = inspected.toDouble() / secs
    val eta = if (rate > 0) "${((total - inspected) / rate).toLong() / 60}m" else "unknown"
    return "trust: reconcile screening $inspected/$total service(s) ($pct%), eta $eta (${secs}s elapsed)"
}

/**
 * Retries, said out loud but not per attempt. Returns whether this call was
 * the FIRST failure, which is the one that earns a stack trace.
 *
 * A retry loop that prints only its first failure cannot be told from a loop
 * that stopped failing. Both go quiet.
 */
internal fun reconcileFailures(
    everyMillis: Long = PROGRESS_EVERY_MS,
    now: () -> Long = System::currentTimeMillis,
    emit: (String) -> Unit = ::println,
): (Int, Throwable?) -> Boolean {
    val lastAt =
        java.util.concurrent.atomic
            .AtomicLong(0)
    return { attempt, cause ->
        val t = now()
        val first = lastAt.get() == 0L
        val prev = lastAt.get()
        if (first || t - prev >= everyMillis) {
            lastAt.set(t)
            emit(
                if (first) {
                    "trust: engine not answering yet (${cause?.message?.take(80)}); waiting for it before ranking is usable"
                } else {
                    "trust: reconcile still retrying, attempt $attempt (last: ${cause?.message?.take(80)})"
                },
            )
        }
        first
    }
}

/** Between progress lines — the walk calls back per page, and a log is not a metrics feed. */
private const val PROGRESS_EVERY_MS = 30_000L
