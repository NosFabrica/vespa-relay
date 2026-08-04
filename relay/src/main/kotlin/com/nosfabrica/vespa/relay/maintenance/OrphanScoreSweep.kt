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

import com.nosfabrica.vespa.eventstore.store.VespaEventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

/**
 * Delete every kind-30382 signed by a service no stored 10040 names — cards
 * that rank nothing and are read by nobody, which a by-kind 30382 sync accrues
 * by the million (`SWEEP_ORPHAN_SCORES_ON_START`).
 *
 * A deletion is not a tombstone: the same by-kind stream re-downloads what
 * this frees on its next walk, so reclaiming space and narrowing that filter
 * are one job, not two.
 *
 * [dryRun] reports what would be deleted and writes nothing — run it first.
 */
fun launchOrphanScoreSweep(
    scope: CoroutineScope,
    store: VespaEventStore,
    dryRun: Boolean,
) {
    scope.launch {
        val startedMs = System.currentTimeMillis()
        println(
            "sweep: ${if (dryRun) "DRY RUN — no writes" else "DELETING orphan scores"}" +
                " — kind 30382 from services no stored 10040 names",
        )
        var lastReport = 0L
        runCatching {
            store.sweepOrphanScores(dryRun) { done, totalServices, swept, totalScores ->
                // Paced: the callback fires per page over millions of cards.
                val now = System.currentTimeMillis()
                if (now - lastReport >= 15_000) {
                    lastReport = now
                    val pct = if (totalScores > 0) " (${swept * 100L / totalScores}%)" else ""
                    println("sweep: $done/$totalServices service(s), $swept/$totalScores score(s)$pct")
                }
            }
        }.onSuccess { report ->
            val secs = (System.currentTimeMillis() - startedMs) / 1000
            if (report.refused) {
                println(
                    "sweep: REFUSED — no readable 10040 attribution, so every score would look orphaned." +
                        " Nothing was touched; mirror a provider list first",
                )
            } else {
                // Counts and three examples, not the full orphan list — that
                // once printed a 38,920-character log line.
                val eg = report.orphans.take(3).joinToString { it.take(8) + "…" }
                println(
                    "sweep: ${if (dryRun) "would delete" else "deleted"} ${report.scoresSwept} score(s)" +
                        " from ${report.orphans.size} orphan service(s) of ${report.servicesSeen} seen" +
                        (if (report.remapped.isNotEmpty()) ", ${report.remapped.size} remapped mid-sweep and left alone" else "") +
                        " in ${secs}s" +
                        (if (eg.isNotEmpty()) " (e.g. $eg)" else "") +
                        if (dryRun) " — set SWEEP_ORPHAN_SCORES_ON_START=true to apply" else "",
                )
            }
        }.onFailure { e ->
            // Shutdown cancellation is not a failed sweep; rethrowing keeps
            // every shutdown log free of a phantom FAILED line.
            if (e is CancellationException) throw e
            println("sweep: FAILED after ${(System.currentTimeMillis() - startedMs) / 1000}s: ${e.message}")
        }
    }
}
