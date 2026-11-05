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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.DEFERRED_SWEEPS
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.MAX_RETRY_AFTER_MS
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/** When each deferred url may be dialled again, and which standing `prime`s a run of deferrals withdraws. */
internal class DeferralBackoff(
    /** One sweep's period: how long a deferred url backs off, and the unit a deferring `prime` ages in. */
    private val sweepMs: Long,
) {
    /**
     * When each url a deferral set aside may be dialled again. In memory, not signed: a 429 is a fact
     * about our dialling, and the sweep and the fast lane both wait it out. Read and written on the scheduler alone.
     */
    private val backoffUntil = HashMap<NormalizedRelayUrl, Long>()

    /** Full sweeps in a row that deferred each url, a wait included; any outcome clears it. */
    private val deferredSweeps = HashMap<NormalizedRelayUrl, Int>()

    /** Of [urls], those still backing off from a deferral; the fast lane leaves them out before any pass dials. */
    fun backingOff(urls: Collection<NormalizedRelayUrl>): Set<NormalizedRelayUrl> {
        val nowMs = System.currentTimeMillis()
        return urls.filterTo(HashSet()) { url -> backoffUntil[url]?.let { it > nowMs } == true }
    }

    /** Drop the state of every url outside [named]. */
    fun retainOnly(named: Set<NormalizedRelayUrl>) {
        backoffUntil.keys.retainAll(named)
        deferredSweeps.keys.retainAll(named)
    }

    /** Start each backing-off deferral's wait: one sweep, or as long as the relay asked, up to [MAX_RETRY_AFTER_MS]. */
    fun backOff(batch: FitnessBatch) {
        val nowMs = System.currentTimeMillis()
        for ((url, why) in batch.deferred) {
            if (!why.backsOff) continue
            val asked = batch.retryAfterMs[url]?.coerceAtMost(MAX_RETRY_AFTER_MS) ?: 0L
            backoffUntil[url] = nowMs + maxOf(sweepMs, asked)
        }
        // A url with an outcome is not deferring, whatever it did before.
        for (url in batch.earned.keys) {
            backoffUntil.remove(url)
            deferredSweeps.remove(url)
        }
    }

    /**
     * The standing `prime`s among [deferring] that a sweep withdraws: deferred [DEFERRED_SWEEPS] sweeps
     * in a row, or graded longer ago than that many periods. Counts this sweep first.
     */
    fun staleWhileDeferring(
        held: RelayVerdictRecord.Verdicts,
        deferring: Set<NormalizedRelayUrl>,
    ): Set<NormalizedRelayUrl> {
        for (url in deferring) deferredSweeps.merge(url, 1, Int::plus)
        val floor = nowSeconds() - DEFERRED_SWEEPS * sweepMs / 1000
        return deferring.filterTo(HashSet()) { url ->
            val standing = held.fitness[url]
            standing?.value == Verdict.PRIME.value &&
                ((deferredSweeps[url] ?: 0) >= DEFERRED_SWEEPS || (standing.measuredAt?.let { it < floor } == true))
        }
    }
}
