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

import com.nosfabrica.vespa.relay.peers.RelayDiscovery
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentHashMap

/** The boot retirement: the fitness grades this monitor signed that it would no longer sign. */
object FitnessRetirement {
    /** What [retireStale] took back, counting the retirements that landed. */
    data class Retired(
        /** Grades signed under an older [RelayVerdictRecord.FITNESS_EPOCH]. */
        val staleEpochs: Int,
        /** Grades still written on `s`, the software tag. */
        val legacyGrades: Int,
    )

    /**
     * Take back, in one walk of our records at boot, every grade signed under an older
     * [RelayVerdictRecord.FITNESS_EPOCH] and every grade still on `s`, where `["s", "dead"]` reads as
     * a relay running software called `dead`. Each url is retired once. Runs before any pass.
     */
    suspend fun retireStale(
        store: IEventStore,
        record: RelayVerdictRecord,
        author: String,
        progress: Processors.Handle? = null,
    ): Retired {
        val stale = LinkedHashSet<NormalizedRelayUrl>()
        val legacy = LinkedHashSet<NormalizedRelayUrl>()
        val grades = Verdict.entries.mapTo(HashSet()) { it.value }
        val legacyWords = LEGACY_GRADES.toHashSet()
        RelayDiscovery.scan(
            store,
            Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(author)),
            SCAN_PAGE,
            caller = StoreCalls.CALLER_MONITOR_VERDICTS,
            logPrefix = "monitor",
        ) { event ->
            val held = event as? RelayDiscoveryEvent ?: return@scan
            val url = held.relay() ?: return@scan
            // The two questions the tag index used to be asked, `#l` on our words and `#s` on the old ones.
            if (carries(held, RelayVerdictRecord.LABEL_TAG, grades) && isStaleGrade(held)) stale += url
            if (carries(held, RelayVerdictRecord.LEGACY_STATUS_TAG, legacyWords)) legacy += url
        }
        val retired = retire(record, (stale + legacy).toList(), progress)
        val result = Retired(stale.count { it in retired }, legacy.count { it in retired })
        if (stale.isNotEmpty()) {
            System.err.println(
                "monitor: fitness — retired ${result.staleEpochs} of ${stale.size} verdict(s) taken under older rules; " +
                    "they read as unmeasured until the next sweep re-takes them",
            )
        }
        if (legacy.isNotEmpty()) {
            System.err.println(
                "monitor: fitness — retired ${result.legacyGrades} of ${legacy.size} grade(s) still written on the `s` tag; " +
                    "they re-grade onto NIP-32 labels on the next sweep",
            )
        }
        return result
    }

    /** Does [event] carry a [name] tag whose value is one of [values], as a NIP-01 tag filter matches? */
    private fun carries(
        event: RelayDiscoveryEvent,
        name: String,
        values: Set<String>,
    ): Boolean = event.tags.any { it.size > 1 && it[0] == name && it[1] in values }

    /** Our grade on [event], under rules older than this build's; somebody else's label is never one. */
    private fun isStaleGrade(event: RelayDiscoveryEvent): Boolean {
        val grade = ourGrade(event) ?: return false
        return grade.getOrNull(RelayVerdictRecord.LABEL_EPOCH_INDEX) != RelayVerdictRecord.FITNESS_EPOCH
    }

    /**
     * Withdraw a verdict from each of [urls], several at a time, and return the ones that landed.
     * The edits read their records off one walk, taken only if there is anything to retire. No pass may run beside it.
     */
    private suspend fun retire(
        record: RelayVerdictRecord,
        urls: List<NormalizedRelayUrl>,
        progress: Processors.Handle?,
    ): Set<NormalizedRelayUrl> {
        if (urls.isEmpty()) return emptySet()
        progress?.measuring(urls.size, Processors.UNIT_VERDICT)
        val gate = Semaphore(RETIRE_CONCURRENCY)
        val landed = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
        record.beginSweep()
        try {
            coroutineScope {
                for (url in urls) {
                    launch {
                        gate.withPermit { if (record.retireFitness(url) != null) landed += url }
                        progress?.attempted()
                    }
                }
            }
        } finally {
            record.endSweep()
        }
        return landed
    }

    private const val RETIRE_CONCURRENCY = 16

    /** This monitor's grade on a record. */
    private fun ourGrade(event: RelayDiscoveryEvent): Array<String>? = event.tags.firstOrNull(RelayVerdictRecord::isFitnessLabel)

    /** What the old build could have written on `s`. May only grow; must not follow a later rename. */
    val LEGACY_GRADES = Verdict.entries.map { it.value } + "syncable"

    /** Records per page when the boot retractions walk our own corpus. */
    const val SCAN_PAGE = 2_000
}
