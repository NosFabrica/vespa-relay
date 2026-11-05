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

import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.NETWORK_CLEARNET
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.NETWORK_TOR
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.RETRYING_WRITES
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.STAGE_WITHDRAW
import com.nosfabrica.vespa.relay.peers.RelayFacts
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap

/** The fitness pass's verdict writes: a batch's grades and withdrawals, and the queue of those the store did not take. */
internal class FitnessWrites(
    private val record: RelayVerdictRecord,
    private val progress: Processors.Handle,
    /** The proxy, where there is one; the `n` tag must name what carried the measurement. */
    private val tor: TorTransport?,
    private val publishDeadlineMs: Long,
    private val publishWedgeBudgetMs: Long,
    private val writeConcurrency: Int,
) {
    /** How a batch's writes went, for its report. */
    class Written(
        val tally: VerdictWrites,
        /** Inherited verdicts left standing because the record already carries them. */
        val skipped: Int,
        val waiting: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        /** Standing `prime`s on urls that kept deferring, owed a withdrawal. */
        val stale: Set<NormalizedRelayUrl>,
        val withdrawn: Set<NormalizedRelayUrl>,
        val withdrawalsPending: Int,
    )

    /**
     * The url each batch's write loop stopped on, or no entry when it wrote everything. Keyed by
     * label because the sweep and the fast lane share this object, on the one scheduler coroutine.
     */
    private val writeCursors = HashMap<String, String>()

    /** One write the store did not take: when it was earned, the grade it signs (null takes one back), and the write. */
    private class Pending(
        val earnedAt: Long,
        val grade: String?,
        val write: suspend () -> Boolean,
    )

    /**
     * Writes a batch earned and the store did not take, per url, replayed by [flush] rather
     * than dropped until the next sweep. A later batch's decision about the url replaces it. Concurrent:
     * the status thread reads its size.
     */
    private val pending = ConcurrentHashMap<NormalizedRelayUrl, Pending>()

    /** Earned writes the store has not taken yet. */
    val pendingCount: Int get() = pending.size

    /** Where [label]'s next batch starts writing, or null from the top. */
    fun cursorOf(label: String): String? = writeCursors[label]

    /** Drop every queued write about a url outside [named]. */
    fun retainOnly(named: Set<NormalizedRelayUrl>) {
        pending.keys.retainAll(named)
    }

    /**
     * Replay the writes the store did not take, under the same limits as a batch's own, dropping any
     * the record has since overtaken. Returns how many landed. Never runs beside a batch's [write].
     */
    suspend fun flush(): Int {
        if (pending.isEmpty()) return 0
        val queued = HashMap(pending)
        progress.phase(RETRYING_WRITES)
        try {
            val held = readBeforeReplay(queued.keys) ?: return 0
            val overtaken = queued.filter { (url, write) -> overtaken(url, write, held) }
            for ((url, write) in overtaken) pending.remove(url, write)
            val urls = (queued.keys - overtaken.keys).sortedBy { it.url }
            progress.measuring(urls.size, Processors.UNIT_VERDICT)
            val tally =
                writeEach(urls, writeConcurrency, publishDeadlineMs, VerdictWrites.PUBLISH_WEDGE_LIMIT, publishWedgeBudgetMs, progress, VerdictWrites.STAGE_PUBLISH) { url ->
                    queued.getValue(url).write()
                }
            // Removed only if still the write that landed: a batch may have queued a newer one meanwhile.
            for (url in tally.stored) pending.remove(url, queued.getValue(url))
            System.err.println(
                "router: fitness — ${tally.stored.size} of ${urls.size} pending write(s) landed" +
                    (if (overtaken.isEmpty()) "" else ", ${overtaken.size} dropped as overtaken by the record") +
                    (tally.stoppedBy?.let { "; the rest wait again because $it" }.orEmpty()),
            )
            return tally.stored.size
        } finally {
            progress.clearMeasuring()
            progress.phase(Processors.IDLE)
        }
    }

    /**
     * The record over the queued urls, under the write deadline: the retry runs on the monitor's one
     * scheduler, so a read that never answers would stop every sweep and lane behind it. Null leaves the queue as it is.
     */
    private suspend fun readBeforeReplay(urls: Set<NormalizedRelayUrl>): RelayVerdictRecord.Verdicts? {
        val why =
            try {
                withTimeoutOrNull(publishDeadlineMs) { record.load(urls) }?.let { return it }
                "the read outlived the ${fmtDuration(publishDeadlineMs)} write deadline"
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                "the read failed (${e.javaClass.simpleName})"
            }
        System.err.println("router: fitness [$RETRYING_WRITES] — could not read the record before replaying ${urls.size} pending write(s): $why; they stay queued for the next retry")
        return null
    }

    /**
     * Has the record moved past this queued write: a grade measured after it, or, for a `prime`, a
     * stability refusal or a fold the roster would otherwise be overruled on.
     */
    private fun overtaken(
        url: NormalizedRelayUrl,
        write: Pending,
        held: RelayVerdictRecord.Verdicts,
    ): Boolean {
        val standing = held.fitness[url]?.measuredAt
        if (standing != null && standing > write.earnedAt) return true
        return write.grade == Verdict.PRIME.value && (url in held.inconsistent || url in held.aliases)
    }

    /**
     * The batch's grades, several at a time and each under its own wall clock, then the withdrawals
     * its waiting urls owe. What the store does not take waits in [pending].
     */
    suspend fun write(
        label: String,
        held: RelayVerdictRecord.Verdicts,
        waiting: Map<NormalizedRelayUrl, NormalizedRelayUrl>,
        stale: Set<NormalizedRelayUrl>,
        outcomes: Map<NormalizedRelayUrl, FitnessOutcome>,
        readings: Map<NormalizedRelayUrl, RelayDocument.Reading>,
    ): Written {
        val order = writeOrder(label, outcomes.keys)
        // The position moves to the writes, which outnumber the dials: a free refusal costs no socket.
        progress.measuring(order.size, Processors.UNIT_VERDICT)
        val toWrite =
            order.filter { url ->
                val standing = inherited(url, outcomes.getValue(url), held)
                if (standing) progress.attempted()
                !standing
            }
        val measuredAt = nowSeconds()
        val writes = toWrite.associateWith { url -> gradeWrite(url, outcomes.getValue(url), readings[url], measuredAt) }
        val tally =
            writeEach(toWrite, writeConcurrency, publishDeadlineMs, VerdictWrites.PUBLISH_WEDGE_LIMIT, publishWedgeBudgetMs, progress, VerdictWrites.STAGE_PUBLISH) { url ->
                writes.getValue(url)()
            }
        tally.resumeAt?.let { writeCursors[label] = it.url }
        // A waiting url's standing `prime` is withdrawn, not replaced: whether it is its own relay is the fold's to say.
        // A deferring one's likewise: a relay that will not let us measure it has earned no claim either way.
        val unprimed = (waiting.keys.filter { held.fitness[it]?.value == Verdict.PRIME.value } + stale).distinct().sortedBy { it.url }
        val withdrawals = unprimed.associateWith { url -> suspend { record.retireFitness(url) != null } }
        // A stopped batch means the store is not answering; the withdrawals wait with the unwritten grades.
        val withdrawn =
            if (unprimed.isEmpty() || tally.stoppedBy != null) {
                emptySet()
            } else {
                progress.measuring(unprimed.size, Processors.UNIT_VERDICT)
                writeEach(unprimed, writeConcurrency, publishDeadlineMs, VerdictWrites.PUBLISH_WEDGE_LIMIT, publishWedgeBudgetMs, progress, STAGE_WITHDRAW) { url ->
                    withdrawals.getValue(url)()
                }.stored
            }
        // This batch's decision replaces whatever waited for these urls; a url it decided nothing about keeps its write.
        for (url in outcomes.keys + waiting.keys + stale) pending.remove(url)
        for ((url, write) in writes) if (url !in tally.stored) pending[url] = Pending(measuredAt, outcomes.getValue(url).verdict.value, write)
        for ((url, withdraw) in withdrawals) if (url !in withdrawn) pending[url] = Pending(measuredAt, null, withdraw)
        return Written(tally, order.size - toWrite.size, waiting, stale, withdrawn, unprimed.size - withdrawn.size)
    }

    /** [urls] by url, resumed where [label]'s last batch stopped, so the wedge limits do not drop the same tail every pass. */
    private fun writeOrder(
        label: String,
        urls: Collection<NormalizedRelayUrl>,
    ): List<NormalizedRelayUrl> {
        val order = urls.sortedBy { it.url }
        val resumeFrom = writeCursors[label]?.let { c -> order.indexOfFirst { it.url >= c } } ?: 0
        // Cleared up front, so a throw mid-write cannot leave the next batch resuming at a url never reached.
        writeCursors.remove(label)
        // -1 is a cursor sorting past everything this batch holds; it wraps to the top as index 0 does.
        return if (resumeFrom <= 0) order else order.subList(resumeFrom, order.size) + order.subList(0, resumeFrom)
    }

    /**
     * An untested verdict the record already carries word for word, evidence included. Re-signing it
     * would stamp a fresh `measured-at` on what this pass did not test, which would make it immortal.
     */
    private fun inherited(
        url: NormalizedRelayUrl,
        outcome: FitnessOutcome,
        held: RelayVerdictRecord.Verdicts,
    ): Boolean = !outcome.tested && held.fitness[url]?.let { it.value == outcome.verdict.value && it.evidence == outcome.evidence } == true

    /** One url's grade as a write, stamped with when it was measured so a replay keeps that time. */
    private fun gradeWrite(
        url: NormalizedRelayUrl,
        outcome: FitnessOutcome,
        reading: RelayDocument.Reading?,
        measuredAt: Long,
    ): suspend () -> Boolean {
        val facts = factsOf(url, outcome, reading)
        return {
            record.publishFitness(
                url = url,
                status = outcome.verdict.value,
                evidence = outcome.evidence,
                pageable = outcome.pageable,
                nip77 = outcome.nip77,
                compliant = outcome.compliant,
                facts = facts,
                measuredAt = measuredAt,
            ) != null
        }
    }

    /**
     * The NIP-66 payload for one url: what the dial measured, over what the document claimed.
     * Measured beats advertised tag by tag; only `auth` is measured, and only in the positive direction.
     */
    private fun factsOf(
        url: NormalizedRelayUrl,
        outcome: FitnessOutcome,
        reading: RelayDocument.Reading?,
    ): RelayFacts {
        val doc = reading?.doc
        val measured =
            outcome.authRequired?.let { listOf(RelayFacts.requirement(RelayFacts.REQUIREMENT_AUTH, it)) }.orEmpty()
        return RelayFacts(
            network = network(url),
            rttOpenMs = reading?.openMs,
            rttReadMs = outcome.rttReadMs,
            requirements = RelayFacts.merge(measured = measured, advertised = doc?.requirements.orEmpty()),
            software = doc?.software,
            version = doc?.version,
            supportedNips = doc?.supportedNips.orEmpty(),
        )
    }

    /** NIP-66's `n`, from the same predicate the dial uses. */
    private fun network(url: NormalizedRelayUrl): String = if (tor?.routes(url) == true) NETWORK_TOR else NETWORK_CLEARNET
}
