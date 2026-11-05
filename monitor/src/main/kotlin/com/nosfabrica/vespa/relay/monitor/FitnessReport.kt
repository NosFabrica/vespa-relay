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
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.MAX_ABANDONED_NAMED
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.NIP77_DEADLINE_MS
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.PENDING_WRITES
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.WAITING_ON_FOLD
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/** A fitness batch's log lines and its row's counts. */
internal class FitnessReport(
    private val progress: Processors.Handle,
    private val writes: FitnessWrites,
) {
    /** The batch's log lines and its row's counts. [written] is null, and [outcomes] empty, on the guard's refuse-to-publish path. */
    fun report(
        label: String,
        candidates: Int,
        startedMs: Long,
        batch: FitnessBatch,
        outcomes: Map<NormalizedRelayUrl, FitnessOutcome>,
        written: FitnessWrites.Written?,
    ) {
        val byVerdict =
            if (written == null) {
                emptyMap()
            } else {
                outcomes.values
                    .groupingBy { it.verdict }
                    .eachCount()
            }
        val counts = byVerdict.entries.sortedByDescending { it.value }.joinToString { "${it.key.value} x${it.value}" }
        System.err.println(
            "monitor: fitness [$label] — $candidates candidate(s) in ${(System.currentTimeMillis() - startedMs) / 1000}s: $counts" +
                "; ${batch.downloaded} event(s) downloaded",
        )
        reportSetAside(label, batch)
        if (written != null) reportWrites(label, outcomes.size, written)
        val waiting = written?.waiting?.size ?: 0
        progress.counts {
            Verdict.entries.mapNotNull { v -> byVerdict[v]?.let { Processors.Count(v.value, it.toLong()) } } +
                Processors.Count(WAITING_ON_FOLD, waiting.toLong()) +
                // Live, not the batch's: it falls as the retries land.
                Processors.Count(PENDING_WRITES, writes.pendingCount.toLong())
        }
    }

    /** One line of the report, printed only when [count] is above zero. */
    private class Line(
        val count: Int,
        val sentence: (Int) -> String,
    )

    private fun print(
        label: String,
        lines: List<Line>,
    ) {
        for (line in lines) if (line.count > 0) System.err.println("monitor: fitness [$label] — " + line.sentence(line.count))
    }

    /** One line per way a url left this batch with no verdict, or with less of one; only when there were any. */
    private fun reportSetAside(
        label: String,
        batch: FitnessBatch,
    ) {
        val lookups = batch.deferredFor(Deferral.LOOKUP_UNEXPLAINED)
        val unrecognised = batch.deferredFor(Deferral.UNRECOGNISED)
        val deferredCounts = batch.deferredCounts()
        print(
            label,
            listOf(
                Line(batch.unmeasured.size) {
                    "$it url(s) answered nothing at all, no verdict written: " +
                        "no EOSE, no CLOSED and no transport reason on any rung, or our own resolver, route or socket layer failing"
                },
                Line(lookups.size) {
                    "$it url(s) on ${batch.hostsOf(lookups)} host(s) failed the lookup with no reason left " +
                        "to read, no verdict written; counted once per host as this monitor failing to dial, since our resolver " +
                        "fails that way as readily as the name"
                },
                Line(unrecognised.size) {
                    "$it url(s) on ${batch.hostsOf(unrecognised)} host(s) gave up in words " +
                        "we do not recognise, no verdict written; counted once per host as this monitor failing to dial, since a reset, " +
                        "an EOF or a cancellation is our own socket layer as readily as the relay"
                },
                Line(deferredCounts.values.sum()) {
                    "$it url(s) set aside, no verdict written and NOT counted as this monitor failing to dial: " +
                        deferredCounts.entries.sortedByDescending { e -> e.value }.joinToString { e -> "${e.value} x ${e.key}" }
                },
                Line(batch.backingOff.size) {
                    "$it url(s) still backing off from an earlier deferral, not dialled; " +
                        "the first sweep after the wait lapses dials them again"
                },
                Line(batch.abandoned.size) { "gave up on $it url(s) at the per-url deadline, no verdict written: " + named(batch.abandoned) },
                Line(batch.negOpenCut) {
                    "$it NEG-OPEN(s) cut at the ${NIP77_DEADLINE_MS / 1000}s wall clock; " +
                        "no nip77 fact published for them, which is our clock and NOT the relay declining"
                },
                Line(batch.secondPageCut) {
                    "$it second page(s) cut at our own wall clock; those urls " +
                        "were graded on ONE page — our clock, and NOT the relay ignoring a cursor"
                },
                Line(batch.pageUnproven) {
                    "$it url(s) carry NO `pageable` claim: nothing came back to " +
                        "page from, or page two never answered. Their grade stands; the claim is simply not one this " +
                        "pass earned"
                },
                Line(batch.cutLate) {
                    "$it url(s) ran past the per-url deadline AFTER earning a verdict; " +
                        "the verdict stands and was written, but the dial did not finish"
                },
            ),
        )
    }

    /** The first [MAX_ABANDONED_NAMED] of [urls] in order, and how many more there were. */
    private fun named(urls: List<NormalizedRelayUrl>): String {
        val named =
            urls
                .map { it.url }
                .sorted()
                .take(MAX_ABANDONED_NAMED)
        val more = if (urls.size > named.size) " (+${urls.size - named.size} more)" else ""
        return named.joinToString() + more
    }

    /** What the store took of the batch's writes, and what it is still owed. */
    private fun reportWrites(
        label: String,
        graded: Int,
        written: FitnessWrites.Written,
    ) {
        val tally = written.tally
        // The pass ended abnormally: the verdicts were earned and the store would not take them.
        val unwritten = graded - tally.published - written.skipped
        print(
            label,
            listOf(
                Line(written.waiting.size) {
                    "$it url(s) on ${written.waiting.values.toSet().size} server(s) wait on " +
                        "the fold behind another url of the same server, not graded; ${written.withdrawn.count { u -> u in written.waiting }} standing `prime` grade(s) " +
                        "taken back. The fold has not said whether those paths are their own relays, so only the server's stand-in is graded"
                },
                Line(written.stale.size) {
                    "$it standing `prime` grade(s) on urls that kept deferring " +
                        "($DEFERRED_SWEEPS sweeps in a row, or a grade older than $DEFERRED_SWEEPS sweeps), " +
                        "${written.withdrawn.count { u -> u in written.stale }} taken back; nothing negative is said, they read as ungraded until a dial answers"
                },
                Line(written.withdrawalsPending) {
                    "$it standing `prime` grade(s) NOT taken back yet: the store " +
                        "did not take the batch's writes" + (tally.stoppedBy?.let { why -> " ($why)" }.orEmpty()) +
                        ". They wait with the unwritten grades and are retried every ${fmtDuration(AliasMonitor.DEFAULT_RETRY_MS)}, not next sweep"
                },
                Line(written.skipped) {
                    "$it inherited verdict(s) left standing: the record already carries " +
                        "them and this pass dialled nothing for them, so re-signing would refresh a measurement it did not take"
                },
                Line(unwritten) {
                    "$it earned verdict(s) NOT written: ${unwrittenParts(it, tally).joinToString(", ")}; " +
                        "they are retried every ${fmtDuration(AliasMonitor.DEFAULT_RETRY_MS)} until the store takes them" +
                        (writes.cursorOf(label)?.let { c -> ", and the next batch's writes START at $c so the same tail is not dropped twice" }.orEmpty())
                },
            ),
        )
    }

    /** Why [unwritten] earned verdicts did not land, one clause per cause. */
    private fun unwrittenParts(
        unwritten: Int,
        tally: VerdictWrites,
    ): List<String> {
        val dropped = unwritten - tally.wedged - tally.declined
        return buildList {
            if (tally.wedged > 0) add("${tally.wedged} write(s) hit the per-write store deadline")
            if (tally.declined > 0) add("${tally.declined} write(s) failed outright with the store answering")
            if (dropped > 0) add("the remaining $dropped were dropped rather than paying the deadline each")
            // Never nested under `dropped`, which can be zero or negative when a batch stops on its last urls.
            tally.stoppedBy?.let { add("the batch stopped because $it") }
        }
    }
}
