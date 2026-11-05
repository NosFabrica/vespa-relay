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
            "router: fitness [$label] — $candidates candidate(s) in ${(System.currentTimeMillis() - startedMs) / 1000}s: $counts" +
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

    /** One line per way a url left this batch with no verdict, or with less of one; only when there were any. */
    private fun reportSetAside(
        label: String,
        batch: FitnessBatch,
    ) {
        val unmeasured = batch.unmeasured.size
        if (unmeasured > 0) {
            System.err.println(
                "router: fitness [$label] — $unmeasured url(s) answered nothing at all, no verdict written: " +
                    "no EOSE, no CLOSED and no transport reason on any rung, or our own resolver, route or socket layer failing",
            )
        }
        val lookups = batch.deferredFor(Deferral.LOOKUP_UNEXPLAINED)
        if (lookups.isNotEmpty()) {
            System.err.println(
                "router: fitness [$label] — ${lookups.size} url(s) on ${batch.hostsOf(lookups)} host(s) failed the lookup with no reason left " +
                    "to read, no verdict written; counted once per host as this router failing to dial, since our resolver " +
                    "fails that way as readily as the name",
            )
        }
        val unrecognised = batch.deferredFor(Deferral.UNRECOGNISED)
        if (unrecognised.isNotEmpty()) {
            System.err.println(
                "router: fitness [$label] — ${unrecognised.size} url(s) on ${batch.hostsOf(unrecognised)} host(s) gave up in words " +
                    "we do not recognise, no verdict written; counted once per host as this router failing to dial, since a reset, " +
                    "an EOF or a cancellation is our own socket layer as readily as the relay",
            )
        }
        val deferredCounts = batch.deferredCounts()
        if (deferredCounts.isNotEmpty()) {
            val why = deferredCounts.entries.sortedByDescending { it.value }.joinToString { "${it.value} x ${it.key}" }
            System.err.println(
                "router: fitness [$label] — ${deferredCounts.values.sum()} url(s) set aside, no verdict written and NOT " +
                    "counted as this router failing to dial: $why",
            )
        }
        if (batch.backingOff.isNotEmpty()) {
            System.err.println(
                "router: fitness [$label] — ${batch.backingOff.size} url(s) still backing off from an earlier deferral, not dialled; " +
                    "the first sweep after the wait lapses dials them again",
            )
        }
        val abandonedCount = batch.abandoned.size
        if (abandonedCount > 0) {
            val named =
                batch.abandoned
                    .map { it.url }
                    .sorted()
                    .take(MAX_ABANDONED_NAMED)
            val more = if (abandonedCount > named.size) " (+${abandonedCount - named.size} more)" else ""
            System.err.println(
                "router: fitness [$label] — gave up on $abandonedCount url(s) at the per-url deadline, no verdict written: ${named.joinToString()}$more",
            )
        }
        val negOpenCut = batch.negOpenCut
        if (negOpenCut > 0) {
            System.err.println(
                "router: fitness [$label] — $negOpenCut NEG-OPEN(s) cut at the ${NIP77_DEADLINE_MS / 1000}s wall clock; " +
                    "no nip77 fact published for them, which is our clock and NOT the relay declining",
            )
        }
        val secondPageCut = batch.secondPageCut
        if (secondPageCut > 0) {
            System.err.println(
                "router: fitness [$label] — $secondPageCut second page(s) cut at our own wall clock; those urls " +
                    "were graded on ONE page — our clock, and NOT the relay ignoring a cursor",
            )
        }
        val pageUnproven = batch.pageUnproven
        if (pageUnproven > 0) {
            System.err.println(
                "router: fitness [$label] — $pageUnproven url(s) carry NO `pageable` claim: nothing came back to " +
                    "page from, or page two never answered. Their grade stands; the claim is simply not one this " +
                    "pass earned",
            )
        }
        val cutLate = batch.cutLate
        if (cutLate > 0) {
            System.err.println(
                "router: fitness [$label] — $cutLate url(s) ran past the per-url deadline AFTER earning a verdict; " +
                    "the verdict stands and was written, but the dial did not finish",
            )
        }
    }

    /** What the store took of the batch's writes, and what it is still owed. */
    private fun reportWrites(
        label: String,
        graded: Int,
        written: FitnessWrites.Written,
    ) {
        val tally = written.tally
        if (written.waiting.isNotEmpty()) {
            System.err.println(
                "router: fitness [$label] — ${written.waiting.size} url(s) on ${written.waiting.values.toSet().size} server(s) wait on " +
                    "the fold behind another url of the same server, not graded; ${written.withdrawn.count { it in written.waiting }} standing `prime` grade(s) " +
                    "taken back. The fold has not said whether those paths are their own relays, so only the server's stand-in is graded",
            )
        }
        if (written.stale.isNotEmpty()) {
            System.err.println(
                "router: fitness [$label] — ${written.stale.size} standing `prime` grade(s) on urls that kept deferring " +
                    "($DEFERRED_SWEEPS sweeps in a row, or a grade older than $DEFERRED_SWEEPS sweeps), " +
                    "${written.withdrawn.count { it in written.stale }} taken back; nothing negative is said, they read as ungraded until a dial answers",
            )
        }
        if (written.withdrawalsPending > 0) {
            System.err.println(
                "router: fitness [$label] — ${written.withdrawalsPending} standing `prime` grade(s) NOT taken back yet: the store " +
                    "did not take the batch's writes" + (tally.stoppedBy?.let { " ($it)" }.orEmpty()) +
                    ". They wait with the unwritten grades and are retried every ${fmtDuration(AliasMonitor.DEFAULT_RETRY_MS)}, not next sweep",
            )
        }
        if (written.skipped > 0) {
            System.err.println(
                "router: fitness [$label] — ${written.skipped} inherited verdict(s) left standing: the record already carries " +
                    "them and this pass dialled nothing for them, so re-signing would refresh a measurement it did not take",
            )
        }
        // The pass ended abnormally: the verdicts were earned and the store would not take them.
        val unwritten = graded - tally.published - written.skipped
        if (unwritten <= 0) return
        val dropped = unwritten - tally.wedged - tally.declined
        val parts =
            buildList {
                if (tally.wedged > 0) add("${tally.wedged} write(s) hit the per-write store deadline")
                if (tally.declined > 0) add("${tally.declined} write(s) failed outright with the store answering")
                if (dropped > 0) add("the remaining $dropped were dropped rather than paying the deadline each")
                // Never nested under `dropped`, which can be zero or negative when a batch stops on its last urls.
                tally.stoppedBy?.let { add("the batch stopped because $it") }
            }
        System.err.println(
            "router: fitness [$label] — $unwritten earned verdict(s) NOT written: ${parts.joinToString(", ")}; " +
                "they are retried every ${fmtDuration(AliasMonitor.DEFAULT_RETRY_MS)} until the store takes them" +
                (writes.cursorOf(label)?.let { ", and the next batch's writes START at $it so the same tail is not dropped twice" }.orEmpty()),
        )
    }
}
