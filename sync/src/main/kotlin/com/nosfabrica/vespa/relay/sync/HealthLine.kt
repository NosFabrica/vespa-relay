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
package com.nosfabrica.vespa.relay.sync

/** One minute's reading of the process: everything the health line says, read before it is worded. */
internal data class HealthReading(
    val heapUsedMb: Long,
    val heapMaxMb: Long,
    val queueDepth: Int,
    val queueCapacity: Int,
    /** [bottleneckOf]'s answer, read once so the line and the document cannot disagree. */
    val bottleneck: String,
    val arrivingPerSec: Int,
    val eventsPerSec: Int,
    val connected: Int,
    val fatals: Long,
    val retracted: Long,
    /** The serving relay's pressure, already worded; null when ingest does not yield to it. */
    val pressure: String?,
    val ingestWorkers: Int,
    val ingestWorkersRunning: Int,
    val lostToStore: Long,
    /** Workers inside a batch and the oldest batch's age, which only a `wedged` line prints. */
    val inBatch: Int,
    val oldestBatchSec: Long,
    /** The longest outstanding store call, worded; null when none is. */
    val oldestStoreCall: String?,
) {
    val heapPct: Long get() = if (heapMaxMb > 0) heapUsedMb * 100 / heapMaxMb else 0
}

/**
 * Where the constraint is. A full queue is `ingest` only while it drains; a worker held inside
 * one batch for minutes is `wedged`.
 */
internal fun bottleneckOf(
    wedged: Boolean,
    depth: Int,
    capacity: Int,
    rate: Int,
): String =
    when {
        wedged -> "wedged"
        depth >= capacity -> "ingest"
        depth == 0 && rate == 0 -> "upstream"
        depth == 0 -> "downloads"
        else -> "mixed"
    }

/** The once-a-minute health line, worded from one [HealthReading]. */
internal fun healthLine(r: HealthReading): String =
    "router: health heap ${r.heapUsedMb}/${r.heapMaxMb}MB (${r.heapPct}%)" +
        (if (r.heapPct >= 90) " !! AT THE CEILING" else "") +
        ", ingest queue ${r.queueDepth}/${r.queueCapacity}" + queueClause(r) +
        ", ${r.arrivingPerSec} ev/s in, ${r.eventsPerSec} ev/s out" +
        ", ${r.connected} connected" +
        (if (r.fatals > 0) ", ${r.fatals} FATAL error(s) — threads were killed" else "") +
        (if (r.retracted > 0) ", ${r.retracted} record(s) DELETED as retracted upstream" else "") +
        (r.pressure?.let { ", $it" } ?: "") +
        stoppedWorkersClause(r) +
        (
            if (r.lostToStore > 0) {
                ", ${r.lostToStore} event(s) FAILED to reach the store (walks fetch them again, see bandsWithheld; " +
                    "a count that keeps climbing is a store that keeps failing — check the schema)"
            } else {
                ""
            }
        )

/** What the queue's depth means, by [HealthReading.bottleneck]. */
private fun queueClause(r: HealthReading): String =
    when (r.bottleneck) {
        "wedged" -> {
            " FULL and NOT DRAINING — ingest is wedged, not backpressured: " +
                "${r.inBatch}/${r.ingestWorkers} worker(s) in a batch, the oldest for ${r.oldestBatchSec}s. " +
                "The store stopped answering; look there, not at the relays" +
                (
                    r.oldestStoreCall?.let { ". Longest store call: $it" }
                        ?: ". No store call is outstanding — the workers are held somewhere other than the store"
                )
        }

        "ingest" -> {
            " FULL (ingest is the limit — downloads are backpressured)"
        }

        "upstream" -> {
            " empty (nothing is arriving — the limit is upstream of ingest)"
        }

        "downloads" -> {
            " drained (ingest is keeping up; downloads are the limit)"
        }

        else -> {
            ""
        }
    }

private fun stoppedWorkersClause(r: HealthReading): String {
    val stopped = r.ingestWorkers - r.ingestWorkersRunning
    return if (stopped > 0) ", $stopped of ${r.ingestWorkers} ingest worker(s) HAVE STOPPED — that share of the queue has no drain" else ""
}

/**
 * Per-second rates of two running totals between successive readings: what left a batch and
 * what went in. Only the pair tells a full queue at 0 ev/s from a fan-out gone quiet.
 */
internal class IngestRates(
    private var lastAtMs: Long,
) {
    private var lastEvents = 0L
    private var lastSubmitted = 0L

    /** Out and in, per second, since the previous call. */
    fun next(
        events: Long,
        submitted: Long,
        nowMs: Long,
    ): Pair<Int, Int> {
        val windowMs = (nowMs - lastAtMs).coerceAtLeast(1)
        val out = ((events - lastEvents) * 1000.0 / windowMs).toInt()
        val arriving = ((submitted - lastSubmitted) * 1000.0 / windowMs).toInt()
        lastEvents = events
        lastSubmitted = submitted
        lastAtMs = nowMs
        return out to arriving
    }
}
