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

import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap

/** What one batch of verdict writes came to. [resumeAt] is, when a limit stopped it, the earliest url not stored. */
internal class VerdictWrites(
    val published: Int,
    val declined: Int,
    val wedged: Int,
    val stoppedBy: String?,
    val resumeAt: NormalizedRelayUrl?,
    /** The urls whose write the store took. */
    val stored: Set<NormalizedRelayUrl> = emptySet(),
)

/** The limits a pass's verdict writes run under; the defaults are every pass's. */
internal data class WriteLimits(
    /** Verdict writes in flight at once; each is its own record, so they never race. */
    val concurrency: Int = WRITE_CONCURRENCY,
    /** The wall clock on one write. */
    val deadlineMs: Long = PUBLISH_DEADLINE_MS,
    /** Unanswered writes in a row, by start order, before the writer stops; reset by every write the store answers. */
    val wedgeLimit: Int = PUBLISH_WEDGE_LIMIT,
    /** Write time the writer may lose to unanswered writes; the run limit alone misses an alternating store. */
    val wedgeBudgetMs: Long = PUBLISH_WEDGE_BUDGET_MS,
) {
    companion object {
        /** Sized against the store's worst honest case; the fault it bounds is not slowness but forever. */
        const val PUBLISH_DEADLINE_MS = 60_000L

        const val WRITE_CONCURRENCY = 16

        const val PUBLISH_WEDGE_LIMIT = 3

        const val PUBLISH_WEDGE_BUDGET_MS = 20L * 60 * 1000
    }
}

/**
 * Verdict writes under one set of limits across every [write] call a pass makes: each url is its own
 * record under [WriteLimits.deadlineMs], so writes never race, and once the store stops answering every later write is refused.
 */
internal class VerdictWriter(
    private val limits: WriteLimits,
    private val progress: Processors.Handle?,
    private val stage: String,
    /** Whether each write moves [progress]'s position; false where the position counts something else. */
    private val tick: Boolean = true,
) {
    private val lock = Any()
    private val permits = Semaphore(limits.concurrency)

    var published = 0
        private set
    var declined = 0
        private set
    var wedged = 0
        private set

    /** Writes offered after a limit stopped the writer, and never started. */
    var refused = 0
        private set

    /** Which limit stopped the writer, in words, or null while it runs. */
    @Volatile
    var stoppedBy: String? = null
        private set

    private var lostMs = 0L
    private var started = 0L

    // "In a row" by start order, so a slow write finishing after quick answers to later ones does not
    // read as a run: a wedge counts only while no write started after it has been answered.
    private var lastAnswered = -1L
    private val runOf = ArrayList<Long>()

    /** Every write that did not land: wedged, declined, or refused after a stop. */
    val unwritten: Int get() = synchronized(lock) { declined + wedged + refused }

    /** One write per url, [WriteLimits.concurrency] at a time in [urls]' order. Returns the urls the store took. */
    suspend fun write(
        urls: List<NormalizedRelayUrl>,
        /** True stored, false the store answering and the write still failing. */
        write: suspend (NormalizedRelayUrl) -> Boolean,
    ): Set<NormalizedRelayUrl> {
        val stored = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
        coroutineScope {
            for ((i, url) in urls.withIndex()) {
                permits.acquire()
                val seq =
                    synchronized(lock) {
                        if (stoppedBy != null) {
                            refused += urls.size - i
                            null
                        } else {
                            started++
                        }
                    }
                if (seq == null) {
                    permits.release()
                    break
                }
                launch {
                    try {
                        if (attempt(url, seq, write)) stored += url
                    } finally {
                        permits.release()
                    }
                }
            }
        }
        return stored
    }

    /** One write under the deadline, its outcome folded into the limits. True when the store took it. */
    private suspend fun attempt(
        url: NormalizedRelayUrl,
        seq: Long,
        write: suspend (NormalizedRelayUrl) -> Boolean,
    ): Boolean {
        progress?.holding(url.url, stage)
        val startedMs = System.currentTimeMillis()
        val wrote =
            try {
                withTimeoutOrNull(limits.deadlineMs) {
                    try {
                        write(url)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (_: Exception) {
                        false
                    }
                }
            } finally {
                progress?.released(url.url)
            }
        synchronized(lock) {
            when (wrote) {
                true -> published++
                false -> declined++
                null -> wedged++
            }
            if (wrote != null) {
                lastAnswered = maxOf(lastAnswered, seq)
                runOf.removeAll { it < lastAnswered }
            } else {
                if (seq > lastAnswered) runOf += seq
                // A wedged slot costs the batch its share of the throughput, not the whole wall clock.
                lostMs += (System.currentTimeMillis() - startedMs) / limits.concurrency
                if (stoppedBy == null) {
                    stoppedBy =
                        when {
                            runOf.size >= limits.wedgeLimit -> "${runOf.size} write(s) in a row went unanswered"
                            lostMs >= limits.wedgeBudgetMs -> "${lostMs / 1000}s of this batch was spent waiting on writes that never came back"
                            else -> null
                        }
                }
            }
        }
        if (tick) progress?.attempted()
        return wrote == true
    }
}

/**
 * One verdict write per url under [limits], in [urls]' order. Each url is its own record, so the
 * writes never race; a store that stops answering ends the batch, counted by start order.
 */
internal suspend fun writeEach(
    urls: List<NormalizedRelayUrl>,
    limits: WriteLimits,
    progress: Processors.Handle?,
    stage: String,
    /** True stored, false the store answering and the write still failing. */
    write: suspend (NormalizedRelayUrl) -> Boolean,
): VerdictWrites {
    val writer = VerdictWriter(limits, progress, stage)
    val stored = writer.write(urls, write)
    // Completion order is not write order: writes started before the one that tripped the limit may not have landed.
    val resumeAt = if (writer.stoppedBy == null) null else urls.firstOrNull { it !in stored }
    return VerdictWrites(writer.published, writer.declined, writer.wedged, writer.stoppedBy, resumeAt, stored)
}

/** The one stage that is not a dial: the record edit that puts a verdict down. */
internal const val STAGE_PUBLISH = "verdict write"
