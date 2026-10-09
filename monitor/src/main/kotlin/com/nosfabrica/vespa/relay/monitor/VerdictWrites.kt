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

/** What one batch of verdict writes came to. [resumeAt] is the url whose write tripped a limit, if one did. */
internal class VerdictWrites(
    val published: Int,
    val declined: Int,
    val wedged: Int,
    val stoppedBy: String?,
    val resumeAt: NormalizedRelayUrl?,
)

/**
 * One verdict write per url, [concurrency] at a time in [urls]' order, each under [deadlineMs].
 * Each url is its own record, so the writes never race; a store that stops answering ends the batch.
 */
internal suspend fun writeEach(
    urls: List<NormalizedRelayUrl>,
    concurrency: Int,
    deadlineMs: Long,
    /** Unanswered writes in a row, by completion, before the batch stops. */
    wedgeLimit: Int,
    /** Write time the batch may lose to unanswered writes; the run limit alone misses an alternating store. */
    wedgeBudgetMs: Long,
    progress: Processors.Handle?,
    stage: String,
    /** True stored, false the store answering and the write still failing. */
    write: suspend (NormalizedRelayUrl) -> Boolean,
): VerdictWrites {
    val lock = Any()
    var published = 0
    var declined = 0
    var wedged = 0
    var lostMs = 0L
    var stoppedBy: String? = null
    var stoppedAt: NormalizedRelayUrl? = null
    // "In a row" by start order, so a slow write finishing after quick answers to later ones does not
    // read as a run: a wedge counts only while no write started after it has been answered.
    var lastAnswered = -1
    val runOf = ArrayList<Int>()
    val permits = Semaphore(concurrency)
    coroutineScope {
        for ((i, url) in urls.withIndex()) {
            permits.acquire()
            if (synchronized(lock) { stoppedBy != null }) {
                permits.release()
                break
            }
            launch {
                try {
                    progress?.holding(url.url, stage)
                    val startedMs = System.currentTimeMillis()
                    val wrote =
                        try {
                            withTimeoutOrNull(deadlineMs) {
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
                            lastAnswered = maxOf(lastAnswered, i)
                            runOf.removeAll { it < lastAnswered }
                        } else {
                            if (i > lastAnswered) runOf += i
                            // A wedged slot costs the batch its share of the throughput, not the whole wall clock.
                            lostMs += (System.currentTimeMillis() - startedMs) / concurrency
                            if (stoppedBy == null) {
                                stoppedBy =
                                    when {
                                        runOf.size >= wedgeLimit -> "${runOf.size} write(s) in a row went unanswered"
                                        lostMs >= wedgeBudgetMs -> "${lostMs / 1000}s of this batch was spent waiting on writes that never came back"
                                        else -> null
                                    }
                                // At this url, not after it: the write that tripped the limit did not land.
                                if (stoppedBy != null) stoppedAt = url
                            }
                        }
                    }
                    progress?.attempted()
                } finally {
                    permits.release()
                }
            }
        }
    }
    return VerdictWrites(published, declined, wedged, stoppedBy, stoppedAt)
}
