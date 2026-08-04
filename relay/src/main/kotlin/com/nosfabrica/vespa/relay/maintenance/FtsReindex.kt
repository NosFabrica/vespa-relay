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
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.FtsReindexProgress
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.io.File

/**
 * Re-derive every event's search fields, in the background — a one-off
 * migration (`REINDEX_FTS_ON_START`), not a boot step. Needed after a store
 * upgrade that changes SearchExtractors or adds fed search fields, which a
 * Vespa reindex cannot produce — only a put can.
 *
 * The walk is resumable: the cursor is persisted to [cursorFile] so a failed
 * page costs a retry, not the 12M events an in-memory cursor once threw away.
 * It runs behind the server and is never awaited — as a startup barrier it
 * would be an hours-long outage.
 */
fun launchFtsReindex(
    scope: CoroutineScope,
    store: VespaEventStore,
    cursorFile: String,
) {
    scope.launch {
        val startedMs = System.currentTimeMillis()
        println("fts: reindexing the whole corpus in the background — search results may be incomplete until it finishes")
        var total = 0L
        var pages = 0
        // The denominator, asked for once. Null rather than a guess if it
        // fails: an unknown denominator is better than a wrong one.
        val expected = runCatching { store.count(Filter()) }.getOrNull()?.toLong()
        var cursor: String? =
            runCatching {
                File(cursorFile)
                    .takeIf { it.isFile }
                    ?.readText()
                    ?.trim()
                    ?.ifBlank { null }
            }.getOrNull()
        if (cursor != null) println("fts: resuming from a saved cursor")
        try {
            do {
                // A page can fail for reasons unrelated to its contents (Vespa
                // under memory pressure answers with an HTML error body). Retry
                // the SAME cursor a few times before giving up.
                var progress: FtsReindexProgress? = null
                var attempt = 0
                while (progress == null) {
                    progress =
                        runCatching { store.reindexFullTextSearch(cursor) }
                            .onFailure { e ->
                                if (++attempt > FTS_PAGE_RETRIES) throw e
                                System.err.println("fts: page failed (${e.message?.take(80)}) — retry $attempt/$FTS_PAGE_RETRIES in ${attempt * 5}s")
                            }.getOrNull()
                    if (progress == null) delay(attempt * 5_000L)
                }
                cursor = progress.cursor
                runCatching { File(cursorFile).writeText(cursor ?: "") }
                total += progress.processedThisBatch
                // Every page would be a flood; never would be silence.
                if (++pages % 50 == 0) {
                    val secs = (System.currentTimeMillis() - startedMs) / 1000
                    val rate = if (secs > 0) total / secs else 0
                    val pct = expected?.takeIf { it > 0 }?.let { " (${total * 100 / it}%)" } ?: ""
                    val eta =
                        if (expected != null && rate > 0 && expected > total) {
                            ", ETA ~${fmtDuration((expected - total) / rate * 1000)}"
                        } else {
                            ""
                        }
                    println(
                        "fts: reindexed ${total}${expected?.let { "/$it" } ?: ""} event(s)$pct" +
                            " in ${fmtDuration(secs * 1000)}, $rate/s$eta",
                    )
                }
            } while (!progress.done)
            println("fts: reindex complete — $total event(s) in ${fmtDuration(System.currentTimeMillis() - startedMs)}")
            // Done means done: a leftover cursor would resume a finished walk
            // from its tail on the next boot with the flag still set.
            runCatching { File(cursorFile).delete() }
        } catch (e: Exception) {
            System.err.println(
                "fts: reindex FAILED after $total event(s): ${e.message}" +
                    " — the cursor is saved, so restarting with REINDEX_FTS_ON_START resumes here",
            )
        }
    }
}

/** Retries for one page before the walk gives up. */
private const val FTS_PAGE_RETRIES = 5
