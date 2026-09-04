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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.FtsReindexProgress
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
 * Re-put every event so its fed search fields are re-derived, in the
 * background (`REINDEX_FTS_ON_START`). It repairs a fed field only: a column
 * Vespa derives at index time stays empty after this walk and needs a Vespa
 * reindex instead, see docs/migrations.md.
 *
 * The cursor is persisted to [cursorFile] so a failed page costs a retry, not
 * the walk. Never awaited: as a boot barrier it would be an hours-long outage.
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
        var cursor: String? =
            runCatching {
                File(cursorFile)
                    .takeIf { it.isFile }
                    ?.readText()
                    ?.trim()
                    ?.ifBlank { null }
            }.getOrNull()
        if (cursor != null) println("fts: resuming from a saved cursor")
        // On a resumed run `total` counts only the remainder, so no denominator
        // is honest; an unknown one beats a wrong one.
        val expected = if (cursor != null) null else runCatching { store.count(Filter()) }.getOrNull()?.toLong()
        try {
            do {
                // A page can fail for reasons unrelated to its contents; retry
                // the same cursor a few times before giving up.
                var progress: FtsReindexProgress? = null
                var attempt = 0
                while (progress == null) {
                    progress =
                        runCatching { store.reindexFullTextSearch(cursor) }
                            .onFailure { e ->
                                // runCatching catches cancellation too, and a
                                // shutdown mid-page is not a failed page.
                                if (e is CancellationException) throw e
                                if (++attempt > FTS_PAGE_RETRIES) throw e
                                System.err.println("fts: page failed (${e.message?.take(80)}) — retry $attempt/$FTS_PAGE_RETRIES in ${attempt * 5}s")
                            }.getOrNull()
                    if (progress == null) delay(attempt * 5_000L)
                }
                cursor = progress.cursor
                // Temp file and move: a cursor truncated mid-kill fails every
                // resume until someone deletes it by hand.
                runCatching {
                    val f = File(cursorFile)
                    f.absoluteFile.parentFile?.mkdirs()
                    val tmp = File(f.absoluteFile.parentFile, f.name + ".tmp")
                    tmp.writeText(cursor ?: "")
                    Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING)
                }
                total += progress.processedThisBatch
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
            // A leftover cursor would resume a finished walk from its tail on
            // the next boot with the flag still set.
            runCatching { File(cursorFile).delete() }
        } catch (e: CancellationException) {
            // Shutdown mid-walk: the cursor is saved and the next boot resumes.
            throw e
        } catch (e: Exception) {
            System.err.println(
                "fts: reindex FAILED after $total event(s): ${e.message}" +
                    " — the cursor is saved, so restarting with REINDEX_FTS_ON_START resumes here",
            )
        }
    }
}

private const val FTS_PAGE_RETRIES = 5
