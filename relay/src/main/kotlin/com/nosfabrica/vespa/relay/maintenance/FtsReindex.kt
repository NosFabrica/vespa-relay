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
 * Re-put every event so its fed search fields are re-derived, in the background
 * (`REINDEX_FTS_ON_START`). Repairs fed fields only: a column Vespa derives at index time needs a
 * Vespa reindex instead, see docs/migrations.md. The cursor is persisted to [cursorFile] per page.
 *
 * [kinds] (`REINDEX_FTS_KINDS`) scopes the walk to those kinds, evaluated server-side, so the
 * rest of the corpus is never shipped or parsed; null walks everything. The store's cursor is only
 * valid for the scope that produced it, so the file records the scope and a mismatch starts over.
 */
fun launchFtsReindex(
    scope: CoroutineScope,
    store: VespaEventStore,
    cursorFile: String,
    kinds: List<Int>? = null,
) {
    scope.launch {
        val startedMs = System.currentTimeMillis()
        val what = if (kinds == null) "the whole corpus" else "${kinds.size} kind(s)"
        println("fts: reindexing $what in the background — search results may be incomplete until it finishes")
        var total = 0L
        var lastLineMs = startedMs
        val saved =
            runCatching {
                File(cursorFile)
                    .takeIf { it.isFile }
                    ?.readText()
                    ?.let(::decodeFtsCursor)
            }.onFailure { e ->
                System.err.println("fts: the saved cursor in $cursorFile is unreadable (${e.message?.take(80)}) — starting over")
            }.getOrNull()
        var cursor: String? = resumableCursor(saved, kinds)
        if (cursor != null) {
            println("fts: resuming from a saved cursor")
        } else if (saved?.cursor != null) {
            println("fts: the saved cursor belongs to ${ftsScopeLabel(saved.kinds)}, not this run's ${ftsScopeLabel(kinds)} — starting over")
        }
        // On a resumed run `total` counts only the remainder, so no denominator is honest.
        val expected =
            if (cursor != null) null else runCatching { store.count(Filter(kinds = kinds)) }.getOrNull()?.toLong()
        try {
            do {
                // The same cursor is retried a few times before giving up.
                var progress: FtsReindexProgress? = null
                var attempt = 0
                while (progress == null) {
                    progress =
                        runCatching {
                            if (kinds == null) {
                                store.reindexFullTextSearch(cursor)
                            } else {
                                // The kinds overload is the concrete store's, not IEventStore's.
                                store.store.reindexFullTextSearch(kinds, cursor)
                            }
                        }.onFailure { e ->
                            // A shutdown mid-page is not a failed page.
                            if (e is CancellationException) throw e
                            if (++attempt > FTS_PAGE_RETRIES) throw e
                            System.err.println("fts: page failed (${e.message?.take(80)}) — retry $attempt/$FTS_PAGE_RETRIES in ${attempt * 5}s")
                        }.getOrNull()
                    if (progress == null) delay(attempt * 5_000L)
                }
                cursor = progress.cursor
                // Temp file and move: a cursor truncated mid-kill fails every resume.
                runCatching {
                    val f = File(cursorFile)
                    f.absoluteFile.parentFile?.mkdirs()
                    val tmp = File(f.absoluteFile.parentFile, f.name + ".tmp")
                    tmp.writeText(encodeFtsCursor(kinds, cursor))
                    Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING)
                }
                total += progress.processedThisBatch
                // On wall time, not pages: a scoped walk is a few dozen sparse pages, each of which
                // may take seconds, so a page count would leave it nearly silent.
                val nowMs = System.currentTimeMillis()
                if (nowMs - lastLineMs >= FTS_PROGRESS_EVERY_MS) {
                    lastLineMs = nowMs
                    val secs = (nowMs - startedMs) / 1000
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
            // A leftover cursor would resume a finished walk from its tail on the next boot.
            runCatching { File(cursorFile).delete() }
        } catch (e: CancellationException) {
            // Shutdown mid-walk: the cursor is saved and the next boot resumes.
            throw e
        } catch (e: Exception) {
            val resume =
                if (cursor != null) {
                    "the cursor is saved, so restarting with REINDEX_FTS_ON_START resumes here"
                } else {
                    "no page completed, so a restart starts from the beginning"
                }
            System.err.println("fts: reindex FAILED after $total event(s): ${e.message} — $resume")
        }
    }
}

private const val FTS_PAGE_RETRIES = 5
private const val FTS_PROGRESS_EVERY_MS = 60_000L

/** A saved reindex position: the scope it was taken under (null = whole corpus) and the store's cursor. */
internal data class FtsCursor(
    val kinds: List<Int>?,
    val cursor: String?,
)

private const val FTS_SCOPE_PREFIX = "kinds="

/** The cursor file's text: a scope line, then the store's opaque cursor. */
internal fun encodeFtsCursor(
    kinds: List<Int>?,
    cursor: String?,
): String = FTS_SCOPE_PREFIX + (kinds?.joinToString(",") ?: "*") + "\n" + (cursor ?: "")

/**
 * Reads [encodeFtsCursor]'s text. A file with no scope line predates scoping and was written by the
 * whole-corpus walk, so it resumes that walk and nothing else.
 */
internal fun decodeFtsCursor(text: String): FtsCursor {
    val lines = text.trim().lines()
    val first = lines.firstOrNull().orEmpty()
    if (!first.startsWith(FTS_SCOPE_PREFIX)) return FtsCursor(null, text.trim().ifBlank { null })
    val scope = first.removePrefix(FTS_SCOPE_PREFIX).trim()
    val kinds = if (scope == "*") null else scope.split(',').map { it.trim().toInt() }
    return FtsCursor(
        kinds,
        lines
            .drop(1)
            .joinToString("\n")
            .trim()
            .ifBlank { null },
    )
}

/** The saved cursor if it was taken under [kinds]; the store's cursor is only valid for the scope that produced it. */
internal fun resumableCursor(
    saved: FtsCursor?,
    kinds: List<Int>?,
): String? = saved?.takeIf { it.kinds == kinds }?.cursor

private fun ftsScopeLabel(kinds: List<Int>?): String = if (kinds == null) "the whole corpus" else "kinds ${kinds.joinToString(",")}"

/**
 * `REINDEX_FTS_KINDS`: unset or blank is the whole corpus (null). Anything else must be a list of
 * kind numbers, and a value that is not stops the boot: falling back to the whole corpus would turn
 * a typo into a walk of every document, the very cost the scope exists to avoid.
 */
fun parseReindexKinds(raw: String?): List<Int>? {
    val text = raw?.trim()?.takeIf { it.isNotEmpty() } ?: return null
    val tokens = text.split(',', ' ', '\n', '\t').map { it.trim() }.filter { it.isNotEmpty() }
    // Separators alone are not "every kind": the store refuses an empty scope page by page.
    if (tokens.isEmpty()) error("REINDEX_FTS_KINDS='$text' names no kinds. Unset it to reindex the whole corpus.")
    val kinds =
        tokens.map { token ->
            token.toIntOrNull()?.takeIf { it in 0..65535 }
                ?: error("REINDEX_FTS_KINDS='$text': '$token' is not a kind number (0-65535). Unset it to reindex the whole corpus.")
        }
    return kinds.distinct().sorted()
}
