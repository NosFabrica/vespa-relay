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

import com.nosfabrica.vespa.relay.peers.SocketEnds
import com.nosfabrica.vespa.relay.util.fmtCount
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Why negentropy windows failed to reconcile: the socket's own end where one landed, else
 * quartz's sentence, counted per cause and per (relay, cause). Measures; changes nothing.
 */
internal class NegentropyDrops(
    /** Null where nothing records the sockets: the probes and the tests. */
    private val ends: SocketEnds? = null,
    private val now: () -> Long = System::currentTimeMillis,
) {
    private class Tally {
        var windows = 0L
        var largestSentMin: Int? = null
        var largestSentMax: Int? = null
        var lastAtMs = 0L

        @Synchronized
        fun add(
            largestSent: Int?,
            atMs: Long,
        ) {
            windows++
            lastAtMs = atMs
            if (largestSent != null) {
                largestSentMin = minOf(largestSentMin ?: largestSent, largestSent)
                largestSentMax = maxOf(largestSentMax ?: largestSent, largestSent)
            }
        }
    }

    private val total = AtomicLong()
    private val byCause = ConcurrentHashMap<String, Tally>()
    private val byRelay = ConcurrentHashMap<Pair<String, String>, Tally>()

    /** Taken before a window's reconcile, so what is read back belongs to that window. */
    fun mark(url: NormalizedRelayUrl): SocketEnds.Mark? = ends?.mark(url)

    /** Counts one window that could not reconcile and returns the sentence for its log line. */
    fun record(
        url: NormalizedRelayUrl,
        mark: SocketEnds.Mark?,
        /** Our entries in the window; null for a window too narrow to count. */
        localCount: Int?,
        /** Quartz's own sentence, `NegentropySyncException.detail`. */
        detail: String,
    ): String {
        val seen = mark?.let { ends?.since(it) }
        val end = seen?.end
        val socketWent = SOCKET_SENTENCES.any { detail.startsWith(it) }
        val cause =
            when {
                // Only when quartz says the socket went: a refusal in the relay's words is its own cause.
                socketWent && end != null -> end.cause

                socketWent && seen != null -> "no socket end: ${SocketEnds.foldDigits(detail).take(CAUSE_CHARS)}"

                else -> SocketEnds.foldDigits(detail).take(CAUSE_CHARS)
            }
        val largestSent = seen?.sent?.largest?.bytes
        val at = now()
        total.incrementAndGet()
        tally(byCause, cause, MAX_CAUSES, OTHER_CAUSE).add(largestSent, at)
        tally(byRelay, url.url to cause, MAX_PAIRS, OTHER_CAUSE to cause).add(largestSent, at)
        return detail +
            (seen?.let { s -> " — " + (end?.toString() ?: "the socket did not end; ${s.sent ?: "no socket"}") + "; ${negIn(s.negFramesIn)}, ${s.elapsedMs}ms after the ask" } ?: "") +
            "; " + (localCount?.let { "we hold ${fmtCount(it)} event(s) in the window" } ?: "the window is uncounted here")
    }

    private fun negIn(frames: Long?) = frames?.let { "$it NEG-MSG answered our NEG-OPEN" } ?: "no NEG-OPEN went out"

    /** [key]'s tally, a new one while there is room, else [overflow]'s. */
    private fun <K : Any> tally(
        map: ConcurrentHashMap<K, Tally>,
        key: K,
        max: Int,
        overflow: K,
    ): Tally = map[key] ?: map.computeIfAbsent(if (map.size < max) key else overflow) { Tally() }

    /** The `negentropyDrops` member of the progress document, or null before the first drop. */
    fun document(): JsonObject? {
        if (total.get() == 0L) return null
        val relays = byRelay.entries.sortedByDescending { it.value.windows }
        return buildJsonObject {
            put("windows", total.get())
            putJsonArray("causes") {
                for ((cause, t) in byCause.entries.sortedByDescending { it.value.windows }) {
                    add(
                        buildJsonObject {
                            put("cause", cause)
                            put("windows", t.windows)
                            t.largestSentMin?.let { put("largestSentMin", it) }
                            t.largestSentMax?.let { put("largestSentMax", it) }
                        },
                    )
                }
            }
            putJsonArray("byRelay") {
                for ((key, t) in relays.take(PUBLISHED_RELAYS)) {
                    add(
                        buildJsonObject {
                            put("relay", key.first)
                            put("cause", key.second)
                            put("windows", t.windows)
                            t.largestSentMax?.let { put("largestSentMax", it) }
                            put("lastAt", t.lastAtMs / 1000)
                        },
                    )
                }
            }
            put("omitted", (relays.size - PUBLISHED_RELAYS).coerceAtLeast(0))
        }
    }

    companion object {
        private const val CAUSE_CHARS = 80

        /** quartz's `detail` openings for a reconcile whose socket went away or never came. */
        private val SOCKET_SENTENCES = listOf("closed: relay disconnected", "could not connect", "connection closed before")
        private const val MAX_CAUSES = 256
        private const val OTHER_CAUSE = "other"
        private const val MAX_PAIRS = 4_096
        private const val PUBLISHED_RELAYS = 200
    }
}
