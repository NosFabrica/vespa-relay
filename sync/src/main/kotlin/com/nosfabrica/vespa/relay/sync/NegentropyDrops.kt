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

import com.nosfabrica.vespa.relay.util.fmtCount
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.single.RelayDisconnect
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Why negentropy windows failed to reconcile, counted per cause and per (relay, cause), off
 * quartz's typed failure and the socket end it carries. One socket end is counted once, however
 * many windows it failed: a per-kind reconcile inside a dense window shares its enclosing
 * window's socket, so a drop there fails both. Measures; changes nothing.
 */
internal class NegentropyDrops(
    private val now: () -> Long = System::currentTimeMillis,
) {
    private class Tally {
        private var windows = 0L
        private var largestSentMin: Int? = null
        private var largestSentMax: Int? = null
        private var lastAtMs = 0L

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

        /** One instant of this row, read under the lock that writes it. */
        @Synchronized
        fun snapshot() = Row(windows, largestSentMin, largestSentMax, lastAtMs)
    }

    private class Row(
        val windows: Long,
        val largestSentMin: Int?,
        val largestSentMax: Int?,
        val lastAtMs: Long,
    )

    private val total = AtomicLong()
    private val byCause = ConcurrentHashMap<String, Tally>()
    private val byRelay = ConcurrentHashMap<Pair<String, String>, Tally>()

    /** Per relay, the last socket end counted. quartz hands every session on that socket the same object. */
    private val lastEnd = ConcurrentHashMap<String, RelayDisconnect>()

    /** Counts one window that could not reconcile and returns the sentence for its log line. */
    fun record(
        url: NormalizedRelayUrl,
        /** Our entries in the window; null for a window too narrow to count. */
        localCount: Int?,
        failure: NegentropySyncException,
    ): String {
        val sent = failure.disconnect?.sent ?: failure.sent
        val counted = firstWindowOf(url, failure.disconnect)
        if (counted) {
            val cause = causeOf(failure)
            val largestSent = sent?.largestBytes?.takeIf { sent.count > 0 }
            val at = now()
            total.incrementAndGet()
            tally(byCause, cause, MAX_CAUSES, OTHER_CAUSE).add(largestSent, at)
            tally(byRelay, url.url to cause, MAX_PAIRS, OTHER_CAUSE to cause).add(largestSent, at)
        }
        return failure.detail +
            (failure.disconnect?.let { " — $it" } ?: sent?.let { " — the socket did not end; $it" } ?: "") +
            (failure.rounds?.let { "; $it NEG-MSG round(s) completed" } ?: "") +
            "; " + (localCount?.let { "we hold ${fmtCount(it)} event(s) in the window" } ?: "the window is uncounted here") +
            (if (counted) "" else "; the socket end an earlier window was counted for, not counted again")
    }

    /**
     * False when [end] was already counted for [url]: the same object, so the same socket, even
     * when two windows on it fail at once. A failure with the socket still up is its own.
     */
    private fun firstWindowOf(
        url: NormalizedRelayUrl,
        end: RelayDisconnect?,
    ): Boolean {
        if (end == null) return true
        if (lastEnd.size >= MAX_PAIRS && !lastEnd.containsKey(url.url)) lastEnd.clear()
        return lastEnd.put(url.url, end) !== end
    }

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
        val relays = byRelay.entries.map { it.key to it.value.snapshot() }.sortedByDescending { it.second.windows }
        return buildJsonObject {
            put("windows", total.get())
            putJsonArray("causes") {
                for ((cause, t) in byCause.entries.map { it.key to it.value.snapshot() }.sortedByDescending { it.second.windows }) {
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
        private const val REASON_CHARS = 60
        private val DIGITS = Regex("[0-9]+")

        /** [s] with every run of digits as `#`, so a reason carrying counts or addresses is one cause. */
        internal fun foldDigits(s: String): String = DIGITS.replace(s, "#")

        /**
         * A short key to count a failure by: how its socket ended where it did, else what kind of
         * failure quartz says it was, with the relay's own words for a refusal.
         */
        internal fun causeOf(failure: NegentropySyncException): String {
            val end = failure.disconnect
            val kind = failure.failure
            return when {
                end != null -> causeOf(end)
                kind == NegentropySyncException.Failure.DISCONNECTED -> "disconnected, no end recorded"
                kind == NegentropySyncException.Failure.CONNECT_TIMEOUT -> "no connection, no dial ended"
                kind == NegentropySyncException.Failure.SILENT -> "silent"
                kind == NegentropySyncException.Failure.REFUSED -> "refused: ${foldDigits(failure.detail).take(CAUSE_CHARS)}"
                kind != null -> kind.name.lowercase().replace('_', ' ')
                else -> foldDigits(failure.detail).take(CAUSE_CHARS)
            }
        }

        private fun causeOf(end: RelayDisconnect): String =
            when (end.side) {
                RelayDisconnect.Side.RELAY -> "relay closed ${end.code ?: "?"}" + (end.reason?.takeIf { it.isNotBlank() }?.let { " ${foldDigits(it).take(REASON_CHARS)}" } ?: "")
                RelayDisconnect.Side.CLIENT -> "we closed (${end.reason ?: "no reason"})"
                RelayDisconnect.Side.TRANSPORT -> "transport ${end.errorType ?: "failure"}" + (end.code?.let { " http $it" } ?: "")
            }

        private const val MAX_CAUSES = 256
        private const val OTHER_CAUSE = "other"
        private const val MAX_PAIRS = 4_096
        private const val PUBLISHED_RELAYS = 200
    }
}
