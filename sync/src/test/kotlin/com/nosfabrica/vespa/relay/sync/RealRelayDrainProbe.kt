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

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * Asks the `indexers` relays, over the real network, whether an empty page
 * comes back EOSEd and whether an unfloored leg can end at all. Prints a
 * report per relay and asserts nothing. Selected by `-DrealRelayProbe=true`.
 */
class RealRelayDrainProbe {
    /** The legs `SyncCoverage.legs` hands the `indexers` stream: kind 10002 below each relay's oldest relay list. */
    private val legs =
        listOf(
            Triple("wss://purplepag.es", 1_676_121_767L, "3.1y of kind 0 below it"),
            Triple("wss://user.kindpag.es", 1_748_273_096L, "1.8y below it"),
            Triple("wss://directory.yabu.me", 1_762_093_077L, "4.4mo below it"),
            // Controls: kinds agree on these two, so they have no phantom leg.
            Triple("wss://profiles.nostr1.com", 1_676_163_321L, "kinds agree — control"),
            Triple("wss://indexer.coracle.social", 1_676_163_321L, "kinds agree — control"),
        )

    @Test
    fun reportHowTheIndexersEndAnEmptyPage() {
        if (System.getProperty("realRelayProbe") != "true") {
            println("[skip] RealRelayDrainProbe — set -DrealRelayProbe=true to dial the public internet")
            return
        }
        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)

        println("=".repeat(78))
        println("Does an empty page come back EOSEd? (kind 10002, below each relay's floor)")
        println("=".repeat(78))
        try {
            for ((url, until, note) in legs) {
                val relay = RelayUrlNormalizer.normalize(url)
                val leg = Filter(kinds = listOf(10002), until = until)
                // `fetchAllPages` has no ceiling of its own; one relay that keeps answering would hang the probe.
                var events = 0
                var oldest = Long.MAX_VALUE
                val outcome =
                    runCatching {
                        runBlocking {
                            withTimeoutOrNull(PER_RELAY_MS) {
                                client.fetchAllPages(relay, listOf(leg), idleTimeoutMs = 20_000L) { e ->
                                    events++
                                    if (e.createdAt < oldest) oldest = e.createdAt
                                }
                            }
                        }
                    }
                val reach = if (events > 0) ", oldest seen $oldest" else ""
                val verdict =
                    outcome.fold(
                        onSuccess = { r ->
                            when {
                                r == null -> "STILL GOING at ${PER_RELAY_MS / 1000}s — $events event(s)$reach"
                                r.drained -> "${r.end} (${r.downloaded} event(s)$reach) — LEG CLOSES"
                                else -> "${r.end} (${r.downloaded} event(s)$reach) — leg stays open"
                            }
                        },
                        onFailure = { "threw ${it::class.simpleName}: ${it.message}" },
                    )
                println("  %-32s %s".format(url.removePrefix("wss://"), verdict))
                println("  %-32s   until=%d, %s".format("", until, note))
            }
        } finally {
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(78))
    }

    /**
     * Walks each indexer twice from [TRAP_CEILING], once with the leg as
     * [SyncCoverage.legs] builds it and once through [flooredForPaging]. Read
     * `end`: `DRAINED` closes the leg, `UNPAGEABLE` leaves it to re-walk every
     * boot. `lowest until` is what the walk asked for, not what it was answered at.
     */
    @Test
    fun reportWhetherAnUnflooredLegCanEndAtAll() {
        if (System.getProperty("realRelayProbe") != "true") {
            println("[skip] RealRelayDrainProbe — set -DrealRelayProbe=true to dial the public internet")
            return
        }
        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)

        println("=".repeat(78))
        println("Does the `indexers` leg TERMINATE? (kinds [0, 10002], from $TRAP_CEILING down)")
        println("  unfloored = the leg as legs() builds it; floored = flooredForPaging()")
        println("  read `end`: DRAINED closes the leg, UNPAGEABLE leaves it to re-walk every boot")
        println("=".repeat(78))
        try {
            for ((url, _, _) in legs) {
                val relay = RelayUrlNormalizer.normalize(url)
                println("  ${url.removePrefix("wss://")}")
                for (floored in listOf(false, true)) {
                    val leg = Filter(kinds = listOf(0, 10002), until = TRAP_CEILING)
                    val asked = if (floored) leg.flooredForPaging() else leg
                    var events = 0
                    var pages = 0
                    var lowest = Long.MAX_VALUE
                    val outcome =
                        runCatching {
                            runBlocking {
                                withTimeoutOrNull(TERMINATION_MS) {
                                    client.fetchAllPages(
                                        relay,
                                        listOf(asked),
                                        idleTimeoutMs = 20_000L,
                                        onNewPage = { until ->
                                            pages++
                                            if (until < lowest) lowest = until
                                        },
                                    ) { events++ }
                                }
                            }
                        }
                    val reached = if (lowest == Long.MAX_VALUE) "no page after the first" else "$lowest"
                    val verdict =
                        outcome.fold(
                            onSuccess = { r ->
                                when (r) {
                                    // Unreachable while the pinned quartz floors its cursor at 0; printing again means that guard regressed.
                                    null -> "NEVER ENDED in ${TERMINATION_MS / 1000}s"

                                    else -> "${r.end} (${r.downloaded} event(s))"
                                }
                            },
                            onFailure = { "threw ${it::class.simpleName}: ${it.message}" },
                        )
                    println("    %-10s %-34s %d page(s), %d event(s), lowest until=%s".format(if (floored) "floored" else "unfloored", verdict, pages, events, reached))
                }
            }
        } finally {
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(78))
    }

    companion object {
        private const val PER_RELAY_MS = 120_000L

        /** Just above the events purplepag.es stamps `created_at = 0`, so one page reaches the cursor that matters. */
        private const val TRAP_CEILING = 1_600_000_000L

        /** Not an idle timeout; the relay answers throughout. How long a walk gets to prove it can end. */
        private const val TERMINATION_MS = 45_000L
    }
}
