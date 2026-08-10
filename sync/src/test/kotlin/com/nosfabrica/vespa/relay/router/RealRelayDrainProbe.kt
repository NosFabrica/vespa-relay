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
package com.nosfabrica.vespa.relay.router

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
 * Asks the five `indexers` relays, over the real network, the question the whole
 * drain change turns on: **does an empty page come back with an EOSE?**
 *
 * Everything else that pins this behaviour scripts a relay's terminal signals,
 * so it pins OUR interpretation of them and nothing about any relay's actual
 * EOSE discipline. Only this can tell the two apart — and it is the difference
 * between the phantom legs closing and the fix being inert in production.
 *
 * OFF by default and not a gate: it dials the public internet, so it is neither
 * hermetic nor reproducible, and a relay being down is not a code regression.
 * It asserts nothing for the same reason — it REPORTS, and a human reads it.
 *
 * ```
 * ./gradlew :sync:test --tests '*RealRelayDrainProbe*' -DrealRelayProbe=true -i
 * ```
 */
class RealRelayDrainProbe {
    /**
     * The exact legs `SyncCoverage.legs` hands the `indexers` stream today,
     * taken from the live `/stats.json` in the conversation that started this:
     * kind 10002 below the oldest relay list each relay holds. Every one of
     * these comes back empty — the question is whether it comes back DRAINED.
     */
    private val legs =
        listOf(
            Triple("wss://purplepag.es", 1_676_121_767L, "3.1y of kind 0 below it"),
            Triple("wss://user.kindpag.es", 1_748_273_096L, "1.8y below it"),
            Triple("wss://directory.yabu.me", 1_762_093_077L, "4.4mo below it"),
            // These two agree across kinds, so they have no phantom leg. Asked
            // anyway: a relay that does NOT drain here would mean the ending is
            // a property of the relay, not of the emptiness.
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
                // A HARD ceiling per relay, which `fetchAllPages` deliberately does
                // not have — its own doc says a walk is bounded by a `limit` or by
                // cancelling the caller, and this is the caller cancelling. Without
                // it, one relay that keeps answering hangs the whole probe, which
                // is what the first run did on coracle.
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
     * The measurement that ended the "purplepag.es stops EOSEing" theory, and the
     * one to re-run before touching [flooredForPaging].
     *
     * It walks each indexer twice from the same ceiling — once with the leg exactly
     * as [SyncCoverage.legs] hands it over (`since = null`, the shape every stream in
     * `router.conf.example` produces), and once floored.
     *
     * READ `end`, NOT `lowest until`. When this probe was written the tell was the
     * cursor going below zero, because nothing stopped it; on the quartz pinned now
     * it cannot, so that column can no longer discriminate. What separates the two
     * runs today is how the walk ENDS: unfloored, purplepag.es answers above the
     * cursor and quartz calls the walk `UNPAGEABLE` — the leg stays open, records no
     * coverage, and the whole 1.49M-event history is re-walked on the next boot;
     * floored, the page below the floor is an EOSE'd empty one, so the walk is
     * `DRAINED` and the leg closes. `lowest until` is still printed because it shows
     * how deep each run got, but note it is the `until` the walk ASKED for, not one
     * it was answered at.
     *
     * The ceiling is deliberately just above the epoch-stamped events rather than
     * `now`: purplepag.es serves ~2,300 kind 0/10002 events a second and holds years
     * of them, so starting at the top would spend a quarter of an hour on history
     * that is not what this measures. Starting here reaches the same cursor in one
     * page.
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
                                    // Kept, but it should no longer be reachable: the
                                    // pinned quartz floors its cursor at 0, so a walk
                                    // cannot run off the bottom of the time axis any
                                    // more. If this ever prints again, the guard
                                    // upstream has regressed — that is the finding,
                                    // not the relay.
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

        /**
         * Just above the events purplepag.es stamps `created_at = 0` — twelve kind
         * 10002s, and the reason a walk there runs off the bottom of the time axis.
         * One page from this ceiling already carries them, so both runs reach the
         * cursor that matters immediately.
         */
        private const val TRAP_CEILING = 1_600_000_000L

        /**
         * Not an idle timeout — the relay is answering the whole time, EOSE on every
         * page. This is how long a walk gets to prove it can END.
         */
        private const val TERMINATION_MS = 45_000L
    }
}
