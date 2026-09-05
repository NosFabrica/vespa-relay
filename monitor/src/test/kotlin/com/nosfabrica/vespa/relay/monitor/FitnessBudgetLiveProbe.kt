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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * Where a url's fitness budget goes: times the pre-probe, NIP-11, ladder and NEG-OPEN
 * separately against real relays, then runs the real [FitnessPass] over the same urls.
 * Asserts nothing. `-DliveBudget=true` selects it; `-DliveBudgetUrls=a,b` picks the urls.
 */
class FitnessBudgetLiveProbe {
    private val urls: List<NormalizedRelayUrl> =
        (
            System.getProperty("liveBudgetUrls")
                ?: listOf(
                    "wss://nip85.nosfabrica.com",
                    "wss://nip85-staging.nosfabrica.com",
                    "wss://nip85-staging.relay.tools",
                    "wss://bucket.coracle.social",
                    "wss://relay.vertexlab.io",
                    "wss://relay.primal.net",
                    "wss://nos.lol",
                    "wss://relay.damus.io",
                    "wss://nip85.brainstorm.world",
                ).joinToString(",")
        ).split(",").mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }

    private class Legs(
        val url: NormalizedRelayUrl,
        var preProbeMs: Long = -1,
        var documentMs: Long = -1,
        var ladderMs: Long = -1,
        var firstPageMs: Long? = null,
        var events: Int = 0,
        var negOpenMs: Long = -1,
        var negOpen: String = "",
    ) {
        val totalMs get() = maxOf(0, preProbeMs) + maxOf(0, documentMs) + maxOf(0, ladderMs) + maxOf(0, negOpenMs)
    }

    @Test
    fun whereTheBudgetGoes() {
        if (System.getProperty("liveBudget") != "true") {
            println("[skip] FitnessBudgetLiveProbe — set -DliveBudget=true to dial the public internet")
            return
        }
        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(15))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        val signer = NostrSignerInternal(KeyPair())
        RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }
        val reachability = ReachabilityProbe(null)
        val document = RelayDocument({ okhttp })
        val probe = AliasProbe.over(client, FitnessPass.FITNESS_TARGET) { IDLE_MS }
        val deadlineMs = probe.deadlineMs(urls.first())
        val anchor = RelayConsistency.settledAnchor(System.currentTimeMillis() / 1000)

        val legs = urls.map { Legs(it) }
        try {
            runBlocking {
                for (leg in legs) {
                    val url = leg.url
                    leg.preProbeMs = timed { runCatching { reachability.canDial(url) } }
                    leg.documentMs = timed { runCatching { document.read(url) } }
                    var seen = 0
                    var window: AliasProbe.Window? = null
                    leg.ladderMs =
                        timed {
                            // First rung only.
                            window = runCatching { probe.window(url, anchor, null) { seen++ } }.getOrNull()
                        }
                    leg.firstPageMs = window?.firstPageMs
                    leg.events = seen
                    // The same call, sliver and idle window as the pass.
                    val sliver =
                        Filter(
                            kinds = null,
                            since = anchor - FitnessPass.NIP77_WINDOW_SECONDS,
                            until = anchor,
                        )
                    leg.negOpenMs =
                        timed {
                            leg.negOpen =
                                runCatching {
                                    client.negentropyReconcileIds(url, sliver, emptyList(), idleTimeoutMs = FitnessPass.NIP77_IDLE_MS)
                                    "answered"
                                }.getOrElse { "declined/failed: ${it.javaClass.simpleName}" }
                        }
                }
            }

            println("=".repeat(112))
            println("WHERE ONE URL'S BUDGET GOES — per-url deadline ${deadlineMs / 1000}s (WINDOWS_PER_URL x ${IDLE_MS / 1000}s)")
            println("=".repeat(112))
            println(
                "%-36s %9s %9s %9s %7s %9s %9s  %s".format(
                    "relay",
                    "pre-probe",
                    "nip-11",
                    "ladder",
                    "events",
                    "rtt-read",
                    "NEG-OPEN",
                    "total / budget",
                ),
            )
            for (l in legs.sortedByDescending { it.totalMs }) {
                println(
                    "%-36s %8dms %8dms %8dms %7d %8sms %8dms  %6.1fs %s".format(
                        l.url.url
                            .removePrefix("wss://")
                            .removeSuffix("/")
                            .take(36),
                        l.preProbeMs,
                        l.documentMs,
                        l.ladderMs,
                        l.events,
                        l.firstPageMs?.toString() ?: "-",
                        l.negOpenMs,
                        l.totalMs / 1000.0,
                        if (l.totalMs >= deadlineMs) "*** OVER BUDGET — this url is ABANDONED every pass ***" else "",
                    ),
                )
            }
            println()
            println("NEG-OPEN outcomes:")
            for (l in legs) {
                println(
                    "  %-36s %6dms  %s".format(
                        l.url.url
                            .removePrefix("wss://")
                            .removeSuffix("/")
                            .take(36),
                        l.negOpenMs,
                        l.negOpen,
                    ),
                )
            }

            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            val processors = Processors()
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = probe,
                    client = client,
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = processors.of("fitness"),
                    document = document,
                )
            println()
            println("=".repeat(112))
            println("THE REAL PASS OVER THE SAME URLS")
            println("=".repeat(112))
            runBlocking {
                pass.measure("live budget", urls, canDial = { reachability.canDial(it) }, onEvent = {}, sockets = Sockets.NONE)
            }
        } finally {
            scope.cancel()
        }
    }

    private inline fun timed(block: () -> Unit): Long {
        val t0 = System.nanoTime()
        block()
        return (System.nanoTime() - t0) / 1_000_000
    }

    private companion object {
        /** The router's default `connectionTimeout`, so the budget printed is production's. */
        const val IDLE_MS = 20_000L
    }
}
