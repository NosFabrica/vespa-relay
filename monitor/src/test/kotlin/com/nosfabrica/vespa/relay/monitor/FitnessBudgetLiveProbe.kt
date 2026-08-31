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
 * WHERE A URL'S BUDGET ACTUALLY GOES, measured one step at a time against real
 * relays — the experiment #172 could not be closed without.
 *
 * The issue supplies a correlation and says so: re-verdict cadence tracks
 * `rtt-read`, and the relays slowest to answer a read sit at a median verdict
 * age twenty times the fast group's. It explicitly does NOT supply a mechanism.
 * Reading the pass, the candidate is the per-url wall clock
 * ([AliasProbe.deadlineMs]) firing on a job that has already earned its verdict
 * — and the only step of that job with no bound of its own is the NEG-OPEN,
 * which is last.
 *
 * That story is arithmetically uncomfortable and the discomfort is the reason
 * this exists. The budget is `WINDOWS_PER_URL` (12) x `connectionTimeout`, four
 * minutes at the default, against a job whose visible parts sum to well under
 * one: a TCP pre-probe, a NIP-11 document, one twenty-event page, one NEG-OPEN.
 * For the deadline to be the cause, something in there has to be spending
 * minutes, and no unit test can say which — quartz's own idle windows are
 * re-armed by traffic, so the answer is a property of the relay and the moment,
 * not of our code.
 *
 * So this times the four steps SEPARATELY, prints each against the budget it
 * draws on, and then runs the real [FitnessPass] over the same urls so the
 * per-step numbers can be checked against the verdict the pass actually
 * published.
 *
 * ```
 * ./gradlew :monitor:test --tests '*FitnessBudgetLiveProbe*' -DliveBudget=true --rerun -i
 * #  …or urls of your own:
 * #  -DliveBudgetUrls='wss://relay.example,wss://other.example'
 * ```
 *
 * Read-only against everybody it dials: the verdicts it earns are signed into
 * an in-memory store and go nowhere.
 */
class FitnessBudgetLiveProbe {
    private val urls: List<NormalizedRelayUrl> =
        (
            System.getProperty("liveBudgetUrls")
                ?: listOf(
                    // The two the issue is about — the mirror's largest
                    // assertion sources, measured at rtt-read ~11.4s and a
                    // verdict age of 68.4h.
                    "wss://nip85.nosfabrica.com",
                    "wss://nip85-staging.nosfabrica.com",
                    // The middle of the same table: 3.6s and 23.9h.
                    "wss://nip85-staging.relay.tools",
                    // …and the fast end, re-verdicted every few hours.
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
                            // The pass's own ladder shape, first rung only:
                            // every url here answers a bare filter, and a rung
                            // that is not taken is not a cost to attribute.
                            window = runCatching { probe.window(url, anchor, null) { seen++ } }.getOrNull()
                        }
                    leg.firstPageMs = window?.firstPageMs
                    leg.events = seen
                    // THE STEP THIS PROBE EXISTS FOR. Same call, same sliver,
                    // same idle window the pass uses — and deliberately with NO
                    // wall clock around it, so the number printed is how long it
                    // would ACTUALLY have run inside a url's budget.
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

            // …AND THE SAME URLS THROUGH THE REAL PASS, so the per-step numbers
            // above can be held against the verdict actually published and the
            // pass's own funnel line.
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
