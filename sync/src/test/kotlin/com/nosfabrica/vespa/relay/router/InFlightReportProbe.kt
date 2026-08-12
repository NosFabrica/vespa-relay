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

import com.nosfabrica.vespa.relay.router.discovery.RelayRotation
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test

/**
 * DOES THE IN-FLIGHT REPORT DESCRIBE WHAT THE LEGS ARE ACTUALLY DOING?
 *
 * Every hermetic test of `RelayRotation.held` feeds it an injected clock and an
 * invented event count, so what they pin is that the numbers are PLUMBED — not
 * that they describe a relay. The distinction is the whole point of the feature:
 * the report exists to tell a relay with a real backlog from a leg that has
 * stopped delivering, and that judgement can only be checked against relays that
 * are really behaving those two ways.
 *
 * So this drives the real path — a real [RelayRotation], `take` / `transferring`
 * / `release` in the shape `DynamicSync.syncOne` uses them, and the real
 * `LegProgress` ticked from the real `fetchAllPages` event callback — against
 * three relays chosen for three different shapes:
 *
 *  - a **firehose** with a deep corpus, which should show `events` climbing and
 *    `quietForSec` pinned near 0 for as long as it runs;
 *  - a **narrow ask** that finishes fast and lets go of its claim;
 *  - a **dead url**, which is where the first run corrected the feature's own
 *    documentation. It was expected to sit on the claim with NO transfer clock;
 *    it reported `transferring 0s` throughout and ended `CANNOT_CONNECT`,
 *    because the websocket connect happens INSIDE the block `transferring`
 *    wraps. So the clock is the transfer SLOT, not the socket, and absent means
 *    "not admitted to the pool" — the guards, or queued behind other legs. Every
 *    place that said otherwise was wrong and now says this instead.
 *
 * The check at the end is the one that matters: for every leg, the count the
 * report published against the count `fetchAllPages` says it delivered. They
 * have to agree, or `events` is decoration.
 *
 * OFF by default and not a gate — it dials the public internet, so it is neither
 * hermetic nor reproducible, and a relay being down is not a code regression. It
 * asserts nothing for the same reason as [RealRelayDrainProbe]: it REPORTS, and
 * a human reads it.
 *
 * ```
 * ./gradlew :sync:test --tests '*InFlightReportProbe*' -DinFlightProbe=true --rerun -i
 * ```
 *
 * `--rerun` is load-bearing — the task is up-to-date-checked, so a second
 * identical run is SKIPPED and prints nothing, which reads as a silent pass.
 */
class InFlightReportProbe {
    /**
     * What each leg is asked, and what shape it is here to produce.
     *
     * The filters are deliberately unbounded below: this probe wants legs that
     * are still running while the reporter looks at them, which is the state the
     * whole feature is about and the one no hermetic test can stage.
     */
    private val legs =
        listOf(
            Leg("wss://directory.yabu.me", Filter(kinds = listOf(0)), "firehose — a deep corpus, should stream"),
            Leg("wss://nos.lol", Filter(kinds = listOf(1), limit = 200), "narrow — should finish and let go"),
            Leg("wss://relay.invalid.nosfabrica.example", Filter(kinds = listOf(1), limit = 10), "dead url — holds a slot while failing to connect"),
        )

    private class Leg(
        val url: String,
        val filter: Filter,
        val shape: String,
    )

    /** What `fetchAllPages` said, to check the report against. */
    private class Truth(
        val result: PagedFetchResult?,
        val thrown: String?,
        val callbackSaw: Long,
    )

    @Test
    fun reportWhetherTheInFlightRowsDescribeTheLegs() {
        if (System.getProperty("inFlightProbe") != "true") {
            println("[skip] InFlightReportProbe — set -DinFlightProbe=true to dial the public internet")
            return
        }
        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        val rotation = RelayRotation()
        val truth = ConcurrentHashMap<String, Truth>()

        println("=".repeat(100))
        println("In-flight rows, read off a live rotation once a second. held/transferring/events/quiet are the")
        println("four numbers `sync.progress.streams[].inFlight` publishes; `-` means the member is ABSENT.")
        println("=".repeat(100))
        try {
            runBlocking {
                val workers =
                    legs.map { leg ->
                        val url = RelayUrlNormalizer.normalize(leg.url)
                        scope.launch {
                            // The claim, exactly as the walk makes it — before
                            // the guards and before any socket, which is what
                            // makes the pre-transfer window observable at all.
                            if (!rotation.take(url)) return@launch
                            val counter = rotation.leg(url)
                            var seen = 0L
                            try {
                                // Stands in for syncOne's guards (strikes, Tor,
                                // the TCP pre-probe): real time on the claim
                                // with no socket open.
                                delay(GUARD_DELAY_MS)
                                var result: PagedFetchResult? = null
                                var thrown: String? = null
                                rotation.transferring(url) {
                                    runCatching {
                                        withTimeoutOrNull(PER_LEG_MS) {
                                            client.fetchAllPages(url, listOf(leg.filter), idleTimeoutMs = 20_000L) { _ ->
                                                // The one line under test, in
                                                // the place DynamicSync puts it.
                                                counter?.received()
                                                seen++
                                            }
                                        }
                                    }.fold(
                                        onSuccess = { result = it },
                                        onFailure = { thrown = "${it::class.simpleName}: ${it.message?.take(60)}" },
                                    )
                                }
                                truth[url.url] = Truth(result, thrown, seen)
                            } finally {
                                rotation.release(url)
                            }
                        }
                    }

                // The reporter: the exact call the progress tick makes.
                val reporter =
                    scope.launch {
                        while (isActive) {
                            val held = rotation.held(System.currentTimeMillis())
                            if (held.relays.isEmpty()) {
                                println("  (nothing held)")
                            } else {
                                for (r in held.relays) {
                                    println(
                                        "  %-42s held %4ds  transferring %6s  events %8d  quiet %4ds".format(
                                            r.relay,
                                            r.heldForSec,
                                            r.transferringForSec?.let { "${it}s" } ?: "-",
                                            r.events,
                                            r.quietForSec,
                                        ),
                                    )
                                }
                                if (held.omitted > 0) println("  …and ${held.omitted} more held, not shown")
                            }
                            println("  " + "-".repeat(96))
                            delay(TICK_MS)
                        }
                    }
                workers.forEach { it.join() }
                reporter.cancel()
            }

            println()
            println("=".repeat(100))
            println("Did `events` describe the leg? Report's count vs what fetchAllPages delivered.")
            println("=".repeat(100))
            for (leg in legs) {
                val url = RelayUrlNormalizer.normalize(leg.url).url
                val t = truth[url]
                val line =
                    when {
                        t == null -> {
                            "no worker completed — the claim was refused or the coroutine died"
                        }

                        t.thrown != null -> {
                            "threw ${t.thrown} after ${t.callbackSaw} callback event(s)"
                        }

                        t.result == null -> {
                            "STILL GOING at ${PER_LEG_MS / 1000}s — ${t.callbackSaw} callback event(s)"
                        }

                        else -> {
                            "${t.result!!.end}: fetchAllPages downloaded=${t.result!!.downloaded}," +
                                " callback saw ${t.callbackSaw}" +
                                if (t.result!!.downloaded.toLong() == t.callbackSaw) " — AGREE" else " — DISAGREE"
                        }
                    }
                println("  %-42s %s".format(url, line))
                println("  %-42s   (%s)".format("", leg.shape))
            }
            println()
            println("A leg that is done no longer appears above: `release` is what removes it, so the last tick")
            println("before a worker returns is the last chance to see it — which is the intended behaviour, since")
            println("the report answers 'what is running now', not 'what ran'.")
        } finally {
            runCatching { client.disconnect() }
            scope.cancel()
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
    }

    private companion object {
        /** Real time on the claim with no socket — the guards, in miniature. */
        const val GUARD_DELAY_MS = 3_000L

        /**
         * A HARD ceiling per leg, which `fetchAllPages` deliberately does not
         * have — see [RealRelayDrainProbe]. Long enough for the firehose to be
         * observed streaming, short enough that the probe ends.
         */
        const val PER_LEG_MS = 45_000L

        /** The progress tick's own cadence. */
        const val TICK_MS = 3_000L
    }
}
