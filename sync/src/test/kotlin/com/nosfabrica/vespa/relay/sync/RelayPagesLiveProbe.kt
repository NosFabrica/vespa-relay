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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.client.reqs.SubscriptionListener
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

/**
 * Dials a busy relay with a live tail open and runs a walk over an empty window beside it, to
 * show the page sampler's connection listener sees the events the walk's `onEvent` never does.
 * Prints its verdict and asserts nothing. Selected by `-DpagesProbe=true`; `-DpagesUrl` picks the relay.
 */
class RelayPagesLiveProbe {
    private val url: NormalizedRelayUrl =
        RelayUrlNormalizer.normalize(System.getProperty("pagesUrl") ?: "wss://relay.damus.io")

    @Test
    fun doesTheSamplerSeeWhatTheWalkCannot() {
        if (System.getProperty("pagesProbe") != "true") {
            println("[skip] RelayPagesLiveProbe — set -DpagesProbe=true to dial the public internet")
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
        val signer = NostrSignerInternal(KeyPair())
        val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }
        val pages = ClientRelayPages(client)
        val now = System.currentTimeMillis() / 1000

        println("=".repeat(90))
        println("RelayPages, wired: does the connection listener see what the walk's onEvent does not?")
        println("  relay ${url.url}")
        println("=".repeat(90))
        try {
            runBlocking {
                // The tail the socket carries while the walk runs, as a visit's does in production.
                val tailed = AtomicInteger()
                client.subscribe(
                    TAIL_SUB,
                    mapOf(url to listOf(Filter(kinds = listOf(1), since = now))),
                    object : SubscriptionListener {
                        override suspend fun onEvent(
                            event: Event,
                            isLive: Boolean,
                            relay: NormalizedRelayUrl,
                            forFilters: List<Filter>?,
                        ) {
                            tailed.incrementAndGet()
                        }
                    },
                )
                delay(TAIL_WARMUP_MS)

                // Stage one: a downloading walk carries events, so a null sample means the seam is wrong.
                val busy = Filter(kinds = listOf(1), since = now - BUSY_WINDOW_SECONDS, until = now)
                val busySampling = pages.arm(url, busy)
                val busyDownloaded =
                    try {
                        client.fetchAllPages(url, listOf(busy), IDLE_MS) { }.downloaded
                    } finally {
                        pages.free(busySampling)
                    }
                val busySample = pages.render(busySampling, busyDownloaded)
                println("  dispatch      a walk that downloaded $busyDownloaded event(s)")
                println("  SAMPLE        ${busySample ?: "nothing — the listener recorded no event"}")
                println(
                    when {
                        busyDownloaded == 0 -> "  …inconclusive: the relay served nothing in that window either"
                        busySample == null -> "  >>> THE SEAM IS WRONG: events crossed the socket and the connection listener saw none"
                        else -> "  >>> the connection listener DOES see events — the seam works"
                    },
                )
                println()

                // Stage two: an empty window drains at once with `downloaded == 0`, the aborting shape
                // without a misbehaving relay.
                val asked = Filter(kinds = listOf(1), since = now - EMPTY_WINDOW_AGO, until = now - EMPTY_WINDOW_AGO + 1)
                val delivered = AtomicInteger()
                // Armed for a production-sized slice first; an empty walk alone drains in milliseconds.
                val sampling = pages.arm(url, asked)
                val before = tailed.get()
                delay(ARMED_WINDOW_MS)
                val walked =
                    try {
                        client.fetchAllPages(url, listOf(asked), IDLE_MS) { delivered.incrementAndGet() }
                    } finally {
                        pages.free(sampling)
                    }
                val duringArmed = tailed.get() - before
                val sample = pages.render(sampling, walked.downloaded)

                println("  tail          carried ${tailed.get()} event(s), $duringArmed of them while the sampler was armed")
                println("  walk          end=${walked.end}, downloaded=${walked.downloaded}, onEvent fired ${delivered.get()} time(s)")
                println("  SAMPLE        ${sample ?: "nothing — the sampler saw no event at all"}")
                println()
                println(
                    when {
                        duringArmed == 0 -> {
                            "  INCONCLUSIVE — the socket carried nothing while the sampler was armed, so there was " +
                                "nothing for the listener to see. Try a busier relay with -DpagesUrl."
                        }

                        sample == null -> {
                            "  THE SAMPLER IS NOT WIRED. $duringArmed event(s) crossed the socket inside the armed " +
                                "window and the listener recorded none of them, which is the one outcome " +
                                "RelayPagesTest could never have caught."
                        }

                        else -> {
                            "  WIRED. The walk downloaded ${walked.downloaded} and the listener still saw the page — " +
                                "which is the whole claim: on an aborting walk the events are there and every " +
                                "instrument downstream of the filter match has already lost them."
                        }
                    },
                )
                client.unsubscribe(TAIL_SUB)
            }
        } finally {
            runCatching { pages.close() }
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(90))
    }

    companion object {
        private const val TAIL_SUB = "pages-probe-tail"
        private const val TAIL_WARMUP_MS = 4_000L

        /** How long the sampler is held armed before the walk. */
        private const val ARMED_WINDOW_MS = 10_000L

        /** A window the relay certainly has kind 1 in. */
        private const val BUSY_WINDOW_SECONDS = 600L
        private const val IDLE_MS = 10_000L

        /** How far back the deliberately-empty window sits: a year, one second wide. */
        private const val EMPTY_WINDOW_AGO = 365L * 24 * 60 * 60
    }
}
