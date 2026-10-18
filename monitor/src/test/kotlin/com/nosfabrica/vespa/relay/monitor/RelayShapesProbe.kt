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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * Asks each url every shape of [AliasProbe.SHAPES] through the fitness pass's own probe, then grades
 * it with the real pass, NIP-42 answered. Asserts nothing. `-DshapesProbe=true`; `-DshapesUrls='wss://a,wss://b'`.
 */
class RelayShapesProbe {
    private val urls: List<NormalizedRelayUrl> =
        (
            System.getProperty("shapesUrls")
                ?: "wss://nos.lol,wss://purplepag.es,wss://indexer.coracle.social,wss://groups.hzrd149.com," +
                "wss://haven.laoc.xyz,wss://ngs.spaces.flotilla.social,wss://relay.samizdat.network,wss://strfry.appx.uk," +
                "wss://bucket.coracle.social"
        ).split(",").mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }

    @Test
    fun whichShapesDoesEachRelayAnswer() {
        if (System.getProperty("shapesProbe") != "true") {
            println("[skip] RelayShapesProbe — set -DshapesProbe=true to dial the public internet")
            return
        }
        val okhttp = OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(20)).build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        val signer = NostrSignerInternal(KeyPair())
        val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }
        val probe = AliasProbe.over(client, FitnessPass.FITNESS_TARGET) { IDLE_MS }
        val anchor = RelayConsistency.settledAnchor(System.currentTimeMillis() / 1000)
        println("=".repeat(96))
        println("Every shape the ladder asks, at the fitness anchor, and the grade the pass gives")
        println("=".repeat(96))
        try {
            for (url in urls) {
                println("  ${url.url}")
                for (shape in AliasProbe.SHAPES) {
                    val w = runBlocking { withTimeoutOrNull(PER_URL_MS) { probe.window(url, anchor, shape) {} } }
                    val said =
                        when {
                            w == null -> "cut at ${PER_URL_MS / 1000}s"
                            w.authRefused -> "AUTH REFUSED"
                            w.ids == null -> "silent (${w.reason?.take(60)})"
                            w.ids!!.isNotEmpty() -> "${w.ids!!.size} event(s)"
                            w.refusal != null -> "CLOSED: ${w.refusal!!.take(70)}"
                            else -> "empty EOSE"
                        }
                    println("    ${"%-16s".format(AliasProbe.shapeOf(shape))} $said")
                }
                val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
                runBlocking {
                    withTimeoutOrNull(4 * PER_URL_MS) {
                        FitnessPass(RelayVerdictRecord(store, signer), probe, client, progress = Processors().of("fitness"))
                            .measure("probe", listOf(url), reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)
                    }
                }
                val label =
                    runBlocking { store.query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND))) }
                        .flatMap { it.tags.toList() }
                        .firstOrNull { it.size > 3 && it[0] == "l" && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }
                println("    GRADE            ${label?.let { "${it[1]} — ${it[RelayVerdictRecord.LABEL_EVIDENCE_INDEX]}" } ?: "none written"}")
            }
        } finally {
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(96))
    }

    companion object {
        private const val IDLE_MS = 15_000L

        private const val PER_URL_MS = 60_000L
    }
}
