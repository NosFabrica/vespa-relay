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

import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllWithHooks
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
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * WHAT QUARTZ SAYS WHEN A RELAY REFUSES A BARE FILTER — the one string the
 * fitness ladder needs and nothing in this repository had measured.
 *
 * `dialVerdict` ends its ladder on any non-null window, so a relay answering the
 * bare rung with `CLOSED blocked: can't handle empty filters` is read as an
 * empty DRAIN and graded `prime` on no evidence at all. Measured once at 46 of
 * 229 hosts — 892 urls — so it is not a corner.
 *
 * Telling that refusal from an honest EOSE needs quartz's terminal reason for
 * the url, and the only place this repository names one is a TEST FAKE
 * (`"closed: blocked: …"`, in `ProbeDeadlineTest`). A fix keyed on a prefix
 * quartz does not actually emit would compile, pass, and do nothing — the shape
 * this session has already been bitten by twice. So this asks real relays and
 * prints the strings verbatim.
 *
 * Two asks per url: a BARE filter, and the same window with `kinds=[1]`. The
 * pair is the finding — a relay that refuses one and serves the other is the
 * population the ladder is failing to measure, and the difference between their
 * two reasons is what the ladder has to key on.
 *
 * OFF by default, asserts NOTHING.
 *
 * ```
 * ./gradlew :monitor:test --tests '*EmptyFilterRefusalProbe*' -DemptyFilterProbe=true --rerun -i
 * #   …or hosts of your own: -DemptyFilterUrls='wss://a.example,wss://b.example'
 * ```
 */
class EmptyFilterRefusalProbe {
    private val urls: List<NormalizedRelayUrl> =
        (
            System.getProperty("emptyFilterUrls")
                ?: "wss://purplepag.es,wss://relay.damus.io,wss://nos.lol,wss://nostr.wine," +
                "wss://relay.nostr.band,wss://relay.primal.net,wss://nostr.mom,wss://relay.snort.social," +
                "wss://nostr.bitcoiner.social,wss://relay.nostr.bg,wss://offchain.pub," +
                "wss://relay.nostrplebs.com,wss://eden.nostr.land,wss://atlas.nostr.land," +
                "wss://relay.noswhere.com,wss://nostr21.com"
        ).split(",").mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }

    @Test
    fun whatDoesARefusalLookLike() {
        if (System.getProperty("emptyFilterProbe") != "true") {
            println("[skip] EmptyFilterRefusalProbe — set -DemptyFilterProbe=true to dial the public internet")
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
        val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }
        val until = AliasProbe.settledAnchor(System.currentTimeMillis() / 1000)

        println("=".repeat(104))
        println("What quartz reports for a BARE filter vs the same window with kinds=[1]")
        println("=".repeat(104))
        try {
            for (url in urls) {
                println("-".repeat(104))
                println("  ${url.url}")
                for ((label, kinds) in listOf("bare" to null, "kinds=[1]" to listOf(1))) {
                    val answer =
                        runBlocking {
                            withTimeoutOrNull(PER_ASK_MS) {
                                val r =
                                    client.fetchAllWithHooks(
                                        filters = mapOf(url to listOf(Filter(limit = 20, until = until, kinds = kinds))),
                                        idleTimeoutMs = IDLE_MS,
                                    ) { _, _ -> true }
                                "${r.events.size} event(s), doneReason=${r.doneReasons[url]?.let { "\"$it\"" } ?: "<none>"}"
                            }
                        } ?: "the ask did not come back inside ${PER_ASK_MS}ms"
                    println("    %-10s %s".format(label, answer))
                }
            }
        } finally {
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(104))
        println("A relay whose BARE row is empty-with-a-reason and whose kinds row carries events is")
        println("the population `dialVerdict` grades `prime` on nothing. The two reason strings are")
        println("what the ladder has to tell apart.")
    }

    companion object {
        private const val IDLE_MS = 12_000L
        private const val PER_ASK_MS = 30_000L
    }
}
