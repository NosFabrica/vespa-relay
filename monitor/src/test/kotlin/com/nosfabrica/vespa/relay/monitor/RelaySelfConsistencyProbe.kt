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
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
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
 * Whether an older anchor makes a relay agree with itself: walks each host twice per anchor age,
 * prints the containment, then what the real [ConsistencyPass] decides. Asserts nothing.
 * `-DselfConsistency=true` selects it; `-DselfConsistencyUrls=a,b` picks the hosts.
 */
class RelaySelfConsistencyProbe {
    private val urls: List<NormalizedRelayUrl> =
        (
            System.getProperty("selfConsistencyUrls")
                ?: "wss://nos.lol,wss://nostr.oxtr.dev,wss://relay.lightning.pub," +
                "wss://fiatjaf.com,wss://multiplexer.huszonegy.world"
        ).split(",").mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }

    @Test
    fun doesAnOlderAnchorBuyConsistency() {
        if (System.getProperty("selfConsistency") != "true") {
            println("[skip] RelaySelfConsistencyProbe — set -DselfConsistency=true to dial the public internet")
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
        val probe = AliasProbe.over(client, RelayAliases.DEFAULT_PROBE_TARGET) { IDLE_MS }
        val now = System.currentTimeMillis() / 1000

        println("=".repeat(78))
        println("Self-consistency by anchor age — two walks, one anchor, same filter")
        println("bar is minSelfOverlap = ${RelayAliases.DEFAULT_MIN_SELF_OVERLAP}")
        println("=".repeat(78))
        try {
            for (url in urls) {
                println("-".repeat(78))
                println("  ${url.url}")
                for ((label, lagSeconds) in ANCHOR_AGES) {
                    val anchor = now - lagSeconds
                    val first =
                        runBlocking { withTimeoutOrNull(PER_WALK_MS) { probe.leaderPrint(url, anchor) {}.leader } }
                    if (first == null || first.ids.isEmpty()) {
                        println("    %-8s no window came back — nothing to compare".format(label))
                        continue
                    }
                    val second =
                        runBlocking { withTimeoutOrNull(PER_WALK_MS) { probe.fingerprint(url, anchor, first.kinds) {} } }
                    val asked = first.kinds?.let { "kinds=$it" } ?: "bare"
                    println("    %-8s %s  (%s)".format(label, containment(first.ids, second.orEmpty()), asked))
                }
                // The real pass at its own anchor, the only line that says whether the relay keeps its place.
                val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
                val consistency = RelayConsistency()
                val pass =
                    ConsistencyPass(
                        consistency = consistency,
                        record = RelayVerdictRecord(store, signer),
                        probe = probe,
                    )
                val decided = runBlocking { withTimeoutOrNull(PER_WALK_MS * 3) { pass.measure("live", listOf(url), canDial = { true }) } }
                val refused = runBlocking { pass.applyVerdicts(listOf(url)) }
                println(
                    "    VERDICT  " +
                        when {
                            decided == null -> "still going — no verdict"
                            decided == 0 -> "unmeasurable, so NOT refused (it stays in the fan-out)"
                            refused.isNotEmpty() -> "REFUSED — removed from the fan-out"
                            else -> "consistent — kept"
                        },
                )
            }
        } finally {
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(78))
    }

    /** How much of the smaller window is in the larger. */
    private fun containment(
        a: Set<String>,
        b: Set<String>,
    ): String {
        val smaller = minOf(a.size, b.size)
        if (smaller == 0) return "second walk came back empty"
        val shared = if (a.size <= b.size) a.count { it in b } else b.count { it in a }
        val score = shared.toDouble() / smaller
        val verdict = if (score >= RelayAliases.DEFAULT_MIN_SELF_OVERLAP) "consistent" else "INCONSISTENT"
        return "%3d + %3d id(s), %3d shared -> %.3f  %s".format(a.size, b.size, shared, score, verdict)
    }

    companion object {
        /** The first is the fold's own anchor; the rest are windows nothing is still being written into. */
        private val ANCHOR_AGES =
            listOf(
                "1min" to AliasProbe.ANCHOR_LAG_SECONDS,
                "1hour" to 60L * 60,
                "1day" to 24L * 60 * 60,
                "7days" to 7L * 24 * 60 * 60,
            )

        private const val IDLE_MS = 20_000L
        private const val PER_WALK_MS = 120_000L
    }
}
