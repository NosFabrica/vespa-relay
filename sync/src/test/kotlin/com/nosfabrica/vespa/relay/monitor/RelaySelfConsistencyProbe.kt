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
import com.nosfabrica.vespa.relay.shared.RelayVerdictRecord
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
 * Does an OLDER anchor make a relay agree with itself?
 *
 * The reproducibility bar ([RelayAliases.reproducible]) walks a url twice from
 * one anchor and asks how much of the window came back. Today that anchor is
 * [AliasProbe.ANCHOR_LAG_SECONDS] — one minute — which settles indexing lag and
 * nothing else. The open question is whether the hosts that fail it are failing
 * because the top of their window is still moving, or because they genuinely do
 * not answer the same question the same way twice.
 *
 * Those two have opposite consequences for using self-consistency as a RELAY
 * QUALITY gate. If an older anchor fixes them, the bar is measuring our own
 * timing and a deeper anchor is a free upgrade. If it does not, the relay is
 * sharded or shuffling, and "inconsistent" is a fact about its architecture
 * rather than its honesty — which is the whole question behind dropping it.
 *
 * Each host is walked twice per anchor age, from the same anchor, through the
 * same filter, and the containment printed is exactly the arithmetic
 * [RelayAliases.reproducible] applies.
 *
 * OFF by default, asserts NOTHING — it dials other people's servers and a relay
 * being down is not a regression.
 *
 * ```
 * ./gradlew :sync:test --tests '*RelaySelfConsistencyProbe*' -DselfConsistency=true --rerun -i
 * #  …or hosts of your own:
 * #  -DselfConsistencyUrls='wss://relay.example,wss://other.example'
 * ```
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
                // …and what the REAL pass decides at the anchor it actually
                // uses, which is the only line that says whether this relay
                // keeps its place in the fan-out.
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

    /** The fold's own arithmetic: how much of the SMALLER window is in the larger. */
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
        /**
         * How far back each walk is anchored. The first is what the fold uses
         * today; the rest test the premise that a window nothing is still being
         * written into is a window a relay can reproduce.
         */
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
