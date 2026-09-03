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
 * WHAT AN HONEST RELAY'S OFF-FILTER SHARE ACTUALLY IS — the measurement
 * [RelayCompliance]'s two bars are waiting on.
 *
 * The bars shipped provisional and they say so. This is the instrument that
 * makes them real, and the reason it exists at all is one paragraph in
 * [RelayConsistency.ANCHOR_LAG_SECONDS]: a single run there suggested a
 * constant that a SECOND run disproved, and the constant would have shipped
 * wrong. Run this more than once before believing anything it prints.
 *
 * ## The three questions, and why each needs the network to answer
 *
 * 1. **Is a compliant relay at 0.000, or merely near it?** The whole design
 *    assumes the honest population is at zero and the dishonest one near one,
 *    with nothing in between — the same emptiness the fold's containment bar
 *    rests on. If real relays sit at 0.02 for reasons nobody has thought of —
 *    an inclusive boundary, a replaceable event served late, a proxy merging
 *    subscriptions — then the ten-percent bar is measuring that instead.
 * 2. **Does anything legitimately over-serve the `limit`?** It is published as
 *    a fact and never graded on, on the argument that an over-served event
 *    still matches. This says how common it is, which is what decides whether
 *    that argument was worth making.
 * 3. **Does the narrow ask cost what it is budgeted?** One REQ at
 *    [AliasProbe.COMPLIANCE_LIMIT] events, per url, per sweep. The elapsed
 *    column is that number against a corpus five figures wide.
 *
 * ## What is printed
 *
 * Per url, for the BARE ladder rung and then for the narrow ask, the counters
 * [AliasProbe.Compliance] holds and the verdict [RelayCompliance] would draw
 * from them. The bare rung's `offKind` is ALWAYS zero and that is not a finding
 * — a bare filter constrains no kind. It is printed anyway because the
 * difference between the two rows is the whole argument for the narrow ask
 * existing.
 *
 * OFF by default, asserts NOTHING — it dials other people's servers and a relay
 * being down is not a regression.
 *
 * ```
 * ./gradlew :monitor:test --tests '*RelayComplianceProbe*' -DcomplianceProbe=true --rerun -i
 * #  …or hosts of your own:
 * #  -DcomplianceUrls='wss://relay.example,wss://other.example'
 * ```
 */
class RelayComplianceProbe {
    private val urls: List<NormalizedRelayUrl> =
        (
            System.getProperty("complianceUrls")
                ?: "wss://nos.lol,wss://nostr.oxtr.dev,wss://relay.lightning.pub," +
                "wss://fiatjaf.com,wss://multiplexer.huszonegy.world,wss://relay.damus.io"
        ).split(",").mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }

    @Test
    fun whatDoesAnHonestRelayScore() {
        if (System.getProperty("complianceProbe") != "true") {
            println("[skip] RelayComplianceProbe — set -DcomplianceProbe=true to dial the public internet")
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
        // The fitness pass's own sizing, because the numbers this prints are
        // meant to be the numbers that pass will see.
        val probe = AliasProbe.over(client, FitnessPass.FITNESS_TARGET) { IDLE_MS }
        val judge = RelayCompliance()
        val anchor = AliasProbe.settledAnchor(System.currentTimeMillis() / 1000)

        println("=".repeat(96))
        println("Filter compliance — what came back against what was asked, anchor ${AliasProbe.ANCHOR_LAG_SECONDS}s back")
        println(
            "bars: refuse at >= ${RelayCompliance.DEFAULT_MIN_OFF_FILTER_EVENTS} off-filter event(s) " +
                "AND >= ${RelayCompliance.DEFAULT_MIN_OFF_FILTER_SHARE} of the answer",
        )
        println("slack on `until` is ${AliasProbe.WINDOW_SLACK_SECONDS}s; the narrow ask is kinds=${AliasProbe.FALLBACK_KINDS} limit=${AliasProbe.COMPLIANCE_LIMIT}")
        println("=".repeat(96))
        try {
            for (url in urls) {
                println("-".repeat(96))
                println("  ${url.url}")

                val ladderAt = System.currentTimeMillis()
                val ladder = runBlocking { withTimeoutOrNull(PER_ASK_MS) { probe.window(url, anchor, null) {} } }
                val ladderMs = System.currentTimeMillis() - ladderAt
                if (ladder?.ids == null) {
                    println("    bare      no window came back${ladder?.reason?.let { " ($it)" }.orEmpty()}")
                } else {
                    println("    bare      ${row(judge, ladder.compliance)}  ${ladderMs}ms")
                }

                val narrowAt = System.currentTimeMillis()
                val narrow = runBlocking { withTimeoutOrNull(PER_ASK_MS) { probe.complianceAsk(url, anchor) {} } }
                val narrowMs = System.currentTimeMillis() - narrowAt
                if (narrow == null) {
                    println("    narrow    the ask did not come back inside ${PER_ASK_MS}ms")
                    continue
                }
                println("    narrow    ${row(judge, narrow)}  ${narrowMs}ms")

                // WHAT THE PASS WOULD ACTUALLY PUBLISH, which is the sum of the
                // two and not either row — the ladder is paid for anyway and the
                // narrow ask is what makes `kinds` checkable at all.
                val both = (ladder?.compliance ?: AliasProbe.Compliance()) + narrow
                println("    VERDICT   ${judge.decide(both)} — ${judge.evidence(both)}")
            }
        } finally {
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(96))
        println("Run it AGAIN before moving a bar. A score that is not reproducible is not a score —")
        println("see the table in RelayConsistency.ANCHOR_LAG_SECONDS for the run that proved it.")
    }

    private fun row(
        judge: RelayCompliance,
        reading: AliasProbe.Compliance,
    ): String =
        "%3d seen, %3d off-kind, %3d off-window, %3d over-limit -> share %.3f  %s".format(
            reading.seen,
            reading.offKind,
            reading.offWindow,
            reading.overLimit,
            judge.share(reading),
            judge.decide(reading),
        )

    companion object {
        private const val IDLE_MS = 20_000L
        private const val PER_ASK_MS = 120_000L
    }
}
