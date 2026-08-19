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
import com.nosfabrica.vespa.relay.progress.Processors
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
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * A WHOLE STABILITY PASS, against the real internet.
 *
 * Every other test of this pass drives a fake [AliasProbe.Page], which can prove
 * the classification is applied and cannot prove the classification is RIGHT:
 * [Silence] matches substrings of text OkHttp and the JDK format, and no unit
 * test can tell us what they actually say. This one runs the real pass over real
 * urls — good, dead, unresolvable and auth-gated — and prints the published
 * partition, so the unrecognised bucket is either empty or naming the strings
 * the table still has to learn.
 *
 * It asserts only what must hold whatever the network does: the partition
 * closes, and the rows sum to `unmeasured`. Which relay answers on a given day
 * is not this repository's business.
 *
 * ```
 * ./gradlew :sync:test --tests '*ConsistencyLivePassProbe*' -DliveConsistency=true --rerun -i
 * #  …or urls of your own:
 * #  -DliveConsistencyUrls='wss://relay.example,wss://other.example'
 * ```
 */
class ConsistencyLivePassProbe {
    private val urls: List<NormalizedRelayUrl> =
        (
            System.getProperty("liveConsistencyUrls")
                ?: listOf(
                    // Answer, and are the fold's own reference hosts.
                    "wss://nos.lol",
                    "wss://nostr.oxtr.dev",
                    "wss://relay.damus.io",
                    // Caps a bare filter, so the kinds rung gets exercised.
                    "wss://purplepag.es",
                    // Measured unstable, twice, at every anchor depth.
                    "wss://fiatjaf.com",
                    // Auth-gated: the ladder must stop on the refusal.
                    "wss://filter.nostr.wine",
                    // A name that cannot resolve, and a port nothing listens on
                    // — the two silences the table is supposed to tell apart.
                    "wss://this-relay-does-not-exist-vespa.example",
                    "wss://localhost:1",
                ).joinToString(",")
        ).split(",").mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }

    @Test
    fun aWholePassOverRealRelays() {
        if (System.getProperty("liveConsistency") != "true") {
            println("[skip] ConsistencyLivePassProbe — set -DliveConsistency=true to dial the public internet")
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
        // Attached for its side effect, exactly as the engine does: without a
        // responder an auth-gated relay reads as a silent one.
        RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
        val processors = Processors()
        val gate =
            ConsistencyPass(
                consistency = RelayConsistency(),
                record = RelayVerdictRecord(store, signer),
                probe = AliasProbe.over(client, RelayAliases.DEFAULT_PROBE_TARGET) { IDLE_MS },
                progress = processors.of("consistency"),
            )

        val started = System.currentTimeMillis()
        try {
            runBlocking { gate.measure("live", urls, canDial = { true }) }
        } finally {
            scope.cancel()
        }
        val work =
            processors
                .snapshot()
                .single()
                .work
                .single()

        println("=".repeat(78))
        println("A live stability pass over ${urls.size} url(s) in ${(System.currentTimeMillis() - started) / 1000}s")
        println("=".repeat(78))
        println("  candidates    ${work.candidates}")
        println("  foldedAway    ${work.foldedAway}")
        println("  consistent    ${work.consistent}")
        println("  inconsistent  ${work.inconsistent}")
        println("  unmeasured    ${work.unmeasured}   (dialled ${work.dialled}, decided ${work.decided})")
        for (row in work.undecided) {
            val under = row.parent?.let { " [under: $it]" } ?: ""
            val top = row.top.joinToString { "${it.host} ${it.urls}" }
            println("    %-42s %3d url(s) on %2d host(s)%s  %s".format(row.reason, row.urls, row.hosts, under, top))
        }

        // THE TWO PROPERTIES THAT DO NOT DEPEND ON THE NETWORK.
        assertEquals(
            work.candidates,
            (work.foldedAway ?: 0) + (work.consistent ?: 0) + (work.inconsistent ?: 0) + work.unmeasured,
            "the partition must close whatever the relays did",
        )
        assertEquals(
            work.unmeasured,
            work.undecided.sumOf { it.urls },
            "every url with no verdict must be under exactly one reason",
        )
        assertEquals(0, work.undecidedOmitted, "no reason may be truncated out of the partition")
    }

    private companion object {
        /** Generous: a live pass is measuring relays, not our patience. */
        const val IDLE_MS = 20_000L
    }
}
