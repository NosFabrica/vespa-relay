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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.publishAndCollectResults
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
 * STAGES THE ENFORCE-MODE RETRACTION SCENARIO against a live stack, so the
 * one decision that destroys data can be watched making the RIGHT deletion
 * and no other:
 *
 *  1. an ephemeral provider publishes two 30382 scores to a real NIP-77 relay
 *     (`-DenforceProviderRelay`, an operator-chosen relay that accepts writes
 *     and reconciles);
 *  2. a 10040 naming that (provider, relay) pairing goes to the LOCAL relay,
 *     so the gated assertions scan discovers the ask;
 *  3. one PHANTOM score by the same provider goes to the local relay ONLY —
 *     the record "the provider no longer serves", manufactured.
 *
 * The running router must then: certify the provider relay, mirror the two
 * real scores, and — on the ask's first retraction audit — delete exactly the
 * phantom, keep both real scores, and cascade nothing (one of three is not a
 * wholesale retraction). The probe only stages and prints ids; the sync log
 * and a REQ for the ids afterwards are the verdict.
 *
 * OFF by default: it publishes fabricated (tiny, ephemeral-keyed) score
 * events to the relay the property names. Point it only at a relay you are
 * comfortable seeding two addressable events on.
 *
 * ```
 * ./gradlew :sync:test --tests '*EnforceRetractionProbe*' \
 *   -DenforceProbe=true -DenforceProviderRelay=wss://... --rerun -i
 * ```
 */
class EnforceRetractionProbe {
    @Test
    fun stageProviderAndPhantom() {
        if (System.getProperty("enforceProbe") != "true") {
            println("[skip] EnforceRetractionProbe — set -DenforceProbe=true and -DenforceProviderRelay=wss://…")
            return
        }
        val providerRelay =
            RelayUrlNormalizer.normalize(
                requireNotNull(System.getProperty("enforceProviderRelay")) { "set -DenforceProviderRelay" },
            )
        val local = RelayUrlNormalizer.normalize(System.getProperty("enforceLocalRelay") ?: "ws://localhost:7777")
        val okhttp = OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(15)).build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        val user = NostrSignerInternal(KeyPair())
        val provider = NostrSignerInternal(KeyPair())
        try {
            runBlocking {
                val now = System.currentTimeMillis() / 1000

                suspend fun score(
                    subject: String,
                    at: Long,
                ) = provider.sign<Event>(at, 30382, arrayOf(arrayOf("d", subject), arrayOf("rank", "42")), "")

                val real1 = score("subject-alpha", now - 2)
                val real2 = score("subject-beta", now - 1)
                val phantom = score("subject-gone", now)

                suspend fun publish(
                    event: Event,
                    to: com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl,
                ) = client.publishAndCollectResults(event, setOf(to), 15)[to]

                println("=".repeat(78))
                println("provider ${provider.pubKey}")
                println("real1   ${real1.id} -> ${providerRelay.url}: ${publish(real1, providerRelay)}")
                println("real2   ${real2.id} -> ${providerRelay.url}: ${publish(real2, providerRelay)}")
                println("phantom ${phantom.id} -> ${local.url} ONLY: ${publish(phantom, local)}")

                val tenForty =
                    user.sign<Event>(
                        now,
                        10040,
                        arrayOf(arrayOf("30382:rank", provider.pubKey, providerRelay.url)),
                        "",
                    )
                println("10040   ${tenForty.id} -> ${local.url}: ${publish(tenForty, local)}")
                println("expect: audit deletes the phantom, keeps real1/real2, cascades nothing")
                println("=".repeat(78))
            }
        } finally {
            scope.cancel()
        }
    }
}
