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
 * Stages the enforce-mode retraction scenario on a live stack: two real 30382 scores on a NIP-77
 * relay (`-DenforceProviderRelay`), a 10040 naming that pairing, and one phantom score on the
 * local relay only (`-DenforceLocalRelay`). The running router's first retraction audit should
 * delete exactly the phantom. Prints the ids and asserts nothing. Selected by `-DenforceProbe=true`.
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
                println("expect: audit deletes the phantom, keeps real1/real2")
                println("=".repeat(78))
            }
        } finally {
            scope.cancel()
        }
    }
}
