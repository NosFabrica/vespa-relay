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
 * SEEDS ONE SIGNED KIND-10040 into a relay under test, so the `resultsFilteredBy`
 * gate and the monitor's 10040 source can be watched doing their jobs on a
 * live stack: the provider list names one real relay (which the monitor
 * should certify) and one dead url (which the gate should hold out of the
 * assertions fan-out forever, at the cost of one monitor probe).
 *
 * OFF by default and pointed at LOCALHOST by default — it publishes a
 * fabricated provider list, which belongs on a relay you run, never a public
 * one. Override the target only with a relay that is yours.
 *
 * ```
 * ./gradlew :sync:test --tests '*Seed10040Probe*' -Dseed10040=true --rerun -i
 * ```
 */
class Seed10040Probe {
    @Test
    fun seedProviderList() {
        if (System.getProperty("seed10040") != "true") {
            println("[skip] Seed10040Probe — set -Dseed10040=true to publish a synthetic 10040 to a LOCAL relay")
            return
        }
        val target = RelayUrlNormalizer.normalize(System.getProperty("seed10040Url") ?: "ws://localhost:7777")
        val okhttp = OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(10)).build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        // Two ephemeral identities: the USER whose 10040 this is, and the
        // PROVIDER the service tags pair each relay with — the same two roles
        // a real provider list separates.
        val user = NostrSignerInternal(KeyPair())
        val provider = NostrSignerInternal(KeyPair())
        try {
            runBlocking {
                val event =
                    user.sign<Event>(
                        System.currentTimeMillis() / 1000,
                        10040,
                        arrayOf(
                            arrayOf("30382:rank", provider.pubKey, "wss://nos.lol"),
                            arrayOf("30382:followers", provider.pubKey, "wss://seed-dead-host.invalid"),
                        ),
                        "",
                    )
                val result = client.publishAndCollectResults(event, setOf(target), 15)[target]
                println("=".repeat(78))
                println("seeded 10040 ${event.id} to ${target.url}: $result")
                println("  provider ${provider.pubKey}")
                println("  relays: wss://nos.lol (should certify), wss://seed-dead-host.invalid (should be held out)")
                println("=".repeat(78))
            }
        } finally {
            scope.cancel()
        }
    }
}
