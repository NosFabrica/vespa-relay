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
 * Publishes one signed kind-10040 naming a live relay and a dead one, so the `gatedBy` gate
 * and the monitor's 10040 source can be watched on a live stack. Asserts nothing. Run with
 * `-Dseed10040=true`; `-Dseed10040Url` (default localhost) must be a relay you run, since
 * the provider list is fabricated.
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
