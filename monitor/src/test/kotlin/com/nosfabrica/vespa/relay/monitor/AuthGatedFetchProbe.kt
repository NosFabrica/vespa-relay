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
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.hasAuthResponder
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
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

/**
 * Whether quartz's derived `pendingOnAuthRequired` default equals the value
 * this router used to pass by hand: fetches the same filter from one relay
 * three ways (explicit true, explicit false, derived) with `hasAuthResponder()`
 * printed beside them. Asserts nothing. Selected by `-DauthGatedProbe=true`;
 * relay via `-DauthGatedUrl=wss://...`.
 */
class AuthGatedFetchProbe {
    private val url: NormalizedRelayUrl =
        RelayUrlNormalizer.normalize(System.getProperty("authGatedUrl") ?: "wss://auth.nostr1.com")

    @Test
    fun doesTheDerivedDefaultMatchTheFlagWeUsedToPass() {
        if (System.getProperty("authGatedProbe") != "true") {
            println("[skip] AuthGatedFetchProbe — set -DauthGatedProbe=true to dial the public internet")
            return
        }
        val okhttp = OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(20)).build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        val signer = NostrSignerInternal(KeyPair())
        // The router's own wiring, and what makes the derived default come out true.
        val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }

        println("=".repeat(78))
        println("${url.url} — hasAuthResponder() = ${client.hasAuthResponder()}")
        println("=".repeat(78))
        try {
            for ((label, pending) in listOf("explicit true" to true, "explicit false" to false, "derived default" to null)) {
                val startedMs = System.currentTimeMillis()
                val result =
                    runBlocking {
                        if (pending == null) {
                            client.fetchAllWithHooks(
                                filters = mapOf(url to listOf(Filter(limit = 100))),
                                idleTimeoutMs = IDLE_MS,
                            ) { _, _ -> true }
                        } else {
                            client.fetchAllWithHooks(
                                filters = mapOf(url to listOf(Filter(limit = 100))),
                                idleTimeoutMs = IDLE_MS,
                                pendingOnAuthRequired = pending,
                            ) { _, _ -> true }
                        }
                    }
                println(
                    "  %-16s %4d event(s) in %5dms   done=%s".format(
                        label,
                        result.events.size,
                        System.currentTimeMillis() - startedMs,
                        result.doneReasons.values
                            .joinToString()
                            .take(60)
                            .ifEmpty { "(none)" },
                    ),
                )
            }
        } finally {
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(78))
    }

    companion object {
        private const val IDLE_MS = 20_000L
    }
}
