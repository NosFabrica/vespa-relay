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
 * Does dropping `pendingOnAuthRequired = true` change anything on the wire?
 *
 * [AliasProbe.over] used to pass it explicitly. Quartz's rework (amethyst #3905,
 * #3906) made the default `hasAuthResponder()` and removed the parameter from
 * every accessory but `fetchAllWithHooks`, on the argument that the correct
 * value is computable from the client — so passing it by hand only creates a
 * way to be wrong later.
 *
 * That argument is only sound here if this router's client really does have a
 * responder, which is a claim about OUR wiring rather than about quartz. This
 * asks the wire instead of the source: the same relay, the same filter, three
 * ways — explicit true, explicit false, and the derived default — with
 * `hasAuthResponder()` printed beside them.
 *
 * Measured against `auth.nostr1.com` on the `1ff1077d58` pin:
 *
 * ```
 * hasAuthResponder() = true
 *   explicit true      91 event(s) in 1485ms   done=eose
 *   explicit false     91 event(s) in  113ms   done=eose
 *   derived default    91 event(s) in  105ms   done=eose
 * ```
 *
 * Two readings, and the second is the one to hold on to. **The derived default
 * is the value this router was hardcoding** — `hasAuthResponder()` is true
 * because `RelayAuthenticator` registers itself — so dropping the explicit flag
 * changed nothing, which is what it was worth running to find out. **And this
 * relay did not exercise the fix at all**: it answered `eose` rather than
 * `auth-required:`, so all three agree and none of them is evidence about the
 * auth path. The 1485ms on the first row is the websocket connect, which the
 * two later rows reuse; reading it as a cost of the flag would be wrong.
 *
 * So this probe pins OUR wiring, not quartz's behaviour. Quartz's own
 * `Nip42AuthGatedFetchTest` drives a relay that really gates, in process.
 *
 * OFF by default, asserts NOTHING — it dials someone else's server, and an
 * allowlist relay declining our ephemeral key is a legitimate answer rather
 * than a regression.
 *
 * ```
 * ./gradlew :sync:test --tests '*AuthGatedFetchProbe*' -DauthGatedProbe=true --rerun -i
 * #  …or a relay of your own: -DauthGatedUrl='wss://relay.example'
 * ```
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
        // The router's own wiring, and the thing under test: this is what is
        // supposed to make the derived default come out true.
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
