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
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.withPermit
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * **Where the 61 seconds per url actually goes at a relay that refuses our
 * credentials — and whether quartz is the one that has to fix it.**
 *
 * A pass against `filter.nostr.wine` spent 184s on three urls. The first theory
 * was "one `idleTimeoutMs` per rung of [AliasProbe.leaderPrint]'s ladder", and it
 * was WRONG: a single ask on a fresh socket comes back in ~1.5s at every host
 * measured, auth-gated or not. The ladder makes SIX asks, though, and only the
 * first meets a connection that has never been refused. Timed individually, on
 * one connection, in the order the pass uses them:
 *
 * ```
 * filter.nostr.wine  rung 1 bare  limit=500:  1601ms  authRefused=true   auth-refused:auth-required…
 *                    rung 1 bare  limit=100: 20007ms  authRefused=false  reason=null
 *                    rung 2 [1]   limit=500: 20004ms  authRefused=false  reason=null
 *                    rung 3 [39000] …        20004ms  authRefused=false  reason=null
 * buzz.relay.tools   every ask:                 ~85ms authRefused=true   auth-refused:auth-required…
 * nos.lol            every ask:                ~170ms authRefused=false  eose
 * ```
 *
 * 1.6s + 20 + 20 + 20 = the 61s, exactly. **The first ask is answered properly
 * and every one after it is answered with nothing** — no CLOSED, no EOSE, no
 * `doneReason` at all, so [AliasProbe.over] reads it as a url that never spoke
 * and the walk waits out the whole window. Confirmed on the wire: after this
 * relay rejects our AUTH (`OK <id> false restricted: user unauthorized`) it
 * ignores every further REQ on that socket. `buzz.relay.tools` is the contrast —
 * it keeps saying `auth-refused` to every ask, which is why it cost 21s for two
 * urls where this cost 121s.
 *
 * **So quartz needs no change.** It reports the refusal terminally, in machine
 * -readable form, on the first ask: `doneReasons[url]` starts with
 * `auth-refused`, which is exactly what
 * [com.vitorpamplona.quartz.nip01Core.relay.client.accessories.FetchAllResult.authRefused]
 * is derived from, and [AliasProbe.over] already reads that map to tell `cannot:`
 * from a real answer. Waiting out the window on the later asks is the only thing
 * quartz COULD do — the relay sends nothing. The waste is ours: we ask five more
 * times after being told, in a way we can already see, that our credentials were
 * refused. A credential refusal is not a complaint about the filter, so no rung
 * of the ladder can fix it.
 *
 * Off by default and asserts nothing: it dials other people's paid relays, and
 * every answer it can get is a legitimate one.
 *
 * ```
 * ./gradlew :sync:test --tests '*AuthRefusalProbe*' -DauthRefusalProbe=true --rerun -i
 * #  …or hosts of your own:
 * #  -DauthRefusalUrls=wss://a.example,wss://b.example
 * ```
 */
class AuthRefusalProbe {
    private val urls =
        (
            System.getProperty("authRefusalUrls")
                ?: "wss://filter.nostr.wine,wss://buzz.relay.tools," +
                "wss://relay.andotherstuff.org,wss://support.flotilla.social,wss://nos.lol"
        ).split(",")
            .mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }

    /**
     * **Which relays are expensive to FINGERPRINT, ranked, over a real corpus.**
     *
     * The per-rung breakdown above explains one host. This asks the population
     * question instead: with the credential stop in place, what does
     * [AliasProbe.leaderPrint] — the exact call a pass makes, once per url —
     * actually cost at each of a few dozen live urls, and which shapes dominate?
     *
     * Concurrent behind a small gate, the way [AliasFolding] runs it, so the wall
     * clock is not the sum of the slow ones.
     *
     * **A CENSUS RUN FROM ONE IP AGAINST RELAYS YOU HAVE BEEN PROBING MEASURES
     * YOUR OWN RATE LIMIT.** The first run of this ranked 14 of 52 urls as SILENT
     * at ~20s each, 79% of the total cost — and `relay.rodbishop.nz` then
     * answered a follow-up with `cannot:Server Misconfigured. Response: 429 Too
     * Many Requests`, which is not a fact about that relay. `relay.damus.io`
     * appearing silent is the same tell: it had served 500 events to a kinds
     * filter minutes earlier. `chorus.bonsai.com` swung from "21s, served
     * nothing" to "1.1s, 100 events" between two runs.
     *
     * So read the SHAPES here, never the totals, unless the run is cold: fresh
     * IP, no prior sweep of the same hosts, and ideally spread over hours. The
     * shapes are stable and the timings are not.
     *
     * ```
     * ./gradlew :sync:test --tests '*AuthRefusalProbe*' -DauthRefusalCensus=true \
     *   -DauthRefusalUrls='wss://a.example,wss://b.example' --rerun -i
     * ```
     */
    @Test
    fun rankWhatEachUrlCostsToFingerprint() {
        if (System.getProperty("authRefusalCensus") != "true") {
            println("[skip] AuthRefusalProbe census — set -DauthRefusalCensus=true")
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
        val anchor = AliasProbe.settledAnchor(System.currentTimeMillis() / 1000)

        println("=".repeat(78))
        println("What one leaderPrint costs, per url, over ${urls.size} live url(s)")
        println("=".repeat(78))
        val rows = java.util.concurrent.ConcurrentHashMap<String, Triple<Long, String, Int>>()
        try {
            runBlocking {
                val gate = kotlinx.coroutines.sync.Semaphore(8)
                kotlinx.coroutines.coroutineScope {
                    for (url in urls) {
                        launch {
                            gate.withPermit {
                                val startedMs = System.currentTimeMillis()
                                val attempt = runCatching { probe.leaderPrint(url, anchor) {} }.getOrNull()
                                val tookMs = System.currentTimeMillis() - startedMs
                                // The three outcomes a pass distinguishes, which
                                // are also the three cost classes.
                                val shape =
                                    when {
                                        attempt == null -> "threw"
                                        attempt.leader != null -> "window(${attempt.leader!!.ids.size})"
                                        attempt.spoke -> "answered, served nothing"
                                        else -> "SILENT"
                                    }
                                rows[url.url] = Triple(tookMs, shape, attempt?.leader?.ids?.size ?: 0)
                                runCatching { client.getOrCreateRelay(url).disconnect() }
                            }
                        }
                    }
                }
            }
        } finally {
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("-".repeat(78))
        for ((u, row) in rows.entries.sortedByDescending { it.value.first }) {
            println("  ${row.first.toString().padStart(6)}ms  ${row.second.padEnd(26)} $u")
        }
        val byShape = rows.values.groupBy { it.second.substringBefore("(") }
        println("-".repeat(78))
        for ((shape, list) in byShape.entries.sortedByDescending { it.value.sumOf { r -> r.first } }) {
            println(
                "  $shape: ${list.size} url(s), ${list.sumOf { it.first } / 1000}s total, " +
                    "median ${list.map { it.first }.sorted()[list.size / 2]}ms",
            )
        }
        println("=".repeat(78))
    }

    @Test
    fun reportWhatARefusedAuthCostsAndSays() {
        if (System.getProperty("authRefusalProbe") != "true") {
            println("[skip] AuthRefusalProbe — set -DauthRefusalProbe=true to dial the public internet")
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
        // The router's own NIP-42 wiring. Without it quartz derives
        // `pendingOnAuthRequired = false` and the timing below would be a
        // measurement of a client the router never runs.
        val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }

        println("=".repeat(78))
        println("The ladder ask by ask, through quartz, at $IDLE_MS ms idle")
        println("=".repeat(78))
        try {
            for (url in urls) {
                println("-".repeat(78))
                println("  ${url.url}")
                // THE LADDER AS THE PASS ACTUALLY WALKS IT, on ONE connection.
                //
                // A single ask on a fresh socket was the first thing measured
                // here and it came back in ~1.5s at every host — which killed
                // the theory that a rung costs a whole idle window and left the
                // 61s-per-url unexplained. The pass does not make ONE ask: it
                // makes six down the filter ladder, and only the first of them
                // meets a connection that has never been refused. So the asks
                // are timed individually, in the order and on the socket the
                // pass uses them.
                for ((n, kinds) in LADDER.withIndex()) {
                    for (size in listOf(500, RelayAliases.FALLBACK_PROBE_PAGE)) {
                        val startedMs = System.currentTimeMillis()
                        val result =
                            runBlocking {
                                client.fetchAllWithHooks(
                                    filters =
                                        mapOf(
                                            url to
                                                listOf(
                                                    Filter(
                                                        limit = size,
                                                        until = System.currentTimeMillis() / 1000 - 60,
                                                        kinds = kinds,
                                                    ),
                                                ),
                                        ),
                                    idleTimeoutMs = IDLE_MS,
                                ) { _, _ -> true }
                            }
                        val tookMs = System.currentTimeMillis() - startedMs
                        val label = kinds?.toString() ?: "bare"
                        println(
                            "    rung ${n + 1} $label limit=$size: ${tookMs}ms, ${result.events.size} event(s), " +
                                "authRefused=${url in result.authRefused}, reason=${result.doneReasons[url]}",
                        )
                    }
                }
                runCatching { client.getOrCreateRelay(url).disconnect() }
            }
        } finally {
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(78))
    }

    private companion object {
        /** The live probe's window, so these numbers sit beside its 184s. */
        const val IDLE_MS = 20_000L

        /** The rungs [AliasProbe.leaderPrint] walks, in order. */
        val LADDER = listOf(null, AliasProbe.FALLBACK_KINDS, RelayAliases.GROUP_METADATA_KINDS)
    }
}
