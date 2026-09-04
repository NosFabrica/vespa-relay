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
 * Where a url's time goes at a relay that refuses our credentials: times the
 * ladder's asks one by one on one socket, and ranks what one `leaderPrint`
 * costs across a corpus. Asserts nothing. Selected by `-DauthRefusalProbe=true`
 * (per-rung timing) or `-DauthRefusalCensus=true` (the ranking); hosts via
 * `-DauthRefusalUrls=a,b`.
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
     * Concurrent behind a small gate, as [AliasFolding] runs it. A census from
     * one IP against hosts already probed measures your own rate limit, so read
     * the shapes and not the totals unless the run is cold.
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
                                // The three outcomes a pass distinguishes are also the three cost classes.
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
        // The router's own NIP-42 wiring; without it quartz derives `pendingOnAuthRequired = false`.
        val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }

        println("=".repeat(78))
        println("The ladder ask by ask, through quartz, at $IDLE_MS ms idle")
        println("=".repeat(78))
        try {
            for (url in urls) {
                println("-".repeat(78))
                println("  ${url.url}")
                // Six asks on one connection, in the pass's order: only the first meets a socket that has never been refused.
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
        /** The live pass's own window. */
        const val IDLE_MS = 20_000L

        /** The rungs [AliasProbe.leaderPrint] walks, in order. */
        val LADDER = listOf(null, AliasProbe.FALLBACK_KINDS, RelayAliases.GROUP_METADATA_KINDS)
    }
}
