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

import com.nosfabrica.vespa.relay.peers.TorSettings
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.peers.probeIdleMs
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * Walks one `.onion` host's urls through a real Tor SOCKS proxy at the clearnet
 * window and at the [probeIdleMs] window, so a group that will not fold can be
 * told apart: too short a window, a relay refusing every fingerprint, or paths
 * that are distinct relays. Asserts nothing. Selected by `-DonionFoldProbe=true`
 * with `-DonionFoldSocks=host:port`; `-DonionFoldUrls=a,b` picks the urls.
 */
class AliasFoldOnionProbe {
    private val urls: List<NormalizedRelayUrl> =
        (
            System.getProperty("onionFoldUrls")
                ?: "ws://sprfrnsx2nhyc6mkkb4hi2hauq7kd3ckudkj3v7djbw4wopqfcotw4id.onion/nexus-glyph," +
                "ws://sprfrnsx2nhyc6mkkb4hi2hauq7kd3ckudkj3v7djbw4wopqfcotw4id.onion/prism-beacon," +
                "ws://sprfrnsx2nhyc6mkkb4hi2hauq7kd3ckudkj3v7djbw4wopqfcotw4id.onion/quebec"
        ).split(",")
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .mapNotNull { RelayUrlNormalizer.normalizeOrNull(it) }

    @Test
    fun reportWhetherTheseOnionUrlsCanFold() {
        if (System.getProperty("onionFoldProbe") != "true") {
            println("[skip] AliasFoldOnionProbe — set -DonionFoldProbe=true to dial a hidden service")
            return
        }
        // `-DonionFoldUrls` is free text; a value normalising to nothing must not reach `urls.first()`.
        if (urls.size < 2) {
            println("[skip] AliasFoldOnionProbe — need at least two urls to compare, got ${urls.size}")
            return
        }
        val socks = System.getProperty("onionFoldSocks") ?: System.getenv("SYNC_TOR_SOCKS") ?: "127.0.0.1:9050"
        val settings = TorSettings.fromEnv(mapOf("SYNC_TOR_SOCKS" to socks))
        if (settings == null) {
            println("[skip] AliasFoldOnionProbe — no SOCKS proxy given")
            return
        }
        // The Tor client is derived from the clearnet one the way the engine derives it.
        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(CLEARNET_TIMEOUT_SEC))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val tor = TorTransport(settings, okhttp)
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { url -> tor.clientFor(url) }, scope)

        println("=".repeat(78))
        println("Can the fold measure these ${urls.size} url(s)? (through ${settings.socksAddress})")
        println("=".repeat(78))
        // With the proxy down every dial fails the way a gone relay does, so ask about the proxy first.
        if (!tor.socksAnswers()) {
            println("  the SOCKS proxy at ${settings.socksAddress} is NOT answering — nothing below would mean anything")
            scope.cancel()
            return
        }
        for (url in urls) println("  ${url.url}   host=${RelayAliases.hostOf(url.url)}")
        val hosts = urls.map { RelayAliases.hostOf(it.url) }.distinct()
        if (hosts.size > 1) {
            println("  NOTE: ${hosts.size} distinct hosts — the fold groups by host, so these are not one group and never fold together")
        }

        val clearnetMs = CLEARNET_TIMEOUT_SEC * 1000L
        val torMs = probeIdleMs(urls.first(), tor, clearnetMs)
        try {
            for (idleMs in listOf(clearnetMs, torMs).distinct()) {
                val label = if (idleMs == clearnetMs) "clearnet window (what the probe used to get)" else "Tor window (probeIdleMs)"
                println("-".repeat(78))
                println("  ${idleMs / 1000}s idle — $label")
                walk(client, idleMs)
            }
        } finally {
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(78))
    }

    /**
     * One fingerprint pass at one window, run the way [AliasFolding.measure] runs
     * it: one anchor for the group, the leader first, then every member against it.
     */
    private fun walk(
        client: NostrClient,
        idleMs: Long,
    ) {
        val aliases = RelayAliases()
        val probe = AliasProbe.over(client, RelayAliases.DEFAULT_PROBE_TARGET) { idleMs }
        val group = aliases.unresolved(urls).firstOrNull()
        if (group == null) {
            println("    nothing to compare — the fold needs two urls on one host")
            return
        }
        val leader = aliases.toProbe(group).first()
        val anchor = AliasProbe.settledAnchor(nowSeconds())
        val prints = LinkedHashMap<NormalizedRelayUrl, Set<String>>()

        val leadAt = System.currentTimeMillis()
        val lead = runBlocking { probe.leaderPrint(leader, anchor) {} }.leader
        val leadMs = System.currentTimeMillis() - leadAt
        val asked = lead?.kinds?.let { "kinds=$it" } ?: "bare filter"
        if (lead == null) {
            println("    leader ${short(leader)} said NOTHING in ${leadMs}ms (bare filter, kinds fallback and group metadata all empty)")
            println("    → no yardstick, so nothing on this host can fold and its members are never dialled by the probe")
            return
        }
        println("    leader ${short(leader)}: ${lead.ids.size} id(s) in ${leadMs}ms, via $asked")
        // Held to the floor for the filter the leader answered, as the real pass does.
        if (!aliases.usableWindow(lead.ids, lead.kinds)) {
            println("    → under the floor for $asked; a window this thin proves nothing either way")
            return
        }
        prints[leader] = lead.ids

        for (url in group.filter { it != leader }) {
            val at = System.currentTimeMillis()
            val print = runBlocking { probe.fingerprint(url, anchor, lead.kinds) {} }
            val ms = System.currentTimeMillis() - at
            if (print == null || print.isEmpty()) {
                println("    ${short(url)}: NOTHING in ${ms}ms — no verdict, stays in the fan-out")
                continue
            }
            prints[url] = print
            // The smaller window's share of the larger, which is what the fold decides on.
            val shared = print.count { it in lead.ids }
            val smaller = minOf(print.size, lead.ids.size)
            val containment = if (smaller == 0) 0.0 else shared.toDouble() / smaller
            println("    ${short(url)}: ${print.size} id(s) in ${ms}ms, $shared shared → containment %.3f".format(containment))
        }

        val learned = aliases.learn(group, leader, prints, lead.kinds)
        println("    verdict: ${learned.folded.size} folded, ${learned.distinct.size} cleared as their own relay")
        for ((alias, canonical) in learned.folded) println("      FOLD ${short(alias)} → ${short(canonical)}")
        for (url in learned.distinct) println("      KEEP ${short(url)}")
        val undecided = group.filter { it !in learned.folded.keys && it !in learned.distinct }
        for (url in undecided) println("      NO VERDICT ${short(url)} — dialled again next cycle, and the one this probe is about")
    }

    /** The path, the only part that differs across one host's urls. */
    private fun short(url: NormalizedRelayUrl): String = "/" + RelayAliases.pathOf(url.url)

    companion object {
        /** `router.conf`'s default connect timeout, kept as the control window. */
        private const val CLEARNET_TIMEOUT_SEC = 20L
    }
}
