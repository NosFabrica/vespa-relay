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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.relay.router.TorSettings
import com.nosfabrica.vespa.relay.router.TorTransport
import com.nosfabrica.vespa.relay.router.probeIdleMs
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
 * Why a `.onion` host's urls are still being dialled one per path — asked of the
 * hidden service, through a real Tor proxy, rather than reasoned about.
 *
 * Three urls of one onion host turned up together in the coverage card's
 * *Running now* line, which means the fold had removed none of them. The fold
 * can only remove a url when a fingerprint pass proved something about it, and
 * for a hidden service there are three quite different ways that can fail to
 * happen — indistinguishable from the outside, opposite responses:
 *
 *  1. **the window was too short.** Quartz's `idleTimeoutMs` runs from the start
 *     of the fetch, so it pays for the circuit as well as the answer, and the
 *     probe used to be handed `connectionTimeout` — the number that sizes a
 *     clearnet TCP handshake. A dial that needs longer returns EMPTY, which is
 *     not a verdict, so the group is re-probed and re-dialled forever. That is
 *     what `probeIdleMs` now fixes, and this probe is how the fix is checked
 *     against the relay it was written for.
 *  2. **the relay will not answer a fingerprint at all** — AUTH-gated, refusing
 *     both the bare filter and [AliasProbe.FALLBACK_KINDS], or holding fewer
 *     than [RelayAliases.DEFAULT_MIN_SAMPLE] events. Then no window is usable,
 *     the fold is correct to keep every url, and no timeout will change it.
 *  3. **the paths really are different endpoints.** Then the urls fold onto
 *     nothing because they are not duplicates, and the fan-out is right.
 *
 * It walks every url twice — once at the old clearnet window and once at the
 * budget [probeIdleMs] now hands a Tor-routed url — so the three cases separate
 * on the printout instead of on a theory.
 *
 * OFF by default, dials the real network through the operator's own Tor, and
 * asserts NOTHING: a hidden service being down is not a code regression, and
 * "these are genuinely three relays" is a legitimate answer.
 *
 * ```
 * ./gradlew :sync:test --tests '*AliasFoldOnionProbe*' -DonionFoldProbe=true \
 *   -DonionFoldSocks=127.0.0.1:9050 --rerun -i
 * # …and for a different host:
 * #  -DonionFoldUrls=ws://abc.onion/one,ws://abc.onion/two
 * ```
 *
 * The other half of the question lives in the store and needs no probe: the
 * verdict is a signed kind-30166 record addressed by the url, so ask this relay
 * what it already decided about each one —
 * `["REQ","v",{"kinds":[30166],"authors":["<this relay's pubkey>"],"#d":[<the urls>]}]`.
 * A record whose `same-as` points elsewhere means the fold DID remove the url
 * and the sighting was a leg that outlived the pass which dialled it; a
 * `same-as` pointing at itself means it was measured and kept; no record at all
 * means it was never measured, which is what this probe is for.
 */
class AliasFoldOnionProbe {
    /** The three urls the coverage card showed running at once, unless told otherwise. */
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
        // Before anything is built: `-DonionFoldUrls` is free text, and a value
        // that normalises to nothing would otherwise reach `urls.first()`.
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
        // The router's own clearnet client, so the Tor one is built from it the
        // way the engine builds it — the dispatcher and the connect timeout are
        // what TorTransport replaces, and inheriting the rest is the point.
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
        // A question about US before any about them: with the proxy down every
        // dial fails in a way that looks exactly like the relay being gone.
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
     * One full fingerprint pass at one window, run the way [AliasFolding.measure]
     * runs it: a fresh set of verdicts, ONE anchor shared by the whole group,
     * the leader alone first — it decides the filter and whether to ask the rest
     * at all — and then every member against it.
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
        val lead = runBlocking { probe.leaderPrint(leader, anchor) {} }
        val leadMs = System.currentTimeMillis() - leadAt
        val asked = lead?.kinds?.let { "kinds=$it" } ?: "bare filter"
        if (lead == null) {
            println("    leader ${short(leader)} said NOTHING in ${leadMs}ms (bare filter, kinds fallback and group metadata all empty)")
            println("    → no yardstick, so nothing on this host can fold and its members are never dialled by the probe")
            return
        }
        println("    leader ${short(leader)}: ${lead.ids.size} id(s) in ${leadMs}ms, via $asked")
        // Against the floor for the filter the leader had to be asked, which is
        // what the real pass does — a group-metadata window is held to
        // [RelayAliases.DEFAULT_GROUP_METADATA_MIN_SAMPLE] instead.
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
            // The containment the fold actually decides on: the SMALLER window's
            // share of the larger, which is what survives one side being
            // truncated by the peer's own default limit.
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

    /** The path, which is the only part that differs across one host's urls. */
    private fun short(url: NormalizedRelayUrl): String = "/" + RelayAliases.pathOf(url.url)

    companion object {
        /**
         * `router.conf`'s own default. It is what the probe was handed before
         * [probeIdleMs], and it is kept here as the control: the interesting
         * printout is the one where the two windows disagree.
         */
        private const val CLEARNET_TIMEOUT_SEC = 20L
    }
}
