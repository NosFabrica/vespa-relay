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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

/**
 * Dials real relays and prints what a fold pass decides about each host group,
 * the containment numbers the verdict rests on, and whether every socket was
 * handed back. Asserts nothing. Selected by `-DliveFoldProbe=true`;
 * `-DliveFoldGroups='wss://a,wss://a/x;wss://b,wss://b/y'` replaces the groups.
 */
class AliasFoldLiveProbe {
    private val groups: List<List<NormalizedRelayUrl>> =
        (
            System.getProperty("liveFoldGroups")
                ?: "wss://fiatjaf.com,wss://fiatjaf.com/ember,wss://fiatjaf.com/xenon-lima;" +
                "wss://nos.lol,wss://nos.lol/cipher-zulu"
        ).split(";")
            .map { group -> group.split(",").mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) } }
            .filter { it.size > 1 }

    @Test
    fun reportWhatAPassDecidesAgainstRealRelays() {
        if (System.getProperty("liveFoldProbe") != "true") {
            println("[skip] AliasFoldLiveProbe — set -DliveFoldProbe=true to dial the public internet")
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
        val keys = KeyPair()
        val signer = NostrSignerInternal(keys)
        // The router's own NIP-42 wiring; without it an AUTH-gated relay serves nothing.
        val authenticator = RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template)) }

        println("=".repeat(78))
        println("What a real pass decides, and whether it gives its sockets back")
        println("=".repeat(78))
        try {
            for (group in groups) {
                val host = RelayAliases.hostOf(group.first().url)
                println("-".repeat(78))
                println("  $host — ${group.size} url(s)")
                val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
                val record = RelayVerdictRecord(store, signer)
                val aliases = RelayAliases()
                val folding =
                    AliasFolding(
                        aliases = aliases,
                        record = record,
                        probe = AliasProbe.over(client, RelayAliases.DEFAULT_PROBE_TARGET) { IDLE_MS },
                    )
                val outstanding = ConcurrentHashMap<NormalizedRelayUrl, Int>()
                val claims = AtomicInteger()
                val sockets =
                    object : Sockets {
                        override fun claim(url: NormalizedRelayUrl) {
                            claims.incrementAndGet()
                            outstanding.merge(url, 1, Int::plus)
                        }

                        override fun release(url: NormalizedRelayUrl) {
                            outstanding.compute(url) { _, n -> ((n ?: 1) - 1).takeIf { it > 0 } }
                            runCatching { client.getOrCreateRelay(url).disconnect() }
                        }
                    }
                // The same walk the pass makes, so the numbers printed are the ones
                // the verdict rests on. The leader is walked twice so its self-score
                // sits beside the cross-scores.
                val probe = AliasProbe.over(client, RelayAliases.DEFAULT_PROBE_TARGET) { IDLE_MS }
                val anchor = AliasProbe.settledAnchor(System.currentTimeMillis() / 1000)
                val wanted = aliases.toProbe(group)
                var leader = wanted.first()
                var lead: AliasProbe.Leader? = null
                for (candidate in wanted.take(AliasFolding.YARDSTICK_ATTEMPTS)) {
                    leader = candidate
                    lead = runBlocking { withTimeoutOrNull(PER_GROUP_MS) { probe.leaderPrint(candidate, anchor) {}.leader } }
                    if (lead != null) break
                    println("    ${candidate.url} said nothing — trying the next url on the host")
                }
                if (lead == null) {
                    println("    no url on this host could be a yardstick — nothing here can fold")
                } else {
                    val asked = lead.kinds?.let { "kinds=$it" } ?: "bare filter"
                    println("    leader ${leader.url}: ${lead.ids.size} id(s) via $asked")
                    val again = runBlocking { withTimeoutOrNull(PER_GROUP_MS) { probe.fingerprint(leader, anchor, lead.kinds) {} } }
                    println("      vs ITSELF on a second walk: ${containment(lead.ids, again.orEmpty(), lead.kinds)}")
                    // The scheme twin folds on the pairing, not on the containment printed beside it.
                    val twin = aliases.plainTwinIn(group, leader)
                    for (url in group.filter { it != leader }) {
                        val print = runBlocking { withTimeoutOrNull(PER_GROUP_MS) { probe.fingerprint(url, anchor, lead.kinds) {} } }
                        val pairing = if (url == twin) "  [scheme twin — folds on the pairing, not on this number]" else ""
                        println("      vs ${url.url}: ${containment(lead.ids, print.orEmpty(), lead.kinds)}$pairing")
                    }
                }
                val startedMs = System.currentTimeMillis()
                val learned =
                    runBlocking {
                        withTimeoutOrNull(PER_GROUP_MS) {
                            folding.measure("live", group, canDial = { true }, sockets = sockets)
                        }
                    }
                val tookMs = System.currentTimeMillis() - startedMs
                if (learned == null) {
                    println("    STILL GOING at ${PER_GROUP_MS / 1000}s — ${claims.get()} dial(s) so far")
                    continue
                }
                val held = runBlocking { record.load(group) }
                println("    $learned new alias(es) from ${claims.get()} dial(s) in ${tookMs / 1000}s")
                println("    published: ${held.aliases.size} fold(s), ${held.distinct.size} cleared")
                for ((alias, canonical) in held.aliases) println("      FOLD  ${alias.url}  ->  ${canonical.url}")
                for (url in held.distinct) println("      KEEP  ${url.url}")
                if (held.aliases.isEmpty() && held.distinct.isEmpty()) {
                    println("      NOTHING PUBLISHED — see the router: line above for which guard refused")
                }
                println("    sockets: ${claims.get()} claimed, ${outstanding.size} still held at the end")
            }
        } finally {
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(78))
    }

    /**
     * The smaller window's share of the larger, which is what
     * [RelayAliases.sameRelay] and [RelayAliases.reproducible] decide on.
     */
    private fun containment(
        a: Set<String>,
        b: Set<String>,
        /** The filter the windows came through; it sets the floor, see `RelayAliases.foldBar`. */
        kinds: List<Int>?,
    ): String {
        val smaller = minOf(a.size, b.size)
        if (smaller == 0) return "nothing came back"
        val shared = if (a.size <= b.size) a.count { it in b } else b.count { it in a }
        val score = shared.toDouble() / smaller
        // A group list is held to its own floor.
        val floor =
            if (kinds == RelayAliases.GROUP_METADATA_KINDS) {
                RelayAliases.DEFAULT_GROUP_METADATA_MIN_SAMPLE
            } else {
                RelayAliases.DEFAULT_MIN_SAMPLE
            }
        val bar =
            when {
                smaller < floor -> "under minSample($floor) — decides nothing"
                score >= RelayAliases.DEFAULT_MIN_SELF_OVERLAP -> "reproducible"
                score >= RelayAliases.DEFAULT_MIN_OVERLAP -> "folds, but under the self bar"
                else -> "below minOverlap(${RelayAliases.DEFAULT_MIN_OVERLAP})"
            }
        return "${b.size} id(s), $shared shared of $smaller -> %.3f  ($bar)".format(score)
    }

    companion object {
        /** The clearnet window the engine passes for a non-Tor url. */
        private const val IDLE_MS = 20_000L

        /** Per-group ceiling: a relay capping every REQ at ten events must not hang a build. */
        private const val PER_GROUP_MS = 300_000L
    }
}
