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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
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
 * The REAL fold, against REAL relays: what does a pass actually decide, and does
 * it hand its sockets back?
 *
 * Everything else that covers this scripts a relay, so it pins our reading of
 * one. Only this can tell the two apart — and three of the fixes it exercises
 * were found in exactly that gap, where the plumbing was right and the
 * description of the relay was not.
 *
 * What each group is here to show:
 *
 *  - **an unreproducible host** — `fiatjaf.com` serves an arbitrary slice per
 *    REQ whatever limit is asked, so its own window does not survive a second
 *    walk (self 0.638, measured). Expect its paths to FOLD anyway: the guard
 *    gates the negative claim only, because a shuffled window drives containment
 *    down, so a sibling clearing the bar against it cleared it in spite of the
 *    noise. What must never appear here is a signed "these are separate relays"
 *    that would pin both urls in the fan-out for thirty days. This bullet said
 *    the opposite — "expect NOTHING to be published" — until the probe was run
 *    and published two folds.
 *  - **an ordinary polluted host** — the happy path, and the regression this
 *    could break. Expect the fabricated path to fold onto the bare url.
 *
 * Sockets are counted through the same [AliasFolding.Sockets] lease the router
 * passes, so a leaked connection shows up as an outstanding claim rather than as
 * a number nobody looks at.
 *
 * OFF by default, asserts NOTHING — it dials other people's servers, a relay
 * being down is not a regression, and "this host cannot be measured" is a
 * legitimate answer.
 *
 * ```
 * ./gradlew :sync:test --tests '*AliasFoldLiveProbe*' -DliveFoldProbe=true --rerun -i
 * #  …or one group of your own:
 * #  -DliveFoldGroups='wss://relay.example,wss://relay.example/alpha'
 * ```
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
        // The router's own NIP-42 wiring. Without it a relay gating reads behind
        // AUTH serves nothing, which is the failure `pendingOnAuthRequired` was
        // added for — so a probe that omits it cannot see the fix work.
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
                val record = RelayAliasRecord(store, signer)
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
                    object : AliasFolding.Sockets {
                        override fun claim(url: NormalizedRelayUrl) {
                            claims.incrementAndGet()
                            outstanding.merge(url, 1, Int::plus)
                        }

                        override fun release(url: NormalizedRelayUrl) {
                            outstanding.compute(url) { _, n -> ((n ?: 1) - 1).takeIf { it > 0 } }
                            runCatching { client.getOrCreateRelay(url).disconnect() }
                        }
                    }
                // THE NUMBERS THE VERDICT RESTS ON, taken with the same walk the
                // pass uses, because "it folded" does not say whether it folded
                // at 0.99 or scraped over the bar at 0.51 — and the second is a
                // host whose next pass could decide the opposite. The leader is
                // walked TWICE so its self-score is on the same page as the
                // cross-scores: a cross number is only worth what the relay's
                // agreement with itself is worth.
                val probe = AliasProbe.over(client, RelayAliases.DEFAULT_PROBE_TARGET) { IDLE_MS }
                val anchor = AliasProbe.settledAnchor(System.currentTimeMillis() / 1000)
                val leader = aliases.toProbe(group).first()
                val lead = runBlocking { withTimeoutOrNull(PER_GROUP_MS) { probe.leaderPrint(leader, anchor) {} } }
                if (lead == null) {
                    println("    leader ${leader.url} said nothing — no yardstick, nothing on this host can fold")
                } else {
                    val asked = lead.kinds?.let { "kinds=$it" } ?: "bare filter"
                    println("    leader ${leader.url}: ${lead.ids.size} id(s) via $asked")
                    val again = runBlocking { withTimeoutOrNull(PER_GROUP_MS) { probe.fingerprint(leader, anchor, lead.kinds) {} } }
                    println("      vs ITSELF on a second walk: ${containment(lead.ids, again.orEmpty())}")
                    for (url in group.filter { it != leader }) {
                        val print = runBlocking { withTimeoutOrNull(PER_GROUP_MS) { probe.fingerprint(url, anchor, lead.kinds) {} } }
                        println("      vs ${url.url}: ${containment(lead.ids, print.orEmpty())}")
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
     * The fold's own arithmetic, spelled out: how much of the SMALLER window
     * appears in the larger, which is what [RelayAliases.sameRelay] decides on
     * and what [RelayAliases.reproducible] re-uses against a url and itself.
     */
    private fun containment(
        a: Set<String>,
        b: Set<String>,
    ): String {
        val smaller = minOf(a.size, b.size)
        if (smaller == 0) return "nothing came back"
        val shared = if (a.size <= b.size) a.count { it in b } else b.count { it in a }
        val score = shared.toDouble() / smaller
        val bar =
            when {
                smaller < RelayAliases.DEFAULT_MIN_SAMPLE -> "under minSample(${RelayAliases.DEFAULT_MIN_SAMPLE}) — decides nothing"
                score >= RelayAliases.DEFAULT_MIN_SELF_OVERLAP -> "reproducible"
                score >= RelayAliases.DEFAULT_MIN_OVERLAP -> "folds, but under the self bar"
                else -> "below minOverlap(${RelayAliases.DEFAULT_MIN_OVERLAP})"
            }
        return "${b.size} id(s), $shared shared of $smaller -> %.3f  ($bar)".format(score)
    }

    companion object {
        /** The clearnet window the engine passes for a non-Tor url. */
        private const val IDLE_MS = 20_000L

        /**
         * A hard ceiling per group, because a fingerprint of a relay that caps
         * every REQ at ten events is thirty round trips per url and this probe
         * must not hang a build.
         */
        private const val PER_GROUP_MS = 300_000L
    }
}
