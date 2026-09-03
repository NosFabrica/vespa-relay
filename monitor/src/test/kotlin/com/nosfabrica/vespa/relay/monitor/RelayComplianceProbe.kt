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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllWithHooks
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
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
import kotlin.test.Test

/**
 * WHAT AN HONEST RELAY'S OFF-FILTER SHARE ACTUALLY IS — the measurement
 * [RelayCompliance]'s two bars are waiting on.
 *
 * The bars shipped provisional and they say so. This is the instrument that
 * makes them real, and the reason it exists at all is one paragraph in
 * [RelayConsistency.ANCHOR_LAG_SECONDS]: a single run there suggested a
 * constant that a SECOND run disproved, and the constant would have shipped
 * wrong. Run this more than once before believing anything it prints.
 *
 * ## The three questions, and why each needs the network to answer
 *
 * 1. **Is a compliant relay at 0.000, or merely near it?** The whole design
 *    assumes the honest population is at zero and the dishonest one near one,
 *    with nothing in between — the same emptiness the fold's containment bar
 *    rests on. If real relays sit at 0.02 for reasons nobody has thought of —
 *    an inclusive boundary, a replaceable event served late, a proxy merging
 *    subscriptions — then the ten-percent bar is measuring that instead.
 * 2. **Does anything legitimately over-serve the `limit`?** It is published as
 *    a fact and never graded on, on the argument that an over-served event
 *    still matches. This says how common it is, which is what decides whether
 *    that argument was worth making.
 * 3. **Does the second page cost what it is budgeted?** One REQ at
 *    [AliasProbe.COMPLIANCE_LIMIT] events, per url, per sweep. The elapsed
 *    column is that number against a corpus five figures wide.
 * 4. **How many relays honour the anchor and then ignore the cursor?** The
 *    `walk` line is #187 measured directly: 137 of them in one 11-minute
 *    window on staging, every one graded `prime` and `pageable: true` by a
 *    pass that had only ever asked one page.
 *
 * ## What is printed
 *
 * Per url, for the BARE ladder rung and then for the second page, the counters
 * [AliasProbe.Compliance] holds and the verdict [RelayCompliance] would draw
 * from them. The bare rung's `offKind` is ALWAYS zero and that is not a finding
 * — a bare filter constrains no kind. It is printed anyway because the
 * difference between the two rows is the whole argument for the second page
 * carrying a `kinds`.
 *
 * OFF by default, asserts NOTHING — it dials other people's servers and a relay
 * being down is not a regression.
 *
 * ```
 * ./gradlew :monitor:test --tests '*RelayComplianceProbe*' -DcomplianceProbe=true --rerun -i
 * #  …or hosts of your own:
 * #  -DcomplianceUrls='wss://relay.example,wss://other.example'
 * ```
 */
class RelayComplianceProbe {
    private val urls: List<NormalizedRelayUrl> =
        (
            System.getProperty("complianceUrls")
                ?: "wss://nos.lol,wss://nostr.oxtr.dev,wss://relay.lightning.pub," +
                "wss://fiatjaf.com,wss://multiplexer.huszonegy.world,wss://relay.damus.io"
        ).split(",").mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }

    @Test
    fun whatDoesAnHonestRelayScore() {
        if (System.getProperty("complianceProbe") != "true") {
            println("[skip] RelayComplianceProbe — set -DcomplianceProbe=true to dial the public internet")
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
        // The fitness pass's own sizing, because the numbers this prints are
        // meant to be the numbers that pass will see.
        val probe = AliasProbe.over(client, FitnessPass.FITNESS_TARGET) { IDLE_MS }
        val judge = RelayCompliance()
        val anchor = AliasProbe.settledAnchor(System.currentTimeMillis() / 1000)

        println("=".repeat(96))
        println("Filter compliance — what came back against what was asked, anchor ${AliasProbe.ANCHOR_LAG_SECONDS}s back")
        println(
            "bars: refuse at >= ${RelayCompliance.DEFAULT_MIN_OFF_FILTER_EVENTS} off-filter event(s) " +
                "AND >= ${RelayCompliance.DEFAULT_MIN_OFF_FILTER_SHARE} of the answer",
        )
        println(
            "slack on the anchor is ${AliasProbe.WINDOW_SLACK_SECONDS}s and ZERO on page two's cursor; " +
                "page two is kinds=${AliasProbe.FALLBACK_KINDS} limit=${AliasProbe.COMPLIANCE_LIMIT}",
        )
        println("=".repeat(96))
        try {
            for (url in urls) {
                println("-".repeat(96))
                println("  ${url.url}")

                val ladderAt = System.currentTimeMillis()
                val ladder = runBlocking { withTimeoutOrNull(PER_ASK_MS) { probe.window(url, anchor, null) {} } }
                val ladderMs = System.currentTimeMillis() - ladderAt
                if (ladder?.ids == null) {
                    println("    bare      no window came back${ladder?.reason?.let { " ($it)" }.orEmpty()}")
                } else {
                    println("    bare      ${row(judge, ladder.compliance)}  ${ladderMs}ms")
                }

                // PAGE TWO, below where page one ended — the ask that carries
                // a `kinds` and proves the cursor moved at the same time. See
                // [AliasProbe.pageBelow] and #187.
                val floor = ladder?.oldestAt
                val narrowAt = System.currentTimeMillis()
                val narrow =
                    if (floor == null) {
                        null
                    } else {
                        runBlocking { withTimeoutOrNull(PER_ASK_MS) { probe.pageBelow(url, floor - 1, null) {} } }
                    }
                val narrowMs = System.currentTimeMillis() - narrowAt
                if (narrow == null) {
                    println("    page two  nothing to page from, or the ask did not come back inside ${PER_ASK_MS}ms")
                    continue
                }
                val walks =
                    when {
                        narrow.seen == 0 -> "DRAINED — the walk terminates"
                        narrow.offWindow == narrow.seen -> "UNPAGEABLE — every event came back above the cursor"
                        else -> "cursor advanced past $floor"
                    }
                println("    page two  ${row(judge, narrow)}  ${narrowMs}ms")
                println("    walk      $walks")

                // WHAT THE PASS WOULD ACTUALLY PUBLISH, which is the sum of the
                // two and not either row — the ladder is paid for anyway and the
                // second page is what makes `kinds` checkable at all.
                val both = (ladder?.compliance ?: AliasProbe.Compliance()) + narrow
                println("    VERDICT   ${judge.decide(both)} — ${judge.evidence(both)}")

                // …AND THE MIRROR'S OWN ASK, which is not this one.
                //
                // The whole of #187 is a DISAGREEMENT between two components,
                // and a probe that only replays the monitor's side cannot show
                // that the fix closes it. The mirror walks `kinds=[0]` under a
                // `since` — the aborts in the issue are "kinds 0, since
                // 1774853116" and the like — and neither field is in the
                // monitor's ask. A relay that honours the cursor for a bare
                // kind-1 ask and ignores it under a `since` would be graded
                // pageable by the fixed pass and STILL abort the mirror, which
                // is the one outcome that would say the fix is aimed wrong.
                for (shape in SYNC_SHAPES) {
                    println("    ${"%-9s".format(shape.label)} ${runBlocking { syncWalk(client, url, shape) }}")
                }
                // …AND THE SAME ASK WITH ITS AUTHORS BOUND, which is what an
                // OUTBOX stream actually sends and the one shape nothing above
                // reproduces. A kind-0 lookup for named authors is a
                // per-(kind, pubkey) CURRENT-VERSION read on most relays, and
                // that path has no reason to consult `until` at all — see
                // docs/proposals/negentropy-replaced-ids.md, which is about the
                // same replaceable-event lookup seen from the write side.
                println("    ${"%-9s".format("outbox k0")} ${runBlocking { outboxWalk(client, url) }}")
            }
        } finally {
            runCatching { authenticator.destroy() }
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(96))
        println("Run it AGAIN before moving a bar. A score that is not reproducible is not a score —")
        println("see the table in RelayConsistency.ANCHOR_LAG_SECONDS for the run that proved it.")
    }

    /**
     * TWO PAGES IN THE MIRROR'S OWN SHAPE — `kinds` and a `since`, walked the
     * way a sync leg walks, and reported in the mirror's vocabulary.
     *
     * Deliberately NOT routed through [AliasProbe]: the point is to ask what
     * the monitor cannot, so it builds its own filter. `until` steps to the
     * oldest of page one MINUS ONE, which is the step a paged walk makes and
     * the one the abort message is about.
     */
    private suspend fun syncWalk(
        client: NostrClient,
        url: NormalizedRelayUrl,
        shape: SyncShape,
    ): String {
        val since = (System.currentTimeMillis() / 1000) - shape.sinceAgo
        val first =
            ask(client, url, Filter(kinds = shape.kinds, since = since, limit = PAGE))
                ?: return "no answer to {kinds=${shape.kinds}, since=$since, limit=$PAGE}"
        if (first.isEmpty()) return "empty first page — nothing to walk from"
        val floor = first.minOf { it.createdAt }
        val until = floor - 1
        val second =
            ask(client, url, Filter(kinds = shape.kinds, since = since, until = until, limit = PAGE))
                ?: return "page one served ${first.size}, page two never answered"
        if (second.isEmpty()) return "page one served ${first.size}, page two DRAINED — the leg closes"
        val above = second.count { it.createdAt > until }
        return "page one served ${first.size} down to $floor; page two asked <= $until and served ${second.size}, " +
            "$above of them ABOVE it" +
            when {
                above == second.size -> "  <<< the mirror ABORTS here (unpageable)"
                above > 0 -> "  <<< partial — the cursor advances, some events ignore it"
                else -> "  — the cursor advanced"
            }
    }

    /**
     * THE OUTBOX SHAPE: `kinds=[0]` bound to AUTHORS this relay demonstrably
     * holds, walked the same two pages.
     *
     * The authors are taken from the relay's own answer, so the walk cannot
     * fail for want of data — every one of them is a pubkey it just served a
     * profile for. That is what makes page two's answer mean something: a
     * relay re-serving the same kind 0 below its own timestamp is answering a
     * question about the CURRENT version rather than the one that was asked.
     */
    private suspend fun outboxWalk(
        client: NostrClient,
        url: NormalizedRelayUrl,
    ): String {
        val since = (System.currentTimeMillis() / 1000) - 365L * 24 * 60 * 60
        val seed = ask(client, url, Filter(kinds = listOf(0), since = since, limit = PAGE))
        if (seed.isNullOrEmpty()) return "no kind 0 to take authors from"
        val authors = seed.map { it.pubKey }.distinct().take(AUTHORS)
        val first =
            ask(client, url, Filter(kinds = listOf(0), authors = authors, since = since, limit = PAGE))
                ?: return "no answer to the authors-bound ask"
        if (first.isEmpty()) return "authors-bound ask came back empty — the relay just served these pubkeys unbound"
        val floor = first.minOf { it.createdAt }
        val until = floor - 1
        val second =
            ask(client, url, Filter(kinds = listOf(0), authors = authors, since = since, until = until, limit = PAGE))
                ?: return "page one served ${first.size} for ${authors.size} author(s); page two never answered"
        if (second.isEmpty()) return "page one served ${first.size} for ${authors.size} author(s); page two DRAINED"
        val above = second.count { it.createdAt > until }
        return "page one served ${first.size} for ${authors.size} author(s) down to $floor; " +
            "page two asked <= $until and served ${second.size}, $above ABOVE it" +
            when {
                above == second.size -> "  <<< the mirror ABORTS here (unpageable)"
                above > 0 -> "  <<< partial"
                else -> "  — the cursor advanced"
            }
    }

    /** One REQ, the events it produced, or null when the relay never answered. */
    private suspend fun ask(
        client: NostrClient,
        url: NormalizedRelayUrl,
        filter: Filter,
    ): List<Event>? =
        withTimeoutOrNull(PER_ASK_MS) {
            val result = client.fetchAllWithHooks(filters = mapOf(url to listOf(filter)), idleTimeoutMs = IDLE_MS) { _, _ -> true }
            val spoke = result.doneReasons.values.any { !it.startsWith("cannot:") }
            if (spoke) result.events.map { it.second } else null
        }

    /** One shape of ask the mirror actually makes — see [syncWalk]. */
    private class SyncShape(
        val label: String,
        val kinds: List<Int>,
        val sinceAgo: Long,
    )

    private fun row(
        judge: RelayCompliance,
        reading: AliasProbe.Compliance,
    ): String =
        "%3d seen, %3d off-kind, %3d off-window, %3d over-limit -> share %.3f  %s".format(
            reading.seen,
            reading.offKind,
            reading.offWindow,
            reading.overLimit,
            judge.share(reading),
            judge.decide(reading),
        )

    companion object {
        private const val IDLE_MS = 12_000L
        private const val PER_ASK_MS = 45_000L

        /** The mirror's page size for this diagnostic — small, and it is the `limit` being tested. */
        private const val PAGE = 20

        /** How many authors the outbox-shaped ask binds — a handful, as a real ask does. */
        private const val AUTHORS = 3

        /**
         * The mirror's asks, as the issue recorded them: `kinds 0, since …` for
         * the profile streams and `kinds 11` for one of the others. Both carry
         * a `since`, which is the field the monitor's own ask has never had.
         */
        private val SYNC_SHAPES =
            listOf(
                SyncShape("sync k0", listOf(0), 365L * 24 * 60 * 60),
                SyncShape("sync k1", listOf(1), 30L * 24 * 60 * 60),
            )
    }
}
