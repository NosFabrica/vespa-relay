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
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
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
 * What an honest relay's off-filter share is: prints the bare rung, the second page, the
 * verdict [RelayCompliance] would draw, and the mirror's own ask shapes walked two pages each.
 * Asserts nothing. `-DcomplianceProbe=true` selects it; `-DcomplianceUrls=a,b` picks the hosts.
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
        // The fitness pass's own sizing.
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

                // Page two, below where page one ended, is the ask that proves the cursor moved.
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
                    // A relay that answers no window of ours and still serves the mirror is the case worth the dial.
                    for (stream in STREAM_SHAPES) {
                        println("    ${"%-9s".format(stream.label)} ${runBlocking { realOutboxWalk(client, url, stream) }}")
                    }
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

                // The pass publishes the sum of the two rows.
                val both = (ladder?.compliance ?: AliasProbe.Compliance()) + narrow
                println("    VERDICT   ${judge.decide(both)} — ${judge.evidence(both)}")

                // The mirror's asks carry `kinds` under a `since`, which the monitor's ask never has.
                for (shape in SYNC_SHAPES) {
                    println("    ${"%-9s".format(shape.label)} ${runBlocking { syncWalk(client, url, shape) }}")
                }
                // The outbox shape binds authors, a read most relays answer without consulting `until`.
                println("    ${"%-9s".format("outbox k0")} ${runBlocking { outboxWalk(client, url) }}")

                // The older leg's first ask carries both bounds, the only shape that reaches `VisitPool.refusedOutright`.
                println("    ${"%-9s".format("older leg")} ${runBlocking { olderLeg(client, url) }}")

                // The ask the mirror aborts on, through quartz's own paged walk.
                for (stream in STREAM_SHAPES) {
                    println("    ${"%-9s".format(stream.label)} ${runBlocking { realOutboxWalk(client, url, stream) }}")
                }
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

    /** Two pages in the mirror's own shape, `kinds` under a `since`, which [AliasProbe] cannot ask. */
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

    /** The outbox shape: `kinds=[0]` bound to authors taken from the relay's own answer. */
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

    /** One ask carrying both bounds on the first page, as the mirror's older leg sends it. */
    private suspend fun olderLeg(
        client: NostrClient,
        url: NormalizedRelayUrl,
    ): String {
        val now = System.currentTimeMillis() / 1000
        val since = now - 30L * 24 * 60 * 60
        val until = now - 24L * 60 * 60
        val events =
            ask(client, url, Filter(kinds = listOf(1), since = since, until = until, limit = PAGE))
                ?: return "no answer to {kinds=[1], since=$since, until=$until}"
        if (events.isEmpty()) return "empty — the relay holds nothing in a 30d..1d window, or served none of it"
        val above = events.count { it.createdAt > until }
        val below = events.count { it.createdAt < since }
        return "served ${events.size} for a [$since, $until] window: $above above the `until`, $below below the `since`" +
            when {
                above == events.size -> "  <<< quartz matches NONE of them: downloaded=0, End.UNPAGEABLE, the mirror ABORTS"
                above > 0 || below > 0 -> "  <<< partial — some of the answer is outside the window asked for"
                else -> "  — the window was honoured"
            }
    }

    /** `contentViaOutbox`'s real ask, walked by quartz's own `fetchAllPages` so the ending is the mirror's. */
    private suspend fun realOutboxWalk(
        client: NostrClient,
        url: NormalizedRelayUrl,
        stream: StreamShape,
    ): String {
        // No `limit`, because the mirror's leg has none; the short window is what ends the walk.
        val since = (System.currentTimeMillis() / 1000) - OUTBOX_WINDOW_SECONDS
        // One author, as `RosterBuilder.asksOf` sends it; a relay refusing the kinds refuses them for any pubkey.
        val seed = ask(client, url, Filter(kinds = listOf(1), since = since, limit = PAGE))
        val known = seed?.firstOrNull()?.pubKey
        val author = known ?: STRANGER
        val walked =
            withTimeoutOrNull(PER_ASK_MS) {
                client.fetchAllPages(url, listOf(Filter(kinds = stream.kinds, authors = listOf(author), since = since)), IDLE_MS) { }
            } ?: return "the walk did not come back inside ${PER_ASK_MS}ms"
        val aborts =
            walked.downloaded == 0 && walked.end != PagedFetchResult.End.DRAINED &&
                walked.end != PagedFetchResult.End.LIMIT_REACHED
        return "${stream.kinds.size} kinds, ${if (known != null) "1 author it serves" else "1 STRANGER (holds nothing recent)"} " +
            "-> end=${walked.end}, downloaded=${walked.downloaded}" +
            if (aborts) "  <<< the mirror ABORTS (refusedOutright)" else "  — the mirror records a band"
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

    /** One visit stream's ask; a fault that follows the stream is the filter's, one that follows the relay is the relay's. */
    private class StreamShape(
        val label: String,
        val kinds: List<Int>,
    )

    /** One shape of ask the mirror makes. */
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

        /** The mirror's page size, the `limit` being tested. */
        private const val PAGE = 20

        /** A pubkey on every relay that holds anything, for when the relay has nothing recent to take one from. */
        private const val STRANGER = "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d"

        private const val AUTHORS = 3

        private const val OUTBOX_WINDOW_SECONDS = 3600L

        /** The two visit streams, verbatim from `router.conf.example`. */
        private val STREAM_SHAPES by lazy {
            listOf(
                StreamShape("content", OUTBOX_KINDS),
                StreamShape("profile", listOf(0, 10002, 10040)),
            )
        }

        /** `contentViaOutbox`'s kinds, verbatim from `router.conf.example`. */
        private val OUTBOX_KINDS = listOf(0, 1, 5, 9, 11, 14, 20, 21, 22, 24, 40, 41, 42, 54, 62, 1010, 1063, 1065, 1068, 1111, 1163, 1301, 1311, 1312, 1313, 1315, 1337, 1617, 1618, 1621, 1622, 1630, 1631, 1632, 1633, 1808, 1985, 2003, 2004, 2473, 3302, 5050, 5100, 5129, 5250, 5302, 5303, 6969, 8333, 9002, 9041, 9321, 9734, 9735, 9736, 9737, 9802, 10002, 10003, 10009, 10040, 10100, 10154, 11871, 12473, 15128, 15129, 30000, 30001, 30002, 30003, 30004, 30005, 30006, 30009, 30015, 30017, 30018, 30019, 30020, 30023, 30030, 30054, 30055, 30063, 30175, 30176, 30177, 30267, 30296, 30297, 30298, 30311, 30312, 30313, 30315, 30382, 30383, 30384, 30385, 30392, 30393, 30394, 30395, 30402, 30617, 30620, 30817, 30818, 31337, 31871, 31872, 31873, 31890, 31922, 31923, 31924, 31925, 31990, 32267, 33401, 33863, 34139, 34235, 34236, 34550, 35128, 35129, 36787, 38000, 38192, 38383, 39000, 39089, 39092, 39701, 40002, 40100, 45001, 45003, 48106)

        /** The mirror's asks; both carry a `since`. */
        private val SYNC_SHAPES =
            listOf(
                SyncShape("sync k0", listOf(0), 365L * 24 * 60 * 60),
                SyncShape("sync k1", listOf(1), 30L * 24 * 60 * 60),
            )
    }
}
