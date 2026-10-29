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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.status.RelayStatusReport
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * An ask a relay refuses as policy is not owed work: after two such refusals it is skipped, counted
 * as unable to run rather than never run, and paged again only once the verdict lapses.
 */
class UnreadableAskTest {
    private val url = RelayUrlNormalizer.normalize("wss://kinds-only.example")
    private val mutes = Filter(kinds = listOf(1984, 10000))
    private val stream = PoolFixture.stream("backfillMutesReports", url, mutes)
    private val year = 31_536_000L

    /** A relay that ends every walk with [end], saying [says] as it does. */
    private class ScriptedRelay(
        private val says: String?,
        @Volatile var end: PagedFetchResult.End,
    ) : RelayComplaints {
        @Volatile private var saidAtMs = 0L

        val answer: suspend (Filter, suspend (Event) -> Unit) -> PagedFetchResult = { _, _ ->
            saidAtMs = System.currentTimeMillis()
            PagedFetchResult(0, end)
        }

        override fun since(
            url: NormalizedRelayUrl,
            sinceMs: Long,
        ): String? = says?.takeIf { end != PagedFetchResult.End.DRAINED && saidAtMs >= sinceMs }
    }

    /** What one visit left: the asks that reached the relay, the tails it opened, and the pair's row. */
    private class Visit(
        val asked: List<Filter>,
        val tails: Map<String, List<Filter>>,
        val units: List<RelayStatusReport.PrimeUnit>,
    )

    /** One whole visit by a fresh pool over [bands], which stand for the state a revisit finds. */
    private fun visit(
        bands: SyncBands,
        relay: ScriptedRelay,
    ): Visit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val fixture = PoolFixture(scope, listOf(stream), relay.answer, bands)
                val pool = fixture.pool(complaints = relay)
                pool.start()
                withTimeout(10_000) {
                    while ((fixture.counts()["visitsRun"] ?: 0) < 1 || pool.primeUnits().any { it.visiting }) delay(10)
                }
                // A skipped ask leaves nothing to wait on; this lets the visit's last steps land.
                delay(200)
                Visit(fixture.asked.toList(), fixture.tails.toMap(), pool.primeUnits())
            } finally {
                scope.cancel()
            }
        }

    private fun refetchRow(bands: SyncBands) =
        AuditSchedule(listOf(stream), bands, null)
            .rows(
                mapOf(url to mapOf(stream.name to RosterBuilder.UnitAsks(listOf(RosterBuilder.Ask(stream, mutes)), setOf(mutes.toJson())))),
                System.currentTimeMillis() / 1000,
            ).getValue(stream.name)
            .single { it.job.startsWith(VisitPool.POOL_REFETCHING) }

    @Test
    fun `the relay's own policy is standing and our load never is`() {
        val closed = PagedFetchResult.End.CLOSED
        val standing =
            listOf(
                "restricted: this relay only serves kind 30078 and 5",
                "blocked: kinds 10000 not allowed",
                "error: bad req: filter validation failed: kind not allowed: 10000",
                "blocked: this index relay does not serve those event kinds",
                "invalid-filter: only wallet kinds are allowed",
                "blocked: invalid query, must have 'h', 'e' or 'a' tag",
                "restricted: members only",
                "auth-required: please authenticate",
            )
        for (said in standing) assertTrue(StandingRefusal.of(closed, said), said)

        val passing =
            listOf(
                "rate-limited: there is a bug in the client, no one should be making so many requests",
                "rate-limited: too many subscriptions",
                "blocked: already 50 subscriptions from this IP",
                "invalid: too many kinds (max 10)",
                "error: the relay could not respond fast enough",
                "error: an event was too large to deliver",
                "error: scraper, /pocket-db/src/lib.rs:782:48",
            )
        for (said in passing) assertFalse(StandingRefusal.of(closed, said), said)
        assertFalse(StandingRefusal.of(closed, null), "a CLOSED with no sentence says nothing about policy")

        assertTrue(StandingRefusal.of(PagedFetchResult.End.AUTH_REQUIRED, null), "our identity, answered and refused")
        for (end in listOf(PagedFetchResult.End.IDLE, PagedFetchResult.End.CANNOT_CONNECT, PagedFetchResult.End.UNPAGEABLE)) {
            assertFalse(StandingRefusal.of(end, "restricted: members only"), "$end is not the sentence's ending")
        }
    }

    @Test
    fun `an ask refused as policy twice is skipped and is not a backlog`() {
        val bands = SyncBands(null, refetchThePastSeconds = year)
        val relay = ScriptedRelay("restricted: this relay only serves kind 30078 and 5", PagedFetchResult.End.CLOSED)

        assertTrue(visit(bands, relay).asked.isNotEmpty())
        assertNull(bands.unreadable(stream.name, url, mutes), "one refusal is not believed")
        assertEquals(1, refetchRow(bands).neverRun)

        assertTrue(visit(bands, relay).asked.isNotEmpty())
        val verdict = assertNotNull(bands.unreadable(stream.name, url, mutes), "the second makes it policy")
        assertEquals("restricted: this relay only serves kind 30078 and 5", verdict.said)

        val third = visit(bands, relay)
        assertEquals(emptyList(), third.asked, "nothing is asked of an unreadable ask")
        assertTrue(third.tails.isEmpty(), "and no tail carries it, since the relay refuses that too")
        assertNotNull(third.units.single().abortReason, "the row still says why the pair holds nothing")
        val row = refetchRow(bands)
        assertEquals(0, row.neverRun, "it is not owed work")
        assertEquals(1, row.cannotRun, "it is counted as unable to run")
    }

    @Test
    fun `refusals about our load and silences never make an ask unreadable`() {
        val bands = SyncBands(null)
        val limited = ScriptedRelay("rate-limited: too many subscriptions", PagedFetchResult.End.CLOSED)
        repeat(3) { assertTrue(visit(bands, limited).asked.isNotEmpty()) }
        assertNull(bands.unreadable(stream.name, url, mutes))

        val quiet = ScriptedRelay(null, PagedFetchResult.End.IDLE)
        repeat(3) { assertTrue(visit(bands, quiet).asked.isNotEmpty()) }
        assertNull(bands.unreadable(stream.name, url, mutes))
    }

    @Test
    fun `a clean read clears the strikes`() {
        val bands = SyncBands(null)
        val relay = ScriptedRelay("blocked: kinds 10000 not allowed", PagedFetchResult.End.CLOSED)
        visit(bands, relay)
        relay.end = PagedFetchResult.End.DRAINED
        visit(bands, relay)
        relay.end = PagedFetchResult.End.CLOSED
        visit(bands, relay)
        assertNull(bands.unreadable(stream.name, url, mutes), "the relay served it in between, so this refusal is the first")
    }

    @Test
    fun `the verdict survives a restart and lapses after a day`() {
        val file = File(Files.createTempDirectory("unreadable").toFile(), "sync-cursors.json")
        val at = System.currentTimeMillis() / 1000
        SyncBands(file).use { bands ->
            repeat(SyncBands.STRIKES_BEFORE_UNREADABLE) { bands.noteUnreadable(stream.name, url, mutes, "the relay closed the subscription", "restricted: members only", at) }
        }
        SyncBands(file).use { bands ->
            assertNotNull(bands.unreadable(stream.name, url, mutes, at), "a restart does not ask twice more to learn it again")
            assertNull(bands.unreadable(stream.name, url, mutes, at + SyncBands.UNREADABLE_TTL_SECONDS), "a policy can change, so it is asked again")
        }
    }
}
