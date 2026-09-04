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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.DeleteMissing
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The schedule row: work leaves `waiting` by its clock running out and by
 * nothing else, and the row agrees with the engine's own gate.
 */
class AuditScheduleTest {
    private val a = RelayUrlNormalizer.normalize("wss://a.example")
    private val b = RelayUrlNormalizer.normalize("wss://b.example")
    private val week = 604_800L
    private val now = 1_800_000_000L

    private fun stream(
        name: String,
        period: Long? = week,
        deleteMissing: DeleteMissing = DeleteMissing.OFF,
        ownedKinds: Set<Int> = emptySet(),
        kinds: List<Int> = listOf(1),
    ) = SyncStream(
        name = name,
        dir = SyncDirection.DOWN,
        filter = Filter(kinds = kinds),
        urls = emptyList(),
        trusted = false,
        negentropySyncThePastSeconds = period,
        deleteMissing = deleteMissing,
        ownedKinds = ownedKinds,
    )

    /** A roster of (url → stream → its asks), the shape the pool hands over. */
    private fun roster(vararg entries: Pair<NormalizedRelayUrl, RosterBuilder.Ask>) =
        entries
            .groupBy({ it.first }, { it.second })
            .mapValues { (_, asks) ->
                asks.groupBy { it.stream.name }.mapValues { (_, mine) ->
                    RosterBuilder.UnitAsks(mine, mine.mapTo(mutableSetOf()) { it.filter.toJson() })
                }
            }

    private fun ask(s: SyncStream) = RosterBuilder.Ask(s, s.filter)

    /**
     * The retraction plane, for its owned-kind projection only. Never dialled;
     * the client and the pipeline satisfy the constructor.
     */
    private fun retractionOver(bands: SyncBands): RetractionAudit {
        val scope = CoroutineScope(SupervisorJob())
        val store = NostrSemanticsStore(InMemoryEventIndex())
        return RetractionAudit(
            client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope),
            store = store,
            bands = bands,
            ingest = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 1), null, null, scope, null, null),
            refusedIds = RefusedIds.disabled(),
        )
    }

    @Test
    fun `an ask never audited is due, and counted apart from one whose period elapsed`() {
        // Never run is counted apart from due, so a fresh deployment's storm reads as a schedule not yet started.
        val bands = SyncBands(null)
        val s = stream("content")
        val schedule = AuditSchedule(listOf(s), bands, retraction = null)

        val rows = schedule.rows(roster(a to ask(s), b to ask(s)), now)
        val row = rows["content"]!!.single { it.job == VisitPool.POOL_NEGENTROPY }
        assertEquals(week, row.everySec)
        assertEquals(2, row.neverRun, "nothing has been verified, so both are first passes")
        assertEquals(0, row.due)
        assertEquals(0, row.waiting)
        assertEquals(null, row.nextInSec, "nothing is counting down — everything is due now")

        // The engine's own gate agrees, which makes the row a certificate.
        assertTrue(schedule.isDue(ask(s), a, week, now))
    }

    @Test
    fun `a verified ask waits out its period, then comes due`() {
        val bands = SyncBands(null)
        val s = stream("content")
        val schedule = AuditSchedule(listOf(s), bands, retraction = null)
        // Verified half a period ago on `a`, a full period ago on `b`.
        bands.record(s.name, a, s.filter, null, null, paged = false, reconciledThrough = now - week / 2)
        bands.record(s.name, b, s.filter, null, null, paged = false, reconciledThrough = now - week)

        val row = schedule.rows(roster(a to ask(s), b to ask(s)), now)["content"]!!.single { it.job == VisitPool.POOL_NEGENTROPY }
        assertEquals(0, row.neverRun)
        assertEquals(1, row.due, "the one whose week ran out")
        assertEquals(1, row.waiting, "and the one still inside it")
        assertEquals(week / 2, row.nextInSec, "the countdown is to the SOONEST of the waiting, not an average")

        assertTrue(schedule.isDue(ask(s), b, week, now), "the elapsed one runs")
        assertFalse(schedule.isDue(ask(s), a, week, now), "the other does not, and that is the claim")
    }

    @Test
    fun `a deleteMissing stream is scheduled on the clock its comparison actually stamps`() {
        val bands = SyncBands(null)
        val s = stream("scores", deleteMissing = DeleteMissing.ON, ownedKinds = setOf(30382), kinds = listOf(30382, 10002))
        val schedule = AuditSchedule(listOf(s), bands, retractionOver(bands))

        // Reconciled just now on the owned projection, the only filter the comparison records.
        val owned = s.filter.copy(kinds = listOf(30382))
        bands.record(s.name, a, owned, null, null, paged = false, reconciledThrough = now)

        val row = schedule.rows(roster(a to ask(s)), now)["scores"]!!.single { it.job == VisitPool.POOL_NEGENTROPY }
        assertEquals(0, row.due, "it was just verified — read on the full filter this said 1 forever")
        assertEquals(0, row.neverRun)
        assertEquals(1, row.waiting)
        assertEquals(week, row.nextInSec)
        assertFalse(schedule.isDue(ask(s), a, week, now), "and the engine will not run it either")
    }

    @Test
    fun `an ask the stream owns no kind of is scheduled by nothing, not due forever`() {
        // Compared by nothing, so scheduled by nothing; counted as due it would be a backlog that never drains.
        val bands = SyncBands(null)
        val s = stream("scores", deleteMissing = DeleteMissing.ON, ownedKinds = setOf(30382), kinds = listOf(10002))
        val schedule = AuditSchedule(listOf(s), bands, retractionOver(bands))

        val row = schedule.rows(roster(a to ask(s)), now)["scores"]!!.single { it.job == VisitPool.POOL_NEGENTROPY }
        assertEquals(0, row.due)
        assertEquals(0, row.neverRun)
        assertEquals(0, row.waiting)
        assertEquals(AuditClock.NOT_SCHEDULED, schedule.clockFor(ask(s), a, week))
        assertFalse(schedule.isDue(ask(s), a, week, now))

        // The same for a router with no retraction plane at all.
        val blind = AuditSchedule(listOf(s), bands, retraction = null)
        assertEquals(AuditClock.NOT_SCHEDULED, blind.clockFor(ask(s), a, week))
    }

    @Test
    fun `a stream that schedules neither re-read gets no rows at all`() {
        // A row of zeroes would claim a schedule exists.
        val s = stream("forward", period = null)
        val rows = AuditSchedule(listOf(s), SyncBands(null), retraction = null).rows(roster(a to ask(s)), now)
        assertEquals(emptyList(), rows["forward"])
    }

    @Test
    fun `a stream riding nothing still gets its row, at zero`() {
        // A stream that audits and holds no relays is waiting on the fitness pass; a missing row would say it does not audit.
        val s = stream("content")
        val rows = AuditSchedule(listOf(s), SyncBands(null), retraction = null).rows(emptyMap(), now)
        val row = rows["content"]!!.single()
        assertEquals(VisitPool.POOL_NEGENTROPY, row.job)
        assertEquals(0, row.due)
        assertEquals(0, row.neverRun)
        assertEquals(0, row.waiting)
        assertEquals(null, row.nextInSec)
    }

    @Test
    fun `the three states partition the asks, whatever the mix`() {
        // `due + neverRun + waiting` is the roster's own count.
        val bands = SyncBands(null)
        val s = stream("content")
        val schedule = AuditSchedule(listOf(s), bands, retraction = null)
        bands.record(s.name, a, s.filter, null, null, paged = false, reconciledThrough = now - week)
        val row = schedule.rows(roster(a to ask(s), b to ask(s)), now)["content"]!!.single()
        assertEquals(2, row.due + row.neverRun + row.waiting, "one verified-and-elapsed, one never run")
    }
}
