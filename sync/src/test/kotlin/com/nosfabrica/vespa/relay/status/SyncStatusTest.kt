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
package com.nosfabrica.vespa.relay.status

import com.nosfabrica.vespa.relay.progress.StreamPhases
import com.nosfabrica.vespa.relay.progress.SyncProgress
import com.nosfabrica.vespa.relay.progress.VisitLedger
import com.nosfabrica.vespa.relay.sync.SweepState
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The mirror's own status document.
 *
 * Every case here is about the boundary this replaced. What the relay used to
 * do was read three files off a shared volume, re-parse them against an
 * allowlist and re-narrate them; the properties worth pinning are the ones that
 * changed when the writer and the reader became one object:
 *
 *  - the document exists at all before a reader arrives, so the first request
 *    is not the 503 that means "nothing computed yet"
 *  - the progress half is the mirror's OWN document rather than a projection
 *    of it, and carries no heartbeat
 *  - a failure in one half is reported beside the other half, not instead of it
 *  - the series accumulates ACROSS passes, which is the one thing a single tick
 *    cannot state and therefore the only reason this class holds state
 */
class SyncStatusTest {
    private fun status(
        progress: SyncProgress = SyncProgress(),
        snapshot: StatsSnapshot = StatsSnapshot(null),
    ) = Triple(
        SyncStatus(SyncBands(null), SweepState(null), progress, snapshot, everySeconds = 30),
        progress,
        snapshot,
    )

    private fun StatsSnapshot.doc(): JsonObject = assertNotNull(served()).doc

    @Test
    fun `a mirror with nothing walked and nothing running publishes an envelope and no section`() {
        val (status, _, snapshot) = status()

        status.publish(nowSeconds = 1_000)

        val doc = snapshot.doc()
        assertEquals(SyncStatus.SCHEMA_VERSION, doc["schema"]!!.jsonPrimitive.content.toInt())
        // The tier is named and its cadence published, so the page polls on
        // what the document says rather than on a guess compiled into it.
        val tier = doc["tiers"]!!.jsonObject[SyncStatus.TIER]!!.jsonObject
        assertEquals(30L, tier["everySeconds"]!!.jsonPrimitive.long)
        // ABSENT, not empty. A mirror that has walked nothing and published no
        // progress has nothing to say, and a `sync` section carrying zeroes
        // reads as a broken mirror rather than as a new one.
        assertNull(doc["sync"])
        assertEquals(0, (tier["sections"] as JsonArray).size)
    }

    @Test
    fun `the progress half is the mirror's own document, heartbeat and all — which is to say without one`() {
        val (status, progress, snapshot) = status()
        progress.publish(listOf(StreamPhases.Stream("content", "rotating", 12, roster = 3, tails = 1)), nowSeconds = 900)

        status.publish(nowSeconds = 1_000)

        val data = snapshot.doc()["sync"]!!.jsonObject["data"]!!.jsonObject
        val published = data["progress"]!!.jsonObject
        assertEquals("content", (published["streams"] as JsonArray)[0].jsonObject["name"]!!.jsonPrimitive.content)
        // The whole point of the move. `writtenAt`/`staleForSec` existed so a
        // reader in ANOTHER process could tell a quiet mirror from a stopped
        // one; this document is served by the process it describes.
        assertNull(published["writtenAt"])
        assertNull(published["staleForSec"])
        // And the glossary ships with the numbers, so a chip can never describe
        // a member in words the router would not use.
        assertTrue(data["terms"]!!.jsonObject.isNotEmpty())
    }

    @Test
    fun `the per-relay rows reach the page, glossary and all`() {
        // The whole seam, end to end: the pool writes a row, the progress
        // document carries it, and the status document serves it with the
        // definitions for the members it just published — which is what stops
        // a card describing `syncedAt` in words the router would not use.
        val (status, progress, snapshot) = status()
        val row =
            VisitLedger.Row(
                relay = "wss://slow.example/",
                outcome = "refused",
                detail = "The relay ended a walk with nothing delivered",
                syncedAt = 700,
                lastVisitAt = 880,
                lastEventAt = 700,
                events = 0,
                failures = 14,
                onRoster = true,
                tailed = false,
                nextVisitInSec = 240,
                heldForSec = null,
                streams = listOf("content"),
            )
        progress.publish(emptyList(), visits = VisitLedger.Snapshot(listOf(row), omitted = 3), nowSeconds = 900)

        status.publish(nowSeconds = 1_000)

        val data = snapshot.doc()["sync"]!!.jsonObject["data"]!!.jsonObject
        val visits = data["progress"]!!.jsonObject["visits"]!!.jsonObject
        assertEquals("wss://slow.example/", (visits["relays"] as JsonArray)[0].jsonObject["relay"]!!.jsonPrimitive.content)
        assertEquals(3L, visits["omitted"]!!.jsonPrimitive.long, "a bounded list that does not say so reads as the whole answer")
        val terms = data["terms"]!!.jsonObject
        for (member in listOf("visits", "outcome", "syncedAt", "lastEventAt", "onRoster", "failures", "nextVisitInSec")) {
            assertTrue(member in terms, "the document defines the members it carries: $member")
        }
    }

    @Test
    fun `the series accumulates across passes, which is the only state this holds`() {
        val (status, progress, snapshot) = status()
        val health = SyncProgress.Health("ingest", eventsPerSec = 900, heapUsedMb = 1, heapMaxMb = 2, sockets = 5, socketCeiling = 10, servingMs = null)

        progress.publish(emptyList(), health = health, nowSeconds = 1_000)
        status.publish(nowSeconds = 1_000)
        progress.publish(emptyList(), health = health, nowSeconds = 1_060)
        status.publish(nowSeconds = 1_060)

        val series =
            snapshot
                .doc()["sync"]!!
                .jsonObject["data"]!!
                .jsonObject["progress"]!!
                .jsonObject["series"]!!
                .jsonObject
        assertEquals(listOf(1_000L, 1_060L), (series["at"] as JsonArray).map { it.jsonPrimitive.long })
        assertEquals(2, (series["eventsPerSec"] as JsonArray).size)
    }

    @Test
    fun `a pass whose clock has not moved appends nothing, so a republish is not a sample`() {
        val (status, progress, snapshot) = status()
        val health = SyncProgress.Health("mixed", eventsPerSec = 4, heapUsedMb = 1, heapMaxMb = 2, sockets = 5, socketCeiling = 10, servingMs = null)
        progress.publish(emptyList(), health = health, nowSeconds = 1_000)

        status.publish(nowSeconds = 1_000)
        status.publish(nowSeconds = 1_000)

        val series =
            snapshot
                .doc()["sync"]!!
                .jsonObject["data"]!!
                .jsonObject["progress"]!!
                .jsonObject["series"]!!
                .jsonObject
        assertEquals(1, (series["at"] as JsonArray).size, "one rollup, one sample — even when the document is rebuilt")
    }
}
