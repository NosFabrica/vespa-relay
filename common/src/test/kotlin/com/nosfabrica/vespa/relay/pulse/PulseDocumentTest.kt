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
package com.nosfabrica.vespa.relay.pulse

import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.PortCall
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.double
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The pulse document, against a real `CostLedger` rather than a hand-built
 * snapshot: what this pins is the JOIN between the store's counters and the
 * page's members, and a fake snapshot would pin only this file against itself.
 *
 * Every assertion is written in the direction its bug would fail — a member
 * quietly renamed empties a panel, and a page drawing nothing looks exactly
 * like a relay doing nothing.
 */
class PulseDocumentTest {
    private fun docOf(
        ledger: CostLedger,
        clientDerived: Boolean = false,
        feed: String? = null,
        held: List<IngestStats.Held> = emptyList(),
        stages: Map<String, IngestStats.Stage> = emptyMap(),
        blocked: Map<String, Map<String, Long>> = emptyMap(),
    ): JsonObject =
        PulseDocument.of(
            metrics = ledger.snapshot(),
            feed = feed,
            title = "Eventstore pulse",
            scope = "a test",
            startedAtMillis = 1_000_000L,
            clientDerived = clientDerived,
            nowMillis = 1_060_000L,
            held = held,
            stages = stages,
            blocked = blocked,
        )

    private fun member(
        doc: JsonObject,
        name: String,
    ): JsonArray? = doc[name]?.jsonArray

    @Test
    fun `an idle process publishes an identity, not a page of zeros`() {
        val doc = docOf(CostLedger())

        assertEquals(PulseDocument.SCHEMA, doc["schema"]!!.jsonPrimitive.int())
        assertEquals(60L, doc["uptimeSeconds"]!!.jsonPrimitive.long, "one minute between start and now")
        // Absent, not empty: a page that draws "0 calls" for a store that has
        // done nothing reads as a store that is answering and returning nothing.
        assertNull(member(doc, "activities"))
        assertNull(member(doc, "engine"))
        assertNull(doc["outcomes"])
        assertNull(doc["locks"])
    }

    @Test
    fun `port calls are grouped under the activity that made them, busiest first`() {
        val ledger = CostLedger()
        // A read that is cheap per call, and a bulk write that is not.
        repeat(4) { ledger.port(Activity.Query, PortCall.Search, nanos = 1_000_000, docs = 50) }
        ledger.port(Activity.BatchInsert, PortCall.Put, nanos = 900_000_000, docs = 5_000)
        ledger.port(Activity.BatchInsert, PortCall.Search, nanos = 100_000_000, docs = 5_000)

        val acts = assertNotNull(member(docOf(ledger), "activities"))

        assertEquals(2, acts.size)
        assertEquals("BatchInsert", acts[0].jsonObject["activity"]!!.jsonPrimitive.content, "the expensive activity sorts first")
        val bulk = acts[0].jsonObject
        assertEquals(1_000L, bulk["ms"]!!.jsonPrimitive.long, "the activity's total is the sum of its ports")
        assertEquals(10_000L, bulk["docs"]!!.jsonPrimitive.long)
        val ports = bulk["ports"]!!.jsonArray
        assertEquals("Put", ports[0].jsonObject["call"]!!.jsonPrimitive.content, "and its ports sort the same way")
        assertEquals(
            0.0002,
            ports[0].jsonObject["callsPerDoc"]!!.jsonPrimitive.double,
            1e-9,
            "one put over 5,000 documents — the number the store's own bulk contract is written in",
        )
    }

    @Test
    fun `a call shape with no histogram publishes no percentile rather than a zero one`() {
        val ledger = CostLedger()
        ledger.port(Activity.BatchInsert, PortCall.Put, nanos = 5_000_000, docs = 100)
        ledger.port(Activity.Query, PortCall.Search, nanos = 5_000_000, docs = 10)

        val acts = assertNotNull(member(docOf(ledger), "activities"))
        val byActivity = acts.associate { it.jsonObject["activity"]!!.jsonPrimitive.content to it.jsonObject }

        val put = byActivity.getValue("BatchInsert")["ports"]!!.jsonArray[0].jsonObject
        // THE BUG THIS IS WRITTEN AGAINST. A write shape keeps no histogram,
        // and "p99 0.00ms" on a page reads as instant when it means unmeasured.
        assertNull(put["p99Ms"], "a write shape keeps no histogram and must publish no percentile")
        assertNull(put["measured"])

        val search = byActivity.getValue("Query")["ports"]!!.jsonArray[0].jsonObject
        assertEquals(1L, search["measured"]!!.jsonPrimitive.long)
        assertTrue(search["p99Ms"]!!.jsonPrimitive.double > 0.0, "a read shape does keep one")
    }

    @Test
    fun `outcomes carry the denominator that makes them a rate`() {
        val ledger = CostLedger()
        ledger.outcome(Activity.BatchInsert, CostLedger.ADMITTED, 190)
        ledger.outcome(Activity.BatchInsert, "duplicate", 810)

        val outcomes = assertNotNull(docOf(ledger)["outcomes"]?.jsonObject)

        assertEquals(190L, outcomes["admitted"]!!.jsonPrimitive.long)
        assertEquals(1_000L, outcomes["offered"]!!.jsonPrimitive.long, "offered is every outcome, admitted or not")
        val reasons = outcomes["byActivity"]!!.jsonArray[0].jsonObject["reasons"]!!.jsonArray
        assertEquals("duplicate", reasons[0].jsonObject["reason"]!!.jsonPrimitive.content, "commonest reason first")
        assertEquals(810L, reasons[0].jsonObject["events"]!!.jsonPrimitive.long)
    }

    @Test
    fun `the engine's matched and served are both published, per profile`() {
        val ledger = CostLedger()
        ledger.engineQuery("trusted", engineNanos = 40_000_000, summaryNanos = 10_000_000, docsMatched = 4_000, hitsServed = 50, degraded = false, rungs = 3)

        val row = assertNotNull(member(docOf(ledger), "engine"))[0].jsonObject

        assertEquals("trusted", row["profile"]!!.jsonPrimitive.content)
        assertEquals(4_000L, row["docsMatched"]!!.jsonPrimitive.long)
        // The pair is the point: matched alone cannot say how much work the
        // client never saw, which is what the observer gate moves most.
        assertEquals(50L, row["hitsServed"]!!.jsonPrimitive.long)
        assertEquals(40L, row["engineMs"]!!.jsonPrimitive.long)
        assertEquals(3L, row["rungs"]!!.jsonPrimitive.long)
        assertEquals(0L, row["degraded"]!!.jsonPrimitive.long, "nothing degraded is a reading, not the absence of one")
    }

    @Test
    fun `wait is published with what it was waiting behind`() {
        val doc =
            docOf(
                CostLedger(),
                held = listOf(IngestStats.Held("lock.gate.hold", System.nanoTime(), "derive 500 subject(s)", "trustGate")),
                blocked = mapOf("lock.ingest.wait" to mapOf("derive 500 subject(s)" to 38_000_000_000L, "write" to 2_000_000_000L)),
            )

        val locks = assertNotNull(doc["locks"]?.jsonObject)
        val held = locks["held"]!!.jsonArray[0].jsonObject
        assertEquals("lock.gate.hold", held["stage"]!!.jsonPrimitive.content)
        assertEquals("derive 500 subject(s)", held["doing"]!!.jsonPrimitive.content, "the holder's own sentence, not the lock's name")
        assertEquals("trustGate", held["mutex"]!!.jsonPrimitive.content, "which mutex, which is not the same as the stage label")

        val wait = locks["wait"]!!.jsonArray[0].jsonObject
        assertEquals(40_000L, wait["ms"]!!.jsonPrimitive.long)
        val behind = wait["behind"]!!.jsonArray
        // The whole reason this member exists: `lock.ingest.wait 40s` prompts a
        // question, `38s of it behind derive` names a fix.
        assertEquals("derive 500 subject(s)", behind[0].jsonObject["holder"]!!.jsonPrimitive.content, "the biggest share first")
        assertEquals(38_000L, behind[0].jsonObject["ms"]!!.jsonPrimitive.long)
    }

    @Test
    fun `a stage with no call count publishes no mean over a denominator that does not exist`() {
        val doc =
            docOf(
                CostLedger(),
                stages =
                    mapOf(
                        "proj.fetch.derive" to IngestStats.Stage(totalNanos = 41_000_000_000, calls = 12, maxNanos = 20_000_000_000),
                        // A lock's wait/hold pair: booked from a duration measured elsewhere.
                        "lock.ingest.wait" to IngestStats.Stage(totalNanos = 38_000_000_000, calls = 0, maxNanos = 0),
                    ),
            )

        val stages = assertNotNull(member(doc, "stages"))
        assertEquals("proj.fetch.derive", stages[0].jsonObject["stage"]!!.jsonPrimitive.content, "busiest first")
        assertEquals(12L, stages[0].jsonObject["calls"]!!.jsonPrimitive.long)
        assertNull(stages[1].jsonObject["calls"], "a stage booked without calls carries none")
        assertNull(stages[1].jsonObject["meanMs"])
    }

    @Test
    fun `the client-derived sections are absent unless the caller asks for them`() {
        val ledger = CostLedger(slowQueryThresholdNanos = 1)
        ledger.byObserver.add("460c25e6", 12)
        ledger.byTerm.add("bitcoin", 30)
        ledger.slowRead(Activity.Query, "trusted", 5_000_000_000, 4_000_000_000, 500_000_000, 50, 4_000, "search 'bitcoin'")

        // THE DEFAULT IS THE SAFE ONE. These name which lenses and which terms
        // are driving the load, and quote the query; `/stats.json`'s rule is
        // that nothing about clients belongs in a public document.
        val closed = docOf(ledger, clientDerived = false)
        assertFalse(closed["clientDerived"]!!.jsonPrimitive.boolean)
        assertNull(closed["hotspots"], "the sketches describe this relay's users, not this relay")
        assertNull(closed["slowReads"], "and the slow-read log quotes what they searched for")

        val open = docOf(ledger, clientDerived = true)
        assertTrue(open["clientDerived"]!!.jsonPrimitive.boolean, "said in the document: a build that serves none and a relay nobody searched look alike")
        assertEquals(
            "460c25e6",
            open["hotspots"]!!
                .jsonObject["observers"]!!
                .jsonArray[0]
                .jsonObject["key"]!!
                .jsonPrimitive.content,
        )
        assertEquals(
            "bitcoin",
            open["hotspots"]!!
                .jsonObject["terms"]!!
                .jsonArray[0]
                .jsonObject["key"]!!
                .jsonPrimitive.content,
        )
        assertEquals("search 'bitcoin'", assertNotNull(member(open, "slowReads"))[0].jsonObject["detail"]!!.jsonPrimitive.content)
    }

    @Test
    fun `gauges are published as their own member, never among the counters`() {
        val ledger = CostLedger()
        ledger.gauge("trust.pending.subjects") { 4_200 }
        ledger.gauge("feed.inflight") { 3 }

        val gauges =
            assertNotNull(member(docOf(ledger), "gauges")).associate {
                it.jsonObject["gauge"]!!.jsonPrimitive.content to it.jsonObject["value"]!!.jsonPrimitive.long
            }

        // Apart from every counter on purpose: a reader that differences a
        // queue depth between two polls gets nonsense, and the separation is
        // the only thing stopping it.
        assertEquals(mapOf("trust.pending.subjects" to 4_200L, "feed.inflight" to 3L), gauges)
    }

    private fun kotlinx.serialization.json.JsonPrimitive.int(): Int = content.toInt()
}
