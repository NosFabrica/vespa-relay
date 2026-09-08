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

import com.nosfabrica.vespa.eventstore.engine.client.EngineResources
import com.nosfabrica.vespa.eventstore.engine.metrics.Activity
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
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
 * The pulse document against a real `CostLedger`, so what is pinned is the join between the
 * store's counters and the page's members. Every assertion is written in the direction its bug
 * would fail: a page drawing nothing looks exactly like a relay doing nothing.
 */
class PulseDocumentTest {
    private fun docOf(
        ledger: CostLedger,
        clientDerived: Boolean = false,
        feed: String? = null,
        held: List<IngestStats.Held> = emptyList(),
        stages: Map<String, IngestStats.Stage> = emptyMap(),
        blocked: Map<String, Map<String, Long>> = emptyMap(),
        headroom: EngineResources.Usage? = null,
    ): JsonObject =
        PulseDocument.of(
            metrics = ledger.snapshot(),
            headroom = headroom,
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

    /**
     * The join `engine` and `activities` never had. Attributing load used to
     * mean an ablation — stop the mirror and watch the number fall.
     */
    @Test
    fun `the document names who made the engine work`() {
        val l = CostLedger()
        l.engineQuery(
            profile = "unranked",
            engineNanos = 9_000_000,
            summaryNanos = 0,
            docsMatched = 90_000_000,
            hitsServed = 10,
            degraded = false,
            activity = Activity.Drain,
            shape = "kinds,authors",
        )
        val row = member(docOf(l), "engineByCaller")?.first()?.jsonObject
        assertNotNull(row, "engineByCaller is missing — the page has nothing to draw")
        assertEquals("Drain", row["activity"]?.jsonPrimitive?.content)
        assertEquals("kinds,authors", row["shape"]?.jsonPrimitive?.content)
        assertEquals(90_000_000, row["docsMatched"]?.jsonPrimitive?.long)
    }

    /** SHAPES, not terms — this page is gated because it quotes searches. */
    @Test
    fun `attribution publishes a shape and never a term`() {
        val l = CostLedger()
        l.engineQuery(
            profile = "search",
            engineNanos = 1,
            summaryNanos = 1,
            docsMatched = 1,
            hitsServed = 1,
            degraded = false,
            activity = Activity.Query,
            shape = "kinds,search,observer",
        )
        val rendered = member(docOf(l), "engineByCaller").toString()
        assertTrue("kinds,search,observer" in rendered, "the shape is what identifies the read")
        assertFalse("bitcoin" in rendered || "npub" in rendered, "no term or key may ride along: $rendered")
    }

    /**
     * The half of the picture the page never had. Two content nodes were lost
     * to OOM while every other panel looked healthy.
     */
    @Test
    fun `the document carries the engine's own headroom, worst node first`() {
        // Built, not parsed: how Vespa's JSON maps onto these readings is the
        // engine's contract and is tested there. This asserts what the PAGE gets.
        val usage =
            EngineResources.Usage(
                nodes =
                    listOf(
                        EngineResources.NodeUsage(host = "vespa-1", memory = 0.72, disk = 0.42, feedBlocked = false),
                        EngineResources.NodeUsage(host = "vespa-0", memory = 0.81, disk = 0.43, feedBlocked = false),
                    ),
                atMillis = 1_060_000L,
            )
        val h = docOf(CostLedger(), headroom = usage)["engineHeadroom"]?.jsonObject
        assertNotNull(h, "engineHeadroom is missing — the page cannot warn about what it cannot see")
        assertEquals(0.81, h["peakMemory"]?.jsonPrimitive?.double)
        assertEquals(false, h["feedBlocked"]?.jsonPrimitive?.boolean)
        val first = h["nodes"]?.jsonArray?.first()?.jsonObject
        assertEquals("vespa-0", first?.get("host")?.jsonPrimitive?.content, "worst node first — it decides whether the cluster feeds")
    }

    private fun member(
        doc: JsonObject,
        name: String,
    ): JsonArray? = doc[name]?.jsonArray

    @Test
    fun `an idle process publishes an identity, not a page of zeros`() {
        val doc = docOf(CostLedger())

        assertEquals(PulseDocument.SCHEMA, doc["schema"]!!.jsonPrimitive.int())
        assertEquals(60L, doc["uptimeSeconds"]!!.jsonPrimitive.long, "one minute between start and now")
        // Absent, not empty: "0 calls" reads as a store answering and returning nothing.
        assertNull(member(doc, "activities"))
        assertNull(member(doc, "engine"))
        assertNull(doc["outcomes"])
        assertNull(doc["locks"])
    }

    @Test
    fun `port calls are grouped under the activity that made them, busiest first`() {
        val ledger = CostLedger()
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
        // A write shape keeps no histogram, and "p99 0.00ms" reads as instant when it means unmeasured.
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
        // Matched alone cannot say how much work the client never saw.
        assertEquals(50L, row["hitsServed"]!!.jsonPrimitive.long)
        assertEquals(40.0, row["engineMs"]!!.jsonPrimitive.double, 1e-9)
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
        // A total wait prompts a question; the wait behind a named holder names a fix.
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
        assertEquals(20_000.0, stages[0].jsonObject["maxMs"]!!.jsonPrimitive.double, 1e-9)
        assertNull(stages[1].jsonObject["calls"], "a stage booked without calls carries none")
        assertNull(stages[1].jsonObject["meanMs"])
    }

    @Test
    fun `the client-derived sections are absent unless the caller asks for them`() {
        val ledger = CostLedger(slowQueryThresholdNanos = 1)
        ledger.byObserver.add("460c25e6", 12)
        ledger.byTerm.add("bitcoin", 30)
        ledger.slowRead(Activity.Query, "trusted", 5_000_000_000, 4_000_000_000, 500_000_000, 50, 4_000, "search 'bitcoin'")

        // Off by default: these name the lenses and terms driving the load and quote the query,
        // and nothing about clients belongs in a public document.
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
    fun `slow reads are published newest first`() {
        val ledger = CostLedger(slowQueryThresholdNanos = 1)
        ledger.slowRead(Activity.Query, "search", 1_000_000_000, 1, 1, 1, 1, "older")
        Thread.sleep(3)
        ledger.slowRead(Activity.Query, "search", 2_000_000_000, 1, 1, 1, 1, "newer")

        val reads = assertNotNull(member(docOf(ledger, clientDerived = true), "slowReads"))

        // The page draws this table in document order under a heading that says "newest first".
        assertEquals(
            listOf("newer", "older"),
            reads.map { it.jsonObject["detail"]!!.jsonPrimitive.content },
        )
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

        // A reader that differences a queue depth between two polls gets nonsense.
        assertEquals(mapOf("trust.pending.subjects" to 4_200L, "feed.inflight" to 3L), gauges)
    }

    private fun kotlinx.serialization.json.JsonPrimitive.int(): Int = content.toInt()
}
