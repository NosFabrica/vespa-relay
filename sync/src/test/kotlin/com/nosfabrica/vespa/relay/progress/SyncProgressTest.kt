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
package com.nosfabrica.vespa.relay.progress

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * The router's own progress document: counts partition, a zero is published, and a section
 * the router cannot answer is absent, not empty.
 */
class SyncProgressTest {
    @Test
    fun `a router with nothing to report still publishes a document`() {
        // A push-only config has no streams and its page must still draw the fatals and processor rows.
        val doc = SyncProgress.document(emptyList(), nowSeconds = 1_770_000_000)

        assertEquals(0L, doc["fatals"]!!.jsonPrimitive.long)
        assertNull(doc["writtenAt"])
    }

    @Test
    fun `the socket budget publishes what it is doing, not only how big it is`() {
        // A queue is the one symptom of a full dispatcher that a slow store or dead roster cannot produce.
        val healthy =
            SyncProgress.Health(
                bottleneck = "downloads",
                eventsPerSec = 900,
                arrivingPerSec = 950,
                heapUsedMb = 1,
                heapMaxMb = 2,
                sockets = 1010,
                socketCeiling = 1024,
                socketsRunning = 1010,
                socketsQueued = 0,
                servingMs = null,
            )
        val h = SyncProgress.document(emptyList(), health = healthy, nowSeconds = 1_000)["health"]!!.jsonObject
        assertEquals(1010L, h["socketsRunning"]!!.jsonPrimitive.long)
        assertEquals(0L, h["socketsQueued"]!!.jsonPrimitive.long, "nothing waiting is the reading, not the absence of one")
        assertEquals(1024L, h["socketCeiling"]!!.jsonPrimitive.long)
    }

    @Test
    fun `both ends of the ingest queue are published, zero included`() {
        // A drain of zero is the same for a stopped store and a quiet fan-out; the arrival rate tells them apart.
        val stalled =
            SyncProgress.Health(
                bottleneck = "ingest",
                eventsPerSec = 0,
                arrivingPerSec = 4_100,
                heapUsedMb = 1,
                heapMaxMb = 2,
                sockets = 30,
                socketCeiling = 1024,
                socketsRunning = 23,
                socketsQueued = 0,
                servingMs = null,
            )
        val h = SyncProgress.document(emptyList(), health = stalled, nowSeconds = 1_000)["health"]!!.jsonObject
        assertEquals(0L, h["eventsPerSec"]!!.jsonPrimitive.long)
        assertEquals(4_100L, h["arrivingPerSec"]!!.jsonPrimitive.long)

        val quiet =
            SyncProgress.Health(
                bottleneck = "upstream",
                eventsPerSec = 0,
                arrivingPerSec = 0,
                heapUsedMb = 1,
                heapMaxMb = 2,
                sockets = 30,
                socketCeiling = 1024,
                socketsRunning = 23,
                socketsQueued = 0,
                servingMs = null,
            )
        val q = SyncProgress.document(emptyList(), health = quiet, nowSeconds = 1_000)["health"]!!.jsonObject
        assertEquals(0L, q["arrivingPerSec"]!!.jsonPrimitive.long, "nothing arriving is the reading, not a member left out")
    }

    @Test
    fun `a stream that has not started publishes its phase and nothing it cannot answer`() {
        val s = StreamPhases.Stream("content", "starting", 12)
        val stream = (SyncProgress.document(listOf(s), nowSeconds = 1_000)["streams"] as kotlinx.serialization.json.JsonArray)[0].jsonObject

        assertEquals("starting", stream["phase"]!!.jsonPrimitive.content)
        assertEquals(12L, stream["phaseForSec"]!!.jsonPrimitive.long)
        assertNull(stream["roster"], "a stream that has not started rides nothing, and says nothing rather than 0")
    }

    @Test
    fun `a rotating stream publishes what it is riding, zero included`() {
        val riding = StreamPhases.Stream("visits", "rotating", 3_480, roster = 412, tails = 300, queued = 18)
        val waiting = StreamPhases.Stream("cold", "rotating", 3_480, roster = 0, tails = 0, queued = 0)

        val rows = SyncProgress.document(listOf(riding, waiting), nowSeconds = 1_000)["streams"] as kotlinx.serialization.json.JsonArray

        assertEquals(412L, rows[0].jsonObject["roster"]!!.jsonPrimitive.long)
        assertEquals(300L, rows[0].jsonObject["liveHeld"]!!.jsonPrimitive.long)
        // The pool's row publishes the whole queue, which cannot be divided back per stream.
        assertEquals(18L, rows[0].jsonObject["awaitingVisit"]!!.jsonPrimitive.long)
        assertEquals(0L, rows[1].jsonObject["roster"]!!.jsonPrimitive.long, "an empty roster is a report, not an absence")
        assertEquals(0L, rows[1].jsonObject["awaitingVisit"]!!.jsonPrimitive.long, "and so is an empty queue")
    }

    @Test
    fun `a stream row too old to split the queue says nothing rather than zero`() {
        // The page subtracts this to draw the remainder, so an invented zero would misplace the whole queue.
        val row = StreamPhases.Stream("visits", "rotating", 3_480, roster = 412, tails = 300)
        val rows = SyncProgress.document(listOf(row), nowSeconds = 1_000)["streams"] as kotlinx.serialization.json.JsonArray

        assertNull(rows[0].jsonObject["awaitingVisit"])
        assertEquals(412L, rows[0].jsonObject["roster"]!!.jsonPrimitive.long, "everything it does know is still said")
    }

    @Test
    fun `a pass in flight publishes its position instead of a countdown`() {
        // The monitor unsets the due time while a pass runs, so the two are never both present.
        val running =
            Processors.Snapshot(
                name = "consistency",
                phase = "measuring",
                phaseForSec = 400,
                passes = 3,
                lastPassAt = 900,
                lastPassSec = 9_720,
                nextInSec = null,
                measuring =
                    Processors.Measuring(
                        unit = Processors.UNIT_URL,
                        attempted = 604,
                        toProbe = 4_728,
                        etaSec = 2_724,
                        quietForSec = 3,
                    ),
                work = emptyList(),
                counts = emptyList(),
            )

        val row =
            (SyncProgress.document(emptyList(), listOf(running), nowSeconds = 1_000)["processors"] as kotlinx.serialization.json.JsonArray)[0]
                .jsonObject

        assertNull(row["nextInSec"], "a pass takes as long as it takes; nothing has computed when the next one is due")
        val measuring = row["measuring"]!!.jsonObject
        assertEquals("url", measuring["unit"]!!.jsonPrimitive.content)
        assertEquals(604L, measuring["attempted"]!!.jsonPrimitive.long)
        assertEquals(4_728L, measuring["toProbe"]!!.jsonPrimitive.long)
        assertEquals(2_724L, measuring["etaSec"]!!.jsonPrimitive.long)
        // A pass one url from done and one wedged on its last url both say `~0s left`; only this separates them.
        assertEquals(3L, measuring["quietForSec"]!!.jsonPrimitive.long)
    }

    @Test
    fun `nothing is published until the first tick, and then the latest is what the page reads`() {
        val progress = SyncProgress()

        assertNull(progress.latest)

        progress.publish(listOf(streamWith()), nowSeconds = 42)
        val first = assertNotNull(progress.latest)
        assertEquals("content", (first["streams"] as JsonArray)[0].jsonObject["name"]!!.jsonPrimitive.content)

        assertNull(first["writtenAt"])

        // Swapped whole, so a reader never sees half of two documents.
        progress.publish(listOf(streamWith(name = "later")), nowSeconds = 43)
        assertEquals("later", (progress.latest!!["streams"] as JsonArray)[0].jsonObject["name"]!!.jsonPrimitive.content)
    }

    @Test
    fun `the relays in flight are NAMED, not just counted`() {
        val stream =
            (
                SyncProgress.document(
                    listOf(
                        streamWith(
                            InFlight(
                                relays =
                                    listOf(
                                        InFlight.Relay(
                                            "wss://slow.example/",
                                            heldForSec = 41_400,
                                            transferringForSec = 41_390,
                                            events = 2,
                                            quietForSec = 41_000,
                                            stage = "paging",
                                            pool = "catching-up",
                                            pagingUntil = 1_689_857_148L,
                                        ),
                                        InFlight.Relay("wss://probing.example/", heldForSec = 4, transferringForSec = null, events = 0, quietForSec = 4),
                                    ),
                                omitted = 118,
                            ),
                        ),
                    ),
                    nowSeconds = 1_000,
                )["streams"] as kotlinx.serialization.json.JsonArray
            )[0].jsonObject

        val rows = stream["inFlight"]!!.jsonObject["relays"] as kotlinx.serialization.json.JsonArray
        assertEquals("wss://slow.example/", rows[0].jsonObject["relay"]!!.jsonPrimitive.content)
        assertEquals(2L, rows[0].jsonObject["events"]!!.jsonPrimitive.long)
        assertEquals(41_000L, rows[0].jsonObject["quietForSec"]!!.jsonPrimitive.long)
        assertNull(
            rows[1].jsonObject["transferringForSec"],
            "absent means NOT on a socket — a 0 would read as a transfer that just started",
        )
        assertEquals(
            1_689_857_148L,
            rows[0].jsonObject["pagingUntil"]!!.jsonPrimitive.long,
            "where the paged cursor is, so a backlog and a stalled walk stop being the same row",
        )
        assertNull(
            rows[1].jsonObject["pagingUntil"],
            "absent means NOT paging — a leg still in the guards has no cursor to report",
        )
        assertEquals(118, stream["inFlight"]!!.jsonObject["omitted"]!!.jsonPrimitive.int, "the cap discloses itself")
        assertEquals(
            "catching-up",
            rows[0].jsonObject["pool"]!!.jsonPrimitive.content,
            "the word a reader groups by, beside the sentence a reader reads",
        )
        assertNull(
            rows[1].jsonObject["pool"],
            "a row in none of the four says nothing rather than claiming one — it is drawn under its own `doing`",
        )
    }

    @Test
    fun `the live pool is named too, at the root, and only when something is tailed`() {
        val live =
            InFlight(
                relays =
                    listOf(
                        InFlight.Relay(
                            "wss://nos.lol/",
                            heldForSec = 41_400,
                            transferringForSec = 41_400,
                            events = 91_002,
                            quietForSec = 3,
                            stage = "holding a live tail",
                            pool = "live",
                        ),
                    ),
                omitted = 0,
            )
        val doc = SyncProgress.document(listOf(streamWith(name = "content")), live = live, nowSeconds = 1_000)

        // At the root: a tail counts arrivals at the url, so per-stream copies would each carry the whole count.
        val rows = doc["live"]!!.jsonObject["relays"] as JsonArray
        assertEquals("wss://nos.lol/", rows[0].jsonObject["relay"]!!.jsonPrimitive.content)
        assertEquals(91_002L, rows[0].jsonObject["events"]!!.jsonPrimitive.long)
        assertEquals(3L, rows[0].jsonObject["quietForSec"]!!.jsonPrimitive.long)
        assertEquals("live", rows[0].jsonObject["pool"]!!.jsonPrimitive.content)
        assertNull((doc["streams"] as JsonArray)[0].jsonObject["live"], "the stream row keeps its COUNT and nothing more")
        assertEquals(0, doc["live"]!!.jsonObject["omitted"]!!.jsonPrimitive.int, "bounded by the tail budget, and it says so")

        // An empty list claims the router holds no tails; a router with no visit pool makes no claim.
        assertNull(SyncProgress.document(listOf(streamWith(name = "content")), nowSeconds = 1_000)["live"])
        assertNull(
            SyncProgress.document(listOf(streamWith(name = "content")), live = InFlight.NONE, nowSeconds = 1_000)["live"],
        )
    }

    @Test
    fun `a stream holding nothing publishes no list rather than an empty one`() {
        // An empty `inFlight` says the router looked; a stream with no registered source never looked.
        val stream =
            (
                SyncProgress.document(
                    listOf(streamWith(InFlight.NONE)),
                    nowSeconds = 1_000,
                )["streams"] as kotlinx.serialization.json.JsonArray
            )[0].jsonObject

        assertNull(stream["inFlight"])
    }

    @Test
    fun `the work that is not a stream is published beside the streams`() {
        val processors =
            listOf(
                Processors.Snapshot(
                    name = "aliasFold",
                    phase = "idle",
                    phaseForSec = 400,
                    passes = 3,
                    lastPassAt = 900,
                    lastPassSec = 42,
                    nextInSec = 20_800,
                    work =
                        listOf(
                            Processors.Work(
                                stream = "content",
                                candidates = 16_752,
                                newUrls = 4_139,
                                unmeasured = 4_021,
                                dialled = 2_000,
                                decided = 118,
                                undecided =
                                    listOf(
                                        Processors.Undecided(
                                            reason = "cooling down from an earlier failed pass",
                                            hosts = 214,
                                            examples = listOf("relay.example"),
                                        ),
                                    ),
                            ),
                        ),
                    counts = emptyList(),
                ),
                Processors.Snapshot(
                    name = "ingest",
                    phase = "running",
                    phaseForSec = 900,
                    passes = null,
                    lastPassAt = null,
                    lastPassSec = null,
                    nextInSec = null,
                    work = emptyList(),
                    counts = listOf(Processors.Count("queued", 12), Processors.Count("capacity", 20_000)),
                ),
            )

        val rows = SyncProgress.document(emptyList(), processors, nowSeconds = 1_000)["processors"] as kotlinx.serialization.json.JsonArray
        val fold = rows[0].jsonObject
        val ingest = rows[1].jsonObject

        assertEquals(20_800L, fold["nextInSec"]!!.jsonPrimitive.long, "a six-hour clock explains a fold that has said nothing")
        val work = (fold["streams"] as kotlinx.serialization.json.JsonArray)[0].jsonObject
        assertEquals(4_021, work["unmeasured"]!!.jsonPrimitive.int, "the progress number")
        // The denominator is the urls that arrived without a verdict, not the whole candidate set.
        assertEquals(4_139, work["newUrls"]!!.jsonPrimitive.int, "what arrived undecided")
        assertEquals(
            "cooling down from an earlier failed pass",
            (work["undecided"]!!.jsonObject["reasons"] as kotlinx.serialization.json.JsonArray)[0]
                .jsonObject["reason"]!!
                .jsonPrimitive.content,
        )
        // A counter-shaped processor has no passes and no clock; its gauges are members.
        assertNull(ingest["passesRun"])
        assertNull(ingest["nextInSec"])
        assertEquals(12L, ingest["queued"]!!.jsonPrimitive.long)
    }

    @Test
    fun `the store calls are named at the root, and absent when nothing books them`() {
        // A batch makes three store calls against three engine paths; this section says which one is held.
        val doc =
            SyncProgress.document(
                emptyList(),
                store =
                    StoreCalls.Snapshot(
                        slowAfterSec = 60,
                        outstanding = 2,
                        issued = 918_233,
                        returned = 918_230,
                        failed = 1,
                        cancelled = 0,
                        calls =
                            listOf(
                                StoreCalls.Call(
                                    caller = StoreCalls.CALLER_INGEST_DEDUP,
                                    op = StoreCalls.OP_EXISTING_IDS,
                                    asked = "2048 id(s)",
                                    issuedAt = 1_769_999_206,
                                    elapsedSec = 794,
                                    outstandingAtIssue = 1,
                                ),
                                StoreCalls.Call(
                                    caller = StoreCalls.CALLER_VISIT_NEGENTROPY,
                                    op = StoreCalls.OP_SNAPSHOT_IDS,
                                    asked = null,
                                    issuedAt = 1_770_000_000,
                                    elapsedSec = 0,
                                    outstandingAtIssue = 2,
                                ),
                            ),
                        omitted = 0,
                        callers =
                            listOf(
                                StoreCalls.Caller(StoreCalls.CALLER_INGEST_DEDUP, 41_022, 41_020, 1, 0, 1, 794),
                                StoreCalls.Caller(StoreCalls.CALLER_VISIT_NEGENTROPY, 12, 11, 0, 0, 1, null),
                            ),
                        ages = listOf(StoreCalls.Age(0, 1), StoreCalls.Age(300, 1)),
                    ),
                nowSeconds = 1_770_000_000,
            )

        val store = doc["store"]!!.jsonObject
        assertEquals(2, store["outstanding"]!!.jsonPrimitive.int)
        val stuck = (store["calls"] as JsonArray).first().jsonObject
        assertEquals(StoreCalls.CALLER_INGEST_DEDUP, stuck["caller"]!!.jsonPrimitive.content)
        assertEquals(StoreCalls.OP_EXISTING_IDS, stuck["op"]!!.jsonPrimitive.content)
        assertEquals("2048 id(s)", stuck["asked"]!!.jsonPrimitive.content)
        assertEquals(794L, stuck["elapsedSec"]!!.jsonPrimitive.long)
        // Zero is a reading: the call queued behind nothing.
        assertEquals(1, stuck["outstandingAtIssue"]!!.jsonPrimitive.int)
        // No `asked` rather than an empty string, so the page can draw "no filter" and mean it.
        assertNull((store["calls"] as JsonArray)[1].jsonObject["asked"])
        assertEquals(0, store["omitted"]!!.jsonPrimitive.int, "a truncation must never be silent, even at zero")

        // The router's own bound, so the page marks rows at the operator's threshold and not a copied default.
        assertEquals(60L, store["slowAfterSec"]!!.jsonPrimitive.long)

        // The live partition closes three ways: callers, age bands and the total, off one read of the rows.
        val callers = store["callers"] as JsonArray
        assertEquals(
            store["outstanding"]!!.jsonPrimitive.int,
            callers.sumOf { it.jsonObject["outstanding"]!!.jsonPrimitive.int },
        )
        // Lifetime counters beside a live count; they partition nothing until the router is quiet.
        val row = callers.first().jsonObject
        for (member in listOf("issued", "returned", "failed", "cancelled")) {
            assertNotNull(row[member], "`$member` must be published even at zero, or a reader cannot tell it from a router too old to say")
        }
        // A caller holding nothing has no age; a zero would read as a call that just started.
        assertNull((store["callers"] as JsonArray)[1].jsonObject["oldestOutstandingSec"])

        assertEquals(
            store["outstanding"]!!.jsonPrimitive.int,
            (store["ages"] as JsonArray).sumOf { it.jsonObject["calls"]!!.jsonPrimitive.int },
        )

        // "This router does not say" and "nothing is outstanding" are opposite claims.
        assertNull(SyncProgress.document(emptyList(), nowSeconds = 1_770_000_000)["store"])
    }

    @Test
    fun `a router that registered no processors publishes none, rather than an empty list`() {
        assertNull(SyncProgress.document(emptyList(), nowSeconds = 1_000)["processors"])
    }

    private fun streamWith(
        inFlight: InFlight? = null,
        name: String = "content",
    ) = StreamPhases.Stream(
        name = name,
        phase = "rotating",
        phaseForSec = 412,
        roster = 3,
        tails = 1,
        inFlight = inFlight,
    )
}
