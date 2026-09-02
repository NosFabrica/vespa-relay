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
 * What the router publishes about itself.
 *
 * The property under test throughout is that THE COUNTS ADD UP. A production
 * document reported 16,752 relays discovered and 5,323 carrying a band with no
 * account of the ~11,400 in between, and every fix for that is worthless if the
 * replacement partition can silently stop being one.
 */
class SyncProgressTest {
    @Test
    fun `a router with nothing to report still publishes a document`() {
        // A push-only config has neither a down upstream nor a dynamic stream,
        // so it reports no streams at all — and its status page must still draw
        // the fatals count, the constraint and the processor rows rather than
        // the "nothing computed yet" card, which means something else entirely.
        val doc = SyncProgress.document(emptyList(), nowSeconds = 1_770_000_000)

        assertEquals(0L, doc["fatals"]!!.jsonPrimitive.long)
        // NO HEARTBEAT. The document was a file the serving relay read, so it
        // stamped the clock for a reader that had no other way to tell a quiet
        // mirror from a stopped one. This process serves the page itself now.
        assertNull(doc["writtenAt"])
    }

    @Test
    fun `the socket budget publishes what it is doing, not only how big it is`() {
        // THE NUMBER THAT ENDS THE ARGUMENT. "Should we raise the socket
        // budget" was answerable only by measuring an ETA — the router
        // published its ceiling and how many relays were connected, and every
        // other symptom of a full dispatcher (a long cycle, an idle-looking
        // pool, relays never reached) is shared with a slow store and a
        // roster of dead hosts. A queue is not: those calls are admissible and
        // OkHttp is holding them.
        //
        // ZERO IS PUBLISHED, on this document's rule: a member that appears
        // only on damage cannot be told from a router too old to say.
        val healthy =
            SyncProgress.Health(
                bottleneck = "downloads",
                eventsPerSec = 900,
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
    fun `a stream that has not started publishes its phase and nothing it cannot answer`() {
        val s = StreamPhases.Stream("content", "starting", 12)
        val stream = (SyncProgress.document(listOf(s), nowSeconds = 1_000)["streams"] as kotlinx.serialization.json.JsonArray)[0].jsonObject

        assertEquals("starting", stream["phase"]!!.jsonPrimitive.content)
        assertEquals(12L, stream["phaseForSec"]!!.jsonPrimitive.long)
        assertNull(stream["roster"], "a stream that has not started rides nothing, and says nothing rather than 0")
    }

    @Test
    fun `a rotating stream publishes what it is riding, zero included`() {
        // The row that said `rotating for 58m` and nothing else, because a visit
        // stream has no pass, no fraction and no cycle for the rest of the
        // document to describe. Zero is the reading worth having: a stream with
        // an empty roster is one waiting on the fitness pass to certify its
        // first relay, and it looked exactly like a stream riding four hundred.
        val riding = StreamPhases.Stream("visits", "rotating", 3_480, roster = 412, tails = 300, queued = 18)
        val waiting = StreamPhases.Stream("cold", "rotating", 3_480, roster = 0, tails = 0, queued = 0)

        val rows = SyncProgress.document(listOf(riding, waiting), nowSeconds = 1_000)["streams"] as kotlinx.serialization.json.JsonArray

        assertEquals(412L, rows[0].jsonObject["roster"]!!.jsonPrimitive.long)
        assertEquals(300L, rows[0].jsonObject["liveHeld"]!!.jsonPrimitive.long)
        // …and how much of that roster is queued for a worker rather than
        // counting down a revisit. The pool's own row publishes the whole
        // queue, which is a sum over streams and cannot be divided back into
        // shares — so a per-stream card could say what a stream is RIDING and
        // never whether it was starved.
        assertEquals(18L, rows[0].jsonObject["awaitingVisit"]!!.jsonPrimitive.long)
        assertEquals(0L, rows[1].jsonObject["roster"]!!.jsonPrimitive.long, "an empty roster is a report, not an absence")
        assertEquals(0L, rows[1].jsonObject["awaitingVisit"]!!.jsonPrimitive.long, "and so is an empty queue")
    }

    @Test
    fun `a stream row too old to split the queue says nothing rather than zero`() {
        // The member has to be ABSENT and not 0 on a router that does not
        // publish it: the page draws the remainder — the share of the roster
        // that is neither running nor queued — by subtracting this, and a zero
        // it invented would quietly report the whole queue as sitting between
        // visits.
        val row = StreamPhases.Stream("visits", "rotating", 3_480, roster = 412, tails = 300)
        val rows = SyncProgress.document(listOf(row), nowSeconds = 1_000)["streams"] as kotlinx.serialization.json.JsonArray

        assertNull(rows[0].jsonObject["awaitingVisit"])
        assertEquals(412L, rows[0].jsonObject["roster"]!!.jsonPrimitive.long, "everything it does know is still said")
    }

    @Test
    fun `a pass in flight publishes its position instead of a countdown`() {
        // The two are never both there — the monitor unsets the due time while a
        // pass runs — and that is why this had to exist: for the hours a
        // stability pass takes, the row's one number was gone and `measuring`
        // stood alone with no size, no position and no end.
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
        // …and the reading `etaSec` cannot give. A pass one url from done and a
        // pass whose last url has wedged both report `~0s left`; only this
        // separates them, so it rides beside the estimate rather than instead
        // of it.
        assertEquals(3L, measuring["quietForSec"]!!.jsonPrimitive.long)
    }

    @Test
    fun `nothing is published until the first tick, and then the latest is what the page reads`() {
        val progress = SyncProgress()

        // The status site answers 503 in this state — "no document yet" and
        // "this mirror is doing nothing" are different facts and must not share
        // a rendering.
        assertNull(progress.latest)

        progress.publish(listOf(streamWith()), nowSeconds = 42)
        val first = assertNotNull(progress.latest)
        assertEquals("content", (first["streams"] as JsonArray)[0].jsonObject["name"]!!.jsonPrimitive.content)

        // THE HEARTBEAT IS GONE. This document used to be a file the serving
        // relay read, so it stamped `writtenAt` for the reader to age; the
        // process that builds it serves it now, and a member whose only use was
        // inferring that this process exists would be a constant.
        assertNull(first["writtenAt"])

        // Swapped whole on the next tick, so a reader never sees half of two
        // documents.
        progress.publish(listOf(streamWith(name = "later")), nowSeconds = 43)
        assertEquals("later", (progress.latest!!["streams"] as JsonArray)[0].jsonObject["name"]!!.jsonPrimitive.content)
    }

    @Test
    fun `the relays in flight are NAMED, not just counted`() {
        // The gap this closes. A production document reported `pending: 2` on a
        // stream that had received two events in eleven and a half hours, and
        // nothing anywhere said which two relays: the count is derived by
        // subtraction, a stalled leg earns no band so the coverage card never
        // draws it, and the logs rotate inside the hour.
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
        // The other half of the same complaint. `tails: 412` is a number every
        // healthy deployment renders and nobody can act on: which relay holds
        // a socket, for how long, and whether anything has ever come down it
        // were all unanswerable from outside the process.
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

        // AT THE ROOT, not under a stream: one subscription per relay carries
        // every wanting stream's filter and counts its arrivals at the url, so
        // dividing it per stream would publish one undivided number once per
        // stream — each copy carrying the whole url's event count.
        val rows = doc["live"]!!.jsonObject["relays"] as JsonArray
        assertEquals("wss://nos.lol/", rows[0].jsonObject["relay"]!!.jsonPrimitive.content)
        assertEquals(91_002L, rows[0].jsonObject["events"]!!.jsonPrimitive.long)
        assertEquals(3L, rows[0].jsonObject["quietForSec"]!!.jsonPrimitive.long)
        assertEquals("live", rows[0].jsonObject["pool"]!!.jsonPrimitive.content)
        assertNull((doc["streams"] as JsonArray)[0].jsonObject["live"], "the stream row keeps its COUNT and nothing more")
        assertEquals(0, doc["live"]!!.jsonObject["omitted"]!!.jsonPrimitive.int, "bounded by the tail budget, and it says so")

        // Absent rather than empty, on the same terms `inFlight` is: an empty
        // list is a claim that this router holds no tails, and a router with no
        // visit pool at all makes no such claim.
        assertNull(SyncProgress.document(listOf(streamWith(name = "content")), nowSeconds = 1_000)["live"])
        assertNull(
            SyncProgress.document(listOf(streamWith(name = "content")), live = InFlight.NONE, nowSeconds = 1_000)["live"],
        )
    }

    @Test
    fun `a stream holding nothing publishes no list rather than an empty one`() {
        // An empty `inFlight` is a claim that the router looked and found
        // nothing running; a stream nothing has registered a source for never
        // looked. They are different statements.
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
        // The alias fold, the stability gate, the NIP-66 monitor, ingest, the
        // healer, the push — every one of them reachable only from a stderr line
        // that rotates inside the hour, and the fold's progress is what "why is
        // this server still wearing forty urls" turns into.
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
        // …and the denominator it is a share of: the urls that arrived with no
        // verdict, which is what the card counts against rather than the whole
        // candidate set. 118 of 4,139 got one here; 16,752 is the set they came
        // in with and moves by a rounding error whatever a pass does.
        assertEquals(4_139, work["newUrls"]!!.jsonPrimitive.int, "what arrived undecided")
        assertEquals(
            "cooling down from an earlier failed pass",
            (work["undecided"]!!.jsonObject["reasons"] as kotlinx.serialization.json.JsonArray)[0]
                .jsonObject["reason"]!!
                .jsonPrimitive.content,
        )
        // A counter-shaped processor answers only what it can: no passes, no
        // clock, and its gauges as members.
        assertNull(ingest["passesRun"])
        assertNull(ingest["nextInSec"])
        assertEquals(12L, ingest["queued"]!!.jsonPrimitive.long)
    }

    @Test
    fun `the store calls are named at the root, and absent when nothing books them`() {
        // THE HALF OF A WEDGE THE INGEST ROW COULD NOT CARRY. `oldestBatchSec`
        // says a worker has been inside a batch pass for 794 seconds; a batch
        // makes three different store calls against three different engine
        // paths, and until this section existed nothing said which.
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
        // The three facts the incident report asked for, on the row.
        assertEquals(StoreCalls.CALLER_INGEST_DEDUP, stuck["caller"]!!.jsonPrimitive.content)
        assertEquals(StoreCalls.OP_EXISTING_IDS, stuck["op"]!!.jsonPrimitive.content)
        assertEquals("2048 id(s)", stuck["asked"]!!.jsonPrimitive.content)
        assertEquals(794L, stuck["elapsedSec"]!!.jsonPrimitive.long)
        // ZERO IS PUBLISHED. A call issued with nothing else out did not queue
        // behind us, which is a reading; a member that appears only when it is
        // non-zero cannot be told from a router too old to say.
        assertEquals(1, stuck["outstandingAtIssue"]!!.jsonPrimitive.int)
        // A call with no filter publishes no `asked` rather than an empty
        // string, so the page can draw "no filter" and mean it.
        assertNull((store["calls"] as JsonArray)[1].jsonObject["asked"])
        assertEquals(0, store["omitted"]!!.jsonPrimitive.int, "a truncation must never be silent, even at zero")

        // THE ROUTER'S OWN BOUND, so the page marks a row at the operator's
        // threshold rather than at a copy of the default — the drift the page
        // avoids everywhere else by never re-deciding what the router decided.
        assertEquals(60L, store["slowAfterSec"]!!.jsonPrimitive.long)

        // THE LIVE PARTITION, WHICH CLOSES THREE WAYS — the callers' share, the
        // age bands, and the total, all off one read of the row set. It is the
        // check a reader would otherwise do by hand, and `accountedFor` on the
        // card reports it when it fails.
        val callers = store["callers"] as JsonArray
        assertEquals(
            store["outstanding"]!!.jsonPrimitive.int,
            callers.sumOf { it.jsonObject["outstanding"]!!.jsonPrimitive.int },
        )
        // …and every lifetime member is present including its zeroes, so a
        // reader can see what a caller has spent as well as what it holds. NOT
        // a partition with `outstanding`: those are lifetime counters beside a
        // live count, and they agree only once the router is quiet — see
        // [StoreCalls.Caller].
        val row = callers.first().jsonObject
        for (member in listOf("issued", "returned", "failed", "cancelled")) {
            assertNotNull(row[member], "`$member` must be published even at zero, or a reader cannot tell it from a router too old to say")
        }
        // …and a caller holding nothing has no age, where a zero would read as
        // a call that had just started.
        assertNull((store["callers"] as JsonArray)[1].jsonObject["oldestOutstandingSec"])

        assertEquals(
            store["outstanding"]!!.jsonPrimitive.int,
            (store["ages"] as JsonArray).sumOf { it.jsonObject["calls"]!!.jsonPrimitive.int },
        )

        // NO SECTION AT ALL where nothing books its calls. "This router does
        // not say" and "nothing is outstanding" are opposite claims, and an
        // empty section would make the first read as the second.
        assertNull(SyncProgress.document(emptyList(), nowSeconds = 1_770_000_000)["store"])
    }

    @Test
    fun `a router that registered no processors publishes none, rather than an empty list`() {
        // An empty array is a claim that this router runs none of them; a router
        // with no signer genuinely has no fold and no NIP-66 monitor, and one
        // built before this existed made no claim either way.
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
