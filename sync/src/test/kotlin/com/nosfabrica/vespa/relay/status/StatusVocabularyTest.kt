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

import com.nosfabrica.vespa.relay.progress.InFlight
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.progress.StatusVocabulary
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.sync.SweepState
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertTrue

/** The glossary checked against the members the two reports actually emit, in both directions. */
class StatusVocabularyTest {
    private val now = 1_769_998_206L

    /**
     * Members that carry no number and need no entry: identifiers, timestamps and containers.
     * Every addition is a judgement that something is self-describing.
     */
    private val selfDescribing =
        setOf(
            "name",
            "stream",
            "phase",
            "phaseForSec",
            "filter",
            "kinds",
            "narrowedBy",
            "cycle",
            "number",
            "streams",
            "rows",
            "from",
            "to",
            "startedAt",
            "endedAt",
            "relay",
            "min",
            "max",
            "complete",
            "fullAt",
            "host",
            "everyKindMin",
            "everyKindMax",
            "sweep",
            "target",
            "cap",
            "mirrors",
            "terms",
            "reconciled",
            "paged",
            "balanced",
            "urls",
            "taken",
            "examples",
            "health",
            "reasons",
            "statuses",
            "freshness",
            // The two halves' containers, as the served document nests them.
            "progress",
        )

    /**
     * The mirror's whole document, from the builders that publish it. Typed against them on
     * purpose: a member renamed or dropped on one side breaks this file rather than passing a
     * hand-written literal that still spells the old name.
     */
    private fun document(): JsonObject {
        val bands = bands()
        val coverage = SyncCoverageReport.build(bands.snapshot(), sweeps().snapshot(), now)!!
        val relays = RelayStatusReport.build(bands.snapshot(), primeUnits(), now)!!
        val progress =
            SyncProgress.document(
                streams = streams(),
                processors = processors(),
                fatals = 1,
                health = health(),
                live = live(),
                store = storeCalls(),
                nowSeconds = now,
            )
        val data =
            buildJsonObject {
                coverage.forEach { (member, value) -> put(member, value) }
                put("relays", relays)
                put("progress", JsonObject(progress + ("series" to GaugeSeries.next(null, progress["health"] as? JsonObject, progress, now)!!)))
            }
        return JsonObject(data + ("terms" to StatusVocabulary.termsFor(data)))
    }

    private val busy = RelayUrlNormalizer.normalize("wss://nos.lol")

    private val quiet = RelayUrlNormalizer.normalize("wss://quiet.example")

    /** The ask both relays owe, and the second one that makes `nos.lol` a two-leg group. */
    private val ask = Filter(kinds = listOf(1))

    private val secondAsk = Filter(kinds = listOf(30023))

    /**
     * The real band file, written through the real [SyncBands]: a reconciled leg (which carries
     * `verifiedAt`), a second leg on the same relay (which makes `legs` exceed the folded rows),
     * and one relay walked but never reconciled.
     */
    private fun bands() =
        SyncBands(null).apply {
            record(
                "content",
                busy,
                ask,
                observedMin = 1_689_857_148,
                observedMax = now,
                paged = true,
                reconciledThrough = now - 41_200,
                drained = true,
            )
            record("content", busy, secondAsk, observedMin = 1_689_857_148, observedMax = now, paged = true)
            record("content", quiet, ask, observedMin = 1_689_857_148, observedMax = now - 4_000_000, paged = true)
        }

    /** A learned peer cursor, so the coverage report publishes the sweep half of its document. */
    private fun sweeps() =
        SweepState(null).apply {
            setTarget(busy, 100_000)
        }

    /** Both readings of every optional row member: one pair carrying each, one carrying none. */
    private fun primeUnits() =
        listOf(
            RelayStatusReport.PrimeUnit(
                relay = busy.url,
                stream = "content",
                askKeys = setOf(ask.toJson(), secondAsk.toJson()),
                visiting = true,
                live = true,
                speaksNegentropy = true,
                watched = false,
                kindCap = 100,
                abortReason = "the relay closed the subscription",
                abortSaid = "error: too many kinds in filter",
                abortAtSec = now - 900,
            ),
            RelayStatusReport.PrimeUnit(quiet.url, "content", setOf(ask.toJson()), visiting = false, live = false),
        )

    /** One stream carrying every optional block: the caps, the clocks, and who is held right now. */
    private fun streams() =
        listOf(
            StreamPhases.Stream(
                name = "content",
                phase = "rotating",
                phaseForSec = 900,
                roster = 30,
                tails = 22,
                queued = 3,
                inFlight = held("wss://slow.example/", "paging"),
                limits = listOf(StreamPhases.Limit(job = "visiting", cap = 128, inUse = 5, deferred = 2)),
                schedule =
                    listOf(
                        StreamPhases.Scheduled(job = "negentropy", everySec = 86_400, due = 4, neverRun = 1, waiting = 25, nextInSec = 3_600),
                    ),
            ),
        )

    private fun held(
        relay: String,
        stage: String,
    ) = InFlight(
        relays =
            listOf(
                InFlight.Relay(
                    relay = relay,
                    heldForSec = 41_400,
                    transferringForSec = 900,
                    events = 4_000,
                    quietForSec = 12,
                    stage = stage,
                    stream = "content",
                    pool = "visits",
                    pagingUntil = now - 3_600,
                ),
            ),
        omitted = 2,
    )

    /**
     * One snapshot per processor shape the mirror publishes, built as the type rather than as
     * JSON: every structural member below comes from `Processors.published`, so one renamed
     * there stops appearing here. The COUNT NAMES are still a list — they are spelled by the
     * jobs that raise them ([FitnessPass], [VisitPool], the ingest loop), and listing them is
     * the same judgement the abort names already carry: a name that moves on one side is noticed.
     */
    private fun processors() =
        listOf(
            // A pass in flight: the countdown, the position, and who it is holding.
            Processors.Snapshot(
                name = "aliasSource",
                phase = Processors.RUNNING,
                phaseForSec = 40,
                passes = 3,
                lastPassAt = now - 880,
                lastPassSec = 60,
                nextInSec = 20_800,
                measuring = Processors.Measuring(unit = Processors.UNIT_SOURCE, attempted = 2, toProbe = 40, etaSec = 90, quietForSec = 12),
                inFlight = Processors.Holding(listOf(Processors.Holding.Held("wss://slow.example/", heldForSec = 41_400, stage = "fingerprint")), omitted = 0),
                work = work(),
                counts =
                    counts(
                        "sourced" to 900,
                        "excluded" to 12,
                        "heldOutDead" to 30,
                        "recordedOnly" to 8,
                        "candidates" to 850,
                        "newUrls" to 3,
                        "foldedAway" to 8,
                        "hosts" to 40,
                        "passesRun" to 3,
                    ),
            ),
            // The fitness verdicts: one member per value of the grade it signs.
            Processors.Snapshot(
                name = "fitness",
                phase = Processors.IDLE,
                phaseForSec = 400,
                passes = 3,
                lastPassAt = now - 880,
                lastPassSec = 60,
                nextInSec = 20_800,
                work = emptyList(),
                counts =
                    counts(
                        "prime" to 30,
                        "dead" to 6,
                        "silent" to 2,
                        "alias" to 3,
                        "inconsistent" to 1,
                        "unpageable" to 1,
                        "noncompliant" to 1,
                        "auth-refused" to 1,
                        "restricted" to 1,
                    ),
            ),
            // The pool: the roster it walks, and the eight ways a visit ends early.
            Processors.Snapshot(
                name = "visits",
                phase = "rotating",
                phaseForSec = 900,
                passes = null,
                lastPassAt = null,
                lastPassSec = null,
                nextInSec = null,
                work = emptyList(),
                counts =
                    counts(
                        "roster" to 30,
                        "rosterVisits" to 44,
                        "awaitingVisit" to 3,
                        "visiting" to 5,
                        "liveHeld" to 22,
                        "visitsRun" to 90,
                        "visitsHeldByIngest" to 0,
                        "negentropyRunning" to 1,
                        "negentropyRuns" to 4,
                        "negentropySkipped" to 3,
                        "negentropyRefused" to 2,
                        "retracted" to 2,
                        "liveEvicted" to 1,
                        "poolReceived" to 4_000,
                        "narrowedRelays" to 9,
                        "abortedVisits" to 2,
                        // Listed rather than derived from the enum, so a name that moved is noticed.
                        "abortedAuthRequired" to 1,
                        "abortedClosed" to 1,
                        "abortedQuiet" to 0,
                        "abortedUnreachable" to 0,
                        "abortedUnpageable" to 0,
                        "abortedGaveUp" to 0,
                        "abortedFailed" to 0,
                        "abortedBackpressured" to 0,
                    ),
            ),
            // A counter with a breakdown under its total, and the ingest queue's own shape.
            Processors.Snapshot(
                name = "ingest",
                phase = Processors.RUNNING,
                phaseForSec = 900,
                passes = null,
                lastPassAt = null,
                lastPassSec = null,
                nextInSec = null,
                work = emptyList(),
                counts =
                    counts(
                        "queued" to 3,
                        "capacity" to 4_096,
                        "accepted" to 91,
                        "rejected" to 12,
                        "lostToStore" to 0,
                        "inBatch" to 500,
                        "workers" to 2,
                        "workersRunning" to 2,
                        "oldestBatchSec" to 4,
                    ),
                reasons = listOf(Processors.Breakdown("duplicate: already have this event", 9)),
            ),
            Processors.Snapshot(
                name = "heal",
                phase = Processors.RUNNING,
                phaseForSec = 900,
                passes = null,
                lastPassAt = null,
                lastPassSec = null,
                nextInSec = null,
                work = emptyList(),
                counts = counts("queued" to 2, "dropped" to 7, "pushed" to 5),
            ),
        )

    private fun counts(vararg pairs: Pair<String, Long>) = pairs.map { (name, value) -> Processors.Count(name, value) }

    /** One idle processor carrying only the counts named, for the per-document selection test. */
    private fun processor(
        name: String,
        vararg pairs: Pair<String, Long>,
    ) = Processors.Snapshot(
        name = name,
        phase = Processors.IDLE,
        phaseForSec = 1,
        passes = null,
        lastPassAt = null,
        lastPassSec = null,
        nextInSec = null,
        work = emptyList(),
        counts = counts(*pairs),
    )

    /** The progress half alone, nested as the served document nests it. */
    private fun progressOf(snapshot: Processors.Snapshot) = buildJsonObject { put("progress", SyncProgress.document(streams = emptyList(), processors = listOf(snapshot), nowSeconds = now)) }

    /** One stream's share of a pass, carrying every optional member and an undecided breakdown. */
    private fun work() =
        listOf(
            Processors.Work(
                stream = "content",
                candidates = 850,
                newUrls = 3,
                foldedAway = 8,
                consistent = 700,
                inconsistent = 12,
                unmeasured = 130,
                dialled = 800,
                decided = 712,
                undecided =
                    listOf(
                        Processors.Undecided(
                            reason = "no TCP answer",
                            parent = "unreachable",
                            hosts = 4,
                            examples = listOf("wss://gone.example/"),
                            top = listOf(Processors.HostCount(host = "gone.example", urls = 9)),
                            urls = 12,
                        ),
                    ),
                undecidedOmitted = 0,
            ),
        )

    private fun health() =
        SyncProgress.Health(
            bottleneck = "ingest",
            eventsPerSec = 900,
            arrivingPerSec = 1_200,
            stageDetail = listOf(SyncProgress.StageDetail(stage = "write", ms = 41_000, calls = 900, meanMs = 45, maxMs = 4_100)),
            lockHeld = SyncProgress.LockHeld(stage = "proj.write", heldMs = 1_200, detail = "40 documents"),
            feed = "feed: 4 connections, 128 streams",
            heapUsedMb = 8_000,
            heapMaxMb = 12_000,
            sockets = 500,
            socketCeiling = 1_024,
            socketsRunning = 480,
            socketsQueued = 20,
            servingMs = 40,
        )

    private fun live() = held("wss://nos.lol/", "tailing")

    private fun storeCalls() =
        StoreCalls.Snapshot(
            slowAfterSec = 60,
            outstanding = 3,
            issued = 918_233,
            returned = 918_230,
            failed = 0,
            cancelled = 0,
            calls = listOf(StoreCalls.Call(caller = "ingest.dedup", op = "existingIds", asked = "2048 id(s)", issuedAt = now - 794, elapsedSec = 794, outstandingAtIssue = 2)),
            omitted = 0,
            callers = listOf(StoreCalls.Caller(caller = "ingest.dedup", issued = 41_022, returned = 41_020, failed = 0, cancelled = 0, outstanding = 2, oldestOutstandingSec = 794)),
            ages = listOf(StoreCalls.Age(fromSec = 0, calls = 1), StoreCalls.Age(fromSec = 900, calls = 2)),
        )

    /** Every member the document publishes, at any depth. */
    private fun publishedMembers(): Set<String> {
        val published = mutableSetOf<String>()

        fun walk(o: JsonObject) {
            for ((member, value) in o) {
                published += member
                when (value) {
                    is JsonObject -> walk(value)
                    is JsonArray -> value.filterIsInstance<JsonObject>().forEach(::walk)
                    else -> Unit
                }
            }
        }
        walk(document())
        return published
    }

    @Test
    fun `every number the sync section publishes has a term`() {
        val undefined = publishedMembers() - selfDescribing - StatusVocabulary.TERMS.keys

        assertTrue(
            undefined.isEmpty(),
            "published with no term, so a reader needs the source to read them: $undefined",
        )
    }

    @Test
    fun `no term describes something the document never publishes`() {
        // Against the real document, not a list kept by hand beside it: a term for a member no
        // builder writes any more is a glossary entry a reader can never reach, and the list
        // could only ever say what somebody remembered to delete from it.
        val concepts =
            setOf(
                // The meanings of "done", defined for the page's own prose rather than a member.
                "scope",
                "settled",
                "open",
                "walkEnvelope",
                "evidence",
                "holdings",
                // Synthesised by the page out of members below it: `pending` is `taken` minus the
                // terminal outcomes, which is what makes the partition add up mid-cycle.
                "corpus",
                "frame",
                "unnamed",
                "accountedFor",
                "pending",
                // A phase's own word, published as the VALUE of `phase` and never as a member.
                "rotating",
            )
        val orphans = StatusVocabulary.TERMS.keys - publishedMembers() - concepts

        assertTrue(
            orphans.isEmpty(),
            "a term for nothing — either the member stopped being published, or the fixture in this " +
                "file no longer drives the builder that writes it: $orphans",
        )
    }

    @Test
    fun `the meanings of done are named apart`() {
        val terms = StatusVocabulary.TERMS

        assertTrue(terms["settled"]!!.jsonPrimitive.content.contains("Nothing outstanding"))
        assertTrue(terms["evidence"]!!.jsonPrimitive.content.contains("not a coverage claim"))
        assertTrue(terms["holdings"]!!.jsonPrimitive.content.contains("NOT PUBLISHED HERE"))
    }

    @Test
    fun `approximations say they are approximations`() {
        assertTrue(
            StatusVocabulary.TERMS["frame"]!!
                .jsonPrimitive.content
                .startsWith("APPROXIMATE"),
        )
    }

    @Test
    fun `a stream-scoped count says it is stream-scoped`() {
        assertTrue(
            StatusVocabulary.TERMS["scope"]!!
                .jsonPrimitive.content
                .contains("per STREAM"),
        )
    }

    @Test
    fun `each document ships the definitions it needs and not the other plane's`() {
        // The mirror's and the monitor's documents publish disjoint halves of one vocabulary.
        // Both through the real builder: `termsFor` reads members, so a document it never sees
        // in that shape would prove nothing about what it selects.
        val mirror = progressOf(processor("ingest", "queued" to 3, "capacity" to 4_096))
        val monitor = progressOf(processor("aliasFold", "foldedAway" to 8, "candidates" to 40))

        val forMirror = StatusVocabulary.termsFor(mirror).keys
        val forMonitor = StatusVocabulary.termsFor(monitor).keys

        assertTrue("queued" in forMirror, "the mirror's document defines the members it carries")
        assertTrue("queued" !in forMonitor, "…and not the ones it does not")
        assertTrue("foldedAway" in forMonitor)
        assertTrue("foldedAway" !in forMirror)
        // Both draw from one map.
        assertTrue((forMirror + forMonitor).all { it in StatusVocabulary.TERMS.keys })
    }
}
