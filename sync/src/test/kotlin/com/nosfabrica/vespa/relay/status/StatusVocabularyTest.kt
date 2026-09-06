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

import com.nosfabrica.vespa.relay.progress.StatusVocabulary
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The glossary checked against the members the two reports actually emit, in both directions. */
class StatusVocabularyTest {
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
        )

    @Test
    fun `every number the sync section publishes has a term`() {
        val coverage =
            SyncCoverageReport.build(
                bandsJson =
                    """
                    {"content": {"{\"kinds\":[1]}": {"wss://a.example/": {"min": 100, "max": 200, "complete": true}}}}
                    """.trimIndent(),
                sweepsJson = null,
                nowSeconds = 1_000,
            )!!
        // Parsed, not re-projected: nothing filters these members on the way to the page.
        val progress =
            Json
                .parseToJsonElement(
                    """
                    {"fatals": 0,
                     "health": {"bottleneck": "wedged", "eventsPerSec": 2350, "arrivingPerSec": 4100, "heapUsedMb": 900, "heapMaxMb": 2048,
                                "sockets": 412, "socketCeiling": 1024, "socketsRunning": 418, "socketsQueued": 0,
                                "servingMs": 18, "feed": "feed ok 4211 inflight 32 lat 18ms",
                                "lockHeldBy": {"stage": "lock.gate.hold", "heldMs": 412000, "doing": "derive 4812 subject(s) in 49 chunk(s), fanout 4"},
                                "stages": [{"stage": "write", "ms": 33300, "calls": 1200, "meanMs": 27, "maxMs": 900},
                                           {"stage": "lock.ingest.wait", "ms": 21900}]},
                     "streams": [{"name": "content", "phase": "rotating", "phaseForSec": 5,
                     "roster": 412, "liveHeld": 300,
                     "inFlight": {"relays": [{"relay": "wss://slow.example/", "heldForSec": 41400,
                                              "transferringForSec": 41390, "events": 2, "quietForSec": 41000,
                                               "doing": "catching up (paging)", "pool": "catching-up",
                                               "pagingUntil": 1689857148}],
                                  "omitted": 118},
                     "limits": [{"job": "visiting", "streamCap": 96, "inUse": 7, "deferred": 0},
                                {"job": "negentropy", "streamCap": 4, "inUse": 2, "deferred": 91}],
                     "schedule": [{"job": "negentropy", "everySec": 604800, "due": 3, "neverRun": 12,
                                   "waiting": 397, "nextInSec": 41200}]}],
                     "relays": {"pairs": 44, "omitted": 2, "unwatched": 3,
                                "statuses": [{"syncStatus": "refused", "pairs": 4}, {"syncStatus": "complete", "pairs": 30}],
                                "freshness": [{"behind": "current", "pairs": 30}, {"behind": "older", "pairs": 14}],
                                "rows": [{"relay": "wss://nos.lol/", "stream": "content", "syncStatus": "paging",
                                          "asks": 40, "bands": 3, "settled": 1, "coveredFrom": 1689857148, "coveredTo": 1769998206,
                                          "behindSec": 90, "behind": "current", "fault": true,
                                          "negentropy": true, "kindCap": 100, "unwatched": true,
                                          "verifiedAgoSec": 41200, "visiting": true, "tailed": true,
                                          "refusedFor": "the relay closed the subscription",
                                          "relaySaid": "error: too many kinds in filter", "refusedAgoSec": 900}]},
                     "live": {"relays": [{"relay": "wss://nos.lol/", "heldForSec": 41400,
                                          "transferringForSec": 41400, "events": 91002, "quietForSec": 3,
                                          "doing": "holding a live tail", "pool": "live", "stream": "content"}],
                              "omitted": 0},
                     "processors": [
                       {"name": "aliasSource", "phase": "collecting", "phaseForSec": 90, "passesRun": 2,
                        "lastPassAt": 880, "lastPassSec": 300,
                        "measuring": {"unit": "source", "attempted": 2, "toProbe": 6},
                        "sourced": 44, "excluded": 1, "heldOutDead": 3, "candidates": 40, "recordedOnly": 6},
                       {"name": "aliasFold", "phase": "idle", "phaseForSec": 400, "passesRun": 3,
                        "lastPassAt": 880, "lastPassSec": 42, "nextInSec": 20800,
                        "sourced": 44, "excluded": 1, "heldOutDead": 3, "recordedOnly": 6,
                        "streams": [{"name": "content", "candidates": 40, "newUrls": 16, "unmeasured": 12, "dialled": 20, "decided": 4,
                          "undecided": {"reasons": [{"reason": "cooling down from an earlier failed pass", "hosts": 2,
                                                     "examples": ["a.example"]}], "omitted": 0}}]},
                       {"name": "consistency", "phase": "measuring", "phaseForSec": 400, "passesRun": 3,
                        "lastPassAt": 880, "lastPassSec": 900,
                        "measuring": {"unit": "url", "attempted": 6, "toProbe": 22, "etaSec": 300, "quietForSec": 4},
                        "inFlight": {"relays": [{"relay": "wss://wedged.example/", "heldForSec": 4454,
                                                 "stage": "paired walk"}], "omitted": 2},
                        "sourced": 44, "excluded": 1, "heldOutDead": 3, "recordedOnly": 6,
                        "streams": [{"name": "all streams", "candidates": 40, "foldedAway": 8, "consistent": 9,
                          "inconsistent": 1, "unmeasured": 22, "dialled": 22, "decided": 2,
                          "undecided": {"reasons": [{"reason": "the connection was refused",
                                                     "parent": "never answered a REQ", "urls": 22, "hosts": 7,
                                                     "top": [{"host": "dead.example", "urls": 9}]}], "omitted": 0}}]},
                       {"name": "ingest", "phase": "running", "phaseForSec": 900,
                        "queued": 3, "capacity": 4096, "accepted": 91, "rejected": 12, "lostToStore": 0,
                        "rejections": {"reasons": [{"reason": "duplicate: already have this event", "events": 9}]}},
                       {"name": "fitness", "phase": "idle", "phaseForSec": 400, "passesRun": 3,
                        "lastPassAt": 880, "lastPassSec": 60, "nextInSec": 20800,
                        "prime": 30, "dead": 6, "silent": 2, "alias": 3, "inconsistent": 1,
                        "unpageable": 1, "noncompliant": 1, "auth-refused": 1, "restricted": 1},
                       {"name": "visits", "phase": "rotating", "phaseForSec": 900,
                        "roster": 30, "rosterVisits": 44, "awaitingVisit": 3, "visiting": 5, "liveHeld": 22,
                        "visitsRun": 90, "visitsHeldByIngest": 0, "negentropyRunning": 1, "negentropyRuns": 4, "negentropySkipped": 3, "negentropyRefused": 2, "retracted": 2, "liveEvicted": 1, "poolReceived": 4000,
                        "narrowedRelays": 9, "abortedVisits": 2, "abortedAuthRequired": 1, "abortedClosed": 1, "abortedQuiet": 0,
                        "abortedUnreachable": 0, "abortedUnpageable": 0, "abortedGaveUp": 0, "abortedFailed": 0,
                        "abortedBackpressured": 0},
                       {"name": "heal", "phase": "running", "phaseForSec": 900, "queued": 2, "dropped": 7, "pushed": 5}],
                     "store": {"outstanding": 3, "slowAfterSec": 60, "issued": 918233, "returned": 918230, "failed": 0, "cancelled": 0,
                               "calls": [{"caller": "ingest.dedup", "op": "existingIds", "asked": "2048 id(s)",
                                          "issuedAt": 1769998206, "elapsedSec": 794, "outstandingAtIssue": 2}],
                               "omitted": 0,
                               "callers": [{"caller": "ingest.dedup", "issued": 41022, "returned": 41020,
                                            "failed": 0, "cancelled": 0, "outstanding": 2,
                                            "oldestOutstandingSec": 794}],
                               "ages": [{"fromSec": 0, "calls": 1}, {"fromSec": 900, "calls": 2}]}}
                    """.trimIndent(),
                ).jsonObject

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
        walk(coverage)
        walk(progress)

        val undefined = published - selfDescribing - StatusVocabulary.TERMS.keys
        assertTrue(
            undefined.isEmpty(),
            "published with no term, so a reader needs the source to read them: $undefined",
        )
    }

    @Test
    fun `no term describes something the document never publishes`() {
        val known =
            selfDescribing +
                setOf(
                    // Concepts rather than members: the meanings of "done", and the one not published.
                    "scope",
                    "settled",
                    "open",
                    "walkEnvelope",
                    "evidence",
                    "holdings",
                    // Synthesised by the page from `sourced` and `recordedOnly`.
                    "corpus",
                    "frame",
                    "unnamed",
                    "rotating",
                    "accountedFor",
                ) +
                setOf(
                    "relays",
                    "hosts",
                    "legs",
                    "rows",
                    "sweeping",
                    "excluded",
                    "pending",
                    "inFlight",
                    "heldForSec",
                    "transferringForSec",
                    "events",
                    "quietForSec",
                    "doing",
                    "pool",
                    "live",
                    "limits",
                    "job",
                    "schedule",
                    "everySec",
                    "due",
                    "neverRun",
                    "waiting",
                    "streamCap",
                    "inUse",
                    "deferred",
                    "stage",
                    // The stage split's shape beside its total: whether a stage's cost was one call
                    // or a hundred thousand, and who holds the write lock while the mirror waits.
                    "calls",
                    "meanMs",
                    "maxMs",
                    "lockHeldBy",
                    "heldMs",
                    "pagingUntil",
                    "omitted",
                ) +
                // The processors: the probe passes, the monitor, ingest, the healer, the push.
                setOf(
                    "passesRun",
                    "processors",
                    "candidates",
                    "newUrls",
                    "sourced",
                    "heldOutDead",
                    "recordedOnly",
                    "foldedAway",
                    "consistent",
                    "inconsistent",
                    "unmeasured",
                    "dialled",
                    "decided",
                    "undecided",
                    "top",
                    "parent",
                    "reason",
                    "lastPassAt",
                    "lastPassSec",
                    "nextInSec",
                    "measuring",
                    "attempted",
                    "toProbe",
                    "unit",
                    "queued",
                    "capacity",
                    "accepted",
                    "rejected",
                    "pushed",
                    "dropped",
                    // The fitness verdicts, one member per value of the `s` tag it signs.
                    "prime",
                    "dead",
                    "silent",
                    "alias",
                    "unpageable",
                    "noncompliant",
                    "auth-refused",
                    "restricted",
                    "roster",
                    "rosterVisits",
                    "awaitingVisit",
                    "visiting",
                    "liveHeld",
                    "visitsRun",
                    "visitsHeldByIngest",
                    "negentropyRunning",
                    "negentropyRuns",
                    "negentropySkipped",
                    "retracted",
                    "abortedVisits",
                    // Listed rather than derived from the enum, so a name that moved on one side is noticed.
                    "abortedAuthRequired",
                    "abortedClosed",
                    "abortedQuiet",
                    "abortedUnreachable",
                    "abortedUnpageable",
                    "abortedGaveUp",
                    "abortedFailed",
                    "abortedBackpressured",
                    "narrowedRelays",
                    "negentropyRefused",
                    "syncStatus",
                    "pairs",
                    "behind",
                    "behindSec",
                    "fault",
                    "unwatched",
                    "negentropy",
                    "kindCap",
                    "coveredFrom",
                    "coveredTo",
                    "verifiedAgoSec",
                    "refusedFor",
                    "relaySaid",
                    "refusedAgoSec",
                    "bands",
                    "asks",
                    "tailed",
                    "liveEvicted",
                    "poolReceived",
                ) +
                setOf(
                    "running",
                    "transferring",
                    "fraction",
                    "etaSec",
                    "collected",
                    "collectedTotal",
                    "slotsFree",
                    "slotsNeeded",
                    "retryInSec",
                    "fatals",
                    "rejections",
                    "lostToStore",
                    "inBatch",
                    "workers",
                    "workersRunning",
                    "oldestBatchSec",
                    "health",
                    "bottleneck",
                    "stages",
                    "stage",
                    "ms",
                    "feed",
                    "eventsPerSec",
                    "arrivingPerSec",
                    "heapUsedMb",
                    "heapMaxMb",
                    "sockets",
                    "socketCeiling",
                    "socketsRunning",
                    "socketsQueued",
                    "servingMs",
                    "series",
                    "at",
                    "heapPct",
                ) +
                setOf(
                    "store",
                    "calls",
                    "caller",
                    "op",
                    "asked",
                    "issuedAt",
                    "elapsedSec",
                    "outstandingAtIssue",
                    "callers",
                    "issued",
                    "returned",
                    "failed",
                    "cancelled",
                    "outstanding",
                    "slowAfterSec",
                    "oldestOutstandingSec",
                    "ages",
                    "fromSec",
                )

        assertEquals(emptySet(), StatusVocabulary.TERMS.keys - known, "a term for nothing")
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
        val mirror =
            Json
                .parseToJsonElement("""{"progress": {"processors": [{"name": "ingest", "queued": 3, "capacity": 4096}]}}""")
                .jsonObject
        val monitor =
            Json
                .parseToJsonElement("""{"progress": {"processors": [{"name": "aliasFold", "foldedAway": 8, "candidates": 40}]}}""")
                .jsonObject

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
