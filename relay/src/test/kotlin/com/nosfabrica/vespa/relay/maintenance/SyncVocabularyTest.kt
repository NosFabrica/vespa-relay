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
package com.nosfabrica.vespa.relay.maintenance

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The glossary against the numbers it is a glossary FOR.
 *
 * The failure this pins is silent and inevitable otherwise: a new count is
 * published, nobody adds a term, and the section is back to numbers that cannot
 * be read without this repository open beside them — which is the whole
 * complaint. So the terms are checked against the members the other two reports
 * actually emit, in both directions.
 */
class SyncVocabularyTest {
    /**
     * Members that carry no NUMBER and need no entry: identifiers, timestamps,
     * and structures whose own children are what a reader looks up.
     *
     * Deliberately short and deliberately explicit. Every addition to it is a
     * decision that something is self-describing, which is exactly the judgement
     * that produced the unreadable section in the first place — so it should be
     * uncomfortable to extend.
     */
    private val selfDescribing =
        setOf(
            // `kinds` is the FILTER's own member, echoed back verbatim — a
            // Nostr kind list needs no gloss from this relay.
            "name",
            "phase",
            "phaseForSec",
            "filter",
            "kinds",
            "narrowedBy",
            "cycle",
            // A pass's own number within its owner. An identifier, like `name`
            // — what it MEANS is `passes`, which has a term of its own.
            "number",
            "streams",
            "rows",
            "from",
            "to",
            "startedAt",
            "endedAt",
            "writtenAt",
            "relay",
            "min",
            "max",
            "complete",
            "fullAt",
            // A hostname, inside a row whose `urls` is the count a reader looks
            // up — the same call `relay` gets inside `foldedOnto`.
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
            // The fold's per-survivor sample urls: strings, and `urls` beside
            // them is the count a reader looks up.
            "examples",
            // The container the constraint's numbers sit in; `bottleneck` and
            // each gauge inside it is what a reader looks up.
            "health",
            // The container `undecided` holds its rows in. `reason`, `hosts`
            // and `examples` inside it are what a reader looks up; the plural
            // is the shape, exactly as `relays` is inside `inFlight`.
            "reasons",
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
        val progress =
            SyncProgressReport.build(
                """
                {"writtenAt": 900, "fatals": 0,
                 "health": {"bottleneck": "ingest", "eventsPerSec": 2350, "heapUsedMb": 900, "heapMaxMb": 2048,
                            "sockets": 412, "socketCeiling": 1024, "servingMs": 18},
                 "streams": [{"name": "content", "phase": "fetching", "phaseForSec": 5,
                 "returned": 12, "running": 128, "transferring": 8, "fraction": 0.33, "etaSec": 3600,
                 "reached": 1700000000, "collected": 10, "collectedTotal": 20, "slotsFree": 0, "slotsNeeded": 20,
                 "nextInSec": 30, "retryInSec": 60, "reason": "connection reset",
                 "inFlight": {"relays": [{"relay": "wss://slow.example/", "pass": 11, "heldForSec": 41400,
                                          "transferringForSec": 41390, "events": 2, "quietForSec": 41000,
                                           "doing": "paging", "pagingUntil": 1689857148}],
                              "omitted": 118},
                 "cycle": {"number": 12, "owner": "dynamic", "startedAt": 800, "outcome": "completed",
                   "urls": {"discovered": 4, "foldedOntoAnother": 1, "taken": 3},
                   "hosts": 2, "relayListAgeSec": 120, "taken": {"delivered": 3, "busy": 1}, "balanced": true, "received": 9,
                   "foldedOnto": {"relays": [{"relay": "wss://a.example/", "urls": 1, "examples": ["wss://a.example/x"]}],
                                  "omitted": 0}},
                 "passes": [{"number": 11, "owner": "dynamic", "startedAt": 700, "endedAt": 780, "outcome": "completed",
                    "urls": {"discovered": 4, "taken": 4}, "taken": {"delivered": 2}, "received": 4},
                   {"number": 12, "owner": "dynamic", "startedAt": 800, "outcome": "running",
                    "urls": {"discovered": 4, "foldedOntoAnother": 1, "taken": 3}, "taken": {"delivered": 3}, "received": 9}]}],
                 "processors": [
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
                    "unpageable": 1, "auth-refused": 1, "restricted": 1},
                   {"name": "visits", "phase": "rotating", "phaseForSec": 900,
                    "roster": 30, "awaitingVisit": 3, "visiting": 5, "tails": 22,
                    "visitsRun": 90, "auditing": 1, "auditsRun": 4, "retracted": 2, "abortedVisits": 2, "evictedTails": 1, "poolReceived": 4000},
                   {"name": "heal", "phase": "running", "phaseForSec": 900, "queued": 2, "dropped": 7, "pushed": 5}]}
                """.trimIndent(),
                nowSeconds = 1_000,
            )!!

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

        val undefined = published - selfDescribing - SyncVocabulary.TERMS.keys
        assertTrue(
            undefined.isEmpty(),
            "published with no term, so a reader needs the source to read them: $undefined",
        )
    }

    @Test
    fun `no term describes something the document never publishes`() {
        // A definition for an absent member is a promise the document does not
        // keep, and it is how a glossary rots into fiction.
        val known =
            selfDescribing +
                setOf(
                    // Names for CONCEPTS rather than for members — the three
                    // meanings of "done" that had to be told apart, plus the
                    // one deliberately NOT published here.
                    "scope",
                    "returned",
                    "settled",
                    "open",
                    "walkEnvelope",
                    "evidence",
                    "holdings",
                    // The coverage tree's root, synthesised by the page from
                    // `sourced` and `recordedOnly` — a name for the sum, which
                    // is why no document member carries it.
                    "corpus",
                    "frame",
                    "unnamed",
                    "outcome",
                    "holding",
                    // The other phase word that reads as a stall and is not —
                    // a visit stream has no pass to be a phase OF, so the word
                    // simply lasts and its numbers are the whole story.
                    "rotating",
                    "accountedFor",
                    "staleForSec",
                ) +
                setOf(
                    "relays",
                    "hosts",
                    "relayListAgeSec",
                    "legs",
                    "sweeping",
                    "rows",
                    "discovered",
                    "foldedOntoAnother",
                    "refusedUnstable",
                    "taken",
                    "delivered",
                    "nothingNew",
                    "unreachable",
                    "transferFailed",
                    "noRoute",
                    "hostStruckOut",
                    "knownDead",
                    "torUnavailable",
                    "busy",
                    "excluded",
                    "foldedOnto",
                    "pending",
                    "received",
                    "inFlight",
                    "heldForSec",
                    "transferringForSec",
                    "events",
                    "quietForSec",
                    "doing",
                    // …and `doing`'s counterpart on the other kind of held row:
                    // a probe leg is a ladder, not a transfer, so what it
                    // publishes is which STEP it is on.
                    "stage",
                    "pagingUntil",
                    "omitted",
                    "owner",
                ) +
                // The passes beside a stream's current cycle, and the work that
                // is not a stream at all — the two probe passes, the NIP-66
                // monitor, ingest, the healer, the push.
                setOf(
                    "passes",
                    "passesRun",
                    "processors",
                    "candidates",
                    // …the share of them that arrived undecided, which is what
                    // the card counts against rather than the whole set.
                    "newUrls",
                    // The candidate set's own partition, and the two nodes above
                    // it that say where the set came from.
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
                    // The ranked hosts under one reason — the tree's deepest
                    // level — and the reason a row refines, which is what nests
                    // the sub-causes of silence under it.
                    "top",
                    "parent",
                    "reason",
                    "lastPassAt",
                    "lastPassSec",
                    "nextInSec",
                    // …and the countdown's opposite half: where the pass
                    // RUNNING right now has got to, in units it names itself.
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
                    // The fitness pass's verdict funnel — each member one value
                    // of the `s` tag it signs — and the rotating pool's row.
                    "prime",
                    "dead",
                    "silent",
                    "alias",
                    "unpageable",
                    "auth-refused",
                    "restricted",
                    "roster",
                    "awaitingVisit",
                    "visiting",
                    "tails",
                    "visitsRun",
                    "auditing",
                    "auditsRun",
                    "retracted",
                    "abortedVisits",
                    "evictedTails",
                    "poolReceived",
                ) +
                // What the phase itself knows, which used to reach a log line
                // and stop there — plus the two facts about the process rather
                // than about a stream.
                setOf(
                    "running",
                    "transferring",
                    "fraction",
                    "etaSec",
                    "reached",
                    "collected",
                    "collectedTotal",
                    "slotsFree",
                    "slotsNeeded",
                    "retryInSec",
                    "pass",
                    "fatals",
                    "rejections",
                    "lostToStore",
                    "health",
                    "bottleneck",
                    "eventsPerSec",
                    "heapUsedMb",
                    "heapMaxMb",
                    "sockets",
                    "socketCeiling",
                    "servingMs",
                    "series",
                    "at",
                    "heapPct",
                )

        assertEquals(emptySet(), SyncVocabulary.TERMS.keys - known, "a term for nothing")
    }

    @Test
    fun `the three meanings of done are named apart`() {
        // The core of the complaint: one word covered a leg that RETURNED, a
        // walk that SETTLED, and the span every kind has EVIDENCE for — and the
        // first, which is the least meaningful, was being read as progress.
        val terms = SyncVocabulary.TERMS

        assertTrue(terms["returned"]!!.jsonPrimitive.content.contains("not progress"))
        assertTrue(terms["settled"]!!.jsonPrimitive.content.contains("Nothing outstanding"))
        assertTrue(terms["evidence"]!!.jsonPrimitive.content.contains("not a coverage claim"))
        // And the fourth thing none of them is.
        assertTrue(terms["holdings"]!!.jsonPrimitive.content.contains("NOT PUBLISHED HERE"))
    }

    @Test
    fun `approximations say they are approximations`() {
        assertTrue(
            SyncVocabulary.TERMS["frame"]!!
                .jsonPrimitive.content
                .startsWith("APPROXIMATE"),
        )
        assertTrue(
            SyncVocabulary.TERMS["returned"]!!
                .jsonPrimitive.content
                .startsWith("APPROACH ONLY"),
        )
    }

    @Test
    fun `the two not-dialled-for-being-dead states state opposite retry policies`() {
        // They were one number called "skipped as dead", which answered "will it
        // try again, and when" in two opposite ways under one label.
        val struck = SyncVocabulary.TERMS["hostStruckOut"]!!.jsonPrimitive.content
        val dead = SyncVocabulary.TERMS["knownDead"]!!.jsonPrimitive.content

        assertTrue(struck.contains("next cycle"), "the cycle-local one says so: $struck")
        assertTrue(dead.contains("TTL"), "the durable one says how long: $dead")
        assertTrue(dead.contains("hostStruckOut"), "and points at the one it is not")
    }

    @Test
    fun `the fold says where the per-url answer actually lives`() {
        // `/stats.json` publishes a bounded summary; the full per-url verdict is
        // a signed record in the store, and a reader has to be told that rather
        // than concluding the information does not exist.
        val fold = SyncVocabulary.TERMS["foldedOnto"]!!.jsonPrimitive.content

        assertTrue(fold.contains("30166"), "got: $fold")
        assertTrue(fold.contains("omitted"), "a truncated list has to disclose the truncation: $fold")
    }

    @Test
    fun `a stream-scoped count says it is stream-scoped`() {
        // One relay settled under one stream and open under another is not a
        // contradiction, and the document has to say why before a reader files
        // it as one.
        assertTrue(
            SyncVocabulary.TERMS["scope"]!!
                .jsonPrimitive.content
                .contains("per STREAM"),
        )
    }
}
