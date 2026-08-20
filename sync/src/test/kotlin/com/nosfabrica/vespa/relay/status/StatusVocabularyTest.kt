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

/**
 * The glossary against the numbers it is a glossary FOR.
 *
 * The failure this pins is silent and inevitable otherwise: a new count is
 * published, nobody adds a term, and the section is back to numbers that cannot
 * be read without this repository open beside them — which is the whole
 * complaint. So the terms are checked against the members the other two reports
 * actually emit, in both directions.
 */
class StatusVocabularyTest {
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
        // The mirror's own document, parsed rather than re-projected: nothing
        // filters these members on their way to the page any more, so a member
        // with no term really does reach a reader — which is what this asserts.
        val progress =
            Json
                .parseToJsonElement(
                    """
                    {"fatals": 0,
                     "health": {"bottleneck": "ingest", "eventsPerSec": 2350, "heapUsedMb": 900, "heapMaxMb": 2048,
                                "sockets": 412, "socketCeiling": 1024, "servingMs": 18},
                     "streams": [{"name": "content", "phase": "rotating", "phaseForSec": 5,
                     "roster": 412, "tails": 300,
                     "inFlight": {"relays": [{"relay": "wss://slow.example/", "heldForSec": 41400,
                                              "transferringForSec": 41390, "events": 2, "quietForSec": 41000,
                                               "doing": "catching up (paging)", "pool": "catching-up",
                                               "pagingUntil": 1689857148}],
                                  "omitted": 118}}],
                     "live": {"relays": [{"relay": "wss://nos.lol/", "heldForSec": 41400,
                                          "transferringForSec": 41400, "events": 91002, "quietForSec": 3,
                                          "doing": "holding a live tail", "pool": "live"}],
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
                        "unpageable": 1, "auth-refused": 1, "restricted": 1},
                       {"name": "visits", "phase": "rotating", "phaseForSec": 900,
                        "roster": 30, "awaitingVisit": 3, "visiting": 5, "tails": 22,
                        "visitsRun": 90, "auditing": 1, "auditsRun": 4, "auditsSkipped": 3, "retracted": 2, "abortedVisits": 2, "evictedTails": 1, "poolReceived": 4000},
                       {"name": "heal", "phase": "running", "phaseForSec": 900, "queued": 2, "dropped": 7, "pushed": 5}]}
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
        // A definition for an absent member is a promise the document does not
        // keep, and it is how a glossary rots into fiction.
        val known =
            selfDescribing +
                setOf(
                    // Names for CONCEPTS rather than for members — the three
                    // meanings of "done" that had to be told apart, plus the
                    // one deliberately NOT published here.
                    "scope",
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
                    // The other phase word that reads as a stall and is not —
                    // a visit stream has no pass to be a phase OF, so the word
                    // simply lasts and its numbers are the whole story.
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
                    // …and `doing`'s OTHER counterpart: the stable word four
                    // tables are grouped by, where `doing` is the sentence
                    // inside them.
                    "pool",
                    // The fourth of those tables, which is the only one that is
                    // a document member of its own — the tails are pool-wide,
                    // so they sit at the root rather than under a stream.
                    "live",
                    // …and `doing`'s counterpart on the other kind of held row:
                    // a probe leg is a ladder, not a transfer, so what it
                    // publishes is which STEP it is on.
                    "stage",
                    "pagingUntil",
                    "omitted",
                ) +
                // The passes beside a stream's current cycle, and the work that
                // is not a stream at all — the two probe passes, the NIP-66
                // monitor, ingest, the healer, the push.
                setOf(
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
                    "auditsSkipped",
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
                    "collected",
                    "collectedTotal",
                    "slotsFree",
                    "slotsNeeded",
                    "retryInSec",
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

        assertEquals(emptySet(), StatusVocabulary.TERMS.keys - known, "a term for nothing")
    }

    @Test
    fun `the meanings of done are named apart`() {
        // The core of the complaint: one word covered a walk that SETTLED and
        // the span every kind has EVIDENCE for. (It covered a third — a fan-out
        // leg that had RETURNED, the least meaningful of them and the one being
        // read as progress — until the fan-out itself went.)
        val terms = StatusVocabulary.TERMS

        assertTrue(terms["settled"]!!.jsonPrimitive.content.contains("Nothing outstanding"))
        assertTrue(terms["evidence"]!!.jsonPrimitive.content.contains("not a coverage claim"))
        // And the fourth thing none of them is.
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
        // One relay settled under one stream and open under another is not a
        // contradiction, and the document has to say why before a reader files
        // it as one.
        assertTrue(
            StatusVocabulary.TERMS["scope"]!!
                .jsonPrimitive.content
                .contains("per STREAM"),
        )
    }

    @Test
    fun `each document ships the definitions it needs and not the other plane's`() {
        // The property `termsFor` exists for. There are two status documents
        // now — the mirror's and the monitor's — and they publish disjoint
        // halves of one vocabulary. Shipping the whole map in both would put a
        // definition for `queued` in the monitor's document and one for
        // `foldedAway` in the mirror's, and a glossary listing members the
        // reader will not find is the way that promise rots into fiction.
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
        // Both still draw from ONE map: a member defined twice is two
        // definitions to keep in step, which is the state this replaced.
        assertTrue((forMirror + forMonitor).all { it in StatusVocabulary.TERMS.keys })
    }
}
