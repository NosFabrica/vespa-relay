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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip32Labeling.LabelEvent
import com.vitorpamplona.quartz.nip50Search.SearchableEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.serviceProviders
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import java.io.File
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The splice, the gate and the placement against a real engine and a production corpus, read back
 * over the wire; only a kind-10040 enrolment is synthesized. Selected by `-DitVespa=<url>` and
 * `-DitCorpus=<dir>`, a corpus written by `relay/tools/fetch-corpus.mjs`.
 */
class ProductionCorpusIT {
    private val vespa = System.getProperty("itVespa")
    private val corpusDir = System.getProperty("itCorpus")
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private fun skip(): String? =
        when {
            vespa == null || corpusDir == null -> "PRODUCTION-IT skipped — needs -DitVespa and -DitCorpus (see the KDoc)"
            !File(corpusDir, "corpus.jsonl").isFile -> "PRODUCTION-IT skipped — no corpus.jsonl in $corpusDir"
            else -> null
        }

    /** The corpus as quartz parses it, through the same `EventFactory` dispatch the relay's reads take. */
    private val corpus: List<Event> by lazy {
        File(corpusDir!!, "corpus.jsonl").readLines().filter { it.isNotBlank() }.map { Event.fromJson(it) }
    }

    private fun <T> withRelay(block: suspend (NostrRelayServer, VespaEventStore) -> T): T? {
        skip()?.let {
            println(it)
            return null
        }
        return VespaEventStore.open(vespa!!, relay = relayUrl, autoDeploy = true).use { store ->
            runBlocking {
                load(store)
                val relay = NostrRelayServer(store, relayUrl)
                try {
                    block(relay, store)
                } finally {
                    relay.close()
                }
            }
        }
    }

    /** The control: the same Vespa read through a store opened with the splice off. */
    private var plainOpen: VespaEventStore? = null

    private fun plainRelay(): NostrRelayServer {
        val opened =
            plainOpen ?: VespaEventStore
                .open(vespa!!, relay = relayUrl, autoDeploy = false, searchExpansion = SearchExpansionLimits.Off)
                .also { plainOpen = it }
        return NostrRelayServer(opened, relayUrl)
    }

    @AfterTest
    fun closePlain() {
        plainOpen?.close()
        plainOpen = null
    }

    private suspend fun load(store: VespaEventStore) {
        val outcomes = store.batchInsert(corpus)
        val written = outcomes.count { it is com.vitorpamplona.quartz.nip01Core.store.IEventStore.InsertOutcome.Accepted }
        // A warm Vespa rejects the duplicates; only an empty engine writes everything.
        println("PRODUCTION-IT corpus: ${corpus.size} events, $written newly written")
    }

    // ------------------------------------------------------------------
    // What production actually holds
    // ------------------------------------------------------------------

    @Test
    fun `the Trusted List kinds carry both real lists and squatters, and the corpus needs two relays to see it`() {
        skip()?.let { return println(it) }
        val inRange = corpus.filter { it.kind in 30392..30395 }
        val titled = inRange.filter { e -> e.tags.any { it.size > 1 && it[0] == "title" && it[1].isNotBlank() } }
        val untitled = inRange - titled.toSet()
        println("PRODUCTION-IT kinds 30392-30395: ${inRange.size} events — ${titled.size} titled, ${untitled.size} untitled")

        // The search relay holds only squatters on these kinds; the titled family lives on the
        // tapestry relay, so the corpus needs both.
        assertTrue(titled.size > 100, "expected the tapestry relay's Trusted Lists in the corpus, got ${titled.size}")
        assertTrue(untitled.size > 10, "expected the search relay's squatters too, got ${untitled.size}")
        assertTrue(
            titled.all { e -> e.tags.any { it.size > 1 && it[0] == "metric" } },
            "a Tapestry list carries a `metric`; these do not look like the family",
        )
    }

    @Test
    fun `a real Trusted List kind event is searchable by hashtag, and expands only for a reader who enrolled its signer`() =
        withRelay { relay, store ->
            val profiles = corpus.filter { it.kind == 0 }.map { it.pubKey }.toSet()
            // Untitled 30392s, reachable by their indexed `t` hashtag; a titled one is a real Tapestry
            // list with its own case below.
            val squatters =
                corpus.filter { e ->
                    e.kind == 30392 &&
                        e.tags.none { it.size > 1 && it[0] == "title" && it[1].isNotBlank() } &&
                        e.tags.any { it.size > 1 && it[0] == "p" && it[1] in profiles }
                }
            if (squatters.isEmpty()) return@withRelay println("PRODUCTION-IT no 30392 naming a pubkey whose profile is in this corpus")
            val hashtag =
                squatters
                    .first()
                    .tags
                    .firstOrNull { it.size > 1 && it[0] == "t" }
                    ?.get(1)
                    ?: return@withRelay println("PRODUCTION-IT the 30392 squatters carry no hashtag in this corpus")
            val named = squatters.flatMap { e -> e.tags.filter { it.size > 1 && it[0] == "p" }.map { it[1] } }.filter { it in profiles }.toSet()
            println("PRODUCTION-IT squatters: ${squatters.size} on 30392, hashtag=$hashtag, naming ${named.size} pubkeys we hold profiles for")

            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = relay.connect { out.add(it) }
            try {
                // Anonymous: nobody's services to check, so no list expands.
                val anon = page(session, out, "anon", """{"kinds":[0,30392],"search":"$hashtag include:spam"}""")
                assertTrue(
                    squatters.any { it.id in anon },
                    "an untitled 30392 is still reachable by its hashtag — that is the point of this case: $anon",
                )
                assertTrue(
                    named.none { pk -> corpus.any { it.kind == 0 && it.pubKey == pk && it.id in anon } },
                    "a lensless read must expand no list, whoever published it: $anon",
                )

                // Only the 10040 is ours; the bare-kind entry is what delegates a Trusted List.
                val delegated = squatters.first()
                val reader = NostrSignerSync()
                val enrolment =
                    reader.sign<Event>(
                        1_700_000_000L,
                        10040,
                        arrayOf(arrayOf("30392", delegated.pubKey, "wss://provider.example")),
                        "",
                    )
                store.batchInsert(listOf(enrolment))

                val mine =
                    delegated.tags
                        .filter { it.size > 1 && it[0] == "p" && it[1] in profiles }
                        .map { it[1] }
                        .toSet()
                val theirs = named - mine

                val lensed =
                    page(session, out, "enrolled", """{"kinds":[0,30392],"search":"$hashtag include:spam observer:${reader.pubKey}"}""")
                val spliced = corpus.filter { it.kind == 0 && it.id in lensed }.map { it.pubKey }.toSet()
                assertEquals(
                    mine,
                    spliced intersect named,
                    "every pubkey the DELEGATED publisher names must ride in, and only those: $lensed",
                )
                assertEquals(
                    emptySet(),
                    spliced intersect theirs,
                    "a publisher this reader did not delegate splices nothing, however alike its events look: $lensed",
                )
                val at = lensed.indexOf(delegated.id)
                assertTrue(at >= 0, "the delegated list is still a hit of its own hashtag: $lensed")

                // Placement is by the list's confidence, not adjacency: a member never passes its pointer,
                // and members keep their confidence order among themselves.
                val memberIds =
                    delegated.tags
                        .filter { it.size > 1 && it[0] == "p" && it[1] in profiles }
                        .mapNotNull { tag -> corpus.firstOrNull { it.kind == 0 && it.pubKey == tag[1] }?.id }
                val placed = memberIds.filter { it in lensed }
                assertTrue(placed.isNotEmpty(), "the delegated list must place at least one member: $lensed")
                assertTrue(
                    placed.all { lensed.indexOf(it) > at },
                    "no member may pass the list that named it: list at $at, members at ${placed.map(lensed::indexOf)}",
                )
                assertEquals(
                    placed,
                    placed.sortedBy(lensed::indexOf),
                    "members ride in the order their list scored them: ${placed.map(lensed::indexOf)}",
                )
            } finally {
                session.close()
            }
        } ?: Unit

    @Test
    fun `production contact cards carry no indexable text, so no card can be a search hit`() {
        skip()?.let { return println(it) }
        val cards = corpus.filterIsInstance<ContactCardEvent>()
        assertTrue(cards.size > 100, "expected a real sample of kind 30382, got ${cards.size}")

        // Production cards are pure metrics and index the empty string, so the assertion half of the
        // expansion is inert; asserted so the day it wakes up this says so.
        val textful = cards.filter { it.indexableContent().isNotBlank() }
        assertEquals(emptyList(), textful.map { it.id }, "a card with indexable text appeared: the assertion path is live now")
    }

    @Test
    fun `real provider lists enrol every service dimension they name, not just rank`() =
        runBlocking {
            skip()?.let { return@runBlocking println(it) }
            val lists = corpus.filterIsInstance<TrustProviderListEvent>()
            assertTrue(lists.size > 50, "expected a real sample of kind 10040, got ${lists.size}")

            // Production names dimensions past `30382:rank`; TrustNotice filters to the ranking service.
            val dimensions = lists.flatMap { it.tags.serviceProviders() }.map { it.service.toValue() }.distinct()
            assertTrue(
                dimensions.count { !it.endsWith(":rank") } > 0,
                "expected production to name non-rank dimensions; got $dimensions",
            )

            // Per-kind admission is the store's test; here only that every entry names a NIP-85 kind.
            val kinds = lists.flatMap { it.tags.serviceProviders() }.map { it.service.kind }.distinct()
            assertTrue(
                kinds.isNotEmpty() && kinds.all { it in 30382..30385 },
                "a 10040 entry outside NIP-85's own kinds reached serviceProviders(): $kinds",
            )
        }

    // ------------------------------------------------------------------
    // The feature, end to end
    // ------------------------------------------------------------------

    /** A real label, its target, and a term from the label's `l` value the target does not contain. */
    private fun labelPair(): Triple<LabelEvent, Event, String>? {
        val byId = corpus.associateBy { it.id }
        for (label in corpus.filterIsInstance<LabelEvent>()) {
            val value = label.labels().map { it.label }.firstOrNull { it.length > 3 && it.any(Char::isLetter) } ?: continue
            val target = label.labeledEvents().firstNotNullOfOrNull(byId::get) ?: continue
            val text = ((target as? SearchableEvent)?.indexableContent() ?: target.content).lowercase()
            if (value.lowercase() !in text) return Triple(label, target, value)
        }
        return null
    }

    @Test
    fun `a real label brings back the real event it points at`() =
        withRelay { relay, _ ->
            val (label, target, value) = labelPair() ?: fail("no label in the corpus points at an event this relay holds")
            println("PRODUCTION-IT label ${label.id.take(12)} l=$value -> ${target.id.take(12)} (kind ${target.kind})")

            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = relay.connect { out.add(it) }
            try {
                val page = page(session, out, "label", """{"kinds":[${target.kind},1985],"search":"$value include:spam"}""")
                val at = page.indexOf(label.id)
                assertTrue(at >= 0, "the label itself must be a hit of its own label value: $value")
                // Directly behind: NIP-32 has no confidence field, an unscored reference reads as full
                // confidence, and a stable sort keeps the pair together.
                assertTrue(
                    page.getOrNull(at + 1) == target.id,
                    "the labelled event must follow its label; page around it: ${page.drop(maxOf(0, at - 1)).take(3)}",
                )
            } finally {
                session.close()
            }
        } ?: Unit

    @Test
    fun `and the same search cannot reach that event without the expansion`() =
        withRelay { _, store ->
            val (_, target, value) = labelPair() ?: fail("no usable label pair in the corpus")
            val plain = plainRelay()
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = plain.connect { out.add(it) }
            try {
                // Same engine, same corpus, same query; only the knob differs.
                val page = page(session, out, "plain", """{"kinds":[${target.kind},1985],"search":"$value include:spam"}""")
                assertTrue(target.id !in page, "the target carries none of the searched text and must not be recalled by it")
            } finally {
                session.close()
                plain.close()
            }
        } ?: Unit

    /** A titled list whose members' profiles this relay holds and whose title shares no word with them. */
    private fun realList(): Chain? {
        val profiles = corpus.filter { it.kind == 0 }.associateBy { it.pubKey }
        for (list in corpus.filter { it.kind == 30392 }) {
            val title = list.tags.firstOrNull { it.size > 1 && it[0] == "title" && it[1].isNotBlank() }?.get(1) ?: continue
            val words = title.lowercase().split(Regex("[^a-z0-9]+")).filter { it.length > 2 }
            if (words.isEmpty()) continue
            val members =
                list.tags
                    .filter { it.size > 1 && it[0] == "p" }
                    .mapNotNull { profiles[it[1]] }
                    .filter { profile -> words.none { it in profile.content.lowercase() } }
            if (members.isNotEmpty()) return Chain(list, title, members)
        }
        return null
    }

    private class Chain(
        val list: Event,
        val title: String,
        val members: List<Event>,
    )

    @Test
    fun `no reader currently enrols the Trusted List publisher, so the enrolment is the one thing synthesized`() {
        skip()?.let { return println(it) }
        val signers = corpus.filter { it.kind == 30392 && it.tags.any { t -> t.size > 1 && t[0] == "metric" } }.map { it.pubKey }.toSet()
        // Kind 10040 is replaceable, so only the newest version per author counts; a merge of two relays
        // hands back both.
        val current = corpus.filterIsInstance<TrustProviderListEvent>().groupBy { it.pubKey }.mapValues { (_, v) -> v.maxBy { it.createdAt } }
        val enrolling = current.values.filter { it.tags.serviceProviders().any { p -> p.pubkey in signers } }
        println("PRODUCTION-IT ${current.size} current provider lists; ${enrolling.size} of them enrol a Trusted List publisher")
        assertEquals(
            emptyList(),
            enrolling.map { it.id },
            "somebody's current 10040 now enrols the list publisher — the case below can drop its synthetic enrolment",
        )
    }

    @Test
    fun `a real Trusted List splices the real profiles of the members it names`() =
        withRelay { _, store ->
            val chain = realList() ?: return@withRelay println("PRODUCTION-IT no titled list with usable member profiles")
            println("PRODUCTION-IT list ${chain.list.id.take(12)} \"${chain.title}\" by ${chain.list.pubKey.take(12)}, ${chain.members.size} member profiles held")

            // The one synthetic event: no current 10040 enrols this publisher.
            val reader = NostrSignerSync()
            val enrolment =
                reader.sign<Event>(
                    1_700_000_000L,
                    10040,
                    // The bare-kind entry is what delegates a Trusted List; `30382:rank` opens 30382 alone.
                    arrayOf(arrayOf("30392", chain.list.pubKey, "wss://tapestry.brainstorm.world/relay")),
                    "",
                )
            store.batchInsert(listOf(enrolment))

            val relay = NostrRelayServer(store, relayUrl)
            val plain = plainRelay()
            try {
                val filter = """{"kinds":[0,30392],"search":"${chain.title} include:spam observer:${reader.pubKey}"}"""

                // The control first: without the expansion, none of these profiles is reachable at all.
                val without = page(plain, "plain", filter)
                assertTrue(chain.list.id in without, "the list is a hit of its own title either way: $without")
                assertEquals(
                    emptyList(),
                    chain.members.map { it.id }.filter { it in without },
                    "a member profile must not be recallable by the list's title on its own",
                )

                val withIt = page(relay, "chain", filter)
                assertEquals(
                    emptyList(),
                    chain.members.map { it.id }.filterNot { it in withIt },
                    "every held member profile must ride in behind the list that names it: $withIt",
                )
                assertTrue(
                    withIt.indexOf(chain.members.first().id) > withIt.indexOf(chain.list.id),
                    "a member follows its list, never precedes it: $withIt",
                )

                // And a reader who enrolled nobody gets the list alone.
                val stranger = NostrSignerSync()
                val ungated = page(relay, "stranger", """{"kinds":[0,30392],"search":"${chain.title} include:spam observer:${stranger.pubKey}"}""")
                assertTrue(chain.list.id in ungated, "the list is still served, whoever is reading")
                assertEquals(
                    emptyList(),
                    chain.members.map { it.id }.filter { it in ungated },
                    "but no member rides in for a reader who enrolled nobody",
                )
            } finally {
                relay.close()
                plain.close()
            }
        } ?: Unit

    /**
     * Turning the floor off (`subjectFloorSpan = null`) must visibly change the answer, or the two
     * ordering assertions could hold for the wrong reason.
     */
    @Test
    fun `a member is placed by its publisher's confidence and never passes its list`() =
        withRelay { _, store ->
            val scored = scoredList() ?: return@withRelay println("PRODUCTION-IT no scored list with enough held member profiles")
            println(
                "PRODUCTION-IT scored list ${scored.list.id.take(12)} \"${scored.title}\" by ${scored.list.pubKey.take(8)}, " +
                    "${scored.held.size} held profiles, confidences ${scored.held.map { it.second }}",
            )

            // Without a rank provider every member is unranked and the case passes while meaning
            // nothing, so pick the signer whose assertions cover these members.
            val wanted = scored.held.mapTo(HashSet()) { it.first.pubKey }
            val rankProvider =
                corpus
                    .filter { it.kind == 30382 }
                    .groupBy { it.pubKey }
                    .mapValues { (_, rows) -> rows.count { row -> row.tags.any { it.size > 1 && it[0] == "d" && it[1] in wanted } } }
                    .maxByOrNull { it.value }
                    ?.takeIf { it.value > 0 }
                    ?.key
                    ?: return@withRelay println("PRODUCTION-IT no 30382 signer ranks these members — trust would be flat, and the comparison would mean nothing")
            val reader = NostrSignerSync()
            store.batchInsert(
                listOf(
                    reader.sign<Event>(
                        1_700_000_000L,
                        10040,
                        arrayOf(
                            arrayOf("30392", scored.list.pubKey, "wss://tapestry.brainstorm.world/relay"),
                            arrayOf("30382:rank", rankProvider, "wss://nip85-staging.nosfabrica.com"),
                        ),
                        "",
                    ),
                ),
            )
            val covered = corpus.count { e -> e.kind == 30382 && e.pubKey == rankProvider && e.tags.any { it.size > 1 && it[0] == "d" && it[1] in wanted } }
            println("PRODUCTION-IT rank provider ${rankProvider.take(8)} ranks $covered of ${wanted.size} held members")

            val filter = """{"kinds":[0,30392],"search":"${scored.title} include:spam observer:${reader.pubKey}"}"""
            val floored = NostrRelayServer(store, relayUrl)
            // The control: the same engine with the floor off.
            val rungOnly =
                VespaEventStore.open(
                    vespa!!,
                    relay = relayUrl,
                    autoDeploy = false,
                    searchExpansion = SearchExpansionLimits(subjectFloorSpan = null),
                )
            val rungRelay = NostrRelayServer(rungOnly, relayUrl)
            try {
                val pages =
                    listOf("rung only (no floor)" to rungRelay, "floored" to floored).map { (label, relay) ->
                        val ids = page(relay, "cmp-${label.take(4)}", filter)
                        val seen =
                            scored.held
                                .map { (event, conf) -> conf to ids.indexOf(event.id) }
                                .filter { it.second >= 0 }
                                .sortedBy { it.second }
                        println("PRODUCTION-IT   $label: pointer #${ids.indexOf(scored.list.id)}, members (confidence -> position) $seen")
                        ids
                    }

                val ids = pages[1]
                val pointerAt = ids.indexOf(scored.list.id)
                assertTrue(pointerAt >= 0, "the list is a hit of its own title")

                val placed = scored.held.map { (event, conf) -> ids.indexOf(event.id) to conf }.filter { it.first >= 0 }
                assertTrue(placed.size >= 4, "need several held members to say anything about their order: $placed")

                assertEquals(
                    placed.sortedBy { it.first }.map { it.second },
                    placed.map { it.second }.sortedDescending(),
                    "down the page, a member's confidence never rises: $placed",
                )

                assertEquals(
                    emptyList(),
                    placed.filter { it.first < pointerAt },
                    "no member passes the list that names it (pointer at #$pointerAt): $placed",
                )

                // And the floor is what did it.
                assertTrue(
                    pages[0] != pages[1],
                    "turning the floor off must change the answer, or these assertions hold for some other reason: ${pages[0]}",
                )
            } finally {
                rungRelay.close()
                rungOnly.close()
                floored.close()
            }
        } ?: Unit

    /** The titled list whose members this corpus holds the most profiles for, with the score each carries. */
    private fun scoredList(): Scored? {
        val profiles = corpus.filter { it.kind == 0 }.associateBy { it.pubKey }
        return corpus
            .filter { it.kind == 30392 }
            .mapNotNull { list ->
                val title = list.tags.firstOrNull { it.size > 1 && it[0] == "title" && it[1].isNotBlank() }?.get(1) ?: return@mapNotNull null
                val words = title.lowercase().split(Regex("[^a-z0-9]+")).filter { it.length > 2 }
                if (words.isEmpty()) return@mapNotNull null
                val held =
                    list.tags
                        .filter { it.size > 3 && it[0] == "p" && it[3].toIntOrNull() != null }
                        .mapNotNull { tag -> profiles[tag[1]]?.let { it to tag[3].toInt() } }
                        // A member whose own bio carries the title is a text hit in its own right.
                        .filter { (profile, _) -> words.none { it in profile.content.lowercase() } }
                        .distinctBy { it.second }
                Scored(list, title, held)
            }.filter { it.held.size >= 4 }
            .maxByOrNull { it.held.size }
    }

    /** A titled list, and the members it scored whose profiles this corpus holds. */
    private class Scored(
        val list: Event,
        val title: String,
        val held: List<Pair<Event, Int>>,
    )

    @Test
    fun `no page of the production corpus sends an event twice`() =
        withRelay { relay, _ ->
            // Several labels naming one subject is the page most likely to send it twice.
            val byTarget = HashMap<String, MutableSet<String>>()
            for (label in corpus.filterIsInstance<LabelEvent>()) {
                val value = label.labels().map { it.label }.firstOrNull { it.length > 2 } ?: continue
                for (target in label.labeledEvents()) byTarget.getOrPut(target) { HashSet() }.add(value)
            }
            val contested =
                byTarget.values
                    .flatten()
                    .groupBy { it }
                    .entries
                    .sortedByDescending { it.value.size }
                    .map { it.key }
                    .distinct()
            assertTrue(contested.isNotEmpty(), "expected label values with targets in this corpus")
            println("PRODUCTION-IT checking ${minOf(contested.size, 8)} contested label values for duplicates")

            for (value in contested.take(8)) {
                // page() throws on a duplicate, so reaching the end is the assertion.
                val page = page(relay, "dup-${value.hashCode()}", """{"limit":200,"search":"$value include:spam"}""")
                assertTrue(page.isNotEmpty(), "\"$value\" should still match its own labels")
            }
        } ?: Unit

    @Test
    fun `a plain recall over the production corpus is answered exactly as before`() =
        withRelay { relay, store ->
            val plain = plainRelay()
            try {
                // A mirror's page: no search text, and a lens token because an anonymous read needs one.
                val filter = """{"kinds":[1985],"limit":50,"search":"include:spam"}"""
                val withIt = page(relay, "recall-on", filter)
                val without = page(plain, "recall-off", filter)
                assertEquals(without, withIt, "a termless recall is untouched by the expansion")
                assertTrue(withIt.isNotEmpty(), "and it returned a real page")
            } finally {
                plain.close()
            }
        } ?: Unit

    // ------------------------------------------------------------------

    private suspend fun page(
        relay: NostrRelayServer,
        subId: String,
        filter: String,
    ): List<String> {
        val out = Collections.synchronizedList(mutableListOf<String>())
        val session = relay.connect { out.add(it) }
        return try {
            page(session, out, subId, filter)
        } finally {
            session.close()
        }
    }

    private suspend fun page(
        session: RelaySession,
        out: List<String>,
        subId: String,
        filter: String,
    ): List<String> {
        session.receive("""["REQ","$subId",$filter]""")
        awaitMessage(out) { it.startsWith("""["EOSE","$subId"]""") }
        val prefix = """["EVENT","$subId","""
        val ids =
            synchronized(out) { out.filter { it.startsWith(prefix) } }.map { frame ->
                ID.find(frame)?.groupValues?.get(1) ?: fail("no id in $frame")
            }
        // Every page this suite reads is checked for duplicates.
        val twice = ids.groupBy { it }.filterValues { it.size > 1 }.keys
        assertEquals(emptySet(), twice, "sent twice on \"$subId\": $twice")
        return ids
    }

    private fun awaitMessage(
        out: List<String>,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + 60_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(10)
        }
        fail("timed out waiting for a matching relay message; got ${out.size} messages")
    }

    private companion object {
        val ID = Regex("\"id\":\"([0-9a-f]{64})\"")
    }
}
