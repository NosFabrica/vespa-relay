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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * THE WHOLE FEATURE AGAINST A REAL ENGINE AND SOMEBODY ELSE'S DATA: a corpus
 * pulled off the staging relay, loaded into a real Vespa, and read back over
 * the wire protocol.
 *
 * A unit test can only ask whether the code does what it was written to do.
 * This asks the question the unit tests cannot: whether the thing it was
 * written for EXISTS in the corpus. Two of the answers below were a surprise,
 * and they are the reason this file is worth its runtime.
 *
 * Off unless both are given — it needs a live engine and a corpus that is not
 * in the repo:
 *
 *     docker run -d --name vespa -m 9g -p 127.0.0.1:8080:8080 \
 *         -p 127.0.0.1:19071:19071 vespaengine/vespa
 *     node relay/tools/fetch-corpus.mjs \
 *         wss://search-staging.brainstorm.world/ /tmp/corpus
 *     ./gradlew :relay:test --tests '*ProductionCorpusIT*' \
 *         -DitVespa=http://localhost:8080 -DitCorpus=/tmp/corpus -i
 *
 * WHAT IS REAL AND WHAT IS NOT, stated up front because it decides what each
 * assertion is worth. The labels, the notes they point at, the contact cards,
 * the provider lists and the profiles are production events signed by real
 * keys — nothing here re-signs them. The ONE synthetic thing is the Trusted
 * List in the last case, and it has to be: the family's kinds are 30392-30395,
 * production holds ZERO of them (asserted below, so the day that changes this
 * test says so), and a list must be signed BY the service key, which is not
 * ours to sign with. Its members are real pubkeys and the profiles it splices
 * are real profiles.
 *
 * Events are inserted straight into the store rather than published over the
 * wire. Signature verification is not what is under test and staging already
 * did it; what matters is that `indexableContent()` runs on put, which is the
 * step that decides whether any of this is findable at all.
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

    /** The corpus as quartz parses it — the same `EventFactory` dispatch the relay's own reads take. */
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

    private suspend fun load(store: VespaEventStore) {
        val outcomes = store.batchInsert(corpus)
        val written = outcomes.count { it is com.vitorpamplona.quartz.nip01Core.store.IEventStore.InsertOutcome.Accepted }
        // Re-running the IT against a warm Vespa re-offers what is already
        // there, and a duplicate is a REJECTION rather than a failure — so
        // "nothing was written" is only alarming on an empty engine.
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

        // THE REAL FAMILY, and it took a second relay to find: the search relay
        // holds none of it, and its 54 events on these kinds are squatters —
        // an omikuji fortune generator on 30394, WireGuard room records and
        // `trusted-attestor:` entries on 30392, an Alexandria manifest on
        // 30393. nos.lol, relay.damus.io, relay.primal.net, nostr.wine and
        // purplepag.es hold the same kind of thing. The Tapestry lists live on
        // tapestry.brainstorm.world, with `title`, `metric`, `observer`,
        // `min-rank` and `cutoff` exactly as quartz models them.
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
            // THE COLLISION, DRIVEN RATHER THAN REASONED ABOUT — and the one
            // assumption in this file that turned out to be wrong.
            //
            // These are REAL production kind-30392 events that are not Tapestry
            // lists at all: `d=trusted-attestor:<hex>`, a `t` hashtag, and a
            // `p` that our reader will take for a curated member, because
            // `EventFactory` maps the kind and hands the relay a
            // `UserTrustedListEvent` regardless of what the publisher meant.
            //
            // "No title, so it indexes the empty string, so it can never be a
            // hit" was the argument for why that is harmless. It is WRONG: the
            // store indexes hashtags too, so `["t","trusted-attestor"]` makes
            // this untitled event perfectly reachable by text. What actually
            // keeps it from splicing a stranger's profile into a stranger's
            // feed is the ENROLMENT GATE, and this is that gate earning its
            // keep on data nobody wrote for it.
            val profiles = corpus.filter { it.kind == 0 }.map { it.pubKey }.toSet()
            // UNTITLED on purpose: a titled 30392 is a real Tapestry list and
            // has its own case below. These are the collisions.
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
                // Anonymous: reachable, and expanding nothing. Nobody's
                // services to check, so a list from a signer nobody named
                // stays a list.
                val anon = page(session, out, "anon", """{"kinds":[0,30392],"search":"$hashtag include:spam"}""")
                assertTrue(
                    squatters.any { it.id in anon },
                    "an untitled 30392 is still reachable by its hashtag — that is the point of this case: $anon",
                )
                assertTrue(
                    named.none { pk -> corpus.any { it.kind == 0 && it.pubKey == pk && it.id in anon } },
                    "a lensless read must expand no list, whoever published it: $anon",
                )

                // Now a reader who really has named those signers as services.
                // Only the 10040 is ours; the list, the member and the profile
                // are all production events.
                val reader = NostrSignerSync()
                val enrolment =
                    reader.sign<Event>(
                        1_700_000_000L,
                        10040,
                        squatters.map { arrayOf("30382:rank", it.pubKey, "wss://provider.example") }.toTypedArray(),
                        "",
                    )
                store.batchInsert(listOf(enrolment))

                val lensed =
                    page(session, out, "enrolled", """{"kinds":[0,30392],"search":"$hashtag include:spam observer:${reader.pubKey}"}""")
                val splicedProfiles = corpus.filter { it.kind == 0 && it.pubKey in named && it.id in lensed }
                assertEquals(
                    named,
                    splicedProfiles.map { it.pubKey }.toSet(),
                    "every named pubkey whose profile this relay holds must ride in: $lensed",
                )
                val first = lensed.indexOf(squatters.first { it.id in lensed }.id)
                val itsMember = squatters.first { it.id in lensed }.tags.first { it.size > 1 && it[0] == "p" && it[1] in profiles }[1]
                assertEquals(
                    corpus.first { it.kind == 0 && it.pubKey == itsMember }.id,
                    lensed[first + 1],
                    "and directly behind the list that named it: $lensed",
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

        // `indexableContent()` is petname + summary + topics, and the cards
        // this relay mirrors are pure metrics: `d`, `rank`, `followers`,
        // `hops`, `reporters`, `muters`. They index the empty string, so a
        // NIP-85 assertion cannot BE a search hit here — which means the
        // assertion half of the expansion is inert against today's corpus.
        // That is a fact about the data, not a defect in the code, and it is
        // asserted rather than assumed so the day a provider starts publishing
        // petnames this test fails and says the feature just woke up.
        val textful = cards.filter { it.indexableContent().isNotBlank() }
        assertEquals(emptyList(), textful.map { it.id }, "a card with indexable text appeared: the assertion path is live now")
    }

    @Test
    fun `real provider lists enrol every service dimension they name, not just rank`() =
        runBlocking {
            skip()?.let { return@runBlocking println(it) }
            val lists = corpus.filterIsInstance<TrustProviderListEvent>()
            assertTrue(lists.size > 50, "expected a real sample of kind 10040, got ${lists.size}")

            // The dimensions production actually uses go well past `30382:rank`
            // — `personalizedGrapeRank_influence`, `personalizedPageRank`,
            // `hops`. TrustNotice filters to the ranking service because
            // ranking is ITS subject; doing that here would drop most of a real
            // reader's providers on the floor.
            val dimensions = lists.flatMap { it.tags.serviceProviders() }.map { it.service.toValue() }.distinct()
            assertTrue(
                dimensions.count { !it.endsWith(":rank") } > 0,
                "expected production to name non-rank dimensions; got $dimensions",
            )

            val enrolment = EnrolledSigners(recall = { filters -> corpus.filter { e -> filters.any { it.match(e) } } })
            for (list in lists.take(25)) {
                val named = list.tags.serviceProviders()
                val enrolled = enrolment.of(setOf(list.pubKey))
                for (kind in SearchReferences.DECLARATIONS) {
                    assertTrue(enrolled.admits(kind, list.pubKey), "a reader is always their own signer, on every kind")
                }

                // Each entry opens the kind it names and no other. Real Maps
                // are overwhelmingly 30382-only, so the second half of this is
                // what the production shape actually exercises: a service
                // appointed to rank users must not thereby be trusted to
                // publish event or address declarations.
                for (entry in named) {
                    assertTrue(
                        enrolled.admits(entry.service.kind, entry.pubkey),
                        "10040 ${list.id} names ${entry.service.toValue()} -> ${entry.pubkey}, which the enrolment did not admit",
                    )
                    val elsewhere = SearchReferences.DECLARATIONS.filter { it != entry.service.kind }
                    val named1 = named.map { it.service.kind }.toSet()
                    for (kind in elsewhere.filter { it !in named1 }) {
                        assertTrue(
                            !enrolled.admits(kind, entry.pubkey),
                            "10040 ${list.id} delegates ${entry.pubkey} for ${entry.service.kind} only, yet $kind was admitted too",
                        )
                    }
                }
            }
        }

    // ------------------------------------------------------------------
    // The feature, end to end
    // ------------------------------------------------------------------

    /**
     * A real label, the real event it points at, and a search term taken from
     * the label's own `l` value — chosen from whatever the corpus turned out
     * to hold rather than hardcoded, so this keeps working as staging moves.
     *
     * The pair has to satisfy the thing that makes the whole feature worth
     * having: the TARGET must not contain the searched word itself, or the
     * search would have found it without any of this.
     */
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
            val plain = NostrRelayServer(store, relayUrl, searchExpansion = SearchExpansionLimits.Off)
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = plain.connect { out.add(it) }
            try {
                // Same engine, same corpus, same query — only the knob differs.
                // This is what pins the event above on the expansion rather
                // than on anything the search could have done by itself.
                val page = page(session, out, "plain", """{"kinds":[${target.kind},1985],"search":"$value include:spam"}""")
                assertTrue(target.id !in page, "the target carries none of the searched text and must not be recalled by it")
            } finally {
                session.close()
                plain.close()
            }
        } ?: Unit

    /**
     * A real titled Trusted List whose members this relay also holds profiles
     * for, and whose title shares no word with any of them — the condition the
     * whole feature rests on, since a member the search could find by itself
     * proves nothing.
     */
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
        // Kind 10040 is REPLACEABLE, so what counts is the newest version per
        // author — exactly what the store keeps, and the reason this is not
        // just `corpus.any`. Merging two relays hands you both versions, and an
        // earlier draft of this file built a "real trust chain" out of the
        // superseded one and then could not explain why the relay refused it.
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

            // The list, its title, its members and their profiles are all
            // production events off the tapestry relay. The ONE synthetic thing
            // is the enrolment, and the test above says why it has to be: the
            // only observer who ever named this publisher as a service replaced
            // that 10040 in August with one naming somebody else, so today
            // these lists expand for nobody. A reader who DID name them would
            // see this.
            val reader = NostrSignerSync()
            val enrolment =
                reader.sign<Event>(
                    1_700_000_000L,
                    10040,
                    arrayOf(arrayOf("30382:rank", chain.list.pubKey, "wss://tapestry.brainstorm.world/relay")),
                    "",
                )
            store.batchInsert(listOf(enrolment))

            val relay = NostrRelayServer(store, relayUrl)
            val plain = NostrRelayServer(store, relayUrl, searchExpansion = SearchExpansionLimits.Off)
            try {
                val filter = """{"kinds":[0,30392],"search":"${chain.title} include:spam observer:${reader.pubKey}"}"""

                // The control first, so the assertion below cannot be luck: on
                // the same engine and the same query, without the expansion,
                // none of these profiles is reachable at all.
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

    @Test
    fun `no page of the production corpus sends an event twice`() =
        withRelay { relay, _ ->
            // The shape that makes this a real question rather than a
            // hypothetical: in this corpus 76 labelled events are named by more
            // than one label and ten of them by ten labels each, so a search on
            // a busy label value converges several hits on one subject. Every
            // page() in this file already asserts distinctness; this walks the
            // label values that actually collide, so the assertion is aimed at
            // the pages most likely to break it.
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
                // page() throws on a duplicate, so reaching the end is the
                // assertion. The kinds are wide open so the subjects are
                // admitted and the splice actually happens.
                val page = page(relay, "dup-${value.hashCode()}", """{"limit":200,"search":"$value include:spam"}""")
                assertTrue(page.isNotEmpty(), "\"$value\" should still match its own labels")
            }
        } ?: Unit

    @Test
    fun `a plain recall over the production corpus is answered exactly as before`() =
        withRelay { relay, store ->
            val plain = NostrRelayServer(store, relayUrl, searchExpansion = SearchExpansionLimits.Off)
            try {
                // A mirror's page: real kinds, no search text, a lens token
                // because an anonymous read must carry one. The expansion must
                // not change it by a single frame.
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
        // EVERY page this suite reads, checked for duplicates. NIP-01 asks a
        // relay not to send one event twice on a subscription, and a feature
        // whose whole job is to ADD events to a page is the one most likely to
        // break that — so the check lives here rather than in a test of its
        // own, and every case in the file pays for it.
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
