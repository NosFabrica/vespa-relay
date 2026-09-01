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

    /**
     * THE CONTROL FOR THE FEATURE ITSELF: the same Vespa, the same documents,
     * the same query — read through a store opened with the splice OFF.
     *
     * A second STORE rather than a second relay, because the knob moved with
     * the feature: it is the store that splices now. Opened once and closed
     * with the class, since every case that wants it wants the same one.
     */
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

                // Now a reader who really has named ONE of those signers as a
                // service. Only the 10040 is ours; the list, the member and the
                // profile are all production events.
                //
                // ONE ENTRY, because the ADR allows one generic entry per kind
                // and a reader cannot delegate three publishers for 30392 —
                // which is the rule this half now exercises against real data.
                // A `30382:rank` entry would delegate nothing here: it appoints
                // a service to rank USERS, and the gate is per kind.
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

                // WHAT PLACEMENT ACTUALLY PROMISES, now that a member is placed
                // by the confidence its list expressed rather than glued behind
                // it. This used to assert `lensed[at + 1]` — the first member
                // directly behind its list — and that PASSED, which is the
                // trap: it holds only when nothing else scores between the
                // list and its top member. Across this corpus only 6 of 33
                // member-bearing lists score their first member at 100, the one
                // case where the tie-break guarantees adjacency, so the old
                // assertion was luck 27 times out of 33 and would have gone red
                // on a corpus refresh rather than on a regression.
                //
                // The two invariants that do hold, whatever the scores:
                //   - a subject never passes its own pointer (its score is the
                //     pointer's times a factor in 0..1, and a tie keeps the
                //     reason above the result);
                //   - members keep their confidence order among themselves,
                //     which for a descending-sorted list is the list's order.
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

            // WHAT THE GATE DOES WITH THESE IS THE STORE'S TEST NOW, and it is
            // white-box by nature: `Delegations` and `Enrolment` are internal to
            // vespa-eventstore, where `SearchExpansionTest` asserts per-kind
            // admission directly. What belongs HERE is the shape of the real
            // data those rules are aimed at — that production names dimensions
            // past `rank`, and that every entry `serviceProviders()` returns
            // names a kind inside NIP-85's own range, which is what makes "one
            // entry, one kind" a rule a gate can keep. `ObserverTrustListIT`
            // asserts the consequence over the wire on one real reader's chain.
            val kinds = lists.flatMap { it.tags.serviceProviders() }.map { it.service.kind }.distinct()
            assertTrue(
                kinds.isNotEmpty() && kinds.all { it in 30382..30385 },
                "a 10040 entry outside NIP-85's own kinds reached serviceProviders(): $kinds",
            )
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
                // DIRECTLY behind, and here that is principled rather than
                // lucky: NIP-32 has no confidence field, an unscored reference
                // is read as FULL confidence, so the subject inherits its
                // label's score exactly and a stable sort keeps the pair
                // together. The Trusted List half of this file cannot assert
                // adjacency for the opposite reason — see the comment there.
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
                    // The BARE-KIND entry, which is what delegates a Trusted
                    // List: `30382:rank` appoints a service to rank users and
                    // opens 30382 alone. This test used to synthesize the rank
                    // form and passed, because the gate was one flat set of
                    // admitted signers — it is per kind now, and this is what
                    // the reader would actually have had to publish.
                    arrayOf(arrayOf("30392", chain.list.pubKey, "wss://tapestry.brainstorm.world/relay")),
                    "",
                )
            store.batchInsert(listOf(enrolment))

            val relay = NostrRelayServer(store, relayUrl)
            val plain = plainRelay()
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

    /**
     * WHERE A MEMBER ACTUALLY LANDS, on somebody else's data — and the whole
     * point of the two store bumps behind it.
     *
     * `spliced_member` places a member at `max(member_rung(), pointer_floor())`.
     * The floor arrived first and was INERT, and this case is what measured
     * that: the two sides were in different units, because the rung was
     * multiplied by `wot_mult(MEMBER)` while `query(pointer_rel)` is the
     * pointer's finished relevance and carries `wot_mult(SIGNER)` — and a
     * Trusted List is signed by a NIP-85 service key nobody follows. One side
     * times 1, the other times up to 251 189. The rung won every time, so
     * members were placed four orders of magnitude above the page, ordered by
     * the READER's trust rather than by what the publisher said, and identically
     * whether the list matched on its title or barely at all.
     *
     * Store PR #93 dropped the trust term from the rung. Both sides are now in
     * the POINTER's units, so the floor binds and is bounded above by the
     * pointer itself. What this case pins, against real confidences and real
     * trust ranks:
     *
     *  - the block is ordered by what the PUBLISHER said, not by what the
     *    reader thinks of each member;
     *  - no member passes the list that names it;
     *  - and turning the floor off (`subjectFloorSpan = null`) visibly changes
     *    the answer — without that the first two could both hold for the wrong
     *    reason, which is exactly how the inert floor hid.
     *
     * That control is NOT the old placement and must not be read as one: with
     * the trust term gone from the rung, `subjectFloorSpan = null` leaves a
     * member on the bare 550..4000 band, which is neither what b2be07e168
     * served nor anything this relay ships. It is here to show the floor is
     * carrying the placement, nothing more.
     *
     * HOW BIG THE MOVE LOOKS depends entirely on what else is on the page, and
     * on this corpus it is one row: the reader\'s enrolled service publishes
     * FIVE near-identical copies of the same list, all at the name tier, so the
     * top of the page is lists and a member can at best tie them. The
     * confidence-100 member moves from #5 to #2 and the rest keep their
     * positions under the copies. The scale change underneath is the real
     * result and it is measured store-side, not here: 2.19e8..1.00e9 before,
     * 1.70e5..2.39e5 after, against a page spanning 610..2.39e5.
     *
     * Everything is production: the list, its title, its members, their scores,
     * their profiles and their trust ranks. Only the enrolment is synthesized,
     * for the reason the case above gives.
     */
    @Test
    fun `a member is placed by its publisher's confidence and never passes its list`() =
        withRelay { _, store ->
            val scored = scoredList() ?: return@withRelay println("PRODUCTION-IT no scored list with enough held member profiles")
            println(
                "PRODUCTION-IT scored list ${scored.list.id.take(12)} \"${scored.title}\" by ${scored.list.pubKey.take(8)}, " +
                    "${scored.held.size} held profiles, confidences ${scored.held.map { it.second }}",
            )

            // THE RANK PROVIDER IS PART OF THE SETUP, not a detail. Without one
            // every member is unranked, `wot_mult()` is the same constant for
            // all of them, and the trust term this case is about would be flat —
            // so the old placement would look correct and the case would report
            // a pass while meaning nothing by it. Pick the signer whose
            // assertions actually COVER these members: one that ranks 400
            // strangers and none of this list leaves trust flat exactly as if
            // nobody were enrolled.
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
            // The placement before the floor existed, on the same engine and the
            // same documents — the control that says the floor is doing the work.
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

                // READ DOWN THE PAGE AND THE CONFIDENCES NEVER RISE. This is the
                // claim the trust term used to break.
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

                // And the floor is what did it — the same query without it puts
                // the members somewhere else.
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

    /** The titled list whose members this corpus holds the most PROFILES for, with the score each carries. */
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
                        // A member whose own bio carries the title is a TEXT
                        // hit in its own right, so its placement says nothing
                        // about the splice.
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
            val plain = plainRelay()
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
