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
 *     node relay/src/test/resources/production-corpus-tool/fetch-corpus.mjs \
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
    fun `the Trusted List family is absent from production, so its half is exercised synthetically`() {
        skip()?.let { return println(it) }
        val lists = corpus.filter { it.kind in 30392..30395 }
        assertEquals(
            emptyList(),
            lists.map { it.id },
            "production has started publishing Trusted Lists — the synthetic case below can become a real one",
        )
    }

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
                val named =
                    list.tags
                        .serviceProviders()
                        .map { it.pubkey }
                        .toSet()
                val enrolled = enrolment.of(setOf(list.pubKey))
                assertTrue(list.pubKey in enrolled, "a reader is always their own signer")
                assertTrue(
                    enrolled.containsAll(named),
                    "10040 ${list.id} names $named; the enrolment resolved $enrolled",
                )
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

    @Test
    fun `a trusted list of real pubkeys splices their real profiles in`() =
        withRelay { _, store ->
            // The one synthetic pointer, for the reason the class KDoc gives.
            // Everything it names is real: the members are pubkeys whose kind-0
            // profiles came off staging, and the profiles are what has to come
            // back.
            val members = corpus.filter { it.kind == 0 }.take(3)
            assertTrue(members.size == 3, "need real profiles in the corpus to be pointed at")

            val service = NostrSignerSync()
            val reader = NostrSignerSync()
            val marker = "vespaitroster${System.nanoTime()}"
            val enrolment =
                reader.sign<Event>(
                    1_700_000_000L,
                    10040,
                    arrayOf(arrayOf("30382:rank", service.pubKey, "wss://provider.example")),
                    "",
                )
            val list =
                service.sign<Event>(
                    1_700_000_100L,
                    30392,
                    arrayOf(arrayOf("d", marker), arrayOf("title", marker)) + members.map { arrayOf("p", it.pubKey) },
                    "",
                )
            store.batchInsert(listOf(enrolment, list))

            val relay = NostrRelayServer(store, relayUrl)
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = relay.connect { out.add(it) }
            try {
                val lens = "include:spam observer:${reader.pubKey}"
                val page = page(session, out, "roster", """{"kinds":[0,30392],"search":"$marker $lens"}""")
                assertEquals(list.id, page.firstOrNull(), "the list is the hit: $page")
                assertEquals(
                    members.map { it.id }.toSet(),
                    page.drop(1).toSet(),
                    "every real member profile must ride in behind it",
                )

                // And not for a reader who never enrolled that service.
                val stranger = NostrSignerSync()
                val ungated =
                    page(session, out, "stranger", """{"kinds":[0,30392],"search":"$marker include:spam observer:${stranger.pubKey}"}""")
                assertEquals(listOf(list.id), ungated, "an unenrolled reader gets the list and nothing else")
            } finally {
                session.close()
                relay.close()
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
        return synchronized(out) { out.filter { it.startsWith(prefix) } }.map { frame ->
            ID.find(frame)?.groupValues?.get(1) ?: fail("no id in $frame")
        }
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
