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
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import java.io.File
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * ONE REAL READER'S TRUST CHAIN, END TO END: a production kind-10040, the
 * Trusted Lists the publisher it delegates actually signed, and the profiles
 * those lists name — loaded into a real Vespa and read back over the wire.
 *
 * This is the case [SearchReferenceExpansionTest] can only simulate. Its
 * fixture signs its own Map and its own lists, so it proves the code does what
 * it was written to do; it cannot prove the shape it was written for is the
 * shape publishers use. Here nothing is signed by us — the Map, the lists and
 * the profiles are production events, and the corpus is walked FROM the Map
 * rather than swept by kind, so every event in it is one this reader can be
 * shown to have asked for.
 *
 * WHAT MAKES THIS READER THE INTERESTING ONE: their Map delegates 30392 with a
 * generic bare-kind entry — `["30392", <pubkey>, <relay>]`, the Tapestry ADR's
 * shape — and NOTHING else names that publisher. A bare kind carries no `:`,
 * so NIP-85's `ServiceProviderTag` has never parsed one, and a gate reading
 * only `serviceProviders()` resolves this reader's list delegations to the
 * empty set. Their lists then come back as bare hits with nothing spliced
 * behind them, silently, with no error anywhere. So the first case below is
 * not only "the feature works": it is the one shape of real delegation that
 * would have failed, on the real Map that carries it.
 *
 * Off unless both are given — it needs a live engine and a corpus that is not
 * in the repo:
 *
 *     docker run -d --name vespa -m 9g -p 127.0.0.1:8080:8080 \
 *         -p 127.0.0.1:19071:19071 vespaengine/vespa
 *     node relay/tools/fetch-observer-corpus.mjs /tmp/obs <observer-hex>
 *     ./gradlew :relay:test --tests '*ObserverTrustListIT*' \
 *         -DitVespa=http://localhost:8080 -DitCorpus=/tmp/obs -i
 *
 * The observer is read from the corpus rather than hardcoded, so pointing the
 * fetch at a different reader re-targets the whole file.
 */
class ObserverTrustListIT {
    private val vespa = System.getProperty("itVespa")
    private val corpusDir = System.getProperty("itCorpus")
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    private fun skip(): String? =
        when {
            vespa == null || corpusDir == null -> "OBSERVER-IT skipped — needs -DitVespa and -DitCorpus (see the KDoc)"
            !File(corpusDir, "corpus.jsonl").isFile -> "OBSERVER-IT skipped — no corpus.jsonl in $corpusDir"
            else -> null
        }

    private val corpus: List<Event> by lazy {
        File(corpusDir!!, "corpus.jsonl").readLines().filter { it.isNotBlank() }.map { Event.fromJson(it) }
    }

    /** The Map is the corpus's root: the one kind-10040 the walk started from. */
    private val map: Event by lazy { corpus.single { it.kind == 10040 } }

    private val observer: HexKey get() = map.pubKey

    private val lists: List<Event> by lazy { corpus.filter { it.kind == 30392 } }

    private fun titleOf(list: Event): String =
        list.tags
            .firstOrNull { it.size > 1 && it[0] == "title" }
            ?.get(1)
            .orEmpty()

    private fun membersOf(list: Event): List<HexKey> = list.tags.filter { it.size > 1 && it[0] == "p" }.map { it[1] }

    /** The kind-0s the corpus actually holds, by author — a named member without one cannot be spliced. */
    private val profiles: Map<HexKey, Event> by lazy { corpus.filter { it.kind == 0 }.associateBy { it.pubKey } }

    /**
     * THE LIST THIS FILE IS ABOUT, chosen from the corpus rather than named:
     * the most-populated one whose title appears in NONE of its members'
     * profiles. That last condition is the whole point of the feature — if a
     * member's profile contained the searched words, plain search would have
     * found it and the splice would prove nothing.
     */
    private val subject: Event by lazy {
        lists
            .filter { list ->
                val words = titleOf(list).split(' ').filter { it.length > 3 }.map { it.lowercase() }
                val named = membersOf(list).mapNotNull { profiles[it] }
                words.isNotEmpty() &&
                    named.isNotEmpty() &&
                    named.none { p -> words.any { w -> p.toJson().lowercase().contains(w) } }
            }.maxByOrNull { membersOf(it).count { m -> m in profiles } }
            ?: fail("no list in the corpus has members whose profiles do not already contain its title")
    }

    private fun <T> withRelay(block: suspend (NostrRelayServer) -> T): T? {
        skip()?.let {
            println(it)
            return null
        }
        return VespaEventStore.open(vespa!!, relay = relayUrl, autoDeploy = true).use { store ->
            runBlocking {
                val written = store.batchInsert(corpus).count { it is IEventStore.InsertOutcome.Accepted }
                // Re-running against a warm Vespa re-offers what is already
                // there, and a duplicate is a rejection rather than a failure.
                println("OBSERVER-IT corpus: ${corpus.size} events, $written newly written")
                val relay = NostrRelayServer(store, relayUrl)
                try {
                    block(relay)
                } finally {
                    relay.close()
                }
            }
        }
    }

    @Test
    fun `the corpus is a real chain and the Map delegates its lists by bare kind`() {
        skip()?.let { return println(it) }

        // Every entry, both shapes, exactly as the relay's own gate splits them.
        val entries = map.tags.filter { it.size > 1 && it[1].length == 64 }.map { it[0] }
        val bareList = entries.filter { !it.contains(':') && it.toIntOrNull() in 30392..30395 }
        println("OBSERVER-IT observer ${observer.take(12)}… map ${map.id.take(12)}… entries $entries")
        println("OBSERVER-IT lists ${lists.size}, subject \"${titleOf(subject)}\" with ${membersOf(subject).size} members")

        assertTrue(bareList.isNotEmpty(), "this file is about the bare-kind shape; the Map carries none: $entries")
        assertTrue(lists.isNotEmpty(), "the delegated publisher signed no 30392 the corpus could fetch")

        // Nothing here is ours. If a re-fetch ever lands on a corpus we signed,
        // every assertion below stops meaning anything.
        assertTrue(lists.all { it.pubKey != observer }, "a list signed by the reader would pass the gate without any delegation")
    }

    @Test
    fun `the reader's own search unpacks the list its Map delegated`() {
        withRelay { relay ->
            val title = titleOf(subject)
            val expected = membersOf(subject).filter { it in profiles }.toSet()

            val page = page(relay, "mine", """{"kinds":[0,30392],"search":"$title include:spam observer:$observer"}""")
            val spliced = page.toSet() - lists.map { it.id }.toSet()
            val wanted = expected.map { profiles.getValue(it).id }.toSet()

            assertTrue(subject.id in page, "the search must find the list itself: $page")
            assertEquals(
                wanted,
                spliced intersect wanted,
                "every member profile the corpus holds must ride in behind the list; missing ${wanted - spliced}",
            )
            println("OBSERVER-IT \"$title\": ${page.size} frames, ${spliced.size} spliced, ${wanted.size} member profiles expected")
        }
    }

    @Test
    fun `a reader who delegated nobody gets the list and none of its members`() {
        withRelay { relay ->
            val title = titleOf(subject)
            // A pubkey with no 10040 in this corpus: the same search, the same
            // list, and nothing behind it. This is the gate, and it is what
            // makes the case above a result rather than a coincidence.
            val stranger = "b7".repeat(32)
            val page = page(relay, "stranger", """{"kinds":[0,30392],"search":"$title include:spam observer:$stranger"}""")
            val listIds = lists.map { it.id }.toSet()

            assertTrue(page.isNotEmpty(), "the search itself must still answer: $page")
            assertEquals(
                emptySet(),
                page.toSet() - listIds,
                "a stranger's read must splice nothing; it spliced ${page.toSet() - listIds}",
            )
        }
    }

    @Test
    fun `a plain recall of the same list splices nothing`() {
        withRelay { relay ->
            // No `search` on the filter, so no expansion at all — the reader is
            // asking for lists, not for a feed, and the members stay behind
            // their own REQ. Same reader, same delegation, different question.
            val page = page(relay, "plain", """{"kinds":[30392],"authors":["${subject.pubKey}"],"search":"include:spam"}""")
            assertTrue(subject.id in page, "the plain recall must still serve the list: $page")
            assertEquals(
                emptySet(),
                page.toSet() - lists.map { it.id }.toSet(),
                "a filter carrying no search text must expand nothing",
            )
        }
    }

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
