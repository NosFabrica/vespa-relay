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
 * One real reader's trust chain end to end: a production kind-10040, the Trusted Lists it delegates by
 * bare kind, and the profiles those lists name, loaded into a live Vespa and read back over the wire.
 * Asserts the delegated lists unpack for that reader and for nobody else. Selected by `-DitVespa=<url>`
 * and `-DitCorpus=<dir>`, a corpus written by `relay/tools/fetch-observer-corpus.mjs`.
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

    /** The kind-0s the corpus holds, by author; a named member without one cannot be spliced. */
    private val profiles: Map<HexKey, Event> by lazy { corpus.filter { it.kind == 0 }.associateBy { it.pubKey } }

    /**
     * The most-populated list whose title appears in none of its members' profiles,
     * so a spliced member cannot be a plain hit of the search.
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
                // A warm Vespa rejects the duplicates; only an empty engine writes everything.
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

        // Nothing here may be ours, or every assertion below stops meaning anything.
        assertTrue(lists.all { it.pubKey != observer }, "a list signed by the reader would pass the gate without any delegation")

        // The relay reads a member's key, never its score, so a spliced member's position means its rank
        // only because the publisher sorted the tags; a score outside quartz's 0..100 reads back as unscored.
        for (list in lists) {
            val scores =
                list.tags
                    .filter { it.size > 3 && it[0] == "p" }
                    .mapNotNull { it[3].toIntOrNull() }
            val members = list.tags.count { it.size > 1 && it[0] == "p" }
            if (members == 0) continue
            assertEquals(members, scores.size, "every member of ${list.id.take(12)}… must carry a score")
            assertTrue(scores.all { it in 0..100 }, "a score outside 0..100 reads back as unscored: $scores")
            assertEquals(scores.sortedDescending(), scores, "members must be ordered by score, best first: $scores")
        }
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
            // A pubkey with no 10040 in this corpus: the same search, the same list, nothing behind it.
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
            // No search text on the filter, so no expansion: same reader, same delegation, different question.
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
