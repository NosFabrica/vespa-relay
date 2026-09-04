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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.engine.query.EventYql
import com.nosfabrica.vespa.eventstore.mapping.DEFAULT_MIN_RANK
import com.nosfabrica.vespa.eventstore.mapping.INCLUDE_SPAM_MIN_RANK
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The whole relay stack driven over the wire: quartz's engine, ObserverBackend, NostrSemanticsStore
 * and a recording in-memory index, fed raw NIP-01 JSON through [NostrRelayServer.connect].
 */
class RelayProtocolTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /** Records each SEARCH query's ranking context (observer, profile, trust floor). */
    private class RecordingIndex : EventIndex {
        val inner = InMemoryEventIndex()
        val searchObservers = Collections.synchronizedList(mutableListOf<String?>())
        val searchQueries = Collections.synchronizedList(mutableListOf<EventQuery>())

        override suspend fun get(id: String) = inner.get(id)

        override suspend fun put(doc: EventDoc) = inner.put(doc)

        override suspend fun remove(id: String) = inner.remove(id)

        override suspend fun search(query: EventQuery): List<EventDoc> {
            if (query.search != null || query.ranking != null) {
                searchObservers += query.observer
                searchQueries += query
            }
            return inner.search(query)
        }

        override suspend fun count(query: EventQuery) = inner.count(query)

        // Delegated, not the interface default: the default answers by search(), which this records.
        override suspend fun countByAuthor(query: EventQuery) = inner.countByAuthor(query)

        override fun close() {}
    }

    private val index = RecordingIndex()
    private val store = NostrSemanticsStore(index, relay = relayUrl)
    private val server = NostrRelayServer(store, relayUrl)
    private val signer = NostrSignerSync()

    @AfterTest
    fun tearDown() {
        server.close()
    }

    @Test
    fun `publishes, answers full filters, and streams live events`() =
        runBlocking {
            val bob = "b2".repeat(32)
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // A live subscription with a full NIP-01 filter, not just a search term.
                session.receive("""["REQ","sub",{"kinds":[1],"#p":["$bob"],"search":"include:spam"}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","sub"]""") }

                val note = signer.sign<Event>(1_700_000_000L, 1, arrayOf(arrayOf("p", bob)), "hi bob")
                session.receive("""["EVENT",${note.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${note.id}",true""") }
                awaitMessage(out) { it.startsWith("""["EVENT","sub",""") && note.id in it }

                session.receive("""["REQ","q2",{"kinds":[1],"authors":["${signer.pubKey}"],"#p":["$bob"],"since":1699999999,"search":"include:spam"}]""")
                awaitMessage(out) { it.startsWith("""["EVENT","q2",""") && note.id in it }
                awaitMessage(out) { it.startsWith("""["EOSE","q2"]""") }

                session.receive("""["COUNT","c1",{"kinds":[1],"search":"include:spam"}]""")
                val count = awaitMessage(out) { it.startsWith("""["COUNT","c1"""") }
                assertTrue("\"count\":1" in count, "exact count from the store: $count")
            } finally {
                session.close()
            }
        }

    @Test
    fun `forged publishes are rejected before the store`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val real = signer.sign<Event>(1_700_000_000L, 1, emptyArray(), "genuine")
                val forged = Event(real.id, real.pubKey, real.createdAt, real.kind, real.tags, "tampered", real.sig)
                session.receive("""["EVENT",${forged.toJson()}]""")
                val ok = awaitMessage(out) { it.startsWith("""["OK","${forged.id}"""") }
                assertTrue(""","false,""" in ok || """,false,""" in ok, "VerifyPolicy must reject: $ok")
                assertEquals(
                    0,
                    store.count(
                        com.vitorpamplona.quartz.nip01Core.relay.filters
                            .Filter(kinds = listOf(1)),
                    ),
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `an anonymous search has no observer and NIP-42 auth supplies one`() =
        runBlocking {
            // A typed MetadataEvent, because search_text derives from the typed event.
            store.insert(MetadataEvent("4".repeat(64), "a1".repeat(32), 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))

            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')

                // Anonymous means the whole corpus, not a house lens: `include:spam` is what the relay
                // requires of an anonymous read, and no observer is conjured for it.
                session.receive("""["REQ","s1",{"kinds":[0],"search":"ali include:spam","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","s1"]""") }
                assertTrue(out.any { it.startsWith("""["EVENT","s1",""") && "alice" in it }, "the stored kind-0 streams back: $out")
                assertTrue(
                    index.searchObservers.filterNotNull().isEmpty(),
                    "an anonymous search carries no observer: ${index.searchObservers}",
                )

                val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                session.receive("""["REQ","s2",{"kinds":[0],"search":"ali","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","s2"]""") }
                assertEquals(signer.pubKey, index.searchObservers.last(), "an authenticated user searches through their OWN web of trust")
            } finally {
                session.close()
            }
        }

    /**
     * The store parses `sort:`/`filter:rank:`/`include:spam`/`observer:` itself, which only works
     * if `search` reaches it verbatim through the whole websocket path.
     */
    @Test
    fun `NIP-50 extensions survive the session to the engine query`() =
        runBlocking {
            store.insert(MetadataEvent("5".repeat(64), "a2".repeat(32), 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // Signed in first: an anonymous read would have to carry `include:spam`, which is one of the tokens under test.
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
                val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                // A kindless searching read also issues a declaration companion, so `last()` may read that
                // instead; the caller's own query is the one with no kinds.
                fun asked() = index.searchQueries.last { it.kinds.isEmpty() }

                session.receive("""["REQ","x1",{"search":"ali","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","x1"]""") }
                assertEquals(DEFAULT_MIN_RANK, asked().minRank, "a plain search is trust-gated by default")

                // The companion may only name declaration kinds signed by someone this reader enrolled; the
                // authors line is what holds the gate. 30385 and 30395 are absent because their members are NIP-73 external ids.
                val declarationKinds = setOf(30382, 30383, 30384, 30392, 30393, 30394, 30000, 39089)

                // Selected by shape, not position: the companion goes out with the caller's query, so which the
                // index records last is a scheduling detail. Every kinded query is checked so no ordering hides one.
                val companions = index.searchQueries.filter { it.kinds.isNotEmpty() }
                assertTrue(companions.isNotEmpty(), "a kindless searching read still issues its declaration companion")
                for (companion in companions) {
                    assertTrue(
                        companion.kinds.all { it in declarationKinds },
                        "the extra read a kindless search now makes is for declaration kinds only: ${companion.kinds}",
                    )
                    assertEquals(listOf(signer.pubKey), companion.authors, "and only from signers this reader enrolled — here, themselves")
                    assertEquals(INCLUDE_SPAM_MIN_RANK, companion.minRank, "its floor is waived on purpose: a service key nobody follows signs the lists it looks for")
                }

                session.receive("""["REQ","x2",{"search":"ali include:spam","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","x2"]""") }
                // `include:spam` sends a floor of 0 rather than omitting one: the floor anchors the trust boost,
                // whose schema default is fail-open, so leaving it out would wreck the ordering.
                assertEquals(
                    INCLUDE_SPAM_MIN_RANK,
                    asked().minRank,
                    "include:spam keeps every hit, and still sends the floor the boost anchors on",
                )
                assertEquals("ali", asked().search, "the extension itself never becomes a term")

                session.receive("""["REQ","x3",{"search":"ali sort:rank filter:rank:gte:7","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","x3"]""") }
                assertEquals(EventYql.RANK_DESC, asked().ranking, "sort:rank picks the profile")
                assertEquals(7.0, asked().minRank, "filter:rank:gte sets the floor")

                // Chronological is the order this path can get wrong in silence: quartz strips `key:value`
                // extensions before the terms, so a store that did not know `recent` would answer in relevance order.
                session.receive("""["REQ","x4",{"search":"ali sort:recent","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","x4"]""") }
                assertEquals(EventYql.RANK_RECENCY_GATED, asked().ranking, "sort:recent picks the gated recency profile")
                assertEquals("ali", asked().search, "and the words still recall — only the ORDER changed")
            } finally {
                session.close()
            }
        }

    @Test
    fun `an authenticated search enrolls the observer through the hook`() =
        runBlocking {
            val enrolled = Collections.synchronizedList(mutableListOf<String>())
            val hooked = NostrRelayServer(store, relayUrl, onObserver = { enrolled.add(it) })
            try {
                val out = Collections.synchronizedList(mutableListOf<String>())
                val session = hooked.connect { out.add(it) }
                try {
                    val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')

                    // Anonymous searches never enroll anyone, declared or not.
                    session.receive("""["REQ","s1",{"kinds":[0],"search":"ali include:spam","limit":10}]""")
                    awaitMessage(out) { it.startsWith("""["EOSE","s1"]""") }
                    assertEquals(emptyList(), enrolled.toList())

                    val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
                    session.receive("""["AUTH",${auth.toJson()}]""")
                    awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                    session.receive("""["REQ","s2",{"kinds":[0],"search":"ali","limit":10}]""")
                    awaitMessage(out) { it.startsWith("""["EOSE","s2"]""") }
                    assertEquals(listOf(signer.pubKey), enrolled.distinct(), "the login becomes a sync observer")
                } finally {
                    session.close()
                }
            } finally {
                hooked.close()
            }
        }

    /**
     * Asserts the wiring only: a verified AUTH reaches the hook with the pubkey that signed it and
     * this connection's send. Which notices a store earns is [TrustNoticeTest]'s job.
     */
    @Test
    fun `signing in reaches the login hook with the connection to answer on`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val hooked = NostrRelayServer(store, relayUrl, onAuthenticated = TrustNotice(store, scope)::check)
            try {
                val out = Collections.synchronizedList(mutableListOf<String>())
                val session = hooked.connect { out.add(it) }
                try {
                    val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
                    val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
                    session.receive("""["AUTH",${auth.toJson()}]""")

                    // The OK is quartz's answer to the AUTH frame; the notices arrive behind it off another scope.
                    awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }
                    val notices = awaitNotices(out, 1)
                    assertTrue(notices.any { "10040" in it }, "an empty store holds no trust provider list for this reader: $notices")
                } finally {
                    session.close()
                }
            } finally {
                hooked.close()
                scope.cancel()
            }
        }

    /** Every NOTICE this connection has been sent, once [count] of them have. */
    private fun awaitNotices(
        out: List<String>,
        count: Int,
    ): List<String> {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            val seen = synchronized(out) { out.filter { it.startsWith("""["NOTICE"""") } }
            if (seen.size >= count) return seen
            Thread.sleep(20)
        }
        fail("timed out waiting for $count NOTICE(s); got: $out")
    }

    /**
     * The search page's hashtag REQ, end to end: `#t`, `#l` and kind-1111 `#I`/`#i` filters. Pins three
     * beliefs about upstream code: `#I` survives the REQ parse uppercase, tag values compare cased,
     * and the union of one REQ's filters is deduped.
     */
    @Test
    fun `the search page's hashtag union reaches the right events`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val at = 1_700_000_100L

                suspend fun publish(
                    kind: Int,
                    tags: Array<Array<String>>,
                    content: String,
                ): Event {
                    val ev = signer.sign<Event>(at, kind, tags, content)
                    session.receive("""["EVENT",${ev.toJson()}]""")
                    awaitMessage(out) { it.startsWith("""["OK","${ev.id}",true""") }
                    return ev
                }

                val tagged = publish(1, arrayOf(arrayOf("t", "nostr")), "tagged the topic")
                val taggedCased = publish(1, arrayOf(arrayOf("t", "Nostr")), "tagged it in mixed case")
                val labelled = publish(1, arrayOf(arrayOf("L", "#t"), arrayOf("l", "nostr")), "labelled itself")
                val rootScope = publish(1111, arrayOf(arrayOf("I", "#nostr"), arrayOf("K", "#")), "a reply deep in the thread")
                val parentScope = publish(1111, arrayOf(arrayOf("i", "#nostr"), arrayOf("k", "#")), "a comment on the topic")
                val otherTopic = publish(1111, arrayOf(arrayOf("i", "#bitcoin"), arrayOf("k", "#")), "a comment on something else")
                val both = publish(1, arrayOf(arrayOf("t", "nostr"), arrayOf("l", "nostr")), "tagged AND labelled")

                // Byte for byte the shape shared/query.js builds for "#nostr".
                session.receive(
                    """["REQ","h",""" +
                        """{"#t":["nostr","Nostr","NOSTR"],"search":"include:spam","limit":40},""" +
                        """{"#l":["nostr","Nostr","NOSTR"],"search":"include:spam","limit":10},""" +
                        """{"kinds":[1111],"#I":["#nostr","nostr"],"search":"include:spam","limit":10},""" +
                        """{"kinds":[1111],"#i":["#nostr","nostr"],"search":"include:spam","limit":10}]""",
                )
                awaitMessage(out) { it.startsWith("""["EOSE","h"]""") }
                val served = synchronized(out) { out.filter { it.startsWith("""["EVENT","h",""") } }

                for (
                (ev, why) in
                listOf(
                    tagged to "the `t` tag",
                    taggedCased to "a MIXED CASE `t` tag — cased matching means the spellings are the ask",
                    labelled to "a NIP-32 `l` label",
                    rootScope to "an UPPERCASE `I` tag: #I is not folded into #i anywhere in the chain",
                    parentScope to "a lowercase `i` tag",
                    both to "tagged and labelled at once",
                )
                ) {
                    assertTrue(served.any { ev.id in it }, "the union must serve the event matched by $why")
                }
                assertEquals(1, served.count { both.id in it }, "an event answering two filters is served once")
                assertTrue(served.none { otherTopic.id in it }, "a comment on another topic is not in this union")
                assertEquals(6, served.size, "exactly the six events the union describes")

                // The control for the spellings: the lowercase ask alone cannot see `t: Nostr`.
                session.receive("""["REQ","lc",{"#t":["nostr"],"search":"include:spam","limit":40}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","lc"]""") }
                val lower = synchronized(out) { out.filter { it.startsWith("""["EVENT","lc",""") } }
                assertTrue(lower.any { tagged.id in it }, "the lowercase ask sees the lowercase tag")
                assertTrue(lower.none { taggedCased.id in it }, "…and is blind to `t: Nostr`: tag values compare cased")
            } finally {
                session.close()
            }
        }

    /**
     * The search page's NIP-73 scope REQ, end to end: two kind-1111 filters, the id in `#I` and `#i`.
     * Pins that a tag value carrying colons and slashes matches byte for byte, and that the `kinds`
     * gate keeps a kind 1 wearing the same tag out. Both url spellings are the page's own ask.
     */
    @Test
    fun `the search page's scope filters reach the comments on the id`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val at = 1_700_000_100L

                suspend fun publish(
                    kind: Int,
                    tags: Array<Array<String>>,
                    content: String,
                ): Event {
                    val ev = signer.sign<Event>(at, kind, tags, content)
                    session.receive("""["EVENT",${ev.toJson()}]""")
                    awaitMessage(out) { it.startsWith("""["OK","${ev.id}",true""") }
                    return ev
                }

                val url = "https://example.com/article"
                val topLevel =
                    publish(
                        1111,
                        arrayOf(arrayOf("I", url), arrayOf("K", "web"), arrayOf("i", url), arrayOf("k", "web")),
                        "a top-level comment carries the scope in both tags",
                    )
                val deepReply =
                    publish(1111, arrayOf(arrayOf("I", url), arrayOf("K", "web")), "a reply deep in the thread: root only")
                val slashSpelled =
                    publish(1111, arrayOf(arrayOf("I", "$url/"), arrayOf("K", "web")), "the same page, spelled with the slash")
                val parentOnly =
                    publish(
                        1111,
                        arrayOf(arrayOf("I", "https://example.com/"), arrayOf("K", "web"), arrayOf("i", url), arrayOf("k", "web")),
                        "rooted elsewhere; the page is only its parent",
                    )
                val otherPage =
                    publish(1111, arrayOf(arrayOf("I", "https://example.com/other"), arrayOf("K", "web")), "a comment on another page")
                val notAComment =
                    publish(1, arrayOf(arrayOf("I", url)), "a kind 1 wearing the same tag")
                val onPodcast =
                    publish(
                        1111,
                        arrayOf(arrayOf("I", "podcast:guid:c90e609a-df1e-596a-bd5e-57bcc8aad6cc"), arrayOf("K", "podcast:guid")),
                        "a comment on a podcast feed",
                    )

                // Byte for byte the shape shared/query.js builds for
                // "site:https://example.com/article".
                session.receive(
                    """["REQ","sc",""" +
                        """{"kinds":[1111],"#I":["$url","$url/"],"search":"include:spam","limit":40},""" +
                        """{"kinds":[1111],"#i":["$url","$url/"],"search":"include:spam","limit":10}]""",
                )
                awaitMessage(out) { it.startsWith("""["EOSE","sc"]""") }
                val served = synchronized(out) { out.filter { it.startsWith("""["EVENT","sc",""") } }

                for (
                (ev, why) in
                listOf(
                    topLevel to "the root scope tag",
                    deepReply to "a reply that names the page only as its thread's root",
                    slashSpelled to "the trailing-slash spelling — the ask carries both, tag values are bytes",
                    parentOnly to "a comment whose PARENT is the page: the #i filter's whole reason",
                )
                ) {
                    assertTrue(served.any { ev.id in it }, "the scope filters must serve the event matched by $why")
                }
                assertEquals(1, served.count { topLevel.id in it }, "an event answering both filters is served once")
                assertTrue(served.none { otherPage.id in it }, "a comment on another page is not in this ask")
                assertTrue(served.none { notAComment.id in it }, "the kinds gate is real: a kind 1 wearing the tag stays out")
                assertEquals(4, served.size, "exactly the four comments the filters describe")

                // The prefixed families ride the same two filters; what this adds is a colon-laden value.
                session.receive(
                    """["REQ","pg",""" +
                        """{"kinds":[1111],"#I":["podcast:guid:c90e609a-df1e-596a-bd5e-57bcc8aad6cc"],"search":"include:spam","limit":40},""" +
                        """{"kinds":[1111],"#i":["podcast:guid:c90e609a-df1e-596a-bd5e-57bcc8aad6cc"],"search":"include:spam","limit":10}]""",
                )
                awaitMessage(out) { it.startsWith("""["EOSE","pg"]""") }
                val podcast = synchronized(out) { out.filter { it.startsWith("""["EVENT","pg",""") } }
                assertTrue(podcast.any { onPodcast.id in it }, "a colon-laden id survives the REQ parse and matches byte for byte")
                assertEquals(1, podcast.size, "…and nothing else answers it")
            } finally {
                session.close()
            }
        }

    /**
     * The search page's `group:<id>` REQ, end to end. Pins that `h` is an indexable tag, that an `#h`
     * ask with no `kinds` reaches chat, threads and replies, that the id matches cased, and that two
     * hosts' 39000 records stay apart while their posts are one set.
     */
    @Test
    fun `the search page's group filters reach the posts and the group record`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val at = 1_700_000_100L

                suspend fun publish(
                    kind: Int,
                    tags: Array<Array<String>>,
                    content: String,
                    with: NostrSignerSync = signer,
                ): Event {
                    val ev = with.sign<Event>(at, kind, tags, content)
                    session.receive("""["EVENT",${ev.toJson()}]""")
                    awaitMessage(out) { it.startsWith("""["OK","${ev.id}",true""") }
                    return ev
                }

                val chat = publish(9, arrayOf(arrayOf("h", "chachi")), "a chat line in the group")
                val thread = publish(11, arrayOf(arrayOf("h", "chachi")), "a thread in the group")
                val reply = publish(1111, arrayOf(arrayOf("h", "chachi")), "a reply in the group")
                val casedId = publish(9, arrayOf(arrayOf("h", "Chachi")), "a chat line in a DIFFERENTLY CASED group")
                val elsewhere = publish(9, arrayOf(arrayOf("h", "zaps")), "a chat line in another group")

                // Byte for byte the shape shared/query.js builds for
                // "group:chachi" with no tab kinds.
                session.receive(
                    """["REQ","gp",""" +
                        """{"#h":["chachi"],"search":"include:spam","limit":40},""" +
                        """{"kinds":[39000],"#d":["chachi"],"search":"include:spam","limit":10}]""",
                )
                awaitMessage(out) { it.startsWith("""["EOSE","gp"]""") }
                val served = synchronized(out) { out.filter { it.startsWith("""["EVENT","gp",""") } }

                for (
                (ev, why) in
                listOf(
                    chat to "kind 9, NIP-29's chat",
                    thread to "kind 11, a thread",
                    reply to "kind 1111, a reply inside the group",
                )
                ) {
                    assertTrue(served.any { ev.id in it }, "the group filters must serve the event matched by $why")
                }
                assertTrue(served.none { elsewhere.id in it }, "another group's chat is not in this ask")
                assertTrue(served.none { casedId.id in it }, "`h` values compare CASED: `Chachi` is a different group")
                assertEquals(3, served.size, "exactly the three posts the filter describes")

                // Two relays each sign a `chachi`; both records are stored, because a 39000 is addressable per (kind, pubkey, d).
                val hostA = NostrSignerSync()
                val hostB = NostrSignerSync()
                val recordA = publish(39000, arrayOf(arrayOf("d", "chachi"), arrayOf("name", "Chachi on A")), "", hostA)
                val recordB = publish(39000, arrayOf(arrayOf("d", "chachi"), arrayOf("name", "Chachi on B")), "", hostB)

                session.receive("""["REQ","gm",{"kinds":[39000],"#d":["chachi"],"search":"include:spam","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","gm"]""") }
                val records = synchronized(out) { out.filter { it.startsWith("""["EVENT","gm",""") } }
                assertTrue(records.any { recordA.id in it }, "one host's record")
                assertTrue(records.any { recordB.id in it }, "…and the other's: addressable replacement is keyed on the AUTHOR too")
                assertEquals(2, records.size, "one id, two hosts, two records — which is what the picker can warn about")
            } finally {
                session.close()
            }
        }

    /**
     * Asserted through the ordering the in-memory engine can produce: it reports no scores, so the
     * store merges on recency. The tag filter holds the newest and the oldest, the label filter the
     * one between; concatenation can only put the label's hit last.
     */
    @Test
    fun `a ranked union is served as one order, not one run per filter`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                suspend fun publish(
                    at: Long,
                    tags: Array<Array<String>>,
                ): Event {
                    val ev = signer.sign<Event>(at, 1, tags, "a note about the topic")
                    session.receive("""["EVENT",${ev.toJson()}]""")
                    awaitMessage(out) { it.startsWith("""["OK","${ev.id}",true""") }
                    return ev
                }

                val oldestTagged = publish(1_700_000_100L, arrayOf(arrayOf("t", "nostr")))
                val labelled = publish(1_700_000_200L, arrayOf(arrayOf("L", "#t"), arrayOf("l", "nostr")))
                val newestTagged = publish(1_700_000_300L, arrayOf(arrayOf("t", "nostr")))

                session.receive(
                    """["REQ","r",""" +
                        """{"#t":["nostr"],"search":"topic include:spam","limit":40},""" +
                        """{"#l":["nostr"],"search":"topic include:spam","limit":10}]""",
                )
                awaitMessage(out) { it.startsWith("""["EOSE","r"]""") }
                val served = synchronized(out) { out.filter { it.startsWith("""["EVENT","r",""") } }

                val order = listOf(newestTagged, labelled, oldestTagged).map { ev -> served.indexOfFirst { ev.id in it } }
                assertTrue(order.none { it < 0 }, "every event of the ranked union is served")
                assertEquals(order.sorted(), order, "one order over the union: the label's hit lands BETWEEN the two tagged ones")
            } finally {
                session.close()
            }
        }

    /**
     * The refusal is `auth-required:` rather than an empty EOSE: that is the prefix NIP-42 clients retry
     * through, and an empty answer is indistinguishable from an empty corpus. Neither way past it
     * needs a signature.
     */
    @Test
    fun `an undeclared read is refused before AUTH, and the two declarations get through`() =
        runBlocking {
            store.insert(MetadataEvent("6".repeat(64), "a3".repeat(32), 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // A plain NIP-01 filter is a read with no lens just as much as a search is.
                session.receive("""["REQ","n1",{"kinds":[0],"limit":10}]""")
                val closed = awaitMessage(out) { it.startsWith("""["CLOSED","n1",""") }
                assertTrue("auth-required:" in closed, "the machine-readable prefix a NIP-42 client retries through: $closed")
                assertTrue("observer:" in closed && "include:spam" in closed, "…and the refusal names both ways past it: $closed")

                session.receive("""["REQ","n2",{"kinds":[0],"search":"ali","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["CLOSED","n2",""") }

                // COUNT is a read too: an ungated count reports the size of the corpus a gated REQ would refuse.
                session.receive("""["COUNT","n3",{"kinds":[0]}]""")
                awaitMessage(out) { it.startsWith("""["CLOSED","n3",""") }

                // One undeclared filter poisons the REQ: NIP-01 ORs them.
                session.receive("""["REQ","n4",{"kinds":[0],"search":"include:spam"},{"kinds":[1]}]""")
                awaitMessage(out) { it.startsWith("""["CLOSED","n4",""") }

                // Way out one: waive the lens. The whole corpus, unranked.
                session.receive("""["REQ","y1",{"kinds":[0],"search":"ali include:spam","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","y1"]""") }
                assertTrue(out.any { it.startsWith("""["EVENT","y1",""") && "alice" in it }, "the waiver is answered in full: $out")

                // Way out two: name one. The store resolves it as the query's own observer.
                session.receive("""["REQ","y2",{"kinds":[0],"search":"ali observer:${"7".repeat(64)}","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","y2"]""") }
                assertEquals("7".repeat(64), index.searchQueries.last().observer, "the named lens is the one the store ranks through")

                // A lens the store cannot resolve is not one: an npub is ignored there.
                session.receive("""["REQ","n5",{"kinds":[0],"search":"ali observer:npub1qqqqqqqq","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["CLOSED","n5",""") }
            } finally {
                session.close()
            }
        }

    /**
     * Quartz's `NegSessionRegistry` runs a NEG-OPEN's filters through the same hook as a REQ; nothing
     * here decides that. Only the refusal is driven over the wire: an admitted NEG-OPEN reaches quartz's
     * negentropy session, which spins forever on a frame that is not a well-formed negentropy message.
     */
    @Test
    fun `a negentropy session declares a lens like any other read`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                session.receive("""["NEG-OPEN","g1",{"kinds":[1],"limit":10},"6100"]""")
                val err = awaitMessage(out) { it.startsWith("""["NEG-ERR","g1",""") }
                assertTrue("auth-required:" in err, "an undeclared reconcile is refused before it is parsed: $err")
            } finally {
                session.close()
            }
        }

    /** The admitted half, asserted on the policy alone: a peer that waives a lens gets through, so this relay stays mirrorable. */
    @Test
    fun `a declared reconcile is admitted by the policy`() {
        val policy = LensRequiredPolicy()
        val declared = ReqCmd("g2", listOf(Filter(kinds = listOf(1), search = "include:spam")))
        val bare = ReqCmd("g3", listOf(Filter(kinds = listOf(1))))
        assertTrue(policy.accept(declared) is PolicyResult.Accepted, "a reconcile that waives a lens gets through")
        assertTrue(policy.accept(bare) is PolicyResult.Rejected, "…and one that declares nothing does not")
    }

    /** After AUTH the connection IS the lens, and nothing has to be declared. */
    @Test
    fun `an authenticated read needs no declaration`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
                session.receive("""["REQ","before",{"kinds":[0],"limit":10}]""")
                awaitMessage(out) { it.startsWith("""["CLOSED","before",""") }

                val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                // The same undeclared read, now answered: the "authenticate and ask again" half of NIP-42.
                session.receive("""["REQ","after",{"kinds":[0],"limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","after"]""") }
                session.receive("""["COUNT","cafter",{"kinds":[0]}]""")
                awaitMessage(out) { it.startsWith("""["COUNT","cafter"""") }
            } finally {
                session.close()
            }
        }

    /** `REQUIRE_READ_LENS=false` is a deployment (a mirror-only relay, a store with no trust data), not a debug switch. */
    @Test
    fun `the gate can be turned off, and then an undeclared read is answered`() =
        runBlocking {
            val open = NostrRelayServer(store, relayUrl, requireReadLens = false)
            try {
                store.insert(MetadataEvent("8".repeat(64), "a4".repeat(32), 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))
                val out = Collections.synchronizedList(mutableListOf<String>())
                val session = open.connect { out.add(it) }
                try {
                    session.receive("""["REQ","o1",{"kinds":[0],"limit":10}]""")
                    awaitMessage(out) { it.startsWith("""["EOSE","o1"]""") }
                    assertTrue(out.none { it.startsWith("""["CLOSED","o1",""") }, "nothing was refused: $out")
                } finally {
                    session.close()
                }
            } finally {
                open.close()
            }
        }

    private fun awaitMessage(
        out: List<String>,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }
}
