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
 * The whole relay stack, driven over the wire protocol: Quartz's engine ->
 * ObserverBackend -> NostrSemanticsStore -> a recording in-memory index.
 * Sessions speak raw NIP-01 JSON through [NostrRelayServer.connect], exactly
 * what the websocket route feeds them.
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

        // Delegate rather than ride the interface default, which the store's
        // KDoc requires of a decorator: the default answers by search(), and
        // this one records every search it sees.
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
                // A live subscription with a full NIP-01 filter — not just a search term.
                session.receive("""["REQ","sub",{"kinds":[1],"#p":["$bob"],"search":"include:spam"}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","sub"]""") }

                // Publish a signed note (VerifyPolicy checks id + signature).
                val note = signer.sign<Event>(1_700_000_000L, 1, arrayOf(arrayOf("p", bob)), "hi bob")
                session.receive("""["EVENT",${note.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${note.id}",true""") }
                // ...and the open subscription sees it live.
                awaitMessage(out) { it.startsWith("""["EVENT","sub",""") && note.id in it }

                // A fresh REQ answers the same event from storage, through author + tag + time filters.
                session.receive("""["REQ","q2",{"kinds":[1],"authors":["${signer.pubKey}"],"#p":["$bob"],"since":1699999999,"search":"include:spam"}]""")
                awaitMessage(out) { it.startsWith("""["EVENT","q2",""") && note.id in it }
                awaitMessage(out) { it.startsWith("""["EOSE","q2"]""") }

                // NIP-45 COUNT over the stored set.
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
            // A searchable profile in the store (search_text derives from the typed event).
            store.insert(MetadataEvent("4".repeat(64), "a1".repeat(32), 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))

            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // The relay advertises NIP-42 on connect.
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')

                // Unauthenticated search: NO observer at all.
                //
                // This used to assert the operator's DEFAULT_OBSERVER, which was
                // right while the observer only reordered results and wrong once
                // the store began treating it as a filter: an anonymous visitor
                // would have been gated to the ~2.7% of profiles anyone has
                // scored, silently. Anonymous now means the whole corpus — and,
                // since LensRequiredPolicy, only when the query SAYS so. The
                // `include:spam` here is what the relay now requires of an
                // anonymous read; what it does NOT do is conjure a house lens,
                // which is what this test is about.
                session.receive("""["REQ","s1",{"kinds":[0],"search":"ali include:spam","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","s1"]""") }
                assertTrue(out.any { it.startsWith("""["EVENT","s1",""") && "alice" in it }, "the stored kind-0 streams back: $out")
                assertTrue(
                    index.searchObservers.filterNotNull().isEmpty(),
                    "an anonymous search carries no observer: ${index.searchObservers}",
                )

                // Authenticate with a real signed kind-22242, then search again.
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
     * NIP-50 extensions must survive the whole websocket path to the engine
     * query. The store parses `sort:`/`filter:rank:`/`include:spam`/`observer:`
     * itself, which only works if it receives `search` verbatim.
     *
     * The mechanism behind that has already changed once: quartz's engine used
     * to strip the extensions before the store, and the relay carried the
     * originals past it on the coroutine context. The IEventStore contract now
     * passes `search` through untouched and that workaround is gone. The test
     * is unchanged across both, which is the point — it asserts the property
     * the relay needs, not the arrangement that currently delivers it, so it
     * keeps working as the session-level net for future quartz bumps.
     */
    @Test
    fun `NIP-50 extensions survive the session to the engine query`() =
        runBlocking {
            store.insert(MetadataEvent("5".repeat(64), "a2".repeat(32), 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // Signed in first: these assertions are about what the store
                // makes of the TOKENS, and an anonymous read now has to carry
                // `include:spam` to be answered at all — which is itself one of
                // the tokens under test and would set the floor it asserts.
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
                val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                // THE QUERY THE REQ ASKED, which is no longer the last row the
                // engine saw. Every REQ below is kindless, and since store
                // 94be3000a1 a kindless searching read also issues a
                // DECLARATION COMPANION — the same terms re-aimed at the
                // Trusted List and Assertion kinds, authors narrowed to the
                // signers this reader enrolled (themselves, here), trust floor
                // waived. `last()` therefore reads that companion, and read the
                // floor assertion below as this relay having dropped its gate.
                // The kinds tell the two apart: the caller asked for none.
                fun asked() = index.searchQueries.last { it.kinds.isEmpty() }

                session.receive("""["REQ","x1",{"search":"ali","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","x1"]""") }
                assertEquals(DEFAULT_MIN_RANK, asked().minRank, "a plain search is trust-gated by default")

                // The companion itself, pinned rather than merely tolerated:
                // what makes its waived floor safe is that it can only name
                // declaration kinds signed by someone this reader enrolled, so
                // a bump that widened either half would land here. IT HAS: at
                // store 2bc79f5f40 the reader's own NIP-51 lists of people
                // joined the set, which is a different family reached by the
                // SAME gate — a reader is always their own signer, so their own
                // list unpacks and a stranger's titled `bitcoin` does not. The
                // two halves are asserted separately below for that reason, and
                // it is the AUTHORS line, not this one, that holds the gate.
                //
                // Spelled out rather than left as the 30382..30395 range it
                // used to be: the kinds no longer form one, and the property
                // this asserts was never "in that range" but "is a kind the
                // store expands a declaration from".
                // NIP-85 assertions (subject: a pubkey / an event / an address),
                // the Trusted Lists (by what their members are), and the two
                // NIP-51 people kinds. 30385 and 30395 are absent because their
                // members are NIP-73 external identifiers, which name no event.
                val declarationKinds = setOf(30382, 30383, 30384, 30392, 30393, 30394, 30000, 39089)
                val companion = index.searchQueries.last()
                assertTrue(
                    companion.kinds.isNotEmpty() && companion.kinds.all { it in declarationKinds },
                    "the extra read a kindless search now makes is for declaration kinds only: ${companion.kinds}",
                )
                assertEquals(listOf(signer.pubKey), companion.authors, "and only from signers this reader enrolled — here, themselves")
                assertEquals(INCLUDE_SPAM_MIN_RANK, companion.minRank, "its floor is waived on purpose: a service key nobody follows signs the lists it looks for")

                session.receive("""["REQ","x2",{"search":"ali include:spam","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","x2"]""") }
                // include:spam SENDS a floor of 0 rather than omitting one. The
                // floor is also the anchor of the default profile's trust boost
                // — log(1 + user_score - min_rank) — and the schema's fail-open
                // default for that feature is -1e9, so leaving it out would not
                // "no floor", it would wreck the ordering. 0 keeps every hit,
                // which is what the extension promises.
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

                // The sort menu's "Newest" (index.html). Chronological is the
                // one order this path can get wrong in SILENCE: quartz strips
                // every `key:value` extension before the terms, so a store that
                // does not know `recent` does not search for the literal and
                // does not complain — it just answers in relevance order under
                // a menu that says newest. Nothing in this repo parses the
                // token, so the pin is the only thing that decides, and this
                // asserts the profile rather than the results because the order
                // itself is the engine's (InMemoryEventIndex has no rank
                // profiles to run).
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

                    // Anonymous searches never enroll anyone — declared or not.
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
     * Signing in gets a reader an answer to the question the protocol gives
     * them no way to ask: whether this relay holds the two things their ranked
     * search depends on. The store treats the lens as a FILTER, so a reader
     * whose chain has not been mirrored here searches an empty relay and is
     * told nothing about it — see [TrustNotice].
     *
     * What this asserts is the WIRING, over the wire: that a verified AUTH
     * reaches the hook at all, carrying the pubkey that signed it and this
     * connection's send. Which notices a given store earns is
     * [TrustNoticeTest]'s job, and so is the silent case — an absence over a
     * socket is only ever a wait that has not finished.
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

                    // The login is not held up by the check: the OK is quartz's
                    // answer to the AUTH frame, and the notices arrive behind it
                    // off a scope that never touched this coroutine.
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
     * The search page's hashtag REQ, end to end — the assumptions the web UI's
     * four filters rest on, none of which this repo owns.
     *
     * A `#hashtag` in the search box becomes a union: `#t` for events tagged
     * with the topic, `#l` for NIP-32 labels, and kind 1111 with the NIP-73
     * external id in `#I` (a comment thread's root scope) or `#i` (its parent).
     * Three of those are load-bearing beliefs about code upstream of here:
     *
     *  - `#I` survives Quartz's REQ parse as an UPPERCASE tag name and is not
     *    folded into `#i`. NIP-01 allows a-zA-Z and Quartz's isIndexableTagName
     *    implements exactly that, but a fold anywhere in the chain would not
     *    fail loudly — the filter would quietly match the wrong events, which
     *    is worse than matching none.
     *  - Tag VALUES compare cased (the engine schema's `match { cased }`), so
     *    an event tagged `t: Nostr` is invisible to a `#t: ["nostr"]` ask. The
     *    page sends the spellings for this reason; asserted here so a future
     *    "normalize tags on write" would break this test rather than the feed.
     *  - The filters of one REQ are ORed and the union is deduped, so an event
     *    answering two of them is delivered once.
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

                // The control for the spellings: the lowercase ask ALONE cannot
                // see `t: Nostr`. This is the assertion that makes the extra
                // values in the filter above a fix rather than decoration.
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
     * The search page's NIP-73 scope REQ, end to end — `site:`, `isbn:`,
     * `doi:`, `podcast:guid:` and the rest become two kind-1111 filters, the
     * id in `#I` (a thread's root) and `#i` (a parent), and the beliefs they
     * rest on are the hashtag union's plus two of their own:
     *
     *  - A tag VALUE carrying colons and slashes — a whole url, or
     *    `podcast:guid:<uuid>` — survives Quartz's REQ parse and the store's
     *    tag matching byte for byte. Nothing in NIP-01 promises that; a parser
     *    that split tag values on `:` would quietly match nothing.
     *  - The `kinds` gate on the filter is real: a kind that is not 1111
     *    carrying the same `I` tag stays out, which is what lets the page send
     *    these filters under any tab without the tab's kinds on them.
     *
     * The two spellings of the url are the page's own ask (scopeIds toggles
     * the trailing slash — one page, two byte-distinct tag values), so a
     * comment written under either spelling has to come back.
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

                // The prefixed families ride the same two filters; what this
                // adds is the value itself being colon-laden.
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
     * The search page's `group:<id>` REQ, end to end — an `#h` filter for what
     * was posted in the group, and a kind-39000 keyed by `#d` for the group
     * itself. Four beliefs, and every one of them is a thing the page would
     * otherwise be taking on trust:
     *
     *  - **`h` is an indexable tag at all.** The store derives `tag_index`
     *    from SINGLE-LETTER tag names only, and `h` is one — so a group filter
     *    costs what a topic filter costs. Nothing outside the store says which
     *    letters it kept.
     *  - **A group post is any kind.** NIP-29 puts chat in 9, threads in 11 and
     *    replies in 1111, and an `#h` ask with no `kinds` has to reach all of
     *    them — which is why the page lets the TAB narrow this filter and gates
     *    only the metadata one.
     *  - **The id is matched CASED**, like every other tag value. `General` and
     *    `general` are two groups, which is why shared/query.js asks for the id
     *    verbatim and does not spread it over spellings the way a hashtag is.
     *  - **Two hosts' groups stay apart in 39000 and NOT in the posts.** A
     *    39000 is addressable per (kind, pubkey, `d`), so the same id signed by
     *    two relay keys is two stored records; the posts carry the bare id and
     *    are one set. That asymmetry is the whole reason the picker can warn
     *    about an ambiguous id while the results cannot separate it.
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

                // The metadata half, and the asymmetry that makes the picker
                // possible. Two relays each sign a `chachi`; both records are
                // stored, because a 39000 is addressable per (kind, pubkey, d).
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
     * A RANKED union comes back as ONE order over all four filters, not as each
     * filter's run end to end.
     *
     * The fourth assumption the search page rests on, and the newest: until
     * store `8a45e4d1a2` a multi-filter REQ with a search string was served as
     * run after run, so the page's export carried a caveat telling readers that
     * a jump back up the trust scale was a seam and not a misranking. That
     * caveat is gone, which makes the merge something this repo now depends on.
     *
     * Asserted through the ordering the in-memory engine CAN produce: it does
     * not rank, so it reports no scores and the store merges on recency
     * instead. That is enough to tell the two behaviors apart — the events are
     * arranged so the tag filter holds the newest AND the oldest, and the label
     * filter the one in between. Concatenation can only put the label's hit
     * last; one merged order has to interleave it.
     *
     * What it cannot check is the merge on real relevance — that needs an
     * engine that ranks, and lives in the store's own integration gate.
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
     * The relay's default before AUTH: a read says whose eyes it is read
     * through, or it is not answered.
     *
     * Every claim here is one a client acts on. The REFUSAL is `auth-required:`
     * rather than a silent empty EOSE because that is the prefix NIP-42 clients
     * already retry through (ours is `web/shared/relay.js`), and because an
     * empty answer to a read this relay declined is indistinguishable from an
     * empty corpus — the confusion the whole gate exists to end.
     *
     * The two ways past it need no signature at all: scores here are public, so
     * `observer:` ranks through any lens on an anonymous socket, and
     * `include:spam` asks for the corpus the relay used to hand over without
     * either side saying so.
     */
    @Test
    fun `an undeclared read is refused before AUTH, and the two declarations get through`() =
        runBlocking {
            store.insert(MetadataEvent("6".repeat(64), "a3".repeat(32), 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // A plain NIP-01 filter — no NIP-50 anything — is a read with
                // no lens just as much as a search is, and is refused the same.
                session.receive("""["REQ","n1",{"kinds":[0],"limit":10}]""")
                val closed = awaitMessage(out) { it.startsWith("""["CLOSED","n1",""") }
                assertTrue("auth-required:" in closed, "the machine-readable prefix a NIP-42 client retries through: $closed")
                assertTrue("observer:" in closed && "include:spam" in closed, "…and the refusal names both ways past it: $closed")

                session.receive("""["REQ","n2",{"kinds":[0],"search":"ali","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["CLOSED","n2",""") }

                // COUNT is a read too, and answers with a number rather than
                // events — which is exactly why it cannot be left open: an
                // ungated count reports the size of the corpus a gated REQ
                // would refuse to serve.
                session.receive("""["COUNT","n3",{"kinds":[0]}]""")
                awaitMessage(out) { it.startsWith("""["CLOSED","n3",""") }

                // ONE undeclared filter poisons the REQ: NIP-01 ORs them, so
                // serving the rest would serve the undeclared question in full.
                session.receive("""["REQ","n4",{"kinds":[0],"search":"include:spam"},{"kinds":[1]}]""")
                awaitMessage(out) { it.startsWith("""["CLOSED","n4",""") }

                // Way out one: waive the lens. The whole corpus, unranked.
                session.receive("""["REQ","y1",{"kinds":[0],"search":"ali include:spam","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","y1"]""") }
                assertTrue(out.any { it.startsWith("""["EVENT","y1",""") && "alice" in it }, "the waiver is answered in full: $out")

                // Way out two: name one. No signature involved — the store
                // resolves it as the query's own observer.
                session.receive("""["REQ","y2",{"kinds":[0],"search":"ali observer:${"7".repeat(64)}","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","y2"]""") }
                assertEquals("7".repeat(64), index.searchQueries.last().observer, "the named lens is the one the store ranks through")

                // …and a lens the store cannot resolve is not one: an npub is
                // ignored there, so accepting it here would answer an unranked
                // read under a token that promised a ranked one.
                session.receive("""["REQ","n5",{"kinds":[0],"search":"ali observer:npub1qqqqqqqq","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["CLOSED","n5",""") }
            } finally {
                session.close()
            }
        }

    /**
     * NIP-77 GOES THROUGH THE SAME GATE, and this pins it in both directions
     * because nothing in this repo decides it: quartz's `NegSessionRegistry`
     * builds a [com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd]
     * out of the NEG-OPEN's filters and runs it through the very hook
     * [LensRequiredPolicy] implements, turning a rejection into `NEG-ERR`.
     *
     * It is the RIGHT answer — a reconcile hands over the ids and times of
     * everything matching a filter, which is the lensless read of the whole
     * corpus this gate exists to stop — but it is quartz's arrangement rather
     * than our decision, so the day it changes, a reconcile silently becomes
     * the one unguarded read here.
     *
     * ONLY THE REFUSAL IS DRIVEN OVER THE WIRE, and that is a deliberate
     * limit rather than half a test. The other half — that a DECLARING peer
     * gets through — cannot be asserted this way, because a NEG-OPEN the gate
     * admits is then handed to quartz's negentropy session, and a frame that
     * is not a well-formed negentropy message never comes back: the message
     * consumer's varint loop reads past the end of the buffer, where
     * `ByteArrayReader.readByte` returns -1 forever and the continuation bit
     * is never clear. It spins at 100% on the thread it is on — which in the
     * server is a Netty I/O thread — and this test would hang the suite.
     * (Reproduced deterministically against `negentropy-jvm` with the frames
     * `6100`, `6101`, `61ff`, …: every truncated frame with a byte after the
     * version byte loops forever. `61` alone terminates.) Building a real
     * negentropy frame here would test quartz's reconciliation rather than
     * this gate, so the refusal above is the whole claim: the ReqCmd hook is
     * what a NEG-OPEN goes through, which is exactly what would break
     * silently.
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

    /**
     * The other half, asserted where it can be: the policy itself, with no
     * session behind it. A NEG-OPEN's filters reach [LensRequiredPolicy] as a
     * [com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd], so a
     * peer that waives a lens is admitted and this relay stays mirrorable.
     */
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

                // The SAME undeclared read, on the same socket, now answered —
                // this is the "authenticate and ask again" half of NIP-42, and
                // the reason the refusal above carries that prefix.
                session.receive("""["REQ","after",{"kinds":[0],"limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","after"]""") }
                session.receive("""["COUNT","cafter",{"kinds":[0]}]""")
                awaitMessage(out) { it.startsWith("""["COUNT","cafter"""") }
            } finally {
                session.close()
            }
        }

    /**
     * `REQUIRE_READ_LENS=false` is the older relay, whole: an undeclared
     * anonymous read is answered out of the unranked corpus. Pinned because
     * that is a deployment (a mirror-only relay, a store with no trust data
     * behind it) and not a debug switch.
     */
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
