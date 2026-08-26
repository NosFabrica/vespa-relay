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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import kotlinx.coroutines.runBlocking
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * THE SUBJECT COMES BACK WITH THE POINTER — for the reader who asked for it. A
 * NIP-50 search that matches a Trusted List, a NIP-85 Trusted Assertion or a
 * NIP-32 label answers with the record that pointer is about, spliced in at
 * the pointer's own position; and for the two provider-published families that
 * happens only when the reader's own kind-10040 named the signer.
 *
 * Every case here rests on the same setup, and it is the setup that makes the
 * assertions mean anything:
 *
 *  - the subjects carry NONE of the words being searched for. Ada's profile
 *    says "Ada Bramble", the labelled note says "the third episode is up" — so
 *    a hit on "podcaster" or on "medical" cannot be the search finding them.
 *  - `reader` publishes a kind-10040 naming `curator` as a `30382:rank`
 *    service, and nothing else. `stranger` is a signer with the same corpus
 *    shape whom nobody named — the control for the enrolment gate, exactly as
 *    the `expansion off` relay is the control for the feature as a whole.
 *
 * Driven over the wire like [RelayProtocolTest] and [TrustedListSearchTest],
 * because the ORDER of the frames is half of what is being asserted and the
 * frame stream is the only place that order exists. The in-memory index
 * matches search text by substring over the same derived columns Vespa
 * indexes, so what is pinned here is which records are admitted and where they
 * land, not how Vespa ranks them.
 *
 * The pointers are built as PLAIN SIGNED EVENTS, tags and all, rather than
 * through Quartz's builders — the same reason [SearchReferences] dispatches on
 * the kind: a quartz without an `EventFactory` branch for these kinds must
 * still be caught by this test rather than have it compile against types the
 * relay would never see.
 */
class SearchReferenceExpansionTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /** Counts the store recalls a REQ costs, so "not expanded" can be told from "expanded to nothing". */
    private class CountingIndex : EventIndex {
        val inner = InMemoryEventIndex()
        val searches = AtomicInteger(0)

        override suspend fun get(id: String) = inner.get(id)

        override suspend fun put(doc: EventDoc) = inner.put(doc)

        override suspend fun remove(id: String) = inner.remove(id)

        override suspend fun search(query: EventQuery): List<EventDoc> {
            searches.incrementAndGet()
            return inner.search(query)
        }

        override suspend fun count(query: EventQuery) = inner.count(query)

        override suspend fun countByAuthor(query: EventQuery) = inner.countByAuthor(query)

        override fun close() {}
    }

    private val index = CountingIndex()
    private val store = NostrSemanticsStore(index, relay = relayUrl)
    private val server = NostrRelayServer(store, relayUrl)

    /** The same store, read by a relay with the expansion off — the control for the feature itself. */
    private val plainServer = NostrRelayServer(store, relayUrl, searchExpansion = SearchExpansionLimits.Off)

    private val reader = NostrSignerSync()
    private val curator = NostrSignerSync()
    private val stranger = NostrSignerSync()
    private val podcaster = NostrSignerSync()

    /** What every anonymous read here declares: the whole corpus, ranked through the reader's eyes. */
    private val lens = "include:spam observer:${reader.pubKey}"

    @AfterTest
    fun tearDown() {
        server.close()
        plainServer.close()
    }

    /** The reader's NIP-85 provider list: `curator` ranks for them, and nobody else does. */
    private val providerList =
        reader.sign<Event>(
            1_699_999_000L,
            10040,
            arrayOf(arrayOf("30382:rank", curator.pubKey, "wss://provider.example")),
            "",
        )

    /** Ada's profile. Note what it does NOT say: "podcaster", or anything else searched for below. */
    private val profile =
        podcaster.sign<Event>(1_700_000_000L, 0, emptyArray(), """{"name":"Ada Bramble","about":"makes things"}""")

    /** The note a label points at. Again: none of the searched words are in it. */
    private val note = podcaster.sign<Event>(1_700_000_100L, 1, emptyArray(), "the third episode is up")

    /** A kind-30392 Trusted List of pubkeys. `title` is the only indexed field; `p` is the membership. */
    private val list = curator.trustedList(1_700_000_200L, "podcasters", "Podcaster Trust List")

    /** The same list, from a signer the reader never enrolled. */
    private val strangerList = stranger.trustedList(1_700_000_250L, "outsiders", "Strangercast Roster")

    /** A NIP-32 label: `l` carries the indexed value, `e` names the note it is about. */
    private val label =
        curator.sign<Event>(
            1_700_000_300L,
            1985,
            arrayOf(
                arrayOf("L", "health"),
                arrayOf("l", "medical", "health"),
                arrayOf("e", note.id),
            ),
            "",
        )

    /** A NIP-85 kind-30382 contact card. Its `d` IS the subject pubkey; `petname` is indexed. */
    private val assertion =
        curator.sign<Event>(
            1_700_000_400L,
            30382,
            arrayOf(
                arrayOf("d", podcaster.pubKey),
                arrayOf("petname", "Bramblecast"),
            ),
            "",
        )

    private fun NostrSignerSync.trustedList(
        createdAt: Long,
        listId: String,
        title: String,
    ) = sign<Event>(
        createdAt,
        30392,
        arrayOf(
            arrayOf("d", listId),
            arrayOf("title", title),
            arrayOf("p", podcaster.pubKey),
        ),
        "",
    )

    private suspend fun seed(
        session: RelaySession,
        out: List<String>,
    ) {
        publish(session, out, providerList, profile, note, list, strangerList, label, assertion)
    }

    private suspend fun publish(
        session: RelaySession,
        out: List<String>,
        vararg events: Event,
    ) {
        for (event in events) {
            session.receive("""["EVENT",${event.toJson()}]""")
            awaitMessage(out) { it.startsWith("""["OK","${event.id}",true""") }
        }
    }

    @Test
    fun `a trusted list's members ride in at the list's own position`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // "Podcaster Trust List" is the only text with the word in it.
                // The profile it names carries none of it — so if Ada comes
                // back, she came back BECAUSE the list named her.
                val page = page(session, out, "members", """{"kinds":[0,30392],"search":"podcaster $lens"}""")
                assertEquals(listOf(list.id, profile.id), page, "the member's profile must follow the list that names it")
            } finally {
                session.close()
            }
        }

    @Test
    fun `with the expansion off the same search answers with the list alone`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = plainServer.connect { out.add(it) }
            try {
                seed(session, out)

                // Same store, same corpus, same filter — only the knob differs.
                // This is what pins the profile above on the expansion rather
                // than on anything the search could have done by itself.
                val page = page(session, out, "members", """{"kinds":[0,30392],"search":"podcaster $lens"}""")
                assertEquals(listOf(list.id), page, "a relay with the expansion off answers with its own hits only")
            } finally {
                session.close()
            }
        }

    @Test
    fun `a list from a service the reader never named does not expand`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // Identical in every way to the list above — same kind, same
                // membership, same shape of title — except that the reader's
                // 10040 does not name its signer. A Trusted List is a
                // provider's computed output, and splicing a stranger's
                // computation into this reader's feed would put it in front of
                // them as if they had enrolled it.
                assertEquals(
                    listOf(strangerList.id),
                    page(session, out, "stranger", """{"kinds":[0,30392],"search":"strangercast $lens"}"""),
                    "an unenrolled signer's list is served, and expands to nothing",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a list the reader signed themselves expands`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // "or by the observer himself": a reader computing their own
                // lists needs no 10040 entry naming themselves, and would have
                // no way to publish a sensible one.
                val own = reader.trustedList(1_700_000_260L, "mine", "Readercast Picks")
                publish(session, out, own)

                assertEquals(
                    listOf(own.id, profile.id),
                    page(session, out, "own", """{"kinds":[0,30392],"search":"readercast $lens"}"""),
                    "the reader's own list expands without naming themselves as a service",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `an anonymous read expands its labels and not its lists`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // `include:spam` with no observer is a legitimate ask here —
                // the whole corpus, unranked. There is then nobody whose
                // services to check, so the provider-published families cannot
                // expand at all...
                assertEquals(
                    listOf(list.id),
                    page(session, out, "anon-list", """{"kinds":[0,30392],"search":"podcaster include:spam"}"""),
                    "with no observer there is no enrolment to check, so no list expands",
                )

                // ...while a NIP-32 label is not a provider's machinery and is
                // deliberately not gated this way.
                assertEquals(
                    listOf(label.id, note.id),
                    page(session, out, "anon-label", """{"kinds":[1,1985],"search":"medical include:spam"}"""),
                    "a label expands for a lensless read too",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a signed-in connection is the observer whose services count`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
                val auth = reader.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                // No `observer:` token anywhere: the connection's own pubkey is
                // the lens, and so it is the 10040 that decides what unpacks.
                assertEquals(
                    listOf(list.id, profile.id),
                    page(session, out, "signed-in", """{"kinds":[0,30392],"search":"podcaster include:spam"}"""),
                    "the connection's pubkey supplies the enrolment when no filter names one",
                )
                assertEquals(
                    listOf(strangerList.id),
                    page(session, out, "signed-in-stranger", """{"kinds":[0,30392],"search":"strangercast include:spam"}"""),
                    "and it gates the same way",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a label brings the note it labels, an assertion brings the profile it is about`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // A label's subject is an `e` tag; the note itself never says "medical".
                assertEquals(
                    listOf(label.id, note.id),
                    page(session, out, "label", """{"kinds":[1,1985],"search":"medical $lens"}"""),
                    "the labelled note must follow its label",
                )

                // An assertion's subject is its `d` tag, and a pubkey resolves
                // to that author's kind-0 rather than to an event id.
                assertEquals(
                    listOf(assertion.id, profile.id),
                    page(session, out, "card", """{"kinds":[0,30382],"search":"bramblecast $lens"}"""),
                    "the profile the contact card is about must follow the card",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a subject that fails the REQ's other filters is not added`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // The kinds case, and the one that surprises people: a REQ that
                // asks only for lists gets only lists. `kinds` is the client
                // saying what it is prepared to receive, and the expansion does
                // not get to overrule it.
                assertEquals(
                    listOf(list.id),
                    page(session, out, "kinds", """{"kinds":[30392],"search":"podcaster $lens"}"""),
                    "a kinds-constrained REQ must not be answered with another kind",
                )

                // The authors case: Ada's profile is authored by Ada, not by
                // the curator whose lists this REQ asked for.
                assertEquals(
                    listOf(list.id),
                    page(
                        session,
                        out,
                        "authors",
                        """{"kinds":[0,30392],"authors":["${curator.pubKey}"],"search":"podcaster $lens"}""",
                    ),
                    "a subject by another author must not slip past an authors filter",
                )

                // The window case: the profile predates the `since` this REQ
                // named, and `since` is part of every-filter-but-the-search.
                assertEquals(
                    listOf(list.id),
                    page(
                        session,
                        out,
                        "since",
                        """{"kinds":[0,30392],"since":1700000150,"search":"podcaster $lens"}""",
                    ),
                    "a subject outside the REQ's window must not be added",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a hit is served once, at its own rank, even when a pointer also names it`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // "bramble" is in Ada's own profile AND in the card's petname,
                // so the profile is both a hit of this search and a subject of
                // one. NIP-01 says a subscription must not be sent the same
                // event twice; the splice is what has to give way.
                val page = page(session, out, "both", """{"kinds":[0,30382],"search":"bramble $lens"}""")
                assertEquals(1, page.count { it == profile.id }, "the profile must appear exactly once: $page")
                assertTrue(assertion.id in page, "the card is still a hit of its own search: $page")
            } finally {
                session.close()
            }
        }

    @Test
    fun `a list of events brings its members and not its metadata`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // A kind-30393 list carries its membership in `e`. Its `p` is
                // the observer the list was computed UNDER — metadata, and the
                // whole reason the reader dispatches on the kind instead of
                // sweeping every reference tag it can find. Ada's profile is
                // reachable (this REQ admits kind 0) and must still not come.
                val episodes =
                    curator.sign<Event>(
                        1_700_000_700L,
                        30393,
                        arrayOf(
                            arrayOf("d", "episodes"),
                            arrayOf("title", "Bramblecast Episodes"),
                            arrayOf("p", podcaster.pubKey),
                            arrayOf("e", note.id),
                        ),
                        "",
                    )
                publish(session, out, episodes)

                assertEquals(
                    listOf(episodes.id, note.id),
                    page(session, out, "episodes", """{"kinds":[0,1,30393],"search":"episodes $lens"}"""),
                    "the `e` members ride in; the `p` observer is metadata and does not",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `an addressable member is resolved by its coordinate`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // A kind-30394 list names addressable events by `kind:pubkey:d`
                // rather than by id — a different lookup shape from both of the
                // others, and the one that has to reassemble the coordinate to
                // recognize what came back.
                val article =
                    podcaster.sign<Event>(
                        1_700_000_800L,
                        30023,
                        arrayOf(arrayOf("d", "show-notes"), arrayOf("title", "Show notes")),
                        "brambles, mostly",
                    )
                val shelf =
                    curator.sign<Event>(
                        1_700_000_900L,
                        30394,
                        arrayOf(
                            arrayOf("d", "shelf"),
                            arrayOf("title", "Longform Shelf"),
                            arrayOf("a", "30023:${podcaster.pubKey}:show-notes"),
                        ),
                        "",
                    )
                publish(session, out, article, shelf)

                assertEquals(
                    listOf(shelf.id, article.id),
                    page(session, out, "shelf", """{"kinds":[30023,30394],"search":"longform $lens"}"""),
                    "the a-coordinate member must resolve to the addressable event it names",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a read with no search text costs no lookups at all`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // A mirror's paging carries `include:spam` and no terms —
                // every anonymous read here has to carry the lens token, so
                // "has a search field" would have put this whole path behind
                // the expansion. It is a recall, and it stays one.
                val before = index.searches.get()
                val page =
                    page(
                        session,
                        out,
                        "recall",
                        """{"kinds":[30392],"authors":["${curator.pubKey}"],"#p":["${podcaster.pubKey}"],"search":"include:spam"}""",
                    )
                assertEquals(listOf(list.id), page)
                assertEquals(1, index.searches.get() - before, "a termless recall must cost exactly its own query")
            } finally {
                session.close()
            }
        }

    @Test
    fun `the per-event cap truncates the splice rather than the page`() =
        runBlocking {
            val capped = NostrRelayServer(store, relayUrl, searchExpansion = SearchExpansionLimits(maxPerEvent = 1))
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = capped.connect { out.add(it) }
            try {
                seed(session, out)
                val second = NostrSignerSync()
                val secondProfile = second.sign<Event>(1_700_000_500L, 0, emptyArray(), """{"name":"Bo Quill"}""")
                val pair =
                    curator.sign<Event>(
                        1_700_000_600L,
                        30392,
                        arrayOf(
                            arrayOf("d", "duo"),
                            arrayOf("title", "Duocast Roster"),
                            arrayOf("p", podcaster.pubKey),
                            arrayOf("p", second.pubKey),
                        ),
                        "",
                    )
                publish(session, out, secondProfile, pair)

                // Two members, room for one — and the one is the FIRST the
                // list names, not whichever the store answered with. That is
                // the whole point of capping the lookup rather than the
                // result: a publisher orders members by the score it computed,
                // so a truncated splice is the top of its own ranking.
                //
                // The list itself is never the thing dropped — a truncated
                // splice still leaves the client the member tags and the `#p`
                // recall that serves them.
                val page = page(session, out, "capped", """{"kinds":[0,30392],"search":"duocast $lens"}""")
                assertEquals(listOf(pair.id, profile.id), page, "the list, then the first member it names")
                assertTrue(secondProfile.id !in page, "the member past the cap is not looked up at all")
            } finally {
                session.close()
                capped.close()
            }
        }

    /**
     * Runs [filter] as subscription [subId] and returns the ids of the EVENT
     * frames it produced, in the order they went out, up to its EOSE.
     */
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
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }

    private companion object {
        /** The `id` of the event object inside an EVENT frame — the first `"id":"…"` in it. */
        val ID = Regex("\"id\":\"([0-9a-f]{64})\"")
    }
}
