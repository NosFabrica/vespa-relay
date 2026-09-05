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
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
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
 * A search hit on a Trusted List, a NIP-85 assertion or a NIP-32 label answers with the record it
 * points at, spliced in behind the pointer; the provider-published families only when the reader's
 * kind-10040 named the signer. Driven over the wire.
 */
class SearchReferenceExpansionTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /**
     * Counts and records the queries a REQ costs. [InMemoryEventIndex] ignores the observer gate,
     * so the lens a lookup is made under shows only in what was asked, never on the wire.
     */
    private class CountingIndex : EventIndex {
        val inner = InMemoryEventIndex()
        val searches = AtomicInteger(0)
        val queries: MutableList<EventQuery> = Collections.synchronizedList(mutableListOf())

        override suspend fun get(id: String) = inner.get(id)

        override suspend fun put(doc: EventDoc) = inner.put(doc)

        override suspend fun remove(id: String) = inner.remove(id)

        override suspend fun search(query: EventQuery): List<EventDoc> {
            searches.incrementAndGet()
            queries.add(query)
            return inner.search(query)
        }

        override suspend fun count(query: EventQuery) = inner.count(query)

        override suspend fun countByAuthor(query: EventQuery) = inner.countByAuthor(query)

        override fun close() {}
    }

    private val index = CountingIndex()

    /**
     * A store over [TrustProjection], the only shape where a 10040 write invalidates the delegation
     * map the expansion reads. The counting index sits under the projection and sees every query.
     */
    private val projection = TrustProjection(index, InMemoryReputationIndex())
    private val store = NostrSemanticsStore(projection, relay = relayUrl)
    private val server = NostrRelayServer(store, relayUrl)

    /** The splice limits live on the store, so a control is a second store over the same index. */
    private fun storeWith(limits: SearchExpansionLimits) = NostrSemanticsStore(projection, relay = relayUrl, searchExpansion = limits)

    /** The control: the same corpus with the expansion off. */
    private val plainServer = NostrRelayServer(storeWith(SearchExpansionLimits.Off), relayUrl)

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

    /**
     * The reader's 10040: `curator` computes for them and nobody else does. One entry per kind,
     * because the gate is keyed by the kind it unpacks.
     */
    private val providerList =
        reader.sign<Event>(
            1_699_999_000L,
            10040,
            arrayOf(
                arrayOf("30382:rank", curator.pubKey, "wss://provider.example"),
                arrayOf("30392", curator.pubKey, "wss://provider.example"),
                arrayOf("30393", curator.pubKey, "wss://provider.example"),
                arrayOf("30394", curator.pubKey, "wss://provider.example"),
            ),
            "",
        )

    /** Ada's profile. It carries none of the words searched for below, so a hit on it is the splice. */
    private val profile =
        podcaster.sign<Event>(1_700_000_000L, 0, emptyArray(), """{"name":"Ada Bramble","about":"makes things"}""")

    /** The note a label points at; none of the searched words are in it. */
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

    /** A NIP-85 kind-30382 contact card. Its `d` is the subject pubkey; `petname` is indexed. */
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

                // Only the list's title carries the word, so Ada comes back only because the list named her.
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

                // Same store, same corpus, same filter; only the knob differs.
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

                // The same list in every way, except that the reader's 10040 does not name its signer.
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

                // A reader computing their own lists needs no 10040 entry naming themselves.
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

                // No observer means nobody's services to check, so no provider-published family expands.
                assertEquals(
                    listOf(list.id),
                    page(session, out, "anon-list", """{"kinds":[0,30392],"search":"podcaster include:spam"}"""),
                    "with no observer there is no enrolment to check, so no list expands",
                )

                // A NIP-32 label is not a provider's machinery and is not gated this way.
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
    fun `a provider list published mid-session takes effect on the next search`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // A reader who has enrolled nobody: the answer a cache is most tempted to keep.
                val newcomer = NostrSignerSync()
                val newLens = "include:spam observer:${newcomer.pubKey}"
                assertEquals(
                    listOf(list.id),
                    page(session, out, "before", """{"kinds":[0,30392],"search":"podcaster $newLens"}"""),
                    "a reader who has enrolled nobody expands nothing",
                )

                // The 10040 write is the one exact invalidation signal the relay has.
                publish(
                    session,
                    out,
                    newcomer.sign<Event>(
                        1_699_999_500L,
                        10040,
                        arrayOf(arrayOf("30392", curator.pubKey, "wss://provider.example")),
                        "",
                    ),
                )

                assertEquals(
                    listOf(list.id, profile.id),
                    page(session, out, "after", """{"kinds":[0,30392],"search":"podcaster $newLens"}"""),
                    "the enrolment must apply on the next search, not after a cache expiry",
                )

                // Per reader: enrolling one must not enrol everybody.
                val bystander = NostrSignerSync()
                assertEquals(
                    listOf(list.id),
                    page(
                        session,
                        out,
                        "bystander",
                        """{"kinds":[0,30392],"search":"podcaster include:spam observer:${bystander.pubKey}"}""",
                    ),
                    "another reader's enrolment is not this reader's",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a delegation opens the kind it names and no other`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // One entry, one kind: the curator's contact card is a computation this reader asked for,
                // the curator's Trusted List of the same users is not.
                val ranksOnly = NostrSignerSync()
                val ranksOnlyLens = "include:spam observer:${ranksOnly.pubKey}"
                publish(
                    session,
                    out,
                    ranksOnly.sign<Event>(
                        1_699_999_800L,
                        10040,
                        arrayOf(arrayOf("30382:rank", curator.pubKey, "wss://provider.example")),
                        "",
                    ),
                )

                assertEquals(
                    listOf(assertion.id, profile.id),
                    page(session, out, "card", """{"kinds":[0,30382],"search":"bramblecast $ranksOnlyLens"}"""),
                    "the kind the entry names must expand",
                )
                assertEquals(
                    listOf(list.id),
                    page(session, out, "list", """{"kinds":[0,30392],"search":"podcaster $ranksOnlyLens"}"""),
                    "a rank delegation must not also open the publisher's Trusted Lists",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a Trusted List publisher is enrolled by the Map's bare-kind entry`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // The bare-kind entry has no `:`, so NIP-85's parser never returns it.
                val subscriber = NostrSignerSync()
                val subscriberLens = "include:spam observer:${subscriber.pubKey}"
                publish(
                    session,
                    out,
                    subscriber.sign<Event>(
                        1_699_999_600L,
                        10040,
                        arrayOf(arrayOf("30392", curator.pubKey, "wss://lists.example")),
                        "",
                    ),
                )

                assertEquals(
                    listOf(list.id, profile.id),
                    page(session, out, "bare-kind", """{"kinds":[0,30392],"search":"podcaster $subscriberLens"}"""),
                    "a bare-kind Map entry must enrol the publisher it delegates to",
                )

                // The entry delegates a kind, not the whole family; it vouches for nobody else.
                assertEquals(
                    listOf(strangerList.id),
                    page(session, out, "bare-kind-stranger", """{"kinds":[0,30392],"search":"strangercast $subscriberLens"}"""),
                    "the entry enrols its own publisher and nobody else",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a reserved named Trusted List entry drives nothing`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // `30392:<name>` is reserved by the ADR: parsed for display, not acted on.
                val reserved = NostrSignerSync()
                publish(
                    session,
                    out,
                    reserved.sign<Event>(
                        1_699_999_700L,
                        10040,
                        arrayOf(arrayOf("30392:podcasters", curator.pubKey, "wss://lists.example")),
                        "",
                    ),
                )

                assertEquals(
                    listOf(list.id),
                    page(
                        session,
                        out,
                        "reserved",
                        """{"kinds":[0,30392],"search":"podcaster include:spam observer:${reserved.pubKey}"}""",
                    ),
                    "a reserved named entry must not open the gate",
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

                // No `observer:` token: the connection's own pubkey is the lens.
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

                // An assertion's subject is its `d` tag; a pubkey resolves to that author's kind-0.
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

                // `kinds` is what the client is prepared to receive; the expansion does not overrule it.
                assertEquals(
                    listOf(list.id),
                    page(session, out, "kinds", """{"kinds":[30392],"search":"podcaster $lens"}"""),
                    "a kinds-constrained REQ must not be answered with another kind",
                )

                // Ada's profile is authored by Ada, not by the curator this REQ asked for.
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

                // The profile predates the REQ's `since`.
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

                // "bramble" is in Ada's profile and in the card's petname: the profile is a hit and a subject.
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

                // A kind-30393 list's membership is `e`; its `p` is the observer it was computed under.
                // Ada's profile is admissible here and must still not come.
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

                // A kind-30394 list names addressable events by `kind:pubkey:d`, not by id.
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
    fun `the plain half of a mixed REQ is answered as a plain recall`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // The first filter searches; the list comes back from the second, a plain recall. Without
                // a per-row test Ada would be spliced behind a list the search never found.
                val page =
                    page(
                        session,
                        out,
                        "mixed",
                        """{"kinds":[0,30392],"authors":["${podcaster.pubKey}"],"search":"podcaster $lens"},""" +
                            """{"kinds":[30392],"authors":["${curator.pubKey}"],"search":"$lens"}""",
                    )
                assertEquals(listOf(list.id), page, "the plain half's rows must not expand")
            } finally {
                session.close()
            }
        }

    @Test
    fun `a sibling filter's waiver does not unlens the subject lookup`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)
                index.queries.clear()

                // The search asks for the reader's lens; the sibling recall waives one. Pooling a
                // subscription's lenses would recall the first filter's subjects with the floor off.
                page(
                    session,
                    out,
                    "sibling",
                    """{"kinds":[0,30392],"search":"podcaster observer:${reader.pubKey}"},""" +
                        """{"kinds":[1],"search":"include:spam"}""",
                )

                // Subject lookups by shape: recalls keyed on an id or on (kind 0, author), which no filter
                // of this REQ asks for; the second filter and the 10040 lookup legitimately carry the waiver.
                val subjectLookups =
                    synchronized(index.queries) {
                        index.queries.filter { it.ids.isNotEmpty() || (it.kinds == listOf(0) && it.authors.isNotEmpty()) }
                    }
                assertTrue(subjectLookups.isNotEmpty(), "the search should have looked a subject up at all")
                subjectLookups.forEach {
                    assertEquals(reader.pubKey, it.observer, "a subject is read through the search's own observer: $it")
                    assertTrue(!it.includeSpam, "and never with a floor a sibling filter waived: $it")
                }
            } finally {
                session.close()
            }
        }

    @Test
    fun `an observer on a non-searching filter does not unlock the expansion`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // The search declares no lens, so it has no enrolment to check; the sibling naming an
                // observer is not the filter that found the list.
                assertEquals(
                    listOf(list.id),
                    page(
                        session,
                        out,
                        "borrowed",
                        """{"kinds":[0,30392],"search":"podcaster include:spam"},""" +
                            """{"kinds":[1],"authors":["${"f0".repeat(32)}"],"search":"include:spam observer:${reader.pubKey}"}""",
                    ),
                    "a lensless search must not borrow a sibling's observer",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `an a-coordinate is matched however the publisher spelled it`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // The store holds hex lower-case, but `Address.parse` accepts an upper-case pubkey; an
                // unnormalized reader recalls the article and drops it for not equalling the raw tag.
                val article =
                    podcaster.sign<Event>(
                        1_700_001_000L,
                        30023,
                        arrayOf(arrayOf("d", "loud-notes"), arrayOf("title", "Loud notes")),
                        "brambles, loudly",
                    )
                val shelf =
                    curator.sign<Event>(
                        1_700_001_100L,
                        30394,
                        arrayOf(
                            arrayOf("d", "loud"),
                            arrayOf("title", "Shouty Shelf"),
                            arrayOf("a", "30023:${podcaster.pubKey.uppercase()}:loud-notes"),
                        ),
                        "",
                    )
                publish(session, out, article, shelf)

                assertEquals(
                    listOf(shelf.id, article.id),
                    page(session, out, "loud", """{"kinds":[30023,30394],"search":"shouty $lens"}"""),
                    "an upper-case coordinate names the same event",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `a pubkey subject the REQ cannot admit does not spend the budget`() =
        runBlocking {
            val capped = NostrRelayServer(storeWith(SearchExpansionLimits(maxPerRequest = 1)), relayUrl)
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = capped.connect { out.add(it) }
            try {
                seed(session, out)

                // Dated before the kind-30392 list and read with `sort:recent`, so the pubkey-naming list is
                // seen first: the only order in which the bug can show.
                val episodes =
                    curator.sign<Event>(
                        1_700_000_150L,
                        30393,
                        arrayOf(arrayOf("d", "eps"), arrayOf("title", "Podcaster Episodes"), arrayOf("e", note.id)),
                        "",
                    )
                publish(session, out, episodes)

                // One subject of budget. The first list names a pubkey this REQ (no kind 0) can never be
                // answered with; charging for it would leave nothing for the note the 30393 list names.
                val page = page(session, out, "budget", """{"kinds":[1,30392,30393],"search":"podcaster sort:recent $lens"}""")
                assertEquals(list.id, page.first(), "the order this test rests on: the pubkey list is seen first")
                assertTrue(note.id in page, "the event subject must still be reachable: $page")
            } finally {
                session.close()
                capped.close()
            }
        }

    @Test
    fun `a search for the external-id family spends the companion recall and nothing more`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // 30385 and 30395 are the NIP-73 external-id pair, whose subjects are urls and ISBNs. A
                // kind-restricted search still fetches the pointer kinds that could name a 30395.
                val external =
                    curator.sign<Event>(
                        1_700_002_000L,
                        30395,
                        arrayOf(arrayOf("d", "isbns"), arrayOf("title", "Bookshelf Roster"), arrayOf("i", "isbn:9780134685991")),
                        "",
                    )
                publish(session, out, external)

                val before = index.queries.size
                assertEquals(
                    listOf(external.id),
                    page(session, out, "external", """{"kinds":[30395],"search":"bookshelf $lens"}"""),
                    "the list is served; it just has nothing to unpack",
                )
                val cost = index.queries.drop(before)
                assertEquals(
                    setOf(listOf(30395), listOf(1985), listOf(30383, 30384), listOf(30393, 30394)),
                    cost.map { it.kinds }.toSet(),
                    "the REQ's own query and the three companions, nothing else: $cost",
                )
                assertTrue(cost.all { it.search == "bookshelf" }, "the recall is all there is — nothing followed the page: $cost")
                val declarations = cost.filter { it.authors.isNotEmpty() }
                assertTrue(declarations.isNotEmpty(), "the enrolled fetch must have happened for the two lines above to have pinned it")
                assertTrue(
                    declarations.all { setOf(reader.pubKey, curator.pubKey).containsAll(it.authors) },
                    "a declaration companion fetches from enrolled signers only: $declarations",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `many pointers naming one subject send it once, at the first that named it`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // Five labels, one note: a page of labels on a busy topic converges on a handful of notes.
                val many =
                    (0 until 5).map { i ->
                        curator.sign<Event>(
                            1_700_003_000L + i,
                            1985,
                            arrayOf(arrayOf("L", "dup"), arrayOf("l", "duplikat", "dup"), arrayOf("e", note.id)),
                            "",
                        )
                    }
                publish(session, out, *many.toTypedArray())

                // page() already rejects duplicates; the count states the property.
                val page = page(session, out, "dup", """{"kinds":[1,1985],"search":"duplikat $lens"}""")
                assertEquals(1, page.count { it == note.id }, "the note must be sent exactly once: $page")
                // The first label in page order, the search's order, not the order they were written.
                val firstLabel = page.indexOfFirst { id -> many.any { it.id == id } }
                assertEquals(
                    firstLabel + 1,
                    page.indexOf(note.id),
                    "and directly behind the first label the search ranked: $page",
                )
                assertEquals(6, page.size, "five labels and one note, nothing else: $page")
            } finally {
                session.close()
            }
        }

    @Test
    fun `a subject that is also a hit keeps its own rank and is not spliced early`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // The note carries the searched word, so it is a hit as well as the label's subject. The row
                // wins: it stays where the search ranked it, and the splice gives way.
                val labelled = podcaster.sign<Event>(1_700_004_000L, 1, emptyArray(), "duplikato in the note itself")
                val label =
                    curator.sign<Event>(
                        1_700_004_100L,
                        1985,
                        arrayOf(arrayOf("L", "dup"), arrayOf("l", "duplikato", "dup"), arrayOf("e", labelled.id)),
                        "",
                    )
                publish(session, out, labelled, label)

                val page = page(session, out, "both", """{"kinds":[1,1985],"search":"duplikato $lens"}""")
                assertEquals(1, page.count { it == labelled.id }, "exactly once: $page")
                assertEquals(setOf(label.id, labelled.id), page.toSet(), "both, and only both: $page")
            } finally {
                session.close()
            }
        }

    @Test
    fun `a row the store hands back twice still goes out once`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)
            } finally {
                session.close()
            }

            // The expansion's own refusal to emit a row twice over a doubling store is stated in
            // vespa-eventstore's `SearchExpansionTest`; no wrapper is left here to state it from.
        }

    @Test
    fun `a lens token is not a search, and costs no lookups at all`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // `include:spam` makes `search` non-empty on every anonymous read; the gate is on search
                // text, so a recall carrying a lens token stays a recall.
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
    fun `members ride in the order the list names them, not the order the store holds them`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // The relay reads a member's key and never its score, so tag order is what makes a spliced
                // member's position mean its rank. Published back to front so recall and tag order disagree.
                val one = NostrSignerSync()
                val two = NostrSignerSync()
                val three = NostrSignerSync()
                val profiles =
                    listOf(one, two, three).mapIndexed { i, signer ->
                        signer.sign<Event>(1_700_000_700L + i, 0, emptyArray(), """{"name":"Member $i"}""")
                    }
                publish(session, out, *profiles.reversed().toTypedArray())

                val ranked =
                    curator.sign<Event>(
                        1_700_000_800L,
                        30392,
                        arrayOf(
                            arrayOf("d", "ranked"),
                            arrayOf("title", "Rankedcast Roster"),
                            arrayOf("p", one.pubKey, "", "90"),
                            arrayOf("p", two.pubKey, "", "50"),
                            arrayOf("p", three.pubKey, "", "10"),
                        ),
                        "",
                    )
                publish(session, out, ranked)

                assertEquals(
                    listOf(ranked.id) + profiles.map { it.id },
                    page(session, out, "ranked", """{"kinds":[0,30392],"search":"rankedcast $lens"}"""),
                    "the list, then its members in the order IT names them",
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `the per-event cap truncates the splice rather than the page`() =
        runBlocking {
            val capped = NostrRelayServer(storeWith(SearchExpansionLimits(maxPerEvent = 1)), relayUrl)
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

                // Two members, room for one: the first the list names, not whichever the store answered with.
                // The list itself is never dropped; a truncated splice still leaves the client the member tags.
                val page = page(session, out, "capped", """{"kinds":[0,30392],"search":"duocast $lens"}""")
                assertEquals(listOf(pair.id, profile.id), page, "the list, then the first member it names")
                assertTrue(secondProfile.id !in page, "the member past the cap is not looked up at all")
            } finally {
                session.close()
                capped.close()
            }
        }

    /** Runs [filter] as subscription [subId] and returns the ids of its EVENT frames in order, up to EOSE. */
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
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }

    private companion object {
        /** The `id` of the event object inside an EVENT frame: the first `"id":"…"` in it. */
        val ID = Regex("\"id\":\"([0-9a-f]{64})\"")
    }
}
