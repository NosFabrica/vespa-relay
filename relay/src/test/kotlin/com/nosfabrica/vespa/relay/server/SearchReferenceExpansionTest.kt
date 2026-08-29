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
 * through Quartz's builders — the same reason the store's `SearchReferences`
 * dispatches on the kind: a quartz without an `EventFactory` branch for these
 * kinds must still be caught by this test rather than have it compile against
 * types the relay would never see.
 */
class SearchReferenceExpansionTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /**
     * Counts the store recalls a REQ costs, so "not expanded" can be told from
     * "expanded to nothing" — and records the queries, because
     * [InMemoryEventIndex] documents that it IGNORES the observer gate
     * (`minRank`/`observer`). The lens a lookup is made under therefore cannot
     * be seen in what comes back on the wire; it can only be seen in what was
     * asked. Same seam [RelayProtocolTest] asserts its NIP-50 extensions on.
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
     * The stack the front door assembles, because the gate is part of it: a
     * store over [TrustProjection] is the only shape where a 10040 write
     * invalidates the delegation map the expansion reads. The counting index
     * sits UNDER the projection, so it still sees every query either makes.
     */
    private val projection = TrustProjection(index, InMemoryReputationIndex())
    private val store = NostrSemanticsStore(projection, relay = relayUrl)
    private val server = NostrRelayServer(store, relayUrl)

    /**
     * The same corpus under different splice limits — the knob moved to the
     * store when the expansion did, so a control is a second store over the
     * SAME index rather than a second relay over the same store.
     */
    private fun storeWith(limits: SearchExpansionLimits) = NostrSemanticsStore(projection, relay = relayUrl, searchExpansion = limits)

    /** The same corpus, read with the expansion off — the control for the feature itself. */
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
     * The reader's Treasure Map: `curator` computes for them, and nobody else
     * does.
     *
     * FOUR ENTRIES FOR ONE PUBLISHER, because the gate is keyed by the kind of
     * the declaration it is about to unpack, and each entry appoints exactly
     * one kind. The NIP-85 shape names a kind AND metric; the three Trusted
     * List kinds take the ADR's generic bare-kind shape. A Map carrying only
     * the first would open the contact card below and leave all three lists
     * shut — which is a case of its own further down.
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
    fun `a provider list published mid-session takes effect on the next search`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // A reader who has enrolled nobody. Their first search resolves
                // that — and it is the answer a cache is most tempted to keep,
                // since most readers have no provider list at all.
                val newcomer = NostrSignerSync()
                val newLens = "include:spam observer:${newcomer.pubKey}"
                assertEquals(
                    listOf(list.id),
                    page(session, out, "before", """{"kinds":[0,30392],"search":"podcaster $newLens"}"""),
                    "a reader who has enrolled nobody expands nothing",
                )

                // Publishing the list HERE is the one exact invalidation signal
                // the relay has, and this is what makes it worth taking: enrol a
                // service and the very next search unpacks it, rather than the
                // next one after the TTL.
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

                // And it is per reader: enrolling one must not enrol everybody.
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

                // ONE ENTRY, ONE KIND. This reader appointed `curator` to rank
                // users and nothing else — so `curator`'s contact card is a
                // computation they asked for, and `curator`'s Trusted List of
                // those same users is not. Same publisher, same subject, two
                // different things to have delegated.
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

                // The OTHER delegation shape. NIP-85 names a kind AND metric
                // (`30382:rank`); Tapestry's Trusted Lists are delegated by a
                // generic bare-kind entry, one of which covers every list of
                // that kind. It has no `:`, so NIP-85's parser has never
                // returned it — read only that side, this reader enrols nobody
                // and their Trusted List gate stays shut.
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

                // And it delegates that KIND, not the whole family: the entry
                // above says 30392, so it cannot vouch for a signer nobody
                // named on any kind.
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

                // `30392:<name>` is RESERVED by the ADR — parsed so a client can
                // display it, explicitly not something to act on until the spec
                // defines it. A gate is the last place to act on a reservation,
                // so this reader is still enrolled in nothing.
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
    fun `the plain half of a mixed REQ is answered as a plain recall`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // Two filters, ORed as NIP-01 says. The FIRST searches, and it
                // admits pointer kinds — so the REQ as a whole is a search and
                // reaches the expansion. But the list comes back from the
                // SECOND, which is a plain recall of the curator's lists and
                // asks no question the expansion has any business answering.
                //
                // The distinction is real rather than academic: Ada's profile
                // WOULD be admitted here (it is a kind 0 by podcaster, which
                // the first filter accepts), so without the per-row test it
                // would be spliced in behind a list the search never found.
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

                // The searching filter asks to be ranked through the reader.
                // The sibling is a plain recall that waives a lens, which is
                // what every reference read the page makes looks like. Pooling
                // a subscription's lenses would recall the FIRST filter's
                // subjects with the trust floor off — a search that asked for
                // the reader's web of trust answered with records that web of
                // trust excludes.
                page(
                    session,
                    out,
                    "sibling",
                    """{"kinds":[0,30392],"search":"podcaster observer:${reader.pubKey}"},""" +
                        """{"kinds":[1],"search":"include:spam"}""",
                )

                // The subject lookups, by their shape: recalls keyed on an id
                // or on (kind 0, author), which no filter of this REQ asks for.
                // The REQ's OWN second filter legitimately carries the waiver —
                // it is the client's plain recall — and the 10040 lookup
                // deliberately does; neither is what this is about.
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

                // The SEARCH declares no lens, so it has no enrolment to check
                // and must expand no list. That the sibling recall names an
                // observer is neither here nor there: it is not the filter that
                // found the list. The sibling is aimed at an author with
                // nothing published so its own hits cannot muddy the page.
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

                // Nostr hex is lower-case by convention and the store holds it
                // that way, but nothing stops a publisher writing the member
                // with an upper-case pubkey — `Address.parse` accepts it, and
                // an unnormalized reader recalls the article and then drops it
                // for failing to equal its own raw tag string. Same shape as an
                // `naddr1…` member, which `Address.parse` also decodes.
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

                // Dated BEFORE the kind-30392 list and read back with
                // `sort:recent`, so the ORDER is pinned rather than left to
                // whatever the engine ranks first: the pubkey-naming list is
                // seen before the event-naming one, which is the only order in
                // which the bug can show at all.
                val episodes =
                    curator.sign<Event>(
                        1_700_000_150L,
                        30393,
                        arrayOf(arrayOf("d", "eps"), arrayOf("title", "Podcaster Episodes"), arrayOf("e", note.id)),
                        "",
                    )
                publish(session, out, episodes)

                // One subject of budget for the whole REQ. The list comes first
                // and names a PUBKEY — which this REQ, admitting no kind 0, can
                // never be answered with. Charging the budget for it anyway
                // leaves nothing for the note the 30393 list names, which the
                // REQ very much does admit.
                val page = page(session, out, "budget", """{"kinds":[1,30392,30393],"search":"podcaster sort:recent $lens"}""")
                assertEquals(list.id, page.first(), "the order this test rests on: the pubkey list is seen first")
                assertTrue(note.id in page, "the event subject must still be reachable: $page")
            } finally {
                session.close()
                capped.close()
            }
        }

    @Test
    fun `a search for the external-id family costs no enrolment lookup`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // 30385 and 30395 are the NIP-73 external-id pair: their
                // subjects are urls and ISBNs, not nostr events, so they can
                // never expand. A REQ naming nothing else must therefore stay
                // off the page-collecting path entirely — and above all must
                // not pay the 10040 recall that gates a family it cannot
                // expand.
                val external =
                    curator.sign<Event>(
                        1_700_002_000L,
                        30395,
                        arrayOf(arrayOf("d", "isbns"), arrayOf("title", "Bookshelf Roster"), arrayOf("i", "isbn:9780134685991")),
                        "",
                    )
                publish(session, out, external)

                val before = index.searches.get()
                assertEquals(
                    listOf(external.id),
                    page(session, out, "external", """{"kinds":[30395],"search":"bookshelf $lens"}"""),
                    "the list is served; it just has nothing to unpack",
                )
                assertEquals(1, index.searches.get() - before, "and it costs exactly its own query")
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

                // Five labels, one note. This is not a corner case: in a
                // production sample 76 targets are named by more than one
                // label and ten of them by ten labels each, so a page of
                // labels on a busy topic converges on the same handful of
                // notes. Without the `sent` set that is five copies of one
                // note on one subscription.
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

                // page() asserts distinctness for every case in this file, so
                // the duplicate would fail here even without the count below —
                // this states the property the file is being asked about.
                val page = page(session, out, "dup", """{"kinds":[1,1985],"search":"duplikat $lens"}""")
                assertEquals(1, page.count { it == note.id }, "the note must be sent exactly once: $page")
                // The first label in PAGE order, which is the search's order —
                // not the order they were written here.
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

                // The note carries the searched word itself, so it is a HIT of
                // this search as well as the subject of the label above it.
                // The row wins: it goes where the search ranked it, and the
                // splice gives way. The reverse — splicing it early and
                // dropping the row — would move an event the client ordered by
                // relevance to a position relevance did not choose.
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

            // The other half of this contract — that the EXPANSION itself
            // refuses to emit a row twice, even over a store that hands the
            // same row back twice — moved into vespa-eventstore with the
            // expansion, where `SearchExpansionTest` states it against a
            // doubling index. It cannot be stated from here any more: there is
            // no wrapper left to put between the relay and its store.
        }

    @Test
    fun `a lens token is not a search, and costs no lookups at all`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                seed(session, out)

                // `include:spam` makes `search` NON-EMPTY on every anonymous
                // read — a mirror's paging, a NIP-77 catch-up, and the dozen
                // plain reference reads `shared/lens.js` stamps. Gating on
                // non-empty would put all of that behind the expansion; gating
                // on TEXT leaves it exactly where it was. This filter is a
                // recall carrying a lens token, and it stays a recall.
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

                // A publisher orders a Trusted List's members by the score it
                // computed for them — every one of the eleven real lists on
                // staging does, all 180 members scored and every list sorted
                // descending. The relay reads the KEY and never the score, so
                // this ordering is the ONLY thing that makes a spliced member's
                // position mean its rank. If the splice followed store order
                // instead, the position would mean whatever the mirror had
                // caught up on.
                //
                // Published back to front on purpose: recall order and tag
                // order disagree here, so only one of them can produce the
                // assertion below.
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
