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
package com.nosfabrica.vespa.relay.router.presence

import com.nosfabrica.vespa.relay.router.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.router.config.RelayExcludes
import com.nosfabrica.vespa.relay.router.config.RelaySource
import com.nosfabrica.vespa.relay.router.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * What a signed-in reader's own events say this mirror should listen to.
 *
 * The pure half of a presence stream, and the half worth pinning: the loop
 * around it is a diff over a map, while this decides which relays get dialled on
 * somebody's behalf and — the part with teeth — WHAT THEY ARE ASKED FOR. A
 * target whose filter forgot its narrowing is a REQ for a whole relay's corpus,
 * opened because one person signed in.
 *
 * The selects are parsed from HOCON rather than built by hand, so these run
 * against the config an operator actually writes — the outbox/inbox pair below
 * is `router.conf.example`'s, verbatim.
 */
class PresenceTargetsTest {
    private val signer = NostrSignerSync()
    private val reader get() = signer.pubKey

    private fun event(
        kind: Int,
        vararg tags: Array<String>,
    ): Event = signer.sign(1_700_000_000L, kind, arrayOf(*tags), "")

    /** The stream an operator writes, parsed — `dynamic.sources` is what presence reads through. */
    private fun stream(
        relaySource: String,
        kinds: String = "[1, 1111, 30023]",
        exclude: String = "",
        maxRelaysPerList: String = "",
    ): SyncStream =
        RouterConfigLoader
            .parse(
                """
                streams { s {
                    dir = "down"
                    sync = "fetch"
                    filter = { "kinds": $kinds }
                    presence = { }
                    $exclude
                    $maxRelaysPerList
                    relaySource = [ $relaySource ]
                } }
                """.trimIndent(),
            ).streams
            .single()

    /**
     * Every event through every source, which is what the loop does: a reader's
     * one kind 10002 answers the outbox scan and the inbox scan both, and that
     * is the case the pair exists for.
     */
    private fun targets(
        stream: SyncStream,
        vararg events: Event,
        maxRelaysPerReader: Int = 8,
    ): List<PresenceTarget> {
        val dynamic = stream.dynamic!!
        val paired = dynamic.sources.flatMap { src -> events.map { src to it } }
        return PresenceTargets.of(paired, stream.filter, dynamic, maxRelaysPerReader)
    }

    private val outboxSource =
        """{ select = [ { kind = 10002, tag = "r", marker = "write", authors = "pubkey" } ], filter = { "kinds": [10002] } }"""

    private val inboxSource =
        """{ select = [ { kind = 10002, tag = "r", marker = "read", "#p" = "pubkey" } ], filter = { "kinds": [10002] } }"""

    private val scoresSource =
        """{ select = [ { kind = 10040, tag = "30382:rank", relay = 2, authors = 1 } ], filter = { "kinds": [10040] } }"""

    // ---- the outbox and inbox pair ------------------------------------------

    @Test
    fun `their outbox is asked for what they wrote, and nobody else's`() {
        // THE ONE THAT MATTERS. Without the narrowing, one person signing in
        // opens a REQ for every kind-1 event on their write relay — a corpus
        // download from somebody's personal relay, per reader.
        val list = event(10002, arrayOf("r", "wss://write.example", "write"))

        val target = targets(stream(outboxSource), list).single()

        assertEquals("wss://write.example/", target.url.url)
        assertEquals(listOf(reader), target.filter.authors)
        assertEquals(listOf(1, 1111, 30023), target.filter.kinds, "the stream still decides WHICH kinds")
        assertNull(target.filter.tags)
    }

    @Test
    fun `their inbox is asked for what mentions them`() {
        // The other half of the outbox model, and the reason presence reuses the
        // select language rather than naming its sources: `#p` bound to the
        // scanned event's own pubkey is already what this grammar says.
        val list = event(10002, arrayOf("r", "wss://inbox.example", "read"))

        val target = targets(stream(inboxSource), list).single()

        assertEquals("wss://inbox.example/", target.url.url)
        assertEquals(mapOf("p" to listOf(reader)), target.filter.tags)
        assertNull(target.filter.authors, "a mention is written by SOMEBODY ELSE — narrowing authors would ask for nothing")
    }

    @Test
    fun `one relay list read two ways gives two different questions`() {
        // A real 10002: an unmarked entry is read AND write, so the same url
        // answers both scans — once for the reader's own events, once for
        // mentions of them. Two targets on one relay, not one blended filter.
        val list = event(10002, arrayOf("r", "wss://both.example"))

        val found = targets(stream("$outboxSource, $inboxSource"), list)

        assertEquals(listOf("wss://both.example/", "wss://both.example/"), found.map { it.url.url })
        assertEquals(setOf(listOf(reader), null), found.map { it.filter.authors }.toSet())
        assertEquals(setOf(null, mapOf("p" to listOf(reader))), found.map { it.filter.tags }.toSet())
    }

    @Test
    fun `the markers keep the two sides apart`() {
        val list =
            event(
                10002,
                arrayOf("r", "wss://write.example", "write"),
                arrayOf("r", "wss://read.example", "read"),
            )

        assertEquals(
            listOf("wss://write.example/"),
            targets(stream(outboxSource), list).map { it.url.url },
            "we do not mirror a reader's own posts FROM their inbox",
        )
        assertEquals(
            listOf("wss://read.example/"),
            targets(stream(inboxSource), list).map { it.url.url },
            "and we do not look for mentions on a write-only relay",
        )
    }

    @Test
    fun `a redundant default port is not a second relay`() {
        // `wss://x/` and `wss://x:443/` are one server written two ways and the
        // normalizer keeps both, so without folding them a reader naming both
        // costs two sockets, two bands and two subscriptions.
        val list =
            event(
                10002,
                arrayOf("r", "wss://write.example", "write"),
                arrayOf("r", "wss://write.example:443", "write"),
            )

        assertEquals(1, targets(stream(outboxSource), list).size)
    }

    // ---- the NIP-85 source, in the same grammar ------------------------------

    @Test
    fun `a rank provider is asked on its own relay for its own key`() {
        val provider = "b".repeat(64)
        val list = event(10040, arrayOf("30382:rank", provider, "wss://scores.example"))

        val target = targets(stream(scoresSource, kinds = "[30382]"), list).single()

        assertEquals("wss://scores.example/", target.url.url)
        assertEquals(listOf(provider), target.filter.authors, "the SIGNER is what ranks, not the card's subject")
        assertEquals(listOf(30382), target.filter.kinds)
    }

    @Test
    fun `two providers stay two pairs rather than becoming a cross product`() {
        // Gathering the services into one set and the relays into another and
        // asking each relay for every service is exactly what `bindings` exists
        // to prevent — and here it is also wrong on the merits: a provider's
        // cards live on the relay its own list names.
        val first = "b".repeat(64)
        val second = "c".repeat(64)
        val list =
            event(
                10040,
                arrayOf("30382:rank", first, "wss://one.example"),
                arrayOf("30382:rank", second, "wss://two.example"),
            )

        assertEquals(
            mapOf("wss://one.example/" to listOf(first), "wss://two.example/" to listOf(second)),
            targets(stream(scoresSource, kinds = "[30382]"), list).associate { it.url.url to it.filter.authors },
        )
    }

    @Test
    fun `only the named service tag is followed`() {
        // A follower count orders a set and cannot rank one — `TrustProjection`
        // builds no influence cell from those cards. The select names the tag,
        // so this costs no code here at all.
        val list =
            event(
                10040,
                arrayOf("30382:rank", "b".repeat(64), "wss://rank.example"),
                arrayOf("30382:followers", "c".repeat(64), "wss://followers.example"),
            )

        assertEquals(
            listOf("wss://rank.example/"),
            targets(stream(scoresSource, kinds = "[30382]"), list).map { it.url.url },
        )
    }

    @Test
    fun `an entry that cannot fill its binding is dropped whole`() {
        // A `["30382:rank", relay]` missing its service would otherwise widen
        // the ask back to every author on that relay — the opposite of what
        // binding it was for.
        val list = event(10040, arrayOf("30382:rank", "wss://scores.example"))

        assertEquals(emptyList(), targets(stream(scoresSource, kinds = "[30382]"), list))
    }

    @Test
    fun `a list naming nothing usable produces no targets rather than failing`() {
        assertEquals(emptyList(), targets(stream(outboxSource), event(10002)))
        assertEquals(emptyList(), targets(stream(scoresSource, kinds = "[30382]"), event(10040)))
    }

    // ---- the bounds ---------------------------------------------------------

    @Test
    fun `an excluded relay does not spend one of the reader's slots`() {
        // Applied before the cap, deliberately: excluding a url should give the
        // reader another relay, not spend a slot on one we were never going to
        // dial. Our OWN url is the case this exists for.
        val list =
            event(
                10002,
                arrayOf("r", "wss://ours.example", "write"),
                arrayOf("r", "wss://a.example", "write"),
                arrayOf("r", "wss://b.example", "write"),
            )

        val found =
            targets(
                stream(outboxSource, exclude = """exclude = [ "wss://ours.example" ]"""),
                list,
                maxRelaysPerReader = 2,
            )

        assertEquals(listOf("wss://a.example/", "wss://b.example/"), found.map { it.url.url })
    }

    @Test
    fun `a relay list too long to be one is ignored whole, and says so`() {
        // Measured on this corpus: 148 pubkeys publish a kind 10002 of 100 to
        // 10,591 entries. Dropped WHOLE rather than truncated, so its author
        // cannot choose what we see by ordering it.
        val tags = (1..20).map { arrayOf("r", "wss://r$it.example", "write") }.toTypedArray()
        val s = stream(outboxSource, maxRelaysPerList = "maxRelaysPerList = 5")
        var refused = 0

        val found =
            PresenceTargets.of(
                s.dynamic!!.sources.map { it to event(10002, *tags) },
                s.filter,
                s.dynamic!!,
                maxRelaysPerReader = 8,
                onOversized = { refused++ },
            )

        assertEquals(emptyList(), found)
        assertEquals(1, refused, "a cap set too low reads from outside like a reader who publishes nothing")
    }

    @Test
    fun `one reader cannot put more than the cap into the fan-out`() {
        val tags = (1..50).map { arrayOf("r", "wss://r$it.example", "write") }.toTypedArray()

        assertEquals(3, targets(stream(outboxSource), event(10002, *tags), maxRelaysPerReader = 3).size)
    }

    // ---- the subscription key -----------------------------------------------

    private fun keyOf(
        s: SyncStream,
        event: Event,
    ) = PresenceTargets
        .of(s.dynamic!!.sources.map { it to event }, s.filter, s.dynamic!!, 8)
        .single()
        .key

    @Test
    fun `two readers naming one provider share a key`() {
        // What makes the subscription set (relay, question) rather than
        // (reader, relay): four hundred readers whose 10040s name one popular
        // provider must put ONE filter on it.
        val tag = arrayOf("30382:rank", "b".repeat(64), "wss://scores.example")
        val s = stream(scoresSource, kinds = "[30382]")

        assertEquals(
            keyOf(s, NostrSignerSync().sign<Event>(1_700_000_000L, 10040, arrayOf(tag), "")),
            keyOf(s, NostrSignerSync().sign<Event>(1_700_000_100L, 10040, arrayOf(tag), "")),
        )
    }

    @Test
    fun `two readers' own outboxes on one relay are two questions`() {
        // The mirror image, and the reason the key is the FILTER and not the
        // url: everyone writes to nos.lol, and each of them is a different ask.
        val tag = arrayOf("r", "wss://nos.lol", "write")
        val s = stream(outboxSource)

        assertTrue(
            keyOf(s, NostrSignerSync().sign<Event>(1_700_000_000L, 10002, arrayOf(tag), "")) !=
                keyOf(s, NostrSignerSync().sign<Event>(1_700_000_000L, 10002, arrayOf(tag), "")),
        )
    }

    // ---- the scan -----------------------------------------------------------

    @Test
    fun `the scan is the source's own filter, narrowed to the one reader`() {
        // Everything else in it is the operator's — including a `limit`, which
        // is how they say what one reader's scan may cost on a regular kind.
        val source = RelaySource(selects = emptyList(), filter = Filter(kinds = listOf(10002), limit = 4))

        val scan = PresenceTargets.scanFor(source, reader)

        assertEquals(listOf(reader), scan.authors)
        assertEquals(listOf(10002), scan.kinds)
        assertEquals(4, scan.limit)
    }

    @Test
    fun `an empty source list yields nothing rather than everything`() {
        val dynamic = RelayDiscoveryConfig(emptyList(), 3_600, 4, RelayExcludes.NONE)

        assertEquals(emptyList(), PresenceTargets.of(emptyList(), Filter(kinds = listOf(1)), dynamic, 8))
    }
}
