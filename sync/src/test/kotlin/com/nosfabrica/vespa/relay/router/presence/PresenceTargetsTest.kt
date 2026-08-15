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

import com.nosfabrica.vespa.relay.router.config.PresenceConfig
import com.nosfabrica.vespa.relay.router.config.PresenceSource
import com.nosfabrica.vespa.relay.router.config.RelayExcludes
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * What a signed-in reader's own lists say this mirror should listen to.
 *
 * The pure half of a presence stream, and the half worth pinning: the loop
 * around it is a diff over a map, while this decides which relays get dialled
 * on somebody's behalf and — the part with teeth — WHAT THEY ARE ASKED FOR. A
 * target whose filter forgot its narrowing is a REQ for a whole relay's corpus,
 * opened because one person signed in.
 */
class PresenceTargetsTest {
    private val signer = NostrSignerSync()

    private fun event(
        kind: Int,
        vararg tags: Array<String>,
    ): Event = signer.sign(1_700_000_000L, kind, arrayOf(*tags), "")

    private fun config(
        maxRelaysPerReader: Int = 8,
        exclude: List<String> = emptyList(),
    ) = PresenceConfig(
        source = PresenceSource.OUTBOX,
        maxRelaysPerReader = maxRelaysPerReader,
        // Compiled the way the loader compiles it, so these tests exercise the
        // production entry classification rather than a set of strings.
        exclude = RelayExcludes.parse(exclude),
    )

    private fun outbox(
        list: Event,
        base: Filter = Filter(kinds = listOf(1)),
        config: PresenceConfig = config(),
    ) = PresenceTargets.of(PresenceSource.OUTBOX, list, base, config)

    private fun scores(
        list: Event,
        base: Filter = Filter(kinds = listOf(30382)),
        config: PresenceConfig = config(),
    ) = PresenceTargets.of(PresenceSource.SCORES, list, base, config)

    // ---- outbox ------------------------------------------------------------

    @Test
    fun `an outbox is the write side, and an unmarked entry is both`() {
        // NIP-65's own rule, and quartz's `parseWriteNorm` is where it lives —
        // a second copy here would be a copy that silently stops matching.
        val list =
            event(
                10002,
                arrayOf("r", "wss://write.example", "write"),
                arrayOf("r", "wss://read.example", "read"),
                arrayOf("r", "wss://both.example"),
            )

        val urls = outbox(list).map { it.url.url }

        assertTrue(urls.any { it.contains("write.example") })
        assertTrue(urls.any { it.contains("both.example") }, "an unmarked r tag is read AND write")
        assertTrue(urls.none { it.contains("read.example") }, "we do not mirror FROM somebody's inbox")
    }

    @Test
    fun `every outbox relay is asked for that reader and nobody else`() {
        // THE ONE THAT MATTERS. Without the narrowing, one person signing in
        // opens a REQ for every kind-1 event on their write relay — which for a
        // popular relay is a corpus download nobody asked for, per reader.
        val list = event(10002, arrayOf("r", "wss://write.example"))

        val target = outbox(list, base = Filter(kinds = listOf(1, 30023))).single()

        assertEquals(listOf(signer.pubKey), target.filter.authors)
        assertEquals(listOf(1, 30023), target.filter.kinds, "the stream still decides WHICH kinds")
    }

    @Test
    fun `a redundant default port is not a second relay`() {
        // `wss://x/` and `wss://x:443/` are one server written two ways, and the
        // normalizer keeps both — so without folding them a reader naming both
        // costs two sockets, two bands and two subscriptions.
        val list =
            event(
                10002,
                arrayOf("r", "wss://write.example"),
                arrayOf("r", "wss://write.example:443"),
            )

        assertEquals(1, outbox(list).size)
    }

    // ---- scores ------------------------------------------------------------

    @Test
    fun `a rank provider is asked on its own relay for its own key`() {
        val provider = "b".repeat(64)
        val list = event(10040, arrayOf("30382:rank", provider, "wss://scores.example"))

        val target = scores(list).single()

        assertEquals("wss://scores.example/", target.url.url)
        assertEquals(listOf(provider), target.filter.authors, "the SIGNER is what ranks, not the card's subject")
        assertEquals(listOf(30382), target.filter.kinds)
    }

    @Test
    fun `two providers stay two pairs rather than becoming a cross product`() {
        // Gathering the services into one set and the relays into another and
        // asking each relay for every service is the mistake `RelaySelect`'s
        // bindings exist to prevent — and here it is also wrong on the merits:
        // a provider's cards are on the relay its own list names.
        val first = "b".repeat(64)
        val second = "c".repeat(64)
        val list =
            event(
                10040,
                arrayOf("30382:rank", first, "wss://one.example"),
                arrayOf("30382:rank", second, "wss://two.example"),
            )

        val targets = scores(list).associate { it.url.url to it.filter.authors }

        assertEquals(
            mapOf("wss://one.example/" to listOf(first), "wss://two.example/" to listOf(second)),
            targets,
        )
    }

    @Test
    fun `only the ranking service is followed`() {
        // A follower count orders a set and cannot rank one — `TrustProjection`
        // builds no influence cell from those cards, so mirroring them would
        // not move a ranked search by one row.
        val list =
            event(
                10040,
                arrayOf("30382:rank", "b".repeat(64), "wss://rank.example"),
                arrayOf("30382:followers", "c".repeat(64), "wss://followers.example"),
            )

        assertEquals(listOf("wss://rank.example/"), scores(list).map { it.url.url })
    }

    @Test
    fun `an entry with no relay hint resolves to nothing`() {
        // Quartz's own decision — `ServiceProviderTag.parse` wants all three
        // fields — and the same reason `TrustNotice` tells such a reader their
        // list "names no usable 30382:rank service".
        val list = event(10040, arrayOf("30382:rank", "b".repeat(64)))

        assertEquals(emptyList(), scores(list))
    }

    @Test
    fun `a list naming nothing usable produces no targets rather than failing`() {
        assertEquals(emptyList(), scores(event(10040)))
        assertEquals(emptyList(), outbox(event(10002)))
    }

    // ---- the bounds --------------------------------------------------------

    @Test
    fun `an excluded relay does not spend one of the reader's slots`() {
        // Applied before the cap, deliberately: excluding a url should give the
        // reader another relay, not spend a slot on one we were never going to
        // dial. Our OWN url is the case this exists for.
        val list =
            event(
                10002,
                arrayOf("r", "wss://ours.example"),
                arrayOf("r", "wss://a.example"),
                arrayOf("r", "wss://b.example"),
            )

        val urls = outbox(list, config = config(maxRelaysPerReader = 2, exclude = listOf("wss://ours.example"))).map { it.url.url }

        assertEquals(listOf("wss://a.example/", "wss://b.example/"), urls)
    }

    @Test
    fun `a relay list too long to be one is capped rather than dialled`() {
        // Measured on this corpus: 148 pubkeys publish a kind 10002 of 100 to
        // 10,591 entries. Without the cap, one of them signing in dials ten
        // thousand relays.
        val tags = (1..50).map { arrayOf("r", "wss://r$it.example") }.toTypedArray()

        assertEquals(3, outbox(event(10002, *tags), config = config(maxRelaysPerReader = 3)).size)
    }

    @Test
    fun `two readers naming one relay and one question share a key`() {
        // What makes the subscription set (relay, question) rather than
        // (reader, relay): four hundred readers whose 10040s name one popular
        // provider must put ONE filter on it.
        val provider = "b".repeat(64)
        val a = NostrSignerSync().sign<Event>(1_700_000_000L, 10040, arrayOf(arrayOf("30382:rank", provider, "wss://scores.example")), "")
        val b = NostrSignerSync().sign<Event>(1_700_000_100L, 10040, arrayOf(arrayOf("30382:rank", provider, "wss://scores.example")), "")

        assertEquals(scores(a).single().key, scores(b).single().key)
    }

    @Test
    fun `two readers' own outboxes on one relay are two questions`() {
        // The mirror image, and the reason the key is the FILTER and not the
        // url: everyone writes to nos.lol, and each of them is a different ask.
        val a = NostrSignerSync().sign<Event>(1_700_000_000L, 10002, arrayOf(arrayOf("r", "wss://nos.lol")), "")
        val b = NostrSignerSync().sign<Event>(1_700_000_000L, 10002, arrayOf(arrayOf("r", "wss://nos.lol")), "")

        assertTrue(outbox(a).single().key != outbox(b).single().key)
    }

    // ---- the store read ----------------------------------------------------

    @Test
    fun `each source reads its own replaceable kind, for one author, once`() {
        // `limit = 1` because both kinds are replaceable and this runs per
        // signed-in reader per poll: a read that could return history would
        // make the cost a function of how often somebody edits their list.
        val reader = "a".repeat(64)

        val outboxFilter = PresenceTargets.listFilter(PresenceSource.OUTBOX, reader)
        assertEquals(listOf(10002), outboxFilter.kinds)
        assertEquals(listOf(reader), outboxFilter.authors)
        assertEquals(1, outboxFilter.limit)

        val scoresFilter = PresenceTargets.listFilter(PresenceSource.SCORES, reader)
        assertEquals(listOf(10040), scoresFilter.kinds)
        assertEquals(listOf(reader), scoresFilter.authors)
        assertEquals(1, scoresFilter.limit)
    }
}
