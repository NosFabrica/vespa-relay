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
package com.nosfabrica.vespa.relay.router.discovery

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * THE DESCRIPTIVE HALF OF THE RECORD — the tags a stranger's crawler reads
 * without knowing this router exists.
 *
 * Worth pinning because every failure here is a signed, month-long, confident
 * claim about somebody else's server: a `0` where we mean "did not measure", a
 * relay advertised as free because its document was silent, a record carrying
 * both `auth` and `!auth`.
 */
class RelayFactsTest {
    @Test
    fun `an absent fact writes no tag rather than a zero`() {
        // A `0` rtt is a relay that answered instantly and an empty `s` is a
        // relay running nothing. Since the writer OWNS these, absent also means
        // "clear what the last pass wrote", which is what keeps a stale reading
        // from outliving the dial that took it.
        assertEquals(emptyList(), RelayFacts().tags().map { it[0] })
        val partial = RelayFacts(network = "clearnet", rttOpenMs = 0)
        assertEquals(listOf("n", "rtt-open"), partial.tags().map { it[0] })
        assertEquals("0", partial.tags().single { it[0] == "rtt-open" }[1], "a measured zero is a measurement; only null is absence")
    }

    @Test
    fun `the tags are NIP-66's spelling, and repeated rather than packed`() {
        val tags =
            RelayFacts(
                network = "tor",
                rttOpenMs = 41,
                rttReadMs = 120,
                requirements = listOf("auth", "!payment"),
                software = "git+https://github.com/hoytech/strfry.git",
                version = "1.0.3",
                supportedNips = listOf(1, 11, 50),
            ).tags()
        // "Tags with more than one value should be repeated, rather than putting
        // all values in a single tag" — NIP-66, and both `R` and `N` are such.
        assertEquals(listOf("auth", "!payment"), tags.filter { it[0] == "R" }.map { it[1] })
        assertEquals(listOf("1", "11", "50"), tags.filter { it[0] == "N" }.map { it[1] })
        assertEquals("41", tags.single { it[0] == "rtt-open" }[1])
        assertEquals("tor", tags.single { it[0] == "n" }[1])
    }

    @Test
    fun `the version rides the software tag rather than minting a letter for itself`() {
        // Minting an undefined single letter to hold a fact is exactly the
        // mistake that put the grade on `s`. Sampled across 12 monitors and 800
        // records, nobody writes a `v` tag at all — so the version goes in the
        // third element, where a reader of `["s", <software>]` never looks.
        val withVersion = RelayFacts(software = "strfry", version = "1.0.3").tags().single { it[0] == "s" }
        assertEquals(listOf("s", "strfry", "1.0.3"), withVersion.toList())
        val without = RelayFacts(software = "strfry").tags().single { it[0] == "s" }
        assertEquals(listOf("s", "strfry"), without.toList(), "a blank version adds no element rather than an empty one")
        assertEquals(listOf("s", "strfry"), RelayFacts(software = "strfry", version = "  ").tags().single { it[0] == "s" }.toList())
        assertTrue(RelayFacts(version = "1.0.3").tags().isEmpty(), "a version with no software has nothing to hang on")
    }

    @Test
    fun `a measured requirement overrides the relay's claim about the same key`() {
        // NIP-66 names this case outright: a monitor's finding MAY contradict
        // what the document advertises. A relay publishing `auth_required:
        // false` that then rejects our key is not open, and copying its claim
        // across would sign the relay's mistake under our name.
        val merged = RelayFacts.merge(measured = listOf("auth"), advertised = listOf("!auth", "!payment"))
        assertEquals(listOf("auth", "!payment"), merged)
        // ONE ENTRY PER KEY. A record holding both `auth` and `!auth` says
        // nothing at all, and it is what a plain concatenation produces on
        // exactly the relays where the disagreement is the interesting part.
        assertEquals(1, merged.count { RelayFacts.subjectOf(it) == "auth" })
    }

    @Test
    fun `nothing measured leaves the document's claims exactly as they are`() {
        assertEquals(
            listOf("!auth", "payment"),
            RelayFacts.merge(measured = emptyList(), advertised = listOf("!auth", "payment")),
        )
        assertEquals(emptyList(), RelayFacts.merge(measured = emptyList(), advertised = emptyList()))
    }

    @Test
    fun `every tag the writer publishes is one it also owns`() {
        // The invariant that keeps a stale reading from outliving its dial: a
        // fact written on a pass this sweep must be replaceable on the next.
        // A field added to `tags()` without a matching entry in OWNED would be
        // carried forward by `edit` forever, which is how the old `n` and
        // `rtt-open` residue survived the writer that produced it.
        val everything =
            RelayFacts(
                network = "clearnet",
                rttOpenMs = 1,
                rttReadMs = 2,
                requirements = listOf("auth"),
                software = "strfry",
                version = "1",
                supportedNips = listOf(1),
            )
        for (tag in everything.tags()) {
            assertTrue(tag[0] in RelayFacts.OWNED, "`${tag[0]}` is written but not owned — it would outlive the pass that wrote it")
        }
        assertTrue(RelayFacts.RTT_WRITE_TAG in RelayFacts.OWNED, "owned though never written: the old passive monitor's readings need clearing")
    }

    @Test
    fun `a document address is the relay's own, over the http scheme that pairs with its socket`() {
        val https = RelayDocument.httpAddressOf(RelayUrlNormalizer.normalize("wss://relay.example/inbox"))
        assertEquals("https://relay.example/inbox", https)
        assertEquals("http://relay.example/", RelayDocument.httpAddressOf(RelayUrlNormalizer.normalize("ws://relay.example")))
        // The port is part of the address: a relay on 8080 serves its document
        // there and nowhere else.
        assertEquals("https://relay.example:8080/", RelayDocument.httpAddressOf(RelayUrlNormalizer.normalize("wss://relay.example:8080")))
        // A url with no host to ask, built RAW: the normalizer refuses to make
        // one, but a record's `d` tag is a stranger's string and reaches this
        // through paths that never went through the normalizer.
        assertNull(RelayDocument.httpAddressOf(NormalizedRelayUrl("wss://")), "no host is nothing to ask")
        assertNull(RelayDocument.httpAddressOf(NormalizedRelayUrl("https://relay.example")), "and neither is a scheme this is not for")
    }
}
