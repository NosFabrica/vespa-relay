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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.peers.RelayFacts
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The descriptive half of the record: the NIP-66 tags a stranger's crawler reads. */
class RelayFactsTest {
    @Test
    fun `an absent fact writes no tag rather than a zero`() {
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
        // NIP-66: tags with more than one value are repeated, not packed.
        assertEquals(listOf("auth", "!payment"), tags.filter { it[0] == "R" }.map { it[1] })
        assertEquals(listOf("1", "11", "50"), tags.filter { it[0] == "N" }.map { it[1] })
        assertEquals("41", tags.single { it[0] == "rtt-open" }[1])
        assertEquals("tor", tags.single { it[0] == "n" }[1])
    }

    @Test
    fun `the version rides the software tag rather than minting a letter for itself`() {
        // No monitor in the wild writes a `v` tag.
        val withVersion = RelayFacts(software = "strfry", version = "1.0.3").tags().single { it[0] == "s" }
        assertEquals(listOf("s", "strfry", "1.0.3"), withVersion.toList())
        val without = RelayFacts(software = "strfry").tags().single { it[0] == "s" }
        assertEquals(listOf("s", "strfry"), without.toList(), "a blank version adds no element rather than an empty one")
        assertEquals(listOf("s", "strfry"), RelayFacts(software = "strfry", version = "  ").tags().single { it[0] == "s" }.toList())
        assertTrue(RelayFacts(version = "1.0.3").tags().isEmpty(), "a version with no software has nothing to hang on")
    }

    @Test
    fun `a measured requirement overrides the relay's claim about the same key`() {
        val merged = RelayFacts.merge(measured = listOf("auth"), advertised = listOf("!auth", "!payment"))
        assertEquals(listOf("auth", "!payment"), merged)
        // One entry per key: a record holding both `auth` and `!auth` says nothing.
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
        // A tag written but not in OWNED is carried forward by `edit` forever.
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
        assertEquals("https://relay.example:8080/", RelayDocument.httpAddressOf(RelayUrlNormalizer.normalize("wss://relay.example:8080")))
        // Built raw: a record's `d` tag is a stranger's string that never went through the normalizer.
        assertNull(RelayDocument.httpAddressOf(NormalizedRelayUrl("wss://")), "no host is nothing to ask")
        assertNull(RelayDocument.httpAddressOf(NormalizedRelayUrl("https://relay.example")), "and neither is a scheme this is not for")
    }
}
