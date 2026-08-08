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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The duplicate-url fold: which discovered urls are one relay, decided on what
 * each of them served rather than on what its url looks like.
 *
 * No probe here — [RelayAliases] takes fingerprints as an argument precisely so
 * the decision can be tested without a relay — so every test states the windows
 * two urls returned and asserts what was concluded from them.
 */
class RelayAliasesTest {
    private fun url(raw: String): NormalizedRelayUrl = RelayUrlNormalizer.normalize(raw)

    /** A window of [n] ids, offset so two windows can be made to overlap by construction. */
    private fun window(
        n: Int,
        from: Int = 0,
    ): Set<String> = (from until from + n).mapTo(HashSet()) { "id%064d".format(it) }

    private val nos = url("wss://nos.lol")
    private val nosAlpha = url("wss://nos.lol/alpha")
    private val nosBeacon = url("wss://nos.lol/beacon-glyph")

    @Test
    fun `two urls serving the same window are one relay`() {
        val aliases = RelayAliases()
        val group = listOf(nos, nosAlpha)
        val print = window(100)

        val learned = aliases.learn(group, mapOf(nos to print, nosAlpha to print))

        assertEquals(mapOf(nosAlpha to nos), learned)
        assertEquals(nos, aliases.canonicalOf(nosAlpha))
        assertEquals(nos, aliases.canonicalOf(nos))
    }

    @Test
    fun `a path serving its own events is left alone`() {
        val aliases = RelayAliases()
        val ditto = url("wss://ditto.pub")
        val dittoRelay = url("wss://ditto.pub/relay")

        val learned = aliases.learn(listOf(ditto, dittoRelay), mapOf(ditto to window(100), dittoRelay to window(100, from = 500)))

        assertTrue(learned.isEmpty())
        assertEquals(dittoRelay, aliases.canonicalOf(dittoRelay))
    }

    @Test
    fun `a window too small to mean anything folds nothing`() {
        val aliases = RelayAliases()
        // Identical, and identical is meaningless at this size: two quiet
        // relays holding the same four events prove nothing about each other.
        val tiny = window(4)

        assertTrue(aliases.learn(listOf(nos, nosAlpha), mapOf(nos to tiny, nosAlpha to tiny)).isEmpty())
    }

    @Test
    fun `a url that never answered is not folded`() {
        val aliases = RelayAliases()

        // Only the leader has a fingerprint. Silence is not evidence.
        val learned = aliases.learn(listOf(nos, nosAlpha), mapOf(nos to window(100)))

        assertTrue(learned.isEmpty())
        assertEquals(nosAlpha, aliases.canonicalOf(nosAlpha))
    }

    @Test
    fun `an unreachable leader decides nothing about the rest`() {
        val aliases = RelayAliases()
        val print = window(100)

        assertTrue(aliases.learn(listOf(nos, nosAlpha, nosBeacon), mapOf(nosAlpha to print, nosBeacon to print)).isEmpty())
    }

    @Test
    fun `a window that merely moved on still matches`() {
        val aliases = RelayAliases()
        // The two dials are seconds apart on a live relay: 60 of the leader's
        // 100 ids are still in the second window, which is over the bar.
        val learned = aliases.learn(listOf(nos, nosAlpha), mapOf(nos to window(100), nosAlpha to window(100, from = 40)))

        assertEquals(mapOf(nosAlpha to nos), learned)
    }

    @Test
    fun `a window that moved on entirely does not`() {
        val aliases = RelayAliases()
        val learned = aliases.learn(listOf(nos, nosAlpha), mapOf(nos to window(100), nosAlpha to window(100, from = 80)))

        assertTrue(learned.isEmpty())
    }

    @Test
    fun `the pathless url is the one kept`() {
        val aliases = RelayAliases()
        val print = window(100)
        // Deliberately ordered worst-first: the leader is chosen by preference,
        // not by position.
        val group = listOf(nosBeacon, nosAlpha, nos)

        aliases.learn(group, group.associateWith { print })

        assertEquals(nos, aliases.canonicalOf(nosAlpha))
        assertEquals(nos, aliases.canonicalOf(nosBeacon))
    }

    @Test
    fun `wss wins over ws on the same host`() {
        val aliases = RelayAliases()
        val secure = url("wss://relay.example.com")
        val plain = url("ws://relay.example.com")
        val print = window(100)

        aliases.learn(listOf(plain, secure), mapOf(plain to print, secure to print))

        assertEquals(secure, aliases.canonicalOf(plain))
    }

    @Test
    fun `grouping is by host, so scheme port and path all reach one group`() {
        val aliases = RelayAliases()
        val candidates = listOf(nos, nosAlpha, url("ws://nos.lol"), url("wss://other.example"))

        val groups = aliases.unresolved(candidates)

        assertEquals(1, groups.size)
        assertEquals(3, groups.single().size)
    }

    @Test
    fun `a host wearing one url is never probed`() {
        val aliases = RelayAliases()

        assertTrue(aliases.unresolved(listOf(nos, url("wss://other.example"))).isEmpty())
    }

    @Test
    fun `a group with every verdict in hand is not probed again`() {
        val aliases = RelayAliases()
        val print = window(100)
        aliases.learn(listOf(nos, nosAlpha), mapOf(nos to print, nosAlpha to print))
        aliases.markDistinct(nos)

        assertTrue(aliases.unresolved(listOf(nos, nosAlpha)).isEmpty())
    }

    @Test
    fun `a url proved distinct is not re-probed, but the leader is`() {
        val aliases = RelayAliases()
        // nosAlpha answered with its own events last cycle, so it is settled.
        aliases.learn(listOf(nos, nosAlpha), mapOf(nos to window(100), nosAlpha to window(100, from = 500)))

        val toProbe = aliases.toProbe(listOf(nos, nosAlpha, nosBeacon))

        // The leader is re-measured because the window it is compared against
        // has moved; the settled url is not.
        assertEquals(listOf(nos, nosBeacon), toProbe)
    }

    @Test
    fun `folding moves what each url was paired with onto the survivor`() {
        val aliases = RelayAliases()
        val print = window(100)
        aliases.learn(listOf(nos, nosAlpha), mapOf(nos to print, nosAlpha to print))

        val folded =
            aliases.fold(
                listOf(
                    DiscoveredRelay(nos, mapOf("authors" to setOf("a"))),
                    DiscoveredRelay(nosAlpha, mapOf("authors" to setOf("b"))),
                ),
            )

        assertEquals(1, folded.size)
        assertEquals(nos, folded.single().url)
        // Dropping the url without carrying its authors would stop asking for
        // author b entirely — a fold that loses data, not duplicates.
        assertEquals(setOf("a", "b"), folded.single().narrow["authors"])
    }

    @Test
    fun `a verdict pointing at a folded url resolves to the end of the chain`() {
        val aliases = RelayAliases()
        aliases.adopt(mapOf(nosAlpha to nos))
        aliases.adopt(mapOf(nosBeacon to nosAlpha))

        assertEquals(nos, aliases.canonicalOf(nosBeacon))
    }

    @Test
    fun `hosts are read without their port and paths without their slashes`() {
        assertEquals("nos.lol", RelayAliases.hostOf("wss://nos.lol:443/alpha"))
        assertEquals("nos.lol", RelayAliases.hostOf("ws://NOS.LOL/"))
        assertEquals("[::1]", RelayAliases.hostOf("ws://[::1]:7777/alpha"))
        assertEquals("alpha", RelayAliases.pathOf("wss://nos.lol:443/alpha"))
        assertEquals("", RelayAliases.pathOf("wss://nos.lol/"))
        assertEquals("remove/tango", RelayAliases.pathOf("wss://nostr.oxtr.dev/remove/tango"))
    }

    @Test
    fun `folding is a no-op until something has been learned`() {
        val aliases = RelayAliases()
        val relays = listOf(DiscoveredRelay(nos), DiscoveredRelay(nosAlpha))

        assertEquals(relays, aliases.fold(relays))
        assertFalse(aliases.size() > 0)
    }
}
