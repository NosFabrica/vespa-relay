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

        val learned = aliases.learn(group, aliases.toProbe(group).first(), mapOf(nos to print, nosAlpha to print))

        assertEquals(mapOf(nosAlpha to nos), learned.folded)
        assertEquals(nos, aliases.canonicalOf(nosAlpha))
        assertEquals(nos, aliases.canonicalOf(nos))
    }

    @Test
    fun `a path serving its own events is left alone`() {
        val aliases = RelayAliases()
        val ditto = url("wss://ditto.pub")
        val dittoRelay = url("wss://ditto.pub/relay")

        val learned = aliases.learn(listOf(ditto, dittoRelay), aliases.toProbe(listOf(ditto, dittoRelay)).first(), mapOf(ditto to window(100), dittoRelay to window(100, from = 500)))

        assertTrue(learned.folded.isEmpty())
        assertEquals(dittoRelay, aliases.canonicalOf(dittoRelay))
    }

    @Test
    fun `a url that answered nothing is not called its own relay`() {
        val aliases = RelayAliases()
        // Measured live: relay.damus.io/lantern-oscar-dynamo answered a probe
        // with ZERO events while its host's other paths folded normally. An
        // empty window is not a null one, so it used to fall past the fold test
        // and out the other side as "probed, and it is its own relay" — a claim
        // published for thirty days on the strength of nothing at all.
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to emptySet()))

        assertTrue(learned.folded.isEmpty())
        assertTrue(nosAlpha !in learned.distinct, "silence was recorded as proof of a distinct relay")
        assertTrue(!aliases.measured(nosAlpha), "a url that said nothing must come back to the fan-out")
    }

    @Test
    fun `a window under the sample floor decides nothing either way`() {
        val aliases = RelayAliases()
        // The satsdisco case: 9 events, all 9 shared with the leader. Too thin
        // to fold on — and equally too thin to call a separate relay, which is
        // the stronger claim of the two.
        val nine = window(9)
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to nine))

        assertTrue(learned.folded.isEmpty())
        assertTrue(learned.distinct.isEmpty(), "a 9-event window was published as a verdict")
        assertTrue(!aliases.measured(nosAlpha))
    }

    @Test
    fun `a leader that answered too thinly clears nobody, itself included`() {
        val aliases = RelayAliases()
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(5), nosAlpha to window(100)))

        assertTrue(learned.folded.isEmpty())
        assertTrue(learned.distinct.isEmpty(), "a thin leader still published verdicts: ${learned.distinct}")
    }

    @Test
    fun `two verdicts that disagree do not pin a url as its own duplicate`() {
        val aliases = RelayAliases()
        // A says "I am B", B says "I am A" — two passes that disagreed, or a
        // record edited by hand. Resolving B's canonical walks back to B, and
        // writing that would make `folded[B] = B`: not a fold, but a url marked
        // as its own duplicate, which `measured` then answers true for forever
        // while `unresolved` drops the group and nothing revisits it.
        aliases.adopt(mapOf(nosAlpha to nos, nos to nosAlpha))

        assertFalse(aliases.measured(nos) && aliases.canonicalOf(nos) == nos && nos in aliases.verdicts().keys)
        assertTrue(nos !in aliases.verdicts().keys, "a url was folded onto itself: ${aliases.verdicts()}")
    }

    @Test
    fun `forgetting a url drops every verdict held about it`() {
        val aliases = RelayAliases()
        val print = window(100)
        aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to print, nosAlpha to print))
        assertTrue(aliases.measured(nosAlpha) && aliases.measured(nos))

        // The store is the record and this map is a cache of it. Without an
        // eviction path the cache outlives the TTL: the record expires, is
        // never republished because `measured` says there is nothing to do, and
        // the fan-out keeps folding on evidence that no longer exists anywhere.
        aliases.forget(listOf(nos, nosAlpha))

        assertFalse(aliases.measured(nosAlpha))
        assertFalse(aliases.measured(nos))
        assertEquals(nosAlpha, aliases.canonicalOf(nosAlpha))
        assertEquals(1, aliases.unresolved(listOf(nos, nosAlpha)).size, "the group must be probeable again")
    }

    @Test
    fun `learn folds onto the leader it was given, not one it recomputes`() {
        val aliases = RelayAliases()
        val print = window(100)
        // `leaderOf` prefers a url something already folded onto, and
        // `canonicals` is mutated by every concurrent pass — so recomputing the
        // leader here could name a different url than the one that was actually
        // fingerprinted as the yardstick, and discard the group's whole pass.
        val learned = aliases.learn(listOf(nos, nosAlpha, nosBeacon), nosAlpha, mapOf(nosAlpha to print, nos to print, nosBeacon to print))

        assertEquals(nosAlpha, aliases.canonicalOf(nos))
        assertEquals(nosAlpha, aliases.canonicalOf(nosBeacon))
        assertEquals(setOf(nos, nosBeacon), learned.folded.keys)
    }

    @Test
    fun `a window too small to mean anything folds nothing`() {
        val aliases = RelayAliases()
        // Identical, and identical is meaningless at this size: two quiet
        // relays holding the same four events prove nothing about each other.
        val tiny = window(4)

        assertTrue(aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to tiny, nosAlpha to tiny)).folded.isEmpty())
    }

    @Test
    fun `a url that never answered is not folded`() {
        val aliases = RelayAliases()

        // Only the leader has a fingerprint. Silence is not evidence.
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100)))

        assertTrue(learned.folded.isEmpty())
        assertEquals(nosAlpha, aliases.canonicalOf(nosAlpha))
    }

    @Test
    fun `an unreachable leader decides nothing about the rest`() {
        val aliases = RelayAliases()
        val print = window(100)

        assertTrue(aliases.learn(listOf(nos, nosAlpha, nosBeacon), aliases.toProbe(listOf(nos, nosAlpha, nosBeacon)).first(), mapOf(nosAlpha to print, nosBeacon to print)).folded.isEmpty())
    }

    @Test
    fun `a window that merely moved on still matches`() {
        val aliases = RelayAliases()
        // The two dials are seconds apart on a live relay: 60 of the leader's
        // 100 ids are still in the second window, which is over the bar.
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to window(100, from = 40)))

        assertEquals(mapOf(nosAlpha to nos), learned.folded)
    }

    @Test
    fun `a window that moved on entirely does not`() {
        val aliases = RelayAliases()
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to window(100, from = 80)))

        assertTrue(learned.folded.isEmpty())
    }

    @Test
    fun `the pathless url is the one kept`() {
        val aliases = RelayAliases()
        val print = window(100)
        // Deliberately ordered worst-first: the leader is chosen by preference,
        // not by position.
        val group = listOf(nosBeacon, nosAlpha, nos)

        aliases.learn(group, aliases.toProbe(group).first(), group.associateWith { print })

        assertEquals(nos, aliases.canonicalOf(nosAlpha))
        assertEquals(nos, aliases.canonicalOf(nosBeacon))
    }

    @Test
    fun `wss wins over ws on the same host`() {
        val aliases = RelayAliases()
        val secure = url("wss://relay.example.com")
        val plain = url("ws://relay.example.com")
        val print = window(100)

        aliases.learn(listOf(plain, secure), aliases.toProbe(listOf(plain, secure)).first(), mapOf(plain to print, secure to print))

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
        aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to print, nosAlpha to print))
        aliases.markDistinct(nos)

        assertTrue(aliases.unresolved(listOf(nos, nosAlpha)).isEmpty())
    }

    @Test
    fun `a url proved distinct is not re-probed, but the leader is`() {
        val aliases = RelayAliases()
        // nosAlpha answered with its own events last cycle, so it is settled.
        aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to window(100, from = 500)))

        val toProbe = aliases.toProbe(listOf(nos, nosAlpha, nosBeacon))

        // The leader is re-measured because the window it is compared against
        // has moved; the settled url is not.
        assertEquals(listOf(nos, nosBeacon), toProbe)
    }

    @Test
    fun `folding moves what each url was paired with onto the survivor`() {
        val aliases = RelayAliases()
        val print = window(100)
        aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to print, nosAlpha to print))

        val folded =
            RelayAliases.foldOnto(
                listOf(
                    DiscoveredRelay(nos, mapOf("authors" to setOf("a"))),
                    DiscoveredRelay(nosAlpha, mapOf("authors" to setOf("b"))),
                ),
                aliases.verdicts(),
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

        assertEquals(relays, RelayAliases.foldOnto(relays, aliases.verdicts()))
        assertFalse(aliases.size() > 0)
    }
}
