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

import com.nosfabrica.vespa.relay.fold.RelayAliases
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** The fold decided on stated windows, with no probe in the loop. */
class RelayAliasesTest {
    private fun url(raw: String): NormalizedRelayUrl = RelayUrlNormalizer.normalize(raw)

    /** A window of [n] ids starting at [from], so two windows overlap by construction. */
    private fun window(
        n: Int,
        from: Int = 0,
    ): Set<String> = (from until from + n).mapTo(HashSet()) { "id%064d".format(it) }

    private val nos = url("wss://nos.lol")
    private val nosAlpha = url("wss://nos.lol/alpha")
    private val nosBeacon = url("wss://nos.lol/beacon-glyph")

    @Test
    fun `two urls serving the same window are one relay`() {
        var aliases = RelayAliases()
        val group = listOf(nos, nosAlpha)
        val print = window(100)

        val learned = aliases.learn(group, aliases.toProbe(group).first(), mapOf(nos to print, nosAlpha to print))
        aliases += learned

        assertEquals(mapOf(nosAlpha to nos), learned.folded)
        assertEquals(nos, aliases.canonicalOf(nosAlpha))
        assertEquals(nos, aliases.canonicalOf(nos))
    }

    @Test
    fun `a path serving its own events is left alone`() {
        var aliases = RelayAliases()
        val ditto = url("wss://ditto.pub")
        val dittoRelay = url("wss://ditto.pub/relay")

        val learned = aliases.learn(listOf(ditto, dittoRelay), aliases.toProbe(listOf(ditto, dittoRelay)).first(), mapOf(ditto to window(100), dittoRelay to window(100, from = 500)))
        aliases += learned

        assertTrue(learned.folded.isEmpty())
        assertEquals(dittoRelay, aliases.canonicalOf(dittoRelay))
    }

    @Test
    fun `a url that answered nothing is not called its own relay`() {
        var aliases = RelayAliases()
        // An empty window is not a null one, and neither is proof of a distinct relay.
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to emptySet()))
        aliases += learned

        assertTrue(learned.folded.isEmpty())
        assertTrue(nosAlpha !in learned.distinct, "silence was recorded as proof of a distinct relay")
        assertTrue(!aliases.measured(nosAlpha), "a url that said nothing must come back to the fan-out")
    }

    @Test
    fun `a window under the sample floor decides nothing either way`() {
        var aliases = RelayAliases()
        val nine = window(9)
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to nine))
        aliases += learned

        assertTrue(learned.folded.isEmpty())
        assertTrue(learned.distinct.isEmpty(), "a 9-event window was published as a verdict")
        assertTrue(!aliases.measured(nosAlpha))
    }

    @Test
    fun `a group-metadata window folds below the general floor`() {
        var aliases = RelayAliases()
        val groups = window(7)

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to groups, nosAlpha to groups), RelayAliases.GROUP_METADATA_KINDS)
        aliases += learned

        assertEquals(mapOf(nosAlpha to nos), learned.folded)
        assertEquals(nos, aliases.canonicalOf(nosAlpha))
    }

    @Test
    fun `the lowered floor belongs to the filter, not to the window`() {
        var aliases = RelayAliases()
        val seven = window(7)

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to seven, nosAlpha to seven))
        aliases += learned

        assertTrue(learned.folded.isEmpty(), "a general window folded on seven ids")
        assertTrue(!aliases.measured(nosAlpha))
    }

    @Test
    fun `a group-metadata window may fold a url but never clear one`() {
        var aliases = RelayAliases()
        val learned =
            aliases.learn(
                listOf(nos, nosAlpha),
                nos,
                mapOf(nos to window(7), nosAlpha to window(7, from = 500)),
                RelayAliases.GROUP_METADATA_KINDS,
            )
        aliases += learned

        assertTrue(learned.folded.isEmpty())
        assertTrue(learned.distinct.isEmpty(), "a thin window cleared a url as its own relay: ${learned.distinct}")
        assertTrue(!aliases.measured(nosAlpha), "an undecided url must come back to the fan-out")
    }

    @Test
    fun `a group list that shares only part of itself is not folded away`() {
        var aliases = RelayAliases()
        // The ratio clears minOverlap, so the shared count has to clear the floor on its own.
        val leader = window(7)
        val partial = setOf(*window(2).toTypedArray(), "id%064d".format(999))

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to leader, nosAlpha to partial), RelayAliases.GROUP_METADATA_KINDS)
        aliases += learned

        assertTrue(learned.folded.isEmpty(), "a path serving a group nobody else has was folded away")
        assertTrue(!aliases.measured(nosAlpha), "and it must stay in the fan-out")
    }

    @Test
    fun `a group list shared in full still folds at the smaller floor`() {
        var aliases = RelayAliases()
        // Exactly the group floor.
        val three = window(3)

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to three, nosAlpha to three), RelayAliases.GROUP_METADATA_KINDS)
        aliases += learned

        assertEquals(mapOf(nosAlpha to nos), learned.folded)
    }

    @Test
    fun `a group-metadata window still refuses a single shared id`() {
        var aliases = RelayAliases()
        val two = window(2)

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to two, nosAlpha to two), RelayAliases.GROUP_METADATA_KINDS)
        aliases += learned

        assertTrue(learned.folded.isEmpty(), "two ids were enough to fold")
    }

    @Test
    fun `the group floor lowers the bar and never raises it`() {
        var strict = RelayAliases(minSample = 2)
        val learned = strict.learn(listOf(nos, nosAlpha), nos, mapOf(nos to window(2), nosAlpha to window(2)), RelayAliases.GROUP_METADATA_KINDS)
        strict += learned

        assertEquals(mapOf(nosAlpha to nos), learned.folded, "minSample below the group floor must still decide")
    }

    @Test
    fun `a leader that answered too thinly clears nobody, itself included`() {
        var aliases = RelayAliases()
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(5), nosAlpha to window(100)))
        aliases += learned

        assertTrue(learned.folded.isEmpty())
        assertTrue(learned.distinct.isEmpty(), "a thin leader still published verdicts: ${learned.distinct}")
    }

    @Test
    fun `two verdicts that disagree do not pin a url as its own duplicate`() {
        var aliases = RelayAliases()
        // A url folded onto itself would be `measured` forever.
        aliases = aliases.over(listOf(nos, nosAlpha), mapOf(nosAlpha to nos, nos to nosAlpha), cleared = emptySet())

        assertFalse(aliases.measured(nos), "a url was folded onto itself")
        // Neither edge: a loop is two passes contradicting each other.
        assertFalse(aliases.measured(nosAlpha), "a loop was folded anyway")
    }

    @Test
    fun `learn folds onto the leader it was given, not one it recomputes`() {
        var aliases = RelayAliases()
        val print = window(100)
        // A recomputed leader may not be the url that was fingerprinted as the yardstick.
        val learned = aliases.learn(listOf(nos, nosAlpha, nosBeacon), nosAlpha, mapOf(nosAlpha to print, nos to print, nosBeacon to print))
        aliases += learned

        assertEquals(nosAlpha, aliases.canonicalOf(nos))
        assertEquals(nosAlpha, aliases.canonicalOf(nosBeacon))
        assertEquals(setOf(nos, nosBeacon), learned.folded.keys)
    }

    @Test
    fun `a window too small to mean anything folds nothing`() {
        val aliases = RelayAliases()
        val tiny = window(4)

        assertTrue(aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to tiny, nosAlpha to tiny)).folded.isEmpty())
    }

    @Test
    fun `a url that never answered is not folded`() {
        var aliases = RelayAliases()

        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100)))
        aliases += learned

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
        var aliases = RelayAliases()
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to window(100, from = 40)))
        aliases += learned

        assertEquals(mapOf(nosAlpha to nos), learned.folded)
    }

    @Test
    fun `a window that moved on entirely does not`() {
        var aliases = RelayAliases()
        val learned = aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to window(100, from = 80)))
        aliases += learned

        assertTrue(learned.folded.isEmpty())
    }

    @Test
    fun `the pathless url is the one kept`() {
        var aliases = RelayAliases()
        val print = window(100)
        // Ordered worst-first, so the leader is chosen by preference and not by position.
        val group = listOf(nosBeacon, nosAlpha, nos)

        aliases += aliases.learn(group, aliases.toProbe(group).first(), group.associateWith<NormalizedRelayUrl, Set<String>> { print })

        assertEquals(nos, aliases.canonicalOf(nosAlpha))
        assertEquals(nos, aliases.canonicalOf(nosBeacon))
    }

    @Test
    fun `wss wins over ws on the same host`() {
        var aliases = RelayAliases()
        val secure = url("wss://relay.example.com")
        val plain = url("ws://relay.example.com")
        val print = window(100)

        aliases += aliases.learn(listOf(plain, secure), aliases.toProbe(listOf(plain, secure)).first(), mapOf(plain to print, secure to print))

        assertEquals(secure, aliases.canonicalOf(plain))
    }

    @Test
    fun `a ws url folds onto its wss twin on a window neither could be compared on`() {
        var aliases = RelayAliases()
        val secure = url("wss://groups.example")
        val plain = url("ws://groups.example")
        // Under `minSample` on both sides; both answering is the whole verdict.
        val nine = window(9)

        val learned = aliases.learn(listOf(secure, plain), secure, mapOf(secure to nine, plain to nine))
        aliases += learned

        assertEquals(mapOf(plain to secure), learned.folded)
        assertEquals(setOf(plain), learned.twins, "the evidence published for this fold is not a containment")
        assertEquals(secure, aliases.canonicalOf(plain))
        assertTrue(aliases.unresolved(listOf(secure, plain)).isEmpty(), "the pair came back for another pass")
    }

    @Test
    fun `a ws url is not folded onto a secure twin that said nothing`() {
        var aliases = RelayAliases()
        val host = url("wss://relay.example")
        val secureInbox = url("wss://relay.example/inbox")
        val plainInbox = url("ws://relay.example/inbox")
        // Folding here would retire a live url in favour of a dead one.
        val learned =
            aliases.learn(listOf(host, secureInbox, plainInbox), host, mapOf(host to window(100), plainInbox to window(100, from = 500)))
        aliases += learned

        assertTrue(learned.twins.isEmpty())
        assertEquals(plainInbox, aliases.canonicalOf(plainInbox))
        assertEquals(secureInbox, aliases.canonicalOf(secureInbox))
    }

    @Test
    fun `a ws url serving what its secure twin does not is left in the fan-out`() {
        var aliases = RelayAliases()
        val secure = url("wss://relay.example")
        val plain = url("ws://relay.example")
        val learned = aliases.learn(listOf(secure, plain), secure, mapOf(secure to window(9), plain to window(100, from = 500)))
        aliases += learned

        assertTrue(learned.twins.isEmpty())
        assertTrue(learned.folded.isEmpty())
        assertEquals(plain, aliases.canonicalOf(plain))
    }

    @Test
    fun `a ws twin folds onto wherever its own secure twin ended up`() {
        var aliases = RelayAliases()
        val host = url("wss://relay.example")
        val secureAlpha = url("wss://relay.example/alpha")
        val plainAlpha = url("ws://relay.example/alpha")
        val group = listOf(host, secureAlpha, plainAlpha)

        aliases += aliases.learn(group, host, group.associateWith<NormalizedRelayUrl, Set<String>> { window(100) })

        assertEquals(host, aliases.canonicalOf(secureAlpha))
        // `canonicalOf` is one hop, so a chain would survive the fold.
        assertEquals(host, aliases.canonicalOf(plainAlpha))
    }

    @Test
    fun `a port the scheme did not imply is a second endpoint, not a twin`() {
        var aliases = RelayAliases()
        val chosen = url("wss://relay.example:8443")
        val plain = url("ws://relay.example")
        val learned = aliases.learn(listOf(chosen, plain), chosen, mapOf(chosen to window(9), plain to window(9)))
        aliases += learned

        assertTrue(learned.folded.isEmpty())

        // The port each scheme implies anyway is not a difference.
        var implied = RelayAliases()
        val secure443 = url("wss://relay.example:443")
        val plain80 = url("ws://relay.example:80")
        val paired = implied.learn(listOf(secure443, plain80), secure443, mapOf(secure443 to window(9), plain80 to window(9)))
        implied += paired

        assertEquals(setOf(plain80), paired.twins)
        assertEquals(secure443, implied.canonicalOf(plain80))
    }

    @Test
    fun `the secure twin is re-dialled so the pair can be compared at all`() {
        var aliases = RelayAliases()
        val host = url("wss://relay.example")
        val secureInbox = url("wss://relay.example/inbox")
        val plainInbox = url("ws://relay.example/inbox")
        // Cleared last pass, the twin has to answer again for a "both answered" to exist.
        aliases = aliases.over(listOf(host, secureInbox), known = emptyMap(), cleared = setOf(host, secureInbox))

        // In preference order, not appended.
        assertEquals(listOf(host, secureInbox, plainInbox), aliases.toProbe(listOf(host, secureInbox, plainInbox)))
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
        var aliases = RelayAliases()
        val print = window(100)
        aliases += aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to print, nosAlpha to print))
        aliases += RelayAliases.Learned(distinct = setOf(nos))

        assertTrue(aliases.unresolved(listOf(nos, nosAlpha)).isEmpty())
    }

    @Test
    fun `a url proved distinct is not re-probed, but the leader is`() {
        var aliases = RelayAliases()
        aliases += aliases.learn(listOf(nos, nosAlpha), aliases.toProbe(listOf(nos, nosAlpha)).first(), mapOf(nos to window(100), nosAlpha to window(100, from = 500)))

        val toProbe = aliases.toProbe(listOf(nos, nosAlpha, nosBeacon))

        // The leader is re-measured because the window it is compared against has moved.
        assertEquals(listOf(nos, nosBeacon), toProbe)
    }

    @Test
    fun `a verdict pointing at a folded url resolves to the end of the chain`() {
        var aliases = RelayAliases()
        // Both edges arrive in one store read, in either order.
        aliases = aliases.over(listOf(nos, nosAlpha, nosBeacon), mapOf(nosBeacon to nosAlpha, nosAlpha to nos), cleared = emptySet())

        assertEquals(nos, aliases.canonicalOf(nosBeacon))
    }

    @Test
    fun `re-reading the store gives the same view`() {
        var aliases = RelayAliases()
        val urls = listOf(nos, nosAlpha, nosBeacon)
        val known = mapOf(nosAlpha to nos, nosBeacon to nos)

        aliases = aliases.over(urls, known, cleared = setOf(nos))
        val before = urls.associateWith { aliases.canonicalOf(it) }
        aliases = aliases.over(urls, known, cleared = setOf(nos))

        assertEquals(before, urls.associateWith { aliases.canonicalOf(it) })
        assertEquals(nos, aliases.canonicalOf(nosAlpha))
        assertTrue(urls.all { aliases.measured(it) }, "a re-read dropped a verdict it had just re-adopted")
        assertTrue(aliases.unresolved(urls).isEmpty(), "a fully decided group came back as unfinished")
    }

    @Test
    fun `a view over the store forgets the urls it no longer has a verdict for`() {
        // Verdicts expire inside `RelayVerdictRecord.load`, so dropping out of the read schedules a re-measure.
        var aliases = RelayAliases()
        val urls = listOf(nos, nosAlpha)
        aliases = aliases.over(urls, mapOf(nosAlpha to nos), cleared = setOf(nos))
        assertTrue(aliases.measured(nosAlpha))

        aliases = aliases.over(urls, emptyMap(), cleared = emptySet())

        assertFalse(aliases.measured(nosAlpha), "an expired fold outlived the store that dropped it")
        assertFalse(aliases.measured(nos), "an expired cleared verdict outlived the store that dropped it")
        assertEquals(nosAlpha, aliases.canonicalOf(nosAlpha))
        assertEquals(1, aliases.unresolved(urls).size, "the group must be probeable again")
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
    fun `learning leaves the view it was asked on as it was`() {
        // What a group proved is applied only once its write lands, by the pass's one writer.
        val aliases = RelayAliases()
        val print = window(100)

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to print, nosAlpha to print))

        assertEquals(mapOf(nosAlpha to nos), learned.folded)
        assertEquals(nosAlpha, aliases.canonicalOf(nosAlpha), "a measurement changed the view before its write landed")
        assertFalse(aliases.measured(nosAlpha))
        assertEquals(nos, (aliases + learned).canonicalOf(nosAlpha))
    }
}
