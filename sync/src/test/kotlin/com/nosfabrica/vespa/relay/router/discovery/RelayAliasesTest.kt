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
    fun `a group-metadata window folds below the general floor`() {
        val aliases = RelayAliases()
        // A NIP-29 relay's complete list of groups, which is what the third rung
        // of the ladder brings back and is SHORT by nature: measured over 21 live
        // hosts the kind-39000 window is min 1, median 9, max 1,302, and of the
        // five hosts the rung recovers two serve under DEFAULT_MIN_SAMPLE
        // (groups.hzrd149.com at 7, groups.fiatjaf.com at 16). Seven ids out of
        // seven is not a thin slice of a feed — it is everything the host has.
        val groups = window(7)

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to groups, nosAlpha to groups), RelayAliases.GROUP_METADATA_KINDS)

        assertEquals(mapOf(nosAlpha to nos), learned.folded)
        assertEquals(nos, aliases.canonicalOf(nosAlpha))
    }

    @Test
    fun `the lowered floor belongs to the filter, not to the window`() {
        val aliases = RelayAliases()
        // The same seven ids, taken through the GENERAL filter, are exactly the
        // coincidence DEFAULT_MIN_SAMPLE exists to refuse. Nothing about a short
        // window earns the lower bar; only the filter that could not ask for more
        // does.
        val seven = window(7)

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to seven, nosAlpha to seven))

        assertTrue(learned.folded.isEmpty(), "a general window folded on seven ids")
        assertTrue(!aliases.measured(nosAlpha))
    }

    @Test
    fun `a group-metadata window may fold a url but never clear one`() {
        val aliases = RelayAliases()
        // The asymmetry the lowered floor rests on. These two paths served
        // DIFFERENT group lists, so nothing folds — and the tempting second step,
        // publishing "each is a relay in its own right" for thirty days, is
        // exactly the relay.damus.io/lantern-oscar-dynamo lie at a smaller scale.
        // Refusing to fold and proving distinctness are different claims, and a
        // seven-id window can only make one of them.
        val learned =
            aliases.learn(
                listOf(nos, nosAlpha),
                nos,
                mapOf(nos to window(7), nosAlpha to window(7, from = 500)),
                RelayAliases.GROUP_METADATA_KINDS,
            )

        assertTrue(learned.folded.isEmpty())
        assertTrue(learned.distinct.isEmpty(), "a thin window cleared a url as its own relay: ${learned.distinct}")
        assertTrue(!aliases.measured(nosAlpha), "an undecided url must come back to the fan-out")
    }

    @Test
    fun `a group list that shares only part of itself is not folded away`() {
        val aliases = RelayAliases()
        // THE HOLE A RATIO CANNOT CLOSE. This path serves three groups, two of
        // which the leader also has — 0.667, over minOverlap, and its window
        // clears a floor of three. On the ratio alone it folds, and the third
        // group, which nothing else on this host serves, stops being mirrored
        // for a month. That is the fold's one unforgivable failure bought for a
        // two-id coincidence, so the shared COUNT has to clear the floor too.
        val leader = window(7)
        val partial = setOf(*window(2).toTypedArray(), "id%064d".format(999))

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to leader, nosAlpha to partial), RelayAliases.GROUP_METADATA_KINDS)

        assertTrue(learned.folded.isEmpty(), "a path serving a group nobody else has was folded away")
        assertTrue(!aliases.measured(nosAlpha), "and it must stay in the fan-out")
    }

    @Test
    fun `a group list shared in full still folds at the smaller floor`() {
        val aliases = RelayAliases()
        // The other side of the same guard: three groups, all three shared. The
        // shared count is the floor exactly, which is the smallest honest fold
        // this filter can produce — and the measured case, since every live pair
        // shared its list entirely (containment 1.000, 6 of 6).
        val three = window(3)

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to three, nosAlpha to three), RelayAliases.GROUP_METADATA_KINDS)

        assertEquals(mapOf(nosAlpha to nos), learned.folded)
    }

    @Test
    fun `a group-metadata window still refuses a single shared id`() {
        val aliases = RelayAliases()
        // Three, not one. A host serving one or two groups hands over one or two
        // ids, and at that width "both urls returned the same list" is the
        // coincidence the floor exists for, whatever filter asked.
        val two = window(2)

        val learned = aliases.learn(listOf(nos, nosAlpha), nos, mapOf(nos to two, nosAlpha to two), RelayAliases.GROUP_METADATA_KINDS)

        assertTrue(learned.folded.isEmpty(), "two ids were enough to fold")
    }

    @Test
    fun `the group floor lowers the bar and never raises it`() {
        // `minOf`, in the one direction it can be observed: a caller that set
        // minSample BELOW the group floor keeps its own number rather than
        // having it raised to three here.
        val strict = RelayAliases(minSample = 2)
        val learned = strict.learn(listOf(nos, nosAlpha), nos, mapOf(nos to window(2), nosAlpha to window(2)), RelayAliases.GROUP_METADATA_KINDS)

        assertEquals(mapOf(nosAlpha to nos), learned.folded, "minSample below the group floor must still decide")
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
        aliases.replace(listOf(nos, nosAlpha), mapOf(nosAlpha to nos, nos to nosAlpha), cleared = emptySet())

        assertFalse(aliases.measured(nos) && aliases.canonicalOf(nos) == nos && nos in aliases.verdicts().keys)
        assertTrue(nos !in aliases.verdicts().keys, "a url was folded onto itself: ${aliases.verdicts()}")
        // Neither edge, rather than whichever one the map iterated first: a
        // loop is two passes contradicting each other and no evidence at all.
        assertTrue(aliases.verdicts().isEmpty(), "a loop was folded anyway: ${aliases.verdicts()}")
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
    fun `a ws url folds onto its wss twin on a window neither could be compared on`() {
        val aliases = RelayAliases()
        val secure = url("wss://groups.example")
        val plain = url("ws://groups.example")
        // Nine events, under `minSample` on both sides — so containment can say
        // nothing either way, `learn` used to publish nothing, and `unresolved`
        // handed this pair back on every pass forever. The two urls name one
        // endpoint and both of them answered, which is the whole verdict.
        val nine = window(9)

        val learned = aliases.learn(listOf(secure, plain), secure, mapOf(secure to nine, plain to nine))

        assertEquals(mapOf(plain to secure), learned.folded)
        assertEquals(setOf(plain), learned.twins, "the evidence published for this fold is not a containment")
        assertEquals(secure, aliases.canonicalOf(plain))
        assertTrue(aliases.unresolved(listOf(secure, plain)).isEmpty(), "the pair came back for another pass")
    }

    @Test
    fun `a ws url is not folded onto a secure twin that said nothing`() {
        val aliases = RelayAliases()
        val host = url("wss://relay.example")
        val secureInbox = url("wss://relay.example/inbox")
        val plainInbox = url("ws://relay.example/inbox")
        // The secure twin was asked and never answered. "Both work" is the
        // condition: folding here retires a live url in favour of a dead one.
        val learned =
            aliases.learn(listOf(host, secureInbox, plainInbox), host, mapOf(host to window(100), plainInbox to window(100, from = 500)))

        assertTrue(learned.twins.isEmpty())
        // Measured against the leader in the ordinary way instead, and it is not
        // that either — so it stays a url of its own, dialled.
        assertEquals(plainInbox, aliases.canonicalOf(plainInbox))
        assertEquals(secureInbox, aliases.canonicalOf(secureInbox))
    }

    @Test
    fun `a ws url serving what its secure twin does not is left in the fan-out`() {
        val aliases = RelayAliases()
        val secure = url("wss://relay.example")
        val plain = url("ws://relay.example")
        // The direction that can lose data: everything the plain url has must
        // already be on the survivor. Here it holds 100 events the secure url
        // never returned, so folding it away would stop mirroring them.
        val learned = aliases.learn(listOf(secure, plain), secure, mapOf(secure to window(9), plain to window(100, from = 500)))

        assertTrue(learned.twins.isEmpty())
        assertTrue(learned.folded.isEmpty())
        assertEquals(plain, aliases.canonicalOf(plain))
    }

    @Test
    fun `a ws twin folds onto wherever its own secure twin ended up`() {
        val aliases = RelayAliases()
        val host = url("wss://relay.example")
        val secureAlpha = url("wss://relay.example/alpha")
        val plainAlpha = url("ws://relay.example/alpha")
        val group = listOf(host, secureAlpha, plainAlpha)

        aliases.learn(group, host, group.associateWith { window(100) })

        assertEquals(host, aliases.canonicalOf(secureAlpha))
        // …and not onto secureAlpha, which is itself an alias: `canonicalOf` is
        // one hop by design, so a chain here is a duplicate that survives the
        // fold and gets dialled every cycle.
        assertEquals(host, aliases.canonicalOf(plainAlpha))
    }

    @Test
    fun `a port the scheme did not imply is a second endpoint, not a twin`() {
        val aliases = RelayAliases()
        val chosen = url("wss://relay.example:8443")
        val plain = url("ws://relay.example")
        // 8443 is a port somebody picked deliberately. Nothing about `ws://x`
        // says it reaches the same service, so this is left to the fingerprint —
        // which, on windows this thin, correctly decides nothing.
        val learned = aliases.learn(listOf(chosen, plain), chosen, mapOf(chosen to window(9), plain to window(9)))

        assertTrue(learned.folded.isEmpty())

        // The port each scheme would have used anyway is not a difference at all.
        val implied = RelayAliases()
        val secure443 = url("wss://relay.example:443")
        val plain80 = url("ws://relay.example:80")
        val paired = implied.learn(listOf(secure443, plain80), secure443, mapOf(secure443 to window(9), plain80 to window(9)))

        assertEquals(setOf(plain80), paired.twins)
        assertEquals(secure443, implied.canonicalOf(plain80))
    }

    @Test
    fun `the secure twin is re-dialled so the pair can be compared at all`() {
        val aliases = RelayAliases()
        val host = url("wss://relay.example")
        val secureInbox = url("wss://relay.example/inbox")
        val plainInbox = url("ws://relay.example/inbox")
        // Last month's pass cleared the secure path as its own endpoint; the
        // plain one turned up today. Without re-dialling the twin there is no
        // "both answered" to be had, and the plain url would be measured against
        // the group's leader — a genuinely different endpoint — disagree with it
        // correctly, and be published as a relay in its own right.
        // Seeded the way it really arrives — a store read with two cleared
        // verdicts in it. `adoptDistinct` was the bulk half of the old
        // forget-then-adopt pair and went with it; see [RelayAliases.replace].
        aliases.replace(listOf(host, secureInbox), known = emptyMap(), cleared = setOf(host, secureInbox))

        // In preference order, not appended: that order is what the yardstick
        // search walks down, and a re-dialled twin is a `wss://` url — the best
        // kind of yardstick a group has.
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
        assertEquals(setOf("a", "b"), folded.single().bindings["authors"])
    }

    @Test
    fun `a verdict pointing at a folded url resolves to the end of the chain`() {
        val aliases = RelayAliases()
        // Both edges arrive together, as one store read, and the answer must
        // not depend on which order they were returned in.
        aliases.replace(listOf(nos, nosAlpha, nosBeacon), mapOf(nosBeacon to nosAlpha, nosAlpha to nos), cleared = emptySet())

        assertEquals(nos, aliases.canonicalOf(nosBeacon))
    }

    @Test
    fun `re-reading the store never leaves a url transiently unfolded`() {
        // What `AliasFolding.adopt` does every pass, and what it must not do on
        // the way. The bulk forget it used to start with left every fold in the
        // candidate set missing until the adopt that followed put them back —
        // and this object is shared by every stream and the monitor's pass, so
        // another stream reading inside that window dialled the duplicates for
        // a whole cycle. `replace` moves each url straight from its old verdict
        // to its new one.
        //
        // The window itself is a race and not directly assertable; what is
        // assertable — and is the invariant the race broke — is that a re-read
        // of the SAME verdicts is a complete no-op on every url in the set.
        val aliases = RelayAliases()
        val urls = listOf(nos, nosAlpha, nosBeacon)
        val known = mapOf(nosAlpha to nos, nosBeacon to nos)

        aliases.replace(urls, known, cleared = setOf(nos))
        val before = urls.associateWith { aliases.canonicalOf(it) }
        aliases.replace(urls, known, cleared = setOf(nos))

        assertEquals(before, urls.associateWith { aliases.canonicalOf(it) })
        assertEquals(nos, aliases.canonicalOf(nosAlpha))
        assertTrue(urls.all { aliases.measured(it) }, "a re-read dropped a verdict it had just re-adopted")
        assertTrue(aliases.unresolved(urls).isEmpty(), "a fully decided group came back as unfinished")
    }

    @Test
    fun `replace clears the urls the store no longer has a verdict for`() {
        // The other half, and the reason this is not simply an adopt: the TTL
        // and the rules epoch expire verdicts inside `RelayVerdictRecord.load`,
        // so a url dropping OUT of what the store returns is how a re-measure
        // is scheduled. Left in memory it would answer `measured` forever.
        val aliases = RelayAliases()
        val urls = listOf(nos, nosAlpha)
        aliases.replace(urls, mapOf(nosAlpha to nos), cleared = setOf(nos))
        assertTrue(aliases.measured(nosAlpha))

        aliases.replace(urls, emptyMap(), cleared = emptySet())

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
    fun `folding is a no-op until something has been learned`() {
        val aliases = RelayAliases()
        val relays = listOf(DiscoveredRelay(nos), DiscoveredRelay(nosAlpha))

        assertEquals(relays, RelayAliases.foldOnto(relays, aliases.verdicts()))
        assertFalse(aliases.size() > 0)
    }
}
