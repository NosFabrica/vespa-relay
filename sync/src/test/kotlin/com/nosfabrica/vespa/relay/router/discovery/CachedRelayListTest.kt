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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * When a stream may start a cycle on the relay list the previous one used.
 *
 * The property under test is not "does it cache" — it is that every way the
 * list can go stale ends the reuse, because each of them is a way the mirror
 * silently does the wrong work: dialling urls the fold has since collapsed,
 * or never widening onto relays the store has since learned about.
 */
class CachedRelayListTest {
    private val builtAt = 1_000_000L

    private fun relay(url: String) = DiscoveredRelay(RelayUrlNormalizer.normalize(url))

    private fun list(
        relays: List<DiscoveredRelay> = listOf(relay("wss://a.example"), relay("wss://b.example")),
        aliasGeneration: Long = 7,
    ) = CachedRelayList(
        relays = relays,
        discovered = 5,
        foldedOntoAnother = 2,
        excluded = 1,
        hosts = 2,
        folded = mapOf("wss://a.example/x" to "wss://a.example/"),
        builtAtMs = builtAt,
        aliasGeneration = aliasGeneration,
    )

    @Test
    fun `a fresh list is reused`() {
        assertTrue(list().reusableAt(builtAt + 30_000, maxAgeSec = 21_600, aliasGeneration = 7))
    }

    @Test
    fun `a list older than the refresh period is not`() {
        // The bound is the stream's own refreshSeconds, which is what that
        // number has always meant: how often the sources are re-read. A dynamic
        // stream is supposed to widen as the store fills, and a cache that
        // never expired would be a fan-out frozen at whatever the store held on
        // the first cycle after boot.
        val cached = list()
        assertTrue(cached.reusableAt(builtAt + 599_000, maxAgeSec = 600, aliasGeneration = 7))
        assertFalse(cached.reusableAt(builtAt + 600_000, maxAgeSec = 600, aliasGeneration = 7))
    }

    @Test
    fun `a fold verdict learned since it was built ends the reuse`() {
        // The probing half of the fold runs on its own clock, so verdicts land
        // between cycles. A list built before them goes on dialling urls now
        // known to be one relay — a socket, a band and a gate slot each, for
        // events the survivor in the same list already delivers.
        assertFalse(list(aliasGeneration = 7).reusableAt(builtAt + 1_000, maxAgeSec = 21_600, aliasGeneration = 8))
    }

    @Test
    fun `an empty list is never reused`() {
        // Discovery found nothing: a cold store, or sources matching nothing
        // yet. The stream is in its retry backoff, and the only question that
        // backoff exists to ask is whether the store has filled since — which
        // reusing the empty answer would never let it ask.
        assertFalse(list(relays = emptyList()).reusableAt(builtAt + 1_000, maxAgeSec = 21_600, aliasGeneration = 7))
    }

    @Test
    fun `each cycle gets its own tally, carrying the list's age`() {
        val cached = list()
        val first = cached.tally(builtAt)
        val second = cached.tally(builtAt + 90_000)

        // Fresh counters, or the second cycle publishes its own dispositions
        // added to the first's against a `taken` that counts each url once —
        // a partition that cannot close.
        first.delivered.addAndGet(3)
        assertEquals(0L, second.delivered.get())

        // Same provenance both times: these urls really were discovered,
        // folded and excluded. Only the age says when.
        assertEquals(first.discovered, second.discovered)
        assertEquals(first.taken, second.taken)
        assertEquals(0L, first.listAgeSec)
        assertEquals(90L, second.listAgeSec)
    }

    @Test
    fun `age never runs backwards`() {
        // System.currentTimeMillis() is not monotonic, and an NTP step is not a
        // reason to publish a negative age.
        assertEquals(0L, list().ageSec(builtAt - 60_000))
    }
}
