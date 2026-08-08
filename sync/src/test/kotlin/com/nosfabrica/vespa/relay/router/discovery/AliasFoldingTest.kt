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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The fold is two halves that run at different times, and the whole point of
 * the split is WHICH half touches the network.
 *
 * [AliasFolding.apply] runs in front of a fan-out on every cycle, so it must
 * never dial; [AliasFolding.measure] runs on [AliasMonitor]'s clock, where a
 * multi-minute probe pass costs nobody a download. A regression that quietly
 * moved a probe back onto the read path would not fail any other test in this
 * repo — it would just make cycles slow again, which is the thing that was
 * wrong in the first place.
 */
class AliasFoldingTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val canonical = RelayUrlNormalizer.normalize("wss://nos.lol")
    private val alias = RelayUrlNormalizer.normalize("wss://nos.lol/cipher-zulu")
    private val elsewhere = RelayUrlNormalizer.normalize("wss://nostr.mom")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /**
     * A relay set where every url serves the SAME 40 events, so any two of them
     * fold — and every dial is counted, which is what these tests assert on.
     */
    private class Upstreams(
        private val corpus: List<Event>,
    ) {
        val dials = AtomicInteger()

        suspend fun fetch(
            @Suppress("UNUSED_PARAMETER") at: NormalizedRelayUrl,
            want: Int,
            until: Long?,
            @Suppress("UNUSED_PARAMETER") kinds: List<Int>?,
        ): List<Event> {
            dials.incrementAndGet()
            return corpus.filter { until == null || it.createdAt <= until }.take(want)
        }
    }

    private fun folding(
        store: NostrSemanticsStore,
        upstreams: Upstreams,
        aliases: RelayAliases = RelayAliases(),
    ) = AliasFolding(
        aliases = aliases,
        record = RelayAliasRecord(store, signer),
        probe = AliasProbe(fetch = upstreams::fetch, target = 40, page = 40, fallbackPage = 40),
    )

    private fun upstreams(): Upstreams = Upstreams((0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") })

    @Test
    fun `apply never dials, however much there is to learn`() =
        runBlocking {
            val up = upstreams()
            val cleaned = folding(newStore(), up).apply(listOf(canonical, alias))

            assertEquals(0, up.dials.get(), "apply() opened ${up.dials.get()} socket(s); it runs on the cycle's critical path")
            // Nothing measured yet, so nothing may be folded away: the only safe
            // reading of "no verdict" is "dial it as it stands".
            assertEquals(listOf(canonical, alias), cleaned.dial)
            assertTrue(cleaned.aliases.isEmpty())
        }

    @Test
    fun `a url is dialled unfolded exactly once, then folds from the store`() =
        runBlocking {
            // The cost of moving the probe off the cycle, stated as a test: the
            // first cycle to see a url cannot fold it, because nothing has
            // measured it yet. The measurement happens between the two.
            val store = newStore()
            val up = upstreams()
            val fold = folding(store, up)

            assertEquals(2, fold.apply(listOf(canonical, alias)).dial.size)
            assertEquals(1, fold.measure("t", listOf(canonical, alias), canDial = { true }))
            assertEquals(listOf(canonical), fold.apply(listOf(canonical, alias)).dial)
        }

    @Test
    fun `a second process reads the verdict without re-probing`() =
        runBlocking {
            // The two halves talk through the STORE and nothing else, which is
            // what makes the split survive a restart. Fresh RelayAliases here
            // stands in for a router that has just booted.
            val store = newStore()
            val prober = upstreams()
            folding(store, prober).measure("t", listOf(canonical, alias), canDial = { true })

            val reader = upstreams()
            val cleaned = folding(store, reader).apply(listOf(canonical, alias))

            assertEquals(listOf(canonical), cleaned.dial)
            assertEquals(mapOf(alias to canonical), cleaned.aliases)
            assertEquals(0, reader.dials.get(), "the reader re-probed what the store already knew")
        }

    @Test
    fun `measure honours the caller's transport guard`() =
        runBlocking {
            val up = upstreams()
            // The leader is refused, so its group can never be compared — and
            // dialling the members anyway would be pure waste.
            val learned = folding(newStore(), up).measure("t", listOf(canonical, alias), canDial = { false })

            assertEquals(0, learned)
            assertEquals(0, up.dials.get())
        }

    @Test
    fun `hosts are never compared across domains`() =
        runBlocking {
            // Two different relays that happen to serve identical events. The
            // fold groups by hostname first, so this can never collapse however
            // well the fingerprints match.
            val store = newStore()
            val fold = folding(store, upstreams())

            assertEquals(0, fold.measure("t", listOf(canonical, elsewhere), canDial = { true }))
            assertEquals(listOf(canonical, elsewhere), fold.apply(listOf(canonical, elsewhere)).dial)
        }

    @Test
    fun `a single url is returned untouched by both halves`() =
        runBlocking {
            val up = upstreams()
            val fold = folding(newStore(), up)

            assertEquals(listOf(canonical), fold.apply(listOf(canonical)).dial)
            assertEquals(0, fold.measure("t", listOf(canonical), canDial = { true }))
            assertEquals(0, up.dials.get())
        }
}
