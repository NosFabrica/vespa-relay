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
package com.nosfabrica.vespa.relay.sync.heal

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Which vanish request may be handed to which relay. A kind 62 delivered to a
 * relay it did not name destroys data nothing re-offers.
 */
class VanishTargetsTest {
    private val relayA = RelayUrlNormalizer.normalize("wss://relay-a.example")
    private val relayB = RelayUrlNormalizer.normalize("wss://relay-b.example")

    private fun vanish(
        n: Int,
        createdAt: Long,
        scope: String,
    ) = RequestToVanishEvent(
        id = "%064x".format(n),
        pubKey = "a1".repeat(32),
        createdAt = createdAt,
        tags = arrayOf(arrayOf("relay", scope)),
        content = "",
        sig = "b2".repeat(32),
    )

    @Test
    fun `an ALL_RELAYS request may go to any relay`() {
        val all = vanish(1, 1_700_000_000L, "ALL_RELAYS")
        assertEquals(all, VanishTargets.pushableTo(listOf(all), relayB))
    }

    @Test
    fun `a request scoped to another relay is never handed to this one`() {
        val scopedToA = vanish(2, 1_700_000_000L, relayA.url)
        assertNull(VanishTargets.pushableTo(listOf(scopedToA), relayB))
    }

    @Test
    fun `a request scoped to this relay is exactly the one it should get`() {
        val scopedToB = vanish(3, 1_700_000_000L, relayB.url)
        assertEquals(scopedToB, VanishTargets.pushableTo(listOf(scopedToB), relayB))
    }

    @Test
    fun `a newer relay-scoped request never masks an older ALL_RELAYS one`() {
        // Filter before the newest-wins pick, or the unpushable newer one is picked and nothing is sent.
        val everywhere = vanish(4, 1_700_000_000L, "ALL_RELAYS")
        val newerScopedToA = vanish(5, 1_700_009_999L, relayA.url)

        assertEquals(
            everywhere,
            VanishTargets.pushableTo(listOf(everywhere, newerScopedToA), relayB),
            "relay B is owed the ALL_RELAYS request regardless of what relay A was later told",
        )
    }

    @Test
    fun `among several pushable requests the newest wins`() {
        val older = vanish(6, 1_700_000_000L, "ALL_RELAYS")
        val newer = vanish(7, 1_700_005_000L, "ALL_RELAYS")
        assertEquals(newer, VanishTargets.pushableTo(listOf(older, newer), relayB))
    }

    @Test
    fun `a relay named alongside others is still named`() {
        val both =
            RequestToVanishEvent(
                id = "%064x".format(8),
                pubKey = "a1".repeat(32),
                createdAt = 1_700_000_000L,
                tags = arrayOf(arrayOf("relay", relayA.url), arrayOf("relay", relayB.url)),
                content = "",
                sig = "b2".repeat(32),
            )
        assertEquals(both, VanishTargets.pushableTo(listOf(both), relayB))
        assertEquals(both, VanishTargets.pushableTo(listOf(both), relayA))
    }

    @Test
    fun `no candidates at all is not an error`() {
        assertNull(VanishTargets.pushableTo(emptyList(), relayB))
    }
}
