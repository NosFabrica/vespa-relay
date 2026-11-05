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

import com.nosfabrica.vespa.relay.peers.Verdict
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A dialled url whose coroutine ended with no result is our side, counted by the batch guard as a cut one is. */
class FitnessBatchUnfiledTest {
    private fun slot(
        i: Int,
        held: Boolean = false,
    ) = DialSlot(RelayUrlNormalizer.normalize("wss://relay$i.example"), held = held)

    @Test
    fun `a dialled url left with no result counts as abandoned, and a held one does not`() {
        val unfiled = slot(1)
        val held = slot(2, held = true)
        val batch = FitnessBatch(emptyMap(), listOf(unfiled, held), backingOff = emptySet())

        assertEquals(listOf(unfiled.url), batch.abandoned)
    }

    @Test
    fun `a batch whose dials mostly ended unfiled refuses to publish the verdicts the rest earned`() {
        // A stray cancellation ending most of the url coroutines, which nothing cut and nothing filed.
        val slots = (0 until FitnessPass.GUARD_FLOOR * 2).map { slot(it) }
        for (s in slots.take(FitnessPass.GUARD_FLOOR)) s.result = DialResult.Graded(FitnessOutcome(Verdict.DEAD, "connection refused"))
        val batch = FitnessBatch(emptyMap(), slots, backingOff = emptySet())

        assertTrue(refusesToPublish("all streams", slots.map { it.url }, batch), "half the dials ended on our side and the rest were published as fact")
    }
}
