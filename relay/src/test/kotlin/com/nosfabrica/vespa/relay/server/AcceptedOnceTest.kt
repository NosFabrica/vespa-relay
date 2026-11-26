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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IEventStore.InsertOutcome
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Only submissions of one id in flight together are judged against each other. */
class AcceptedOnceTest {
    private val event: Event = NostrSignerSync().sign(1_700_000_000L, 1, emptyArray(), "one id")

    /** Answers each submission when the test says, with the outcome it says. */
    private class Held : EventIngest {
        val pending = ArrayList<(InsertOutcome) -> Unit>()

        override suspend fun submit(
            event: Event,
            skipVerify: Boolean,
            onComplete: (InsertOutcome) -> Unit,
        ) {
            pending += onComplete
        }

        fun answer(
            index: Int,
            outcome: InsertOutcome,
        ) = pending[index](outcome)

        override fun hasBacklog() = false

        override fun close() = Unit
    }

    private fun AcceptedOnce.submitted(): MutableList<InsertOutcome> {
        val heard = ArrayList<InsertOutcome>()
        runBlocking { submit(event, false) { heard += it } }
        return heard
    }

    @Test
    fun `a submission after the last one left is judged on its own`() {
        val held = Held()
        val once = AcceptedOnce(held)
        val first = once.submitted()
        held.answer(0, InsertOutcome.Accepted)
        // An ephemeral event sent again is stored by nobody and must be fanned out again.
        val again = once.submitted()
        held.answer(1, InsertOutcome.Accepted)
        assertEquals(listOf<InsertOutcome>(InsertOutcome.Accepted), first)
        assertEquals(listOf<InsertOutcome>(InsertOutcome.Accepted), again)
        assertEquals(0, once.racedAccepts)
    }

    @Test
    fun `a failed submission claims nothing, so the one in flight beside it is accepted`() {
        val held = Held()
        val once = AcceptedOnce(held)
        val failed = once.submitted()
        val second = once.submitted()
        held.answer(0, InsertOutcome.Failed(RejectionReason.INSERT_FAILED))
        held.answer(1, InsertOutcome.Accepted)
        assertTrue(failed.single() is InsertOutcome.Failed)
        assertEquals(listOf<InsertOutcome>(InsertOutcome.Accepted), second)
    }

    @Test
    fun `of three in flight together, whichever is answered first is the one accepted`() {
        val held = Held()
        val once = AcceptedOnce(held)
        val heard = List(3) { once.submitted() }
        held.answer(2, InsertOutcome.Accepted)
        held.answer(0, InsertOutcome.Accepted)
        held.answer(1, InsertOutcome.Accepted)
        assertEquals(InsertOutcome.Accepted, heard[2].single())
        for (i in listOf(0, 1)) assertEquals(RejectionReason.DUPLICATE, (heard[i].single() as IEventStore.InsertOutcome.Rejected).reason)
        assertEquals(2, once.racedAccepts)
    }
}
