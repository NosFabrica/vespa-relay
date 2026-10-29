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
package com.nosfabrica.vespa.relay.ingest

import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/** A bounded wait says which of the three ways it ended, as of the one instant it looked. */
class IngestAcksTest {
    @Test
    fun `a bounded wait tells stored from failed from unanswered`(): Unit =
        runBlocking {
            val acks = IngestAcks()
            val first = acks.track()
            val second = acks.track()
            first(IEventStore.InsertOutcome.Accepted)
            assertEquals(1, assertIs<IngestAcks.Verdicts.Unanswered>(acks.awaitWithin(50)).count)

            // An answer landing after a cut is counted by the next look.
            second(IEventStore.InsertOutcome.Failed("error: store down"))
            assertEquals(1, assertIs<IngestAcks.Verdicts.Failed>(acks.awaitWithin(50)).count)

            val clean = IngestAcks()
            clean.track()(IEventStore.InsertOutcome.Rejected("duplicate: have it"))
            assertEquals(IngestAcks.Verdicts.Stored, clean.awaitWithin(50))
        }
}
