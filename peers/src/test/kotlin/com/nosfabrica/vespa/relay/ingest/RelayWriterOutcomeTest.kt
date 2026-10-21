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

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PublishResult
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import kotlin.test.Test
import kotlin.test.assertEquals

/** The relay's OK read back as the outcome the store would have given, final or not. */
class RelayWriterOutcomeTest {
    private fun outcome(
        accepted: Boolean,
        message: String,
    ) = RelayWriter.outcomeOf(PublishResult(accepted, message))

    @Test
    fun `stored, duplicate and final refusals read as the store's own`() {
        assertEquals(IEventStore.InsertOutcome.Accepted, outcome(true, ""))
        assertEquals(IEventStore.InsertOutcome.Rejected(RejectionReason.DUPLICATE), outcome(true, RejectionReason.DUPLICATE))
        assertEquals(IEventStore.InsertOutcome.Rejected(RejectionReason.REPLACED), outcome(false, RejectionReason.REPLACED))
        assertEquals(IEventStore.InsertOutcome.Rejected("blocked: kind 7 is not accepted"), outcome(false, "blocked: kind 7 is not accepted"))
        assertEquals(IEventStore.InsertOutcome.Rejected("invalid: bad signature or id"), outcome(false, "invalid: bad signature or id"))
    }

    @Test
    fun `what may succeed later is a failure, never a refusal`() {
        for (message in listOf("error: insert failed", "rate-limited: slow down", "auth-required: sign in")) {
            assertEquals(IEventStore.InsertOutcome.Failed(message), outcome(false, message))
        }
        for (message in listOf(PublishResult.NO_RESPONSE, PublishResult.DISCONNECTED, PublishResult.CANNOT_CONNECT_PREFIX + "refused")) {
            assertEquals(IEventStore.InsertOutcome.Failed("error: $message"), outcome(false, message))
        }
    }
}
