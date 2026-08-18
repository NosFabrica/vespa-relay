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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The two decisions that keep the healer from doing damage: which `OK` answers
 * mean "this relay will never take a repair", and how much silence it takes to
 * conclude the same thing.
 */
class OkClassifierTest {
    private fun classify(
        accepted: Boolean,
        message: String,
        transport: Boolean = false,
    ) = OkClassifier.classify(accepted, message, transport)

    @Test
    fun `rate-limited earns no tombstone and no write-closed mark`() {
        // SAFETY. The dangerous row: treating a momentary refusal as policy
        // would suppress the very ids a relay was about to accept a repair for,
        // permanently and silently.
        assertEquals(PushVerdict.RETRY, classify(false, "rate-limited: slow down"))
        assertEquals(PushVerdict.RETRY, classify(false, "error: could not connect to backend"))
    }

    @Test
    fun `auth-required, restricted and blocked each close the relay for writes`() {
        listOf(
            "auth-required: we only accept events from authenticated users",
            "restricted: not on the allow list",
            "blocked: you are banned",
        ).forEach { assertEquals(PushVerdict.CLOSED, classify(false, it), "for '$it'") }
    }

    @Test
    fun `invalid and pow earn nothing, because they are our event and not their policy`() {
        assertEquals(PushVerdict.IGNORE, classify(false, "invalid: bad signature"))
        assertEquals(PushVerdict.IGNORE, classify(false, "pow: difficulty 28 required"))
    }

    @Test
    fun `duplicate earns nothing, because holding our winner is not dropping the loser`() {
        assertEquals(PushVerdict.IGNORE, classify(false, "duplicate: have this event"))
    }

    @Test
    fun `an unrecognised reason prefix earns nothing`() {
        // Unknown means unknown — the same conservatism Unreachability.proves()
        // applies before publishing a claim about someone else's server.
        assertEquals(PushVerdict.IGNORE, classify(false, "nope"))
        assertEquals(PushVerdict.IGNORE, classify(false, ""))
        assertEquals(PushVerdict.IGNORE, classify(false, "banned: mystery prefix"))
    }

    @Test
    fun `a transport failure is silence, not a refusal`() {
        assertEquals(PushVerdict.SILENT, classify(false, "no response within timeout", transport = true))
        assertEquals(PushVerdict.SILENT, classify(false, "disconnected before OK", transport = true))
    }

    @Test
    fun `OK true earns no tombstone on its own`() {
        // Accepted means taken, not that the stale copy was dropped — an
        // archival relay does the first and not the second.
        assertEquals(PushVerdict.ACCEPTED, classify(true, ""))
    }

    @Test
    fun `the prefix is matched case-insensitively and past surrounding whitespace`() {
        assertEquals(PushVerdict.CLOSED, classify(false, "  AUTH-REQUIRED: sign in  "))
    }
}

class WriteCapabilityTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")

    @Test
    fun `a policy refusal closes the relay at once`() {
        val caps = WriteCapability()
        caps.close(relay, "restricted: not on the allow list")
        assertTrue(caps.isClosed(relay))
    }

    @Test
    fun `silence in a single pass never closes a relay`() {
        // One bad session must not cost a relay forever — the same reasoning as
        // the NIP-45 idle-window trap, applied to writes.
        val caps = WriteCapability()
        repeat(20) { caps.strike(relay, passId = 1) }
        assertFalse(caps.isClosed(relay), "20 strikes inside one pass is one bad session, not a verdict")
    }

    @Test
    fun `silence across separate passes eventually closes it`() {
        val caps = WriteCapability()
        caps.strike(relay, passId = 1)
        caps.strike(relay, passId = 1)
        assertFalse(caps.isClosed(relay))
        caps.strike(relay, passId = 2)
        assertTrue(caps.isClosed(relay), "strikes spanning two passes and past the threshold do close it")
    }

    @Test
    fun `an accepted push clears accumulated doubt`() {
        val caps = WriteCapability()
        caps.strike(relay, passId = 1)
        caps.strike(relay, passId = 2)
        caps.succeeded(relay)
        caps.strike(relay, passId = 3)
        assertFalse(caps.isClosed(relay), "a relay that answered once starts its strike count over")
    }

    @Test
    fun `a closed relay stays closed`() {
        val caps = WriteCapability()
        caps.close(relay, "blocked: banned")
        caps.succeeded(relay)
        assertTrue(caps.isClosed(relay), "policy is not undone by a later stray success")
    }

    @Test
    fun `an answer from a relay that never struck still records it as probed`() {
        // The health line prints closed/probed, and counting only relays that
        // had struck or closed meant a fan-out where everything worked read
        // `0/0` — indistinguishable from a healer that never ran.
        val caps = WriteCapability()
        assertEquals(0, caps.probedCount())
        caps.succeeded(relay)
        assertEquals(1, caps.probedCount(), "a relay we successfully pushed to has demonstrably been probed")
        assertEquals(0, caps.closedCount())
    }

    @Test
    fun `a relay that answers after one silence is never closed by later silences`() {
        // The false close this guards. A relay that timed out once and then
        // replied to everything kept its first strike, because only ACCEPTED
        // cleared doubt and a `duplicate`/`invalid` answer is not ACCEPTED.
        // Two more timeouts across later passes then closed a relay that was
        // plainly answering. The Healer now clears on ANY answer; this pins
        // the WriteCapability half of it.
        val caps = WriteCapability()
        caps.strike(relay, passId = 1)
        caps.succeeded(relay)
        caps.strike(relay, passId = 2)
        caps.succeeded(relay)
        caps.strike(relay, passId = 3)
        assertFalse(caps.isClosed(relay), "strikes separated by answers are not a pattern of silence")
    }
}
