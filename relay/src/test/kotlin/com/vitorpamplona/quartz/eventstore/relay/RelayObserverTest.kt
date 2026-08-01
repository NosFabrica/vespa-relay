/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.AuthMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.ClosedMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EoseMessage
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * What gets published to the network under this relay's own key, so the rules
 * about what we are willing to CLAIM matter more than the plumbing: an unmeasured
 * latency must never be reported as a measurement, a relay nobody dialled must
 * never be reported as dead, and one bad minute must not bury a working relay.
 */
class RelayObserverTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private val other = RelayUrlNormalizer.normalize("wss://other.example")

    private fun observer() = RelayObserver()

    private fun client(u: com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl) = FakeRelayClient(u)

    @Test
    fun `an opened connection is timed, not assumed`() {
        val o = observer()
        o.onConnecting(client(url))
        o.onConnected(client(url), 1, true)

        val obs = o.drain().getValue(url)
        assertTrue(obs.reachable)
        assertNotNull(obs.rttOpenMs, "rtt-open must be measured — aggregators rank on it")
        assertNull(obs.error)
    }

    @Test
    fun `a connection that never opened records the reason and no latency`() {
        val o = observer()
        o.onConnecting(client(url))
        o.onCannotConnect(client(url), "Expected HTTP 101 response but was '503 Service Unavailable'")

        val obs = o.drain().getValue(url)
        assertFalse(obs.reachable)
        assertNull(obs.rttOpenMs, "nothing opened, so there is nothing to time")
        assertTrue(obs.error!!.contains("503"))
    }

    @Test
    fun `a relay that answered stays answered through a later failure`() {
        // A relay that worked an hour ago and blipped now is not the same as one
        // that has never answered. Only the flush decides which record to write,
        // so a single failure must not erase the success underneath it.
        val o = observer()
        o.onConnecting(client(url))
        o.onConnected(client(url), 1, true)
        o.onCannotConnect(client(url), "connection reset")

        val obs = o.drain().getValue(url)
        assertTrue(obs.reachable, "one bad minute must not bury a relay that answered")
    }

    @Test
    fun `a reconnect clears the previous attempt's error`() {
        val o = observer()
        o.onConnecting(client(url))
        o.onCannotConnect(client(url), "timeout")
        o.onConnecting(client(url))

        assertNull(o.drain().getValue(url).error, "a stale error would report a live relay as broken forever")
    }

    @Test
    fun `the read clock runs from the first REQ to the first EOSE`() {
        val o = observer()
        o.onConnecting(client(url))
        o.onConnected(client(url), 1, true)
        o.onRequestSent(url)
        o.onIncomingMessage(client(url), "", EoseMessage("sub"))

        assertNotNull(o.drain().getValue(url).rttReadMs)
    }

    @Test
    fun `an EOSE with no REQ behind it times nothing`() {
        // A subscription opened on an already-warm socket says nothing about how
        // fast the relay is; inventing a number for it would be worse than none.
        val o = observer()
        o.onConnecting(client(url))
        o.onConnected(client(url), 1, true)
        o.onIncomingMessage(client(url), "", EoseMessage("sub"))

        assertNull(o.drain().getValue(url).rttReadMs)
    }

    @Test
    fun `a demand for AUTH is recorded, from either shape`() {
        // This is why an unauthenticated crawl finds a relay empty, and it is
        // invisible in the event stream — worth publishing precisely because
        // nothing else explains a relay that connects and serves nothing.
        val challenged = observer()
        challenged.onIncomingMessage(client(url), "", AuthMessage("challenge-string"))
        assertTrue(challenged.drain().getValue(url).authRequired)

        val closed = observer()
        closed.onIncomingMessage(client(url), "", ClosedMessage("sub", "auth-required: we only serve subscribers"))
        assertTrue(closed.drain().getValue(url).authRequired)
    }

    @Test
    fun `draining empties, so an unchanged relay is not re-reported`() {
        // Re-writing a record refreshes its TTL. Doing that for a relay nobody
        // re-measured would turn "checked recently" into a claim we never made.
        val o = observer()
        o.onConnecting(client(url))
        o.onConnected(client(url), 1, true)

        assertEquals(1, o.drain().size)
        assertEquals(0, o.drain().size, "a second flush must have nothing left to say")
        assertEquals(0, o.size())
    }

    @Test
    fun `each relay is observed on its own`() {
        val o = observer()
        o.onConnecting(client(url))
        o.onConnected(client(url), 1, true)
        o.onConnecting(client(other))
        o.onCannotConnect(client(other), "nodename nor servname provided")

        val drained = o.drain()
        assertTrue(drained.getValue(url).reachable)
        assertFalse(drained.getValue(other).reachable)
    }
}
