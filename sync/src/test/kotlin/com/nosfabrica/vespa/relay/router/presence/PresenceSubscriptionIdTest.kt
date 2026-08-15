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
package com.nosfabrica.vespa.relay.router.presence

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The subscription id, which has a wire consequence and two invariants the loop
 * quietly depends on.
 *
 * The natural id — the stream name, the url and the whole filter — is hundreds
 * of characters, and NIP-01 caps a subscription id at 64 with relays enforcing
 * their own lengths below that. So it is hashed, and hashing is where the two
 * invariants can be lost without anything failing: an unstable id re-opens every
 * subscription on every pass, and a stream-blind one lets two streams collide on
 * one REQ.
 */
class PresenceSubscriptionIdTest {
    private val key = "wss://relay.example/ {\"kinds\":[1],\"authors\":[\"${"a".repeat(64)}\"]}"

    @Test
    fun `it is stable, so an unchanged set re-opens nothing`() {
        // The whole reconcile is a diff against what is already open. An id
        // that varied per pass would close and re-open every subscription every
        // `pollSeconds` — a REQ storm on other people's relays, produced by a
        // loop whose own counters would look perfectly steady.
        assertEquals(PresenceSync.subIdFor("authedOutbox", key), PresenceSync.subIdFor("authedOutbox", key))
    }

    @Test
    fun `it carries the stream, so two streams do not collide on one REQ`() {
        // `client.unsubscribe` takes an id. Two streams sharing one would mean
        // either closing the other's subscription, silently.
        assertTrue(PresenceSync.subIdFor("authedOutbox", key) != PresenceSync.subIdFor("authedScores", key))
    }

    @Test
    fun `different questions on one relay are different subscriptions`() {
        val other = "wss://relay.example/ {\"kinds\":[1],\"authors\":[\"${"b".repeat(64)}\"]}"

        assertTrue(PresenceSync.subIdFor("s", key) != PresenceSync.subIdFor("s", other))
    }

    @Test
    fun `it fits in a subscription id, with room under every relay's own cap`() {
        // NIP-01 says 64. Relays enforce their own lengths below it, and a REQ
        // refused for its id is a leg that reads as a relay serving nothing.
        val id = PresenceSync.subIdFor("a-stream-with-a-long-name", key)

        assertTrue(id.length <= 32, "id was ${id.length} chars: $id")
        assertTrue(id.startsWith("vespa-presence-"), "a wire log has to tell these from the mirror's static tails")
    }
}
