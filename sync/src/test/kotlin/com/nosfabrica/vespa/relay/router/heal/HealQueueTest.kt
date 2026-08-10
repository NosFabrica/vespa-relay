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
package com.nosfabrica.vespa.relay.router.heal

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The off-hot-path contract. The queue exists so a reconcile can note that a
 * repair is due in the time it takes to insert into a map, and the two
 * properties below are what stop that promise being quietly withdrawn later.
 */
class HealQueueTest {
    private val a = RelayUrlNormalizer.normalize("wss://a.example")
    private val b = RelayUrlNormalizer.normalize("wss://b.example")

    private fun stale(n: Int) = StaleRef("%064x".format(n), 1_700_000_000L + n)

    @Test
    fun `a popular address enqueued many times occupies one slot`() {
        val q = HealQueue()
        val key = HealKey.content(0, "pk", null)
        repeat(5_000) { q.offer(a, key, stale(it)) }
        assertEquals(1, q.sizeFor(a), "coalescing is per address, not per rejected copy")
    }

    @Test
    fun `coalescing keeps the freshest stale reference`() {
        // The stale id is what a permanent refusal suppresses, so it must be the
        // copy the relay is actually still serving.
        val q = HealQueue()
        val key = HealKey.content(0, "pk", null)
        q.offer(a, key, stale(1))
        q.offer(a, key, stale(2))
        assertEquals(stale(2), q.drain(a)[key])
    }

    @Test
    fun `a full queue drops rather than blocking the producer`() {
        // SAFETY, and deliberately the inverse of IngestPipeline.submit, which
        // suspends rather than lose an event. The asymmetry is the point: an
        // event dropped there is data lost, a heal dropped here is a retry the
        // next cycle rediscovers. A future reader "fixing the inconsistency"
        // would stall the sweep behind the healer — the very thing being fixed.
        val q = HealQueue(perRelayLimit = 10, totalLimit = 100)
        val accepted = (0 until 50).count { q.offer(a, HealKey.content(0, "pk$it", null), stale(it)) }
        assertEquals(10, accepted, "past the limit, offer returns false instead of waiting")
        assertEquals(10, q.sizeFor(a))
        assertEquals(40, q.dropped.get(), "and the drops are counted, not silent")
    }

    @Test
    fun `an existing key is always refreshable even at the limit`() {
        // Overwriting costs no new room, and refusing it would freeze the stale
        // reference at whatever arrived first.
        val q = HealQueue(perRelayLimit = 2, totalLimit = 100)
        val k1 = HealKey.content(0, "pk1", null)
        q.offer(a, k1, stale(1))
        q.offer(a, HealKey.content(0, "pk2", null), stale(2))
        assertTrue(q.offer(a, k1, stale(9)))
        assertEquals(stale(9), q.drain(a)[k1])
    }

    @Test
    fun `draining one relay leaves the others alone`() {
        val q = HealQueue()
        q.offer(a, HealKey.content(0, "pk", null), stale(1))
        q.offer(b, HealKey.content(0, "pk", null), stale(2))
        assertEquals(1, q.drain(a).size)
        assertEquals(0, q.sizeFor(a))
        assertEquals(1, q.sizeFor(b), "the drain is per relay, at the end of that relay's own sync")
    }

    @Test
    fun `the total shrinks when a relay is drained, so one busy relay cannot starve the rest`() {
        val q = HealQueue(perRelayLimit = 5, totalLimit = 5)
        repeat(5) { q.offer(a, HealKey.content(0, "pk$it", null), stale(it)) }
        assertEquals(5, q.size())
        q.drain(a)
        assertEquals(0, q.size())
        assertTrue(q.offer(b, HealKey.content(0, "other", null), stale(1)), "room is returned on drain")
    }

    @Test
    fun `the three modes are distinct keys, and a vanish coalesces per author`() {
        val q = HealQueue()
        q.offer(a, HealKey.content(0, "pk", null), stale(1))
        q.offer(a, HealKey.deletion("pk", "victim-1"), stale(2))
        q.offer(a, HealKey.deletion("pk", "victim-2"), stale(3))
        q.offer(a, HealKey.vanish("pk"), stale(4))
        q.offer(a, HealKey.vanish("pk"), stale(5))
        assertEquals(
            4,
            q.sizeFor(a),
            "content + two distinct deletions + one vanish; every vanished event of an author wants the same push",
        )
    }
}
