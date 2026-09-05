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
import kotlin.test.assertTrue

/** The queue a reconcile notes a due repair into, in the time of one map insert. */
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
        // The stale id is what a permanent refusal suppresses, so it must be the copy still served.
        val q = HealQueue()
        val key = HealKey.content(0, "pk", null)
        q.offer(a, key, stale(1))
        q.offer(a, key, stale(2))
        assertEquals(stale(2), q.drain(a)[key])
    }

    @Test
    fun `a full queue drops rather than blocking the producer`() {
        // A dropped heal is a retry the next offer rediscovers; a blocked sweep is not.
        val q = HealQueue(perRelayLimit = 10, totalLimit = 100)
        val accepted = (0 until 50).count { q.offer(a, HealKey.content(0, "pk$it", null), stale(it)) }
        assertEquals(10, accepted, "past the limit, offer returns false instead of waiting")
        assertEquals(10, q.sizeFor(a))
        assertEquals(40, q.dropped.get(), "and the drops are counted, not silent")
    }

    @Test
    fun `an existing key is always refreshable even at the limit`() {
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

/** The running total is the queue's kill switch: any drift is the healer switching itself off. */
class HealQueueAccountingTest {
    private val a = RelayUrlNormalizer.normalize("wss://a.example")

    private fun stale(n: Int) = StaleRef("%064x".format(n), 1_700_000_000L + n)

    private fun key(n: Int) = HealKey.content(0, "pk%02d".format(n), null)

    @Test
    fun `draining every entry brings the total back to zero`() {
        val q = HealQueue()
        repeat(50) { q.offer(a, key(it), stale(it)) }
        assertEquals(50, q.size())
        assertEquals(50, q.drain(a).size)
        assertEquals(0, q.size(), "a fully drained queue holds nothing")
    }

    @Test
    fun `a bounded drain takes its limit and leaves the rest queued`() {
        val q = HealQueue()
        repeat(10) { q.offer(a, key(it), stale(it)) }

        val first = q.drain(a, limit = 4)
        assertEquals(4, first.size)
        assertEquals(6, q.size(), "what a pass did not attempt must still be owed")

        val second = q.drain(a, limit = 100)
        assertEquals(6, second.size)
        assertEquals(0, q.size())
        assertEquals(
            (0 until 10).map { key(it) }.toSet(),
            first.keys + second.keys,
            "every address is handed over exactly once across the two passes",
        )
    }

    @Test
    fun `a drain of zero takes nothing and owes everything`() {
        val q = HealQueue()
        q.offer(a, key(1), stale(1))
        assertTrue(q.drain(a, limit = 0).isEmpty())
        assertEquals(1, q.size())
    }

    @Test
    fun `an offer racing a drain never inflates the total`() {
        val q = HealQueue()
        val threads =
            (0 until 8).map { t ->
                Thread {
                    repeat(400) { i ->
                        q.offer(a, key(t * 1000 + i), stale(i))
                        if (i % 25 == 0) q.drain(a, limit = 7)
                    }
                }
            }
        threads.forEach { it.start() }
        threads.forEach { it.join() }

        assertEquals(q.sizeFor(a), q.size(), "the running total must equal what is actually queued")
        q.drain(a)
        assertEquals(0, q.sizeFor(a), "a full drain leaves nothing behind")
        assertEquals(0, q.size(), "and settles the counter at zero")
    }

    @Test
    fun `an overwrite racing a drain never deflates the total`() {
        val q = HealQueue()
        val threads =
            (0 until 8).map { t ->
                Thread {
                    repeat(2_000) { i ->
                        q.offer(a, key(i % 5), stale(t * 10_000 + i))
                        if (i % 3 == 0) q.drain(a, limit = 2)
                    }
                }
            }
        threads.forEach { it.start() }
        threads.forEach { it.join() }

        assertEquals(q.sizeFor(a), q.size(), "the running total must equal what is actually queued")
        q.drain(a)
        assertEquals(0, q.sizeFor(a))
        assertEquals(0, q.size(), "a settled queue counts zero — never negative")
    }
}
