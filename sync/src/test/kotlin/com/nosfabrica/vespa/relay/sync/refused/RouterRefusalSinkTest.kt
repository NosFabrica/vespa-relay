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
package com.nosfabrica.vespa.relay.sync.refused

import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.ingest.refused.PermanentRefusals
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.sync.heal.HealKey
import com.nosfabrica.vespa.relay.sync.heal.HealMode
import com.nosfabrica.vespa.relay.sync.heal.HealQueue
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import com.vitorpamplona.quartz.utils.Hex
import java.nio.file.Files
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Which store refusals earn a filter row, which earn a repair, and which earn neither. */
class RouterRefusalSinkTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")

    /** Hashed: the cuckoo filter buckets on the first 16 hex chars, all zero for a small counter. */
    private fun idOf(n: Int): String = Hex.encode(MessageDigest.getInstance("SHA-256").digest("sink-$n".toByteArray()))

    private fun event(
        n: Int,
        kind: Int = 0,
        tags: Array<Array<String>> = emptyArray(),
    ) = Event(
        id = idOf(n),
        pubKey = "a1".repeat(32),
        createdAt = 1_700_000_000L + n,
        kind = kind,
        tags = tags,
        content = "",
        sig = "b2".repeat(32),
    )

    private class Harness {
        val dir = Files.createTempDirectory("sink").toFile().also { it.deleteOnExit() }
        val refused = RefusedIds(dir, 90L * 24 * 60 * 60, 10_000)
        val queue = HealQueue()

        fun sink(suppression: Boolean = true) = RouterRefusalSink(refused, queue, suppression)
    }

    private fun origin(
        content: Boolean = true,
        retractions: Boolean = true,
    ) = IngestOrigin(relay, healContent = content, healRetractions = retractions)

    // ---- what must never be recorded ---------------------------------------

    @Test
    fun `a store failure never becomes a candidate`() {
        // Failed means the event was good and the store broke; a row would make a transient fault permanent.
        val h = Harness()
        assertFalse(PermanentRefusals.isPermanent(RejectionReason.INSERT_FAILED))
        h.sink().onRefused(event(1), origin(), RejectionReason.INSERT_FAILED)
        h.sink().onRefused(event(1), origin(), RejectionReason.INSERT_FAILED)
        assertFalse(h.refused.suppressed(event(1).id, event(1).createdAt))
        assertEquals(0, h.queue.size())
    }

    @Test
    fun `a duplicate never becomes a candidate`() {
        val h = Harness()
        assertFalse(PermanentRefusals.isPermanent(RejectionReason.DUPLICATE))
        h.sink().onRefused(event(2), origin(), RejectionReason.DUPLICATE)
        h.sink().onRefused(event(2), origin(), RejectionReason.DUPLICATE)
        assertFalse(h.refused.suppressed(event(2).id, event(2).createdAt))
    }

    @Test
    fun `an unstorable-text rejection never becomes a candidate`() {
        val h = Harness()
        assertFalse(PermanentRefusals.isPermanent("blocked: text carries a code point the engine cannot store"))
    }

    // ---- what does ----------------------------------------------------------

    @Test
    fun `REPLACED, DELETED, VANISHED and EXPIRED each become candidates`() {
        listOf(
            RejectionReason.REPLACED,
            RejectionReason.DELETED,
            RejectionReason.VANISHED,
            RejectionReason.EXPIRED,
        ).forEach { assertTrue(PermanentRefusals.isPermanent(it), "'$it' should be permanent") }
    }

    @Test
    fun `two refusals of a replaced event suppress it`() {
        val h = Harness()
        val e = event(3)
        h.sink().onRefused(e, origin(), RejectionReason.REPLACED)
        assertFalse(h.refused.suppressed(e.id, e.createdAt), "one refusal is a candidate only")
        h.sink().onRefused(e, origin(), RejectionReason.REPLACED)
        assertTrue(h.refused.suppressed(e.id, e.createdAt))
    }

    @Test
    fun `the repair is queued before the id is ever recorded`() {
        // A suppressed id is never downloaded again, so the reverse order would starve the repair.
        val h = Harness()
        h.sink().onRefused(event(4), origin(), RejectionReason.REPLACED)
        assertEquals(1, h.queue.size(), "the first refusal must already have queued the repair")
    }

    // ---- the amplitude guard -------------------------------------------------

    @Test
    fun `a refusal with no source relay queues no repair`() {
        // Only a refusal of an event a relay served reaches the queue; a push never adds an author.
        val h = Harness()
        h.sink().onRefused(event(5), IngestOrigin.Local, RejectionReason.REPLACED)
        assertEquals(0, h.queue.size())
    }

    @Test
    fun `with the content switch off, replaceable queues nothing while retractions still do`() {
        val h = Harness()
        h.sink().onRefused(event(6), origin(content = false), RejectionReason.REPLACED)
        assertEquals(0, h.queue.size())
        h.sink().onRefused(event(7), origin(content = false), RejectionReason.DELETED)
        assertEquals(1, h.queue.size(), "the two switches are separate because the author asked for one of them")
    }

    @Test
    fun `with both switches off nothing is ever queued`() {
        val h = Harness()
        listOf(RejectionReason.REPLACED, RejectionReason.DELETED, RejectionReason.VANISHED).forEach {
            h.sink().onRefused(event(8), origin(content = false, retractions = false), it)
        }
        assertEquals(0, h.queue.size())
    }

    @Test
    fun `an expired event queues no repair, because no message says a NIP-40 event expired`() {
        val h = Harness()
        h.sink().onRefused(event(9), origin(), RejectionReason.EXPIRED)
        assertEquals(0, h.queue.size())
        h.sink().onRefused(event(9), origin(), RejectionReason.EXPIRED)
        assertTrue(h.refused.suppressed(event(9).id, event(9).createdAt), "but it is still worth remembering")
    }

    // ---- the repair that gets queued ----------------------------------------

    @Test
    fun `a replaced addressable event queues its d tag, so two d values are two repairs`() {
        val h = Harness()
        h.sink().onRefused(
            event(10, kind = 30_382, tags = arrayOf(arrayOf("d", "alice"))),
            origin(),
            RejectionReason.REPLACED,
        )
        h.sink().onRefused(
            event(11, kind = 30_382, tags = arrayOf(arrayOf("d", "bob"))),
            origin(),
            RejectionReason.REPLACED,
        )
        val work = h.queue.drain(relay)
        assertEquals(2, work.size)
        assertEquals(setOf("alice", "bob"), work.keys.map { it.dTag }.toSet())
    }

    @Test
    fun `a deleted event queues the kind 5 that covers it, not the event itself`() {
        val h = Harness()
        val victim = event(12, kind = 1)
        h.sink().onRefused(victim, origin(), RejectionReason.DELETED)
        val key =
            h.queue
                .drain(relay)
                .keys
                .single()
        assertEquals(HealMode.DELETION, key.mode)
        assertEquals(5, key.kind)
        assertEquals(victim.id, key.victimId, "the healer needs to know which event the kind 5 must cover")
    }

    @Test
    fun `a vanished author queues one repair however many of their events were refused`() {
        val h = Harness()
        repeat(20) { h.sink().onRefused(event(100 + it, kind = 1), origin(), RejectionReason.VANISHED) }
        val work = h.queue.drain(relay)
        assertEquals(1, work.size)
        assertEquals(HealKey.vanish("a1".repeat(32)), work.keys.single())
    }

    @Test
    fun `suppression can be off while healing stays on`() {
        val h = Harness()
        val sink = h.sink(suppression = false)
        sink.onRefused(event(13), origin(), RejectionReason.REPLACED)
        sink.onRefused(event(13), origin(), RejectionReason.REPLACED)
        assertFalse(h.refused.suppressed(event(13).id, event(13).createdAt))
        assertEquals(1, h.queue.size(), "repairs are still discovered")
    }
}
