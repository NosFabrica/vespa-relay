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
package com.nosfabrica.vespa.relay.ingest.refused

import com.vitorpamplona.quartz.utils.Hex
import java.io.File
import java.nio.file.Files
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The gate and the partitioning: how many refusals an id needs before it is
 * suppressed, and whether a lookup finds a row a different code path inserted.
 */
class RefusedIdsTest {
    private val epoch = 1_000L

    private fun id(n: Int): String = Hex.encode(MessageDigest.getInstance("SHA-256").digest("refused-$n".toByteArray()))

    private fun dir(): File = Files.createTempDirectory("refused").toFile().also { it.deleteOnExit() }

    private fun refused(
        d: File? = dir(),
        capacity: Int = 10_000,
    ) = RefusedIds(d, epoch, capacity)

    @Test
    fun `one refusal makes a candidate and suppresses nothing`() {
        // A candidate-filter false positive costs one download, never a suppression.
        refused().use { r ->
            assertEquals(RecordOutcome.CANDIDATE, r.record(id(1), 5_000))
            assertFalse(r.suppressed(id(1), 5_000), "one refusal must never suppress")
        }
    }

    @Test
    fun `a second refusal suppresses`() {
        refused().use { r ->
            r.record(id(1), 5_000)
            assertEquals(RecordOutcome.SUPPRESSED, r.record(id(1), 5_000))
            assertTrue(r.suppressed(id(1), 5_000))
        }
    }

    @Test
    fun `a third refusal is a no-op`() {
        refused().use { r ->
            r.record(id(1), 5_000)
            r.record(id(1), 5_000)
            assertEquals(RecordOutcome.ALREADY, r.record(id(1), 5_000))
        }
    }

    @Test
    fun `a permanent push refusal suppresses immediately, with no candidate stage`() {
        // The relay has said outright it will never take the repair.
        refused().use { r ->
            assertEquals(RecordOutcome.SUPPRESSED, r.suppressNow(id(2), 5_000))
            assertTrue(r.suppressed(id(2), 5_000))
        }
    }

    @Test
    fun `a lookup window spanning an epoch boundary consults both epochs`() {
        // Insertion keys on the exact created_at, a sweep lookup on its window,
        // and windows do not respect epoch edges.
        refused().use { r ->
            val at = 2_500L // epoch 2
            r.record(id(3), at)
            r.record(id(3), at)
            assertTrue(r.suppressed(id(3), at))

            // Starts in epoch 1, ends in epoch 3.
            assertTrue(
                r.suppressedInWindow(id(3), since = 1_500, until = 3_500),
                "a window straddling the boundary must still find the row",
            )
            assertFalse(r.suppressedInWindow(id(3), since = 0, until = 999))
        }
    }

    @Test
    fun `retiring an epoch below the floor un-suppresses the ids it held`() {
        refused().use { r ->
            r.record(id(4), 1_500) // epoch 1
            r.record(id(4), 1_500)
            r.record(id(5), 9_500) // epoch 9
            r.record(id(5), 9_500)
            assertTrue(r.suppressed(id(4), 1_500))

            r.retireBelow(9_000)

            assertFalse(r.suppressed(id(4), 1_500), "a retired epoch keeps nothing")
            assertTrue(r.suppressed(id(5), 9_500), "epochs above the floor are untouched")
        }
    }

    @Test
    fun `rows survive a reopen, because a refusal is not invalidated by a restart`() {
        val d = dir()
        refused(d).use { r ->
            r.record(id(6), 5_000)
            r.record(id(6), 5_000)
            r.flush()
        }
        refused(d).use { r ->
            assertTrue(r.suppressed(id(6), 5_000), "the store's verdict did not change because we restarted")
        }
    }

    @Test
    fun `with no directory nothing is recorded and nothing is suppressed`() {
        // Opt-in: a router with nowhere to keep its filters behaves as if they did not exist.
        RefusedIds.disabled().use { r ->
            assertFalse(r.enabled)
            r.record(id(7), 5_000)
            r.record(id(7), 5_000)
            assertFalse(r.suppressed(id(7), 5_000))
            assertFalse(r.suppressedInWindow(id(7), 0, 10_000))
        }
    }

    @Test
    fun `a sealed epoch keeps answering for what it holds and takes nothing new`() {
        // Silently continuing past capacity is the one unacceptable answer.
        refused(capacity = 200).use { r ->
            var sealedAt = -1
            for (i in 0 until 20_000) {
                // Two refusals each, so both tables fill.
                r.record(id(i), 5_000)
                if (r.record(id(i), 5_000) == RecordOutcome.REFUSED_FULL) {
                    sealedAt = i
                    break
                }
            }
            assertTrue(sealedAt > 0, "a small epoch must seal rather than absorb 20k ids")
            assertTrue(r.summary().contains("SEALED"), "sealing must be visible: ${r.summary()}")
            assertTrue(r.suppressed(id(0), 5_000))
        }
    }

    @Test
    fun `epochs are keyed by floor division, so pre-epoch timestamps do not collide`() {
        refused().use { r ->
            assertEquals(0L, r.epochOf(0))
            assertEquals(1L, r.epochOf(1_000))
            assertEquals(-1L, r.epochOf(-1), "floorDiv, not truncation — otherwise -1 and 0 share a partition")
        }
    }
}

/** The window lookup, the only path a sweep reaches. */
class RefusedIdsWindowTest {
    private fun dir() = Files.createTempDirectory("window").toFile().also { it.deleteOnExit() }

    /**
     * Real hashes: the filter slices its bucket and fingerprint straight out of the
     * id, and `"%064x"` counters all land in one bucket with one fingerprint.
     */
    private fun id(n: Int): String = Hex.encode(MessageDigest.getInstance("SHA-256").digest("window-$n".toByteArray()))

    private fun twiceRefuse(
        r: RefusedIds,
        id: String,
        at: Long,
    ) {
        r.record(id, at)
        r.record(id, at)
    }

    @Test
    fun `an open-ended window still finds a suppressed id`() {
        // `since = null` is the ordinary shape of a deleteMissing ask.
        val r = RefusedIds(dir(), 86_400L, 10_000)
        twiceRefuse(r, id(1), 1_780_000_000L)
        assertTrue(r.suppressedInWindow(id(1), null, null))
        r.close()
    }

    @Test
    fun `a window that excludes the id's epoch does not match it`() {
        val r = RefusedIds(dir(), 86_400L, 10_000)
        twiceRefuse(r, id(2), 1_780_000_000L)
        // Two days earlier, closed before the epoch the id landed in.
        assertFalse(r.suppressedInWindow(id(2), 1_779_000_000L, 1_779_500_000L))
        r.close()
    }

    @Test
    fun `an id is found from either side of its own epoch boundary`() {
        // Insertion keys on the exact created_at, lookup on a window, and
        // windows do not respect epoch edges.
        val epoch = 86_400L
        val r = RefusedIds(dir(), epoch, 10_000)
        val at = 1_780_000_000L
        twiceRefuse(r, id(3), at)
        assertTrue(r.suppressedInWindow(id(3), at - epoch, at + epoch), "a window spanning three epochs covers it")
        assertTrue(r.suppressedInWindow(id(3), at, at), "so does the degenerate window on the id itself")
        r.close()
    }

    @Test
    fun `an open-ended window costs the epochs that exist, not the epochs since 1970`() {
        // A guard on cost, not correctness.
        val r = RefusedIds(dir(), 86_400L, 10_000)
        twiceRefuse(r, id(4), 1_780_000_000L)

        val probes = 20_000
        val started = System.nanoTime()
        repeat(probes) { r.suppressedInWindow(id(9_999), null, null) }
        val perCallMicros = (System.nanoTime() - started) / 1_000.0 / probes

        assertTrue(
            perCallMicros < 50.0,
            "an open-ended miss took ${perCallMicros}us per call — it should be proportional to the " +
                "one epoch on disk, not to the number of epochs since 1970",
        )
        r.close()
    }
}
