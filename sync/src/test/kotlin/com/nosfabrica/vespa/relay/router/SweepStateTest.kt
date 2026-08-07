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
package com.nosfabrica.vespa.relay.router

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * The cursor is the reason a sweep of a billion-event corpus is restartable at
 * all, so the cases that matter are the ones where it must NOT be believed: a
 * claim about a different ask, and a claim old enough that the peer has moved on.
 */
class SweepStateTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")
    private val other = RelayUrlNormalizer.normalize("wss://other.example")
    private val notes = Filter(kinds = listOf(1))

    private fun tempFile(): File {
        val f = File.createTempFile("sweep-state", ".json")
        f.delete()
        return f
    }

    @Test
    fun `an unknown peer falls back to the caller's size`() {
        assertEquals(50_000, SweepState(null).target(relay, 50_000))
    }

    @Test
    fun `a learned size and cap survive a restart`() {
        val f = tempFile()
        SweepState(f).use {
            it.learnCap(relay, 1_000_000, 800_000)
            it.setTarget(other, 12_500)
            it.flush()
        }
        val reopened = SweepState(f)
        assertEquals(800_000, reopened.target(relay, 1))
        assertEquals(1_000_000, reopened.peer(relay)?.cap)
        assertEquals(12_500, reopened.target(other, 1))
        assertNull(reopened.peer(other)?.cap, "a peer that never stated a cap must not acquire one")
    }

    @Test
    fun `a cursor survives a restart and is keyed on the peer`() {
        val f = tempFile()
        SweepState(f).use {
            it.advance(SweepState.keyFor(relay, notes), 1_500, 2_000)
            it.flush()
        }
        val reopened = SweepState(f)
        assertEquals(1_500, reopened.reconciled(SweepState.keyFor(relay, notes))?.downTo)
        assertEquals(2_000, reopened.reconciled(SweepState.keyFor(relay, notes))?.upTo)
        assertNull(reopened.reconciled(SweepState.keyFor(other, notes)), "another relay's sweep is not ours")
    }

    @Test
    fun `advancing only ever widens the finished region`() {
        val state = SweepState(null)
        state.advance(SweepState.keyFor(relay, notes), 1_500, 2_000)
        state.advance(SweepState.keyFor(relay, notes), 1_000, 1_499)
        val mark = assertNotNull(state.reconciled(SweepState.keyFor(relay, notes)))
        assertEquals(1_000, mark.downTo)
        assertEquals(2_000, mark.upTo)

        // A window inside what is already claimed changes nothing — it cannot
        // punch a hole in the region by narrowing it.
        state.advance(SweepState.keyFor(relay, notes), 1_800, 1_900)
        assertEquals(1_000, state.reconciled(SweepState.keyFor(relay, notes))?.downTo)
        assertEquals(2_000, state.reconciled(SweepState.keyFor(relay, notes))?.upTo)
    }

    @Test
    fun `the cursor ignores the time bounds and nothing else`() {
        val state = SweepState(null)
        state.advance(SweepState.keyFor(relay, notes), 1_000, 2_000)

        // Same ask, different window: this is what a sweep varies, so it must
        // find its own cursor.
        assertNotNull(state.reconciled(SweepState.keyFor(relay, notes.copy(since = 5, until = 9))))
        // Different ask: reconciling kind 1 says nothing about kind 0.
        assertNull(state.reconciled(SweepState.keyFor(relay, Filter(kinds = listOf(0)))))
        assertNull(state.reconciled(SweepState.keyFor(relay, notes.copy(authors = listOf("a".repeat(64))))))
    }

    @Test
    fun `a stale cursor is not acted on`() {
        val fresh = SweepState(null, staleAfterSeconds = 3_600)
        fresh.advance(SweepState.keyFor(relay, notes), 1_000, 2_000)
        assertNotNull(fresh.reconciled(SweepState.keyFor(relay, notes)))

        // Zero horizon: anything written before this instant is already too old.
        val stale = SweepState(null, staleAfterSeconds = -1)
        stale.advance(SweepState.keyFor(relay, notes), 1_000, 2_000)
        assertNull(stale.reconciled(SweepState.keyFor(relay, notes)), "an aged claim must be re-compared, not trusted")
    }

    @Test
    fun `finishing a leg drops its cursor and leaves the peer's size`() {
        val state = SweepState(null)
        state.setTarget(relay, 12_500)
        state.advance(SweepState.keyFor(relay, notes), 1_000, 2_000)
        state.finish(SweepState.keyFor(relay, notes))

        assertNull(state.reconciled(SweepState.keyFor(relay, notes)))
        assertEquals(12_500, state.target(relay, 1), "what the peer will take outlives the sweep that learned it")
    }

    @Test
    fun `a corrupt file starts fresh instead of failing the boot`() {
        val f = tempFile()
        f.writeText("{not json")
        val state = SweepState(f)
        assertEquals(0, state.size())
        assertEquals(7, state.target(relay, 7))
    }

    @Test
    fun `no file configured keeps everything in memory`() {
        val state = SweepState(null)
        state.advance(SweepState.keyFor(relay, notes), 1_000, 2_000)
        state.flush()
        assertNotNull(state.reconciled(SweepState.keyFor(relay, notes)))
    }
}
