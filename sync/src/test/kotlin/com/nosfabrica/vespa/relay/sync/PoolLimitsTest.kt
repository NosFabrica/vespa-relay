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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.config.RouterConfig
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * The admission control behind the per-stream caps. A cap that stops capping
 * looks like a cap set higher than the load, so each assertion faces that way.
 */
class PoolLimitsTest {
    private val audit = VisitPool.POOL_NEGENTROPY
    private val catchUp = VisitPool.POOL_CATCHING_UP

    /** A visit-mode stream carrying nothing but the budgets under test. */
    private fun stream(
        name: String,
        maxLiveConcurrency: Int? = null,
    ) = SyncStream(
        name = name,
        dir = SyncDirection.DOWN,
        filter = Filter(kinds = listOf(1)),
        urls = emptyList(),
        trusted = false,
        maxLiveConcurrency = maxLiveConcurrency,
    )

    @Test
    fun `no cap is no gate, and every ask is granted`() {
        // Uncapped is unlimited, never zero; a null read as "none allowed" would stop a mirror on upgrade.
        val limits = PoolLimits(mapOf(("content" to audit) to null))
        repeat(1_000) { assertNotNull(limits.tryHold("content", audit)) }
        assertEquals(0L, limits.deferred("content", audit))
        assertNull(limits.capFor("content", audit), "and it says it is uncapped rather than reporting a number")
        assertNull(limits.heldBy("content", audit), "no cap is no `in use` to report against")
    }

    @Test
    fun `a stream's share bounds that stream and nobody else`() {
        val limits = PoolLimits(mapOf(("content" to audit) to 2, ("indexers" to audit) to 1))
        val a = assertNotNull(limits.tryHold("content", audit))
        assertNotNull(limits.tryHold("content", audit))
        assertNull(limits.tryHold("content", audit), "the third is over content's share")
        assertEquals(1L, limits.deferred("content", audit))

        // One stream at its ceiling does not touch another's share.
        assertNotNull(limits.tryHold("indexers", audit), "a different stream has its own share")
        assertEquals(0L, limits.deferred("indexers", audit))
        // A different job is a different gate.
        assertNotNull(limits.tryHold("content", catchUp))

        a.release()
        assertNotNull(limits.tryHold("content", audit), "a released permit is available again")
    }

    @Test
    fun `a refusal spends nothing, however many times it happens`() {
        // A refusal that walked away holding anything would shrink the share by one per refusal.
        val limits = PoolLimits(mapOf(("content" to audit) to 2))
        val one = assertNotNull(limits.tryHold("content", audit))
        val two = assertNotNull(limits.tryHold("content", audit))
        repeat(50) { assertNull(limits.tryHold("content", audit)) }
        assertEquals(50L, limits.deferred("content", audit))
        assertEquals(2, limits.heldBy("content", audit), "still exactly the two that were granted")

        one.release()
        two.release()
        assertEquals(0, limits.heldBy("content", audit))
        repeat(2) { assertNotNull(limits.tryHold("content", audit), "the whole share is back after 50 refusals") }
    }

    @Test
    fun `releasing twice does not mint a permit`() {
        // `dropTail` races an eviction, a roster drop and a re-open; a double release on a plain semaphore mints a permit.
        val limits = PoolLimits(mapOf(("content" to audit) to 1))
        val hold = assertNotNull(limits.tryHold("content", audit))
        hold.release()
        hold.release()
        hold.release()
        assertNotNull(limits.tryHold("content", audit))
        assertNull(limits.tryHold("content", audit), "still one permit, however many times it was handed back")
    }

    @Test
    fun `an uncapped job's hold is releasable too, and releases nothing`() {
        // `tryHold` answers a Hold for an uncapped job so no caller branches on whether a job is capped;
        // the handle must be inert.
        val limits = PoolLimits(mapOf(("content" to audit) to null))
        val hold = assertNotNull(limits.tryHold("content", audit))
        hold.release()
        hold.release()
        assertNull(limits.capFor("content", audit))
        repeat(100) { assertNotNull(limits.tryHold("content", audit)) }
    }

    @Test
    fun `the live pool is capped even when the config says nothing`() {
        // A tail is released only when the roster drops the relay, so an uncapped live
        // gate is one held socket per relay on the roster.
        val limits =
            PoolLimits.of(
                listOf(
                    stream("content", maxLiveConcurrency = 3),
                    stream("indexers", maxLiveConcurrency = null),
                ),
            )
        assertEquals(3, limits.capFor("content", VisitPool.POOL_LIVE), "a stream that says a number gets its number")
        assertEquals(
            RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY,
            limits.capFor("indexers", VisitPool.POOL_LIVE),
            "and one that says nothing gets the default the socket warning has been assuming all along",
        )
        // The three jobs that something else bounds stay uncapped.
        assertNull(limits.capFor("indexers", VisitPool.JOB_VISITING))
        assertNull(limits.capFor("indexers", VisitPool.POOL_NEGENTROPY))
        assertNull(limits.capFor("indexers", VisitPool.POOL_REFETCHING))
    }

    @Test
    fun `a refusal the caller is going to answer for itself is not work turned away`() {
        // The live pool looks for a spare on every tail past the budget and then earns one
        // by eviction; that look is not work turned away.
        val limits = PoolLimits(mapOf(("content" to VisitPool.POOL_LIVE) to 1))
        val held = assertNotNull(limits.tryHold("content", VisitPool.POOL_LIVE))
        repeat(10) { assertNull(limits.trySpare("content", VisitPool.POOL_LIVE)) }
        assertEquals(0L, limits.deferred("content", VisitPool.POOL_LIVE), "a look for a spare permit is not a refusal")

        // The ask after the eviction is counted: reaching it means the candidate outranked nothing.
        assertNull(limits.tryHold("content", VisitPool.POOL_LIVE))
        assertEquals(1L, limits.deferred("content", VisitPool.POOL_LIVE))
        held.release()
    }

    @Test
    fun `what is out and what was turned away are both readable`() {
        // At the cap is not a fault; at the cap with deferrals climbing is.
        val limits = PoolLimits(mapOf(("content" to audit) to 2))
        assertEquals(0, limits.heldBy("content", audit))
        val one = assertNotNull(limits.tryHold("content", audit))
        assertEquals(1, limits.heldBy("content", audit))
        assertEquals(2, limits.capFor("content", audit))
        assertNotNull(limits.tryHold("content", audit))
        assertNull(limits.tryHold("content", audit))
        assertEquals(2, limits.heldBy("content", audit))
        assertEquals(1L, limits.deferred("content", audit))
        one.release()
        assertEquals(1, limits.heldBy("content", audit))
        assertEquals(1L, limits.deferred("content", audit), "a release is not an un-deferral — it is a rate, not a backlog")
    }
}
