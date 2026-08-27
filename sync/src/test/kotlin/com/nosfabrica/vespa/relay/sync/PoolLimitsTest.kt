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
 * The admission control behind the per-stream workload caps.
 *
 * Every assertion here is written in the direction its failure would go
 * UNNOTICED: a cap that stops capping looks exactly like a cap set higher than
 * the load, and the only way to tell them apart in production is that the
 * machine is busier than it was configured to be.
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
        // The behaviour every deployment has today, and the one this must not
        // change for a config that says nothing: uncapped is unlimited, never
        // zero. A null read as "none allowed" would stop a mirror dead on
        // upgrade.
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

        // The whole point of a share: one stream at its ceiling does not touch
        // another's. Before these existed, a content mirror's audits could
        // occupy every worker an index stream needed and nothing said so.
        assertNotNull(limits.tryHold("indexers", audit), "a different stream has its own share")
        assertEquals(0L, limits.deferred("indexers", audit))
        // …and a different JOB is a different gate again, so a stream capped
        // on its audits is not thereby capped on its catch-up.
        assertNotNull(limits.tryHold("content", catchUp))

        a.release()
        assertNotNull(limits.tryHold("content", audit), "a released permit is available again")
    }

    @Test
    fun `a refusal spends nothing, however many times it happens`() {
        // THE FAILURE THIS RULES OUT. A refusal that walked away holding
        // anything would shrink the share by one per refusal, so a cap of 2
        // would become 1, then a stream that never audits again — with no
        // error, no log line, and a `deferred` counter climbing that reads as
        // the cap doing its job.
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
        // A tail's hold is released by `dropTail`, which races an eviction, a
        // roster drop and a re-open. A double release on a plain semaphore
        // ADDS a permit — the cap silently grows by one every time the race is
        // lost, which is the same class of failure as a leak and harder to
        // see, because the symptom is a machine doing more than it was told to.
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
        // `tryHold` returns a Hold for an uncapped job rather than null, so no
        // caller has to branch on whether a job is capped — a `Hold?` meaning
        // both "refused" and "no cap" would be one `?:` away from a cap that
        // silently admits everything. The handle those callers release must
        // therefore be safe and inert.
        val limits = PoolLimits(mapOf(("content" to audit) to null))
        val hold = assertNotNull(limits.tryHold("content", audit))
        hold.release()
        hold.release()
        assertNull(limits.capFor("content", audit))
        repeat(100) { assertNotNull(limits.tryHold("content", audit)) }
    }

    @Test
    fun `the live pool is capped even when the config says nothing`() {
        // THE ONE JOB THAT CANNOT BE UNCAPPED, and the regression this pins.
        // A visit-job permit is taken INSIDE a visit, so `visitConcurrency` —
        // the pool's own worker count — bounds those even where no share is
        // set. A tail is taken between visits and released only when the
        // roster drops the relay, so an uncapped live gate is one held socket
        // per relay on the roster and nothing above it: the mirror strangles
        // every new connect behind sockets it already holds.
        //
        // It was router-wide (`tailBudget = 600`) and hard until the budgets
        // moved inside the streams, at which point an absent value parsed to
        // null and the default survived only in the boot warning — which went
        // on quoting a number the gate no longer enforced.
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
        // …while the three that something else bounds stay uncapped, so a
        // config that configures nothing runs the pool it always did.
        assertNull(limits.capFor("indexers", VisitPool.JOB_VISITING))
        assertNull(limits.capFor("indexers", VisitPool.POOL_NEGENTROPY))
        assertNull(limits.capFor("indexers", VisitPool.POOL_REFETCHING))
    }

    @Test
    fun `a refusal the caller is going to answer for itself is not work turned away`() {
        // `deferred` is the number that makes a cap actionable: at the cap is
        // not a fault, at the cap WITH work being refused is. The live pool
        // reaches its gate on every tail it opens past the budget and then
        // EARNS one by eviction, so counting that refusal marked every stream
        // sitting at its live budget as starved — permanently, and in the one
        // colour the page uses for a cap that is biting.
        val limits = PoolLimits(mapOf(("content" to VisitPool.POOL_LIVE) to 1))
        val held = assertNotNull(limits.tryHold("content", VisitPool.POOL_LIVE))
        repeat(10) { assertNull(limits.tryHold("content", VisitPool.POOL_LIVE, counted = false)) }
        assertEquals(0L, limits.deferred("content", VisitPool.POOL_LIVE), "the look before an eviction is not a refusal")

        // …and the ask that follows the eviction IS counted: reaching it means
        // the candidate could not outrank anything, which is work turned away.
        assertNull(limits.tryHold("content", VisitPool.POOL_LIVE))
        assertEquals(1L, limits.deferred("content", VisitPool.POOL_LIVE))
        held.release()
    }

    @Test
    fun `what is out and what was turned away are both readable`() {
        // The pair an operator reads together: at the cap is not a fault, at
        // the cap WITH deferrals climbing is the cap turning work away. A
        // deployment cannot tell those apart from either number alone.
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
