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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.refused.IngestOrigin
import com.nosfabrica.vespa.relay.router.refused.RefusalSink
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.EventHasher
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Who a store refusal gets blamed on.
 *
 * The pipeline reads outcome `i` as belonging to event `i` of the batch it
 * sent. That is the store's contract, and everywhere else a breach of it would
 * only skew a counter — here it decides which id goes into a filter that
 * suppresses that id forever. So the mismatch is checked rather than trusted,
 * and these pin both halves: attribution happens when the lists line up, and
 * stops when they do not.
 */
class IngestAttributionTest {
    private fun event(n: Int) =
        Event(
            id = "%064x".format(n),
            pubKey = "a1".repeat(32),
            createdAt = 1_700_000_000L + n,
            kind = 1,
            tags = emptyArray(),
            content = "e$n",
            sig = "b2".repeat(32),
        )

    /** Records what the pipeline decided to blame, and for what reason. */
    private class Recorder : RefusalSink {
        override val tracksOrigins = true

        val refusals: MutableList<Pair<String, String>> = Collections.synchronizedList(mutableListOf())

        override fun isSuppressed(event: Event) = false

        override fun onRefused(
            event: Event,
            origin: IngestOrigin,
            reason: String,
        ) {
            refusals.add(event.id to reason)
        }
    }

    /**
     * A store that answers with the right verdicts in the wrong quantity —
     * the contract breach the guard exists for. Interface delegation carries
     * everything except the one method under test.
     */
    private class Misaligning(
        private val inner: IEventStore,
        private val drop: Int,
    ) : IEventStore by inner {
        override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> = events.map { IEventStore.InsertOutcome.Rejected(RejectionReason.REPLACED) }.dropLast(drop)
    }

    private class Rejecting(
        private val inner: IEventStore,
    ) : IEventStore by inner {
        override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> = events.map { IEventStore.InsertOutcome.Rejected(RejectionReason.REPLACED) }
    }

    private fun config() = RouterConfig(connectionTimeoutSec = 5, streams = emptyList(), ingestConcurrency = 1, ingestBatch = 8)

    private fun base(): IEventStore = NostrSemanticsStore(InMemoryEventIndex(), relay = null)

    private suspend fun run(
        store: IEventStore,
        sink: Recorder,
        count: Int,
    ) {
        val scope = CoroutineScope(SupervisorJob())
        val pipeline = IngestPipeline(store, config(), null, null, scope, null, null, sink)
        try {
            pipeline.start()
            repeat(count) { pipeline.submit(event(it), skipVerify = true, IngestOrigin.Local) }
            // The pipeline is asynchronous by design; wait for the queue to
            // clear rather than for a fixed time.
            var spins = 0
            while (pipeline.queued.get() > 0 && spins++ < 400) delay(25)
            delay(200)
        } finally {
            pipeline.close()
            scope.cancel()
        }
    }

    @Test
    fun `an aligned batch blames each rejection on the event that earned it`() =
        runBlocking {
            val sink = Recorder()
            run(Rejecting(base()), sink, 8)

            assertEquals(8, sink.refusals.size, "every rejection should be attributed")
            assertEquals(
                (0 until 8).map { event(it).id }.toSet(),
                sink.refusals.map { it.first }.toSet(),
                "and to exactly the ids that were sent",
            )
        }

    @Test
    fun `a store that returns fewer outcomes than events attributes nothing`() {
        // SAFETY. `getOrNull(i)` alone would keep going here and blame the
        // wrong events — outcome i belongs to event i only while the lists are
        // the same length. Every id it named would be a candidate for
        // suppression it never earned, and a suppressed id is never downloaded
        // again to notice the mistake. Silence is the correct output; the
        // pipeline logs the breach once instead.
        runBlocking {
            val sink = Recorder()
            run(Misaligning(base(), drop = 3), sink, 8)

            assertTrue(
                sink.refusals.isEmpty(),
                "a misaligned batch must blame nobody, got ${sink.refusals.map { it.first.take(8) }}",
            )
        }
    }

    /**
     * A real event, hashed the way NIP-01 says, so `verifyId` accepts it. The
     * signature is still junk — deliberately: the fast path below runs before
     * verification, and these pin that it is the ID and not the signature that
     * gates what may be remembered.
     */
    private fun hashed(
        n: Int,
        kind: Int = 0,
        createdAt: Long = 1_700_000_000L + n,
        pubKey: String = "a1".repeat(32),
    ): Event {
        val tags = emptyArray<Array<String>>()
        val content = "c$n"
        return Event(
            id = EventHasher.hashId(pubKey, createdAt, kind, tags, content),
            pubKey = pubKey,
            createdAt = createdAt,
            kind = kind,
            tags = tags,
            content = content,
            sig = "b2".repeat(32),
        )
    }

    @Test
    fun `a replaceable superseded inside the batch is reported even though the store never sees it`() =
        runBlocking {
            // The merge hazard. `dropSuperseded` exists to keep a stale
            // replaceable away from verification and the store — which is
            // exactly where the `replaced:` verdict used to come from. If the
            // refusal is not reported from the fast path too, the filter and
            // the healer go quiet in proportion to how well that optimisation
            // works, and a backfill running at 94% replaced would feed them
            // almost nothing.
            val sink = Recorder()
            val older = hashed(1, createdAt = 1_700_000_000L)
            val newer = hashed(2, createdAt = 1_700_009_999L)

            val scope = CoroutineScope(SupervisorJob())
            val pipeline = IngestPipeline(base(), config(), null, null, scope, null, null, sink)
            try {
                pipeline.start()
                pipeline.submit(older, skipVerify = true, IngestOrigin.Local)
                pipeline.submit(newer, skipVerify = true, IngestOrigin.Local)
                var spins = 0
                while (pipeline.queued.get() > 0 && spins++ < 400) delay(25)
                delay(300)
            } finally {
                pipeline.close()
                scope.cancel()
            }

            assertTrue(
                sink.refusals.any { it.first == older.id && it.second.startsWith("replaced") },
                "the superseded copy must still be reported; got ${sink.refusals}",
            )
        }

    @Test
    fun `an event whose id does not match its content is never remembered`() =
        runBlocking {
            // SAFETY. The fast path runs before any verification, so an
            // unchecked id here is attacker-chosen: forge a stale kind 0 for
            // any pubkey, stamp it with the id of an event you want this relay
            // never to fetch, and two of them suppress that id for good. The
            // id hash is what makes the claim self-certifying.
            val sink = Recorder()
            val real = hashed(3, createdAt = 1_700_009_999L)
            val forged =
                Event(
                    id = "%064x".format(0xDEAD),
                    pubKey = "a1".repeat(32),
                    createdAt = 1_700_000_000L,
                    kind = 0,
                    tags = emptyArray(),
                    content = "forged",
                    sig = "b2".repeat(32),
                )

            val scope = CoroutineScope(SupervisorJob())
            val pipeline = IngestPipeline(base(), config(), null, null, scope, null, null, sink)
            try {
                pipeline.start()
                pipeline.submit(forged, skipVerify = true, IngestOrigin.Local)
                pipeline.submit(real, skipVerify = true, IngestOrigin.Local)
                var spins = 0
                while (pipeline.queued.get() > 0 && spins++ < 400) delay(25)
                delay(300)
            } finally {
                pipeline.close()
                scope.cancel()
            }

            assertTrue(
                sink.refusals.none { it.first == forged.id },
                "an id that does not hash its own content must never enter the filter; got ${sink.refusals}",
            )
        }
}
