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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * A STORE THAT STOPS ANSWERING MUST NOT BE ABLE TO HOLD THE MONITOR OPEN —
 * [ProbeDeadlineTest]'s rule, applied to the one stretch of the pass that
 * predates it: the verdict writes after the dials.
 *
 * ## The failure being pinned (#165)
 *
 * A production fitness pass on `vespa-eventstore-staging` finished every one of
 * its 13,560 dials in forty minutes — `attempted == toProbe`, nothing
 * outstanding — and then sat in `measuring fitness` for TEN HOURS. A SIGQUIT
 * dump of 565 threads showed no frame anywhere in monitor code, because the
 * pass was not running: it was SUSPENDED in a store round trip whose response
 * never came. The store's HTTP client carries no read deadline by design (an
 * unlimited query may take as long as it takes; dead connections are caught by
 * HTTP/2 pings), so a request that is lost while the connection stays healthy
 * suspends its caller for the life of the process.
 *
 * The dials above this loop are each bounded by [AliasProbe.deadlineMs]; the
 * writes were bounded by nothing. And because the sweep holds [AliasMonitor]'s
 * pass gate while it runs, the one suspended write also stopped every future
 * sweep and starved the fast lane — `passesRun: 0` forever, no relay re-graded
 * again, masked by quartz's passive record refresh keeping verdict ages
 * looking fresh for every relay still being dialled.
 *
 * ## What the test drives
 *
 * A real pass over urls that all answer, against a store whose `insert` never
 * returns — parked on a `CompletableDeferred` nobody completes, the same shape
 * [ProbeDeadlineTest.stalling] uses and for the same reason: a `delay` would
 * only prove that a timeout cancels a sleeper. The assertions are the pass
 * RETURNING, the wedge limit capping what the store's silence may cost, and
 * the pass's clock stamping a finished pass — which is what frees the sweep
 * loop and the fast lane behind it.
 */
class PublishDeadlineTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    /** Deep enough to clear [RelayAliases.DEFAULT_MIN_SAMPLE], as one page. */
    private fun corpus(n: Int = 40): List<Event> = (0 until n).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }

    private val tinyIdleMs = 20L

    /**
     * A store whose reads answer and whose writes never do — the wedge as it
     * presented: the pass EARNED every verdict (reads, dials, signatures all
     * fine) and could not put one down. Everything but `insert` delegates to a
     * real in-memory store, so `currentRecord`'s read-before-write works and
     * the suspension is exactly where production suspended.
     */
    private class WedgedWrites(
        inner: NostrSemanticsStore,
    ) : IEventStore by inner {
        val insertsAttempted = AtomicInteger()

        override suspend fun insert(event: Event) {
            insertsAttempted.incrementAndGet()
            CompletableDeferred<Unit>().await()
            error("unreachable")
        }
    }

    @Test
    fun `a fitness pass whose store stops taking writes ENDS, pays for at most the wedge limit, and stamps its clock`() =
        runBlocking {
            val store = WedgedWrites(NostrSemanticsStore(InMemoryEventIndex(), relay = self))
            val processors = Processors()
            val handle = processors.of("fitness")
            val answering = (0 until 8).map { RelayUrlNormalizer.normalize("wss://fine$it.example") }
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe =
                        AliasProbe(
                            fetch = { _, _, _, _ -> AliasProbe.Page(corpus()) },
                            target = 40,
                            page = 40,
                            fallbackPage = 40,
                            idleMs = { tinyIdleMs },
                        ),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = handle,
                    // Shrunk for the same reason [ProbeDeadlineTest.tinyIdleMs]
                    // is: the production minute and this line are the same code.
                    publishDeadlineMs = 100L,
                )

            // THE BOUND THIS TEST IS ABOUT. Before the write deadline existed
            // this call did not return at all — ten hours in production, the
            // life of the process in principle — so the assertion is the call
            // completing, generously against the pass's own budget.
            withTimeout(30_000) {
                pass.measure("wedged store", answering, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }

            // The store's silence is paid for AT THE WEDGE LIMIT and not once
            // per earned verdict: eight verdicts at a minute each would be the
            // ten-hour stall wearing a smaller number, and 13,560 of them nine
            // days of it.
            assertEquals(
                FitnessPass.PUBLISH_WEDGE_LIMIT,
                store.insertsAttempted.get(),
                "a wedged store must cost the wedge limit, not one deadline per verdict",
            )

            // …and the pass ENDED as far as the report is concerned: clock
            // stamped, position dropped, phase back between passes. This is
            // what frees [AliasMonitor]'s gate — the sweep loop schedules the
            // next pass and the fast lane stops starving, which is the whole
            // difference between a fault and #165.
            val row = processors.snapshot().single()
            assertEquals(1L, row.passes, "a pass the store wedged must still count as a pass that ran")
            assertEquals(Processors.IDLE, row.phase, "…and must not be left reading `measuring` forever")
            assertNull(row.measuring, "a finished pass holds no position")
        }
}
