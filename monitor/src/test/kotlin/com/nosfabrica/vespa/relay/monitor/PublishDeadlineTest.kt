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
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** A store that stops answering must not hold the monitor open: verdict writes are bounded like dials. */
class PublishDeadlineTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    /** Deep enough to clear [RelayAliases.DEFAULT_MIN_SAMPLE] as one page. */
    private fun corpus(n: Int = 40): List<Event> = (0 until n).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }

    /** A relay that honours the cursor; one that ignores it is graded `unpageable` before any write. */
    private fun paged(
        events: List<Event>,
        want: Int,
        until: Long?,
    ) = AliasProbe.Page(events.filter { until == null || it.createdAt <= until }.take(want))

    private val tinyIdleMs = 20L

    /** Reads answer, writes never do; only `insert` is wedged so the read-before-write still works. */
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
                            fetch = { _, want, until, _ -> paged(corpus(), want, until) },
                            target = 40,
                            page = 40,
                            fallbackPage = 40,
                            idleMs = { tinyIdleMs },
                        ),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = handle,
                    publishDeadlineMs = 100L,
                )

            // The assertion is the call completing at all.
            withTimeout(30_000) {
                pass.measure("wedged store", answering, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }

            assertEquals(
                FitnessPass.PUBLISH_WEDGE_LIMIT,
                store.insertsAttempted.get(),
                "a wedged store must cost the wedge limit, not one deadline per verdict",
            )

            // Clock stamped, position dropped, phase idle: what frees [AliasMonitor]'s gate and the fast lane.
            val row = processors.snapshot().single()
            assertEquals(1L, row.passes, "a pass the store wedged must still count as a pass that ran")
            assertEquals(Processors.IDLE, row.phase, "…and must not be left reading `measuring` forever")
            assertNull(row.measuring, "a finished pass holds no position")
        }

    /** A store that answers every write with a throw, the fast-fail shape of the same outage. */
    private class DecliningWrites(
        inner: NostrSemanticsStore,
    ) : IEventStore by inner {
        val insertsAttempted = AtomicInteger()

        override suspend fun insert(event: Event) {
            insertsAttempted.incrementAndGet()
            error("store declines every write")
        }
    }

    @Test
    fun `a store that fails every write PROMPTLY is reported as a store that wrote nothing`() =
        runBlocking {
            val store = DecliningWrites(NostrSemanticsStore(InMemoryEventIndex(), relay = self))
            val answering = (0 until 5).map { RelayUrlNormalizer.normalize("wss://fine$it.example") }
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe =
                        AliasProbe(
                            fetch = { _, want, until, _ -> paged(corpus(), want, until) },
                            target = 40,
                            page = 40,
                            fallbackPage = 40,
                            idleMs = { tinyIdleMs },
                        ),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    publishDeadlineMs = 100L,
                )

            // The report line is the only place "published" exists.
            val captured = ByteArrayOutputStream()
            val realErr = System.err
            System.setErr(PrintStream(captured, true))
            try {
                withTimeout(30_000) {
                    pass.measure("declining store", answering, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
                }
            } finally {
                System.setErr(realErr)
            }

            assertEquals(5, store.insertsAttempted.get(), "prompt failures must not abandon the batch")
            val err = captured.toString()
            assertTrue("5 earned verdict(s) NOT written" in err, "a store failing every write must not read as a clean pass; got: $err")
            assertTrue("failed outright" in err, "a prompt failure must be told apart from a deadline; got: $err")
        }

    /** One write stalls and the writes around it succeed: ordinary load, not a wedge. */
    private class OneStraggler(
        private val inner: NostrSemanticsStore,
    ) : IEventStore by inner {
        val insertsAttempted = AtomicInteger()

        override suspend fun insert(event: Event) {
            if (insertsAttempted.incrementAndGet() == 2) {
                CompletableDeferred<Unit>().await()
                error("unreachable")
            }
            inner.insert(event)
        }
    }

    @Test
    fun `one stalled write costs ONE verdict, not the rest of the batch`() =
        runBlocking {
            val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            val store = OneStraggler(inner)
            val answering = (0 until 8).map { RelayUrlNormalizer.normalize("wss://fine$it.example") }
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe =
                        AliasProbe(
                            fetch = { _, want, until, _ -> paged(corpus(), want, until) },
                            target = 40,
                            page = 40,
                            fallbackPage = 40,
                            idleMs = { tinyIdleMs },
                        ),
                    client = EmptyNostrClient(),
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = Processors().of("fitness"),
                    publishDeadlineMs = 100L,
                )

            withTimeout(30_000) {
                pass.measure("one straggler", answering, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
            }

            assertEquals(8, store.insertsAttempted.get(), "a lone straggler must not abandon the batch")
            val graded =
                inner
                    .query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey)))
                    .count { event -> event.tags.any { it.size >= 3 && it[0] == "l" && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE } }
            assertEquals(7, graded, "the writes around a straggler must land; only the straggler's verdict waits for the next pass")
        }
}
