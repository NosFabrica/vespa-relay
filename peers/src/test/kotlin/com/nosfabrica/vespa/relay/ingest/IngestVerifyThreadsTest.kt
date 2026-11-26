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
package com.nosfabrica.vespa.relay.ingest

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A batch whose signatures are checked over several threads is answered exactly as one checked on its worker. */
class IngestVerifyThreadsTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://here.example")
    private val signer = NostrSignerSync()

    private fun forgedSignature(event: Event) = Event(event.id, event.pubKey, event.createdAt, event.kind, event.tags, event.content, "f".repeat(128))

    private fun forgedId(event: Event) = Event("e".repeat(64), event.pubKey, event.createdAt, event.kind, event.tags, event.content, event.sig)

    /** Wide enough to pass the parallel floor: notes, forgeries of both kinds, in-batch copies and replaceable contests. */
    private val offer: List<Event> =
        buildList {
            val notes = (0 until 60).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "note $it") }
            addAll(notes)
            repeat(8) { add(forgedSignature(signer.sign(1_700_001_000L + it, 1, emptyArray(), "forged sig $it"))) }
            notes.take(4).forEach { add(forgedId(it)) }
            // A forged copy ahead of the genuine one, and a plain resend.
            val twice = signer.sign<Event>(1_700_002_000L, 1, emptyArray(), "sent twice")
            add(forgedSignature(twice))
            add(twice)
            add(notes[10])
            // Replaceables: the newest version forged, so the older genuine one must win; and one whose newest is genuine.
            repeat(6) { n ->
                val author = NostrSignerSync()
                add(author.sign<Event>(1_700_003_000L, 0, emptyArray(), """{"name":"old $n"}"""))
                val newest = author.sign<Event>(1_700_003_100L, 0, emptyArray(), """{"name":"new $n"}""")
                add(if (n % 2 == 0) forgedSignature(newest) else newest)
            }
        }

    private data class Run(
        val answers: List<Pair<Int, String>>,
        val stored: Set<String>,
    )

    private fun run(verifyThreads: Int): Run =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            val scope = CoroutineScope(Job())
            val pipeline = IngestPipeline(store, IngestTuning(concurrency = 1, batch = 1_000, verifyThreads = verifyThreads), null, null, scope)
            val answers = Collections.synchronizedList(mutableListOf<Pair<Int, String>>())
            // Queued before the worker starts, so the whole offer is one batch.
            offer.forEachIndexed { i, e -> pipeline.submit(e, skipVerify = false) { answers += i to describe(it) } }
            pipeline.start()
            var waitedMs = 0
            while (answers.size < offer.size && waitedMs < 10_000) {
                delay(5)
                waitedMs += 5
            }
            delay(50)
            scope.cancel()
            pipeline.close()
            Run(answers.toList(), store.query<Event>(Filter()).map { it.id }.toSet())
        }

    private fun describe(outcome: IEventStore.InsertOutcome): String =
        when (outcome) {
            IEventStore.InsertOutcome.Accepted -> "accepted"
            is IEventStore.InsertOutcome.Rejected -> "rejected ${outcome.reason}"
            is IEventStore.InsertOutcome.Failed -> "failed ${outcome.reason}"
        }

    @Test
    fun `checking a batch's signatures on four threads answers what one thread answers, in the same order`() {
        val serial = run(verifyThreads = 1)
        val parallel = run(verifyThreads = 4)
        assertEquals(offer.size, serial.answers.size, "every submission answered once")
        assertTrue(serial.answers.count { it.second.startsWith("rejected invalid") } >= 8 + 4 + 1 + 3, "the forgeries were refused: ${serial.answers}")
        assertEquals(serial.answers, parallel.answers)
        assertEquals(serial.stored, parallel.stored)
    }
}
