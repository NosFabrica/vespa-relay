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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.ingest.StoreWriter
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The monitor writes over a connection of its own, so the mirror's writer carries none of its verdicts. */
class MonitorWriterTest {
    private val signer = NostrSignerInternal(KeyPair())
    private val graded = RelayUrlNormalizer.normalize("wss://graded.example")

    /** Writes through to the store, and remembers what it wrote and whether it was closed. */
    private class Recording(
        store: NostrSemanticsStore,
    ) : StoreWriter {
        private val local = StoreWriter.local(store)
        val written = CopyOnWriteArrayList<Event>()

        @Volatile var closed = false

        override val description = "recording"

        override suspend fun batchInsert(events: List<Event>) = local.batchInsert(events).also { written.addAll(events) }

        override suspend fun insert(event: Event) = local.insert(event).also { written.add(event) }

        override suspend fun delete(filter: Filter) = local.delete(filter)

        override fun close() {
            closed = true
        }
    }

    @Test
    fun `the monitor's verdicts go to its own writer and the mirror's carries none`(): Unit =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            // A grade under rules this build no longer signs, which the boot retracts.
            store.insert(
                signer.sign(
                    EventTemplate(
                        nowSeconds(),
                        30166,
                        arrayOf(
                            arrayOf("d", graded.url),
                            arrayOf("l", "prime", RelayVerdictRecord.FITNESS_NAMESPACE, "older rules", nowSeconds().toString(), "0"),
                        ),
                        "",
                    ),
                ),
            )
            val mirror = Recording(store)
            val monitor = Recording(store)
            val engine =
                SyncEngine(
                    store,
                    RouterConfigLoader.parse("streams {}"),
                    signer = signer,
                    writer = mirror,
                    monitorWriter = monitor,
                ).start()
            try {
                // Either writer, so a verdict on the wrong one fails the assertion below rather than the wait.
                withTimeout(30_000) { while ((monitor.written + mirror.written).none { it.kind == 30166 }) delay(20) }
            } finally {
                engine.close()
            }
            assertEquals(listOf(graded.url), monitor.written.filter { it.kind == 30166 }.map { it.tags.first { t -> t[0] == "d" }[1] })
            assertTrue(mirror.written.isEmpty(), "the mirror's writer carried ${mirror.written.size} event(s)")
            assertTrue(mirror.closed && monitor.closed, "closing the engine closes both writers")
        }
}
