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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.relay.server.config.relayLimitsFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.delay
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertTrue

/** A connection whose commands run slower than they arrive stops being read, on either transport, and is read again. */
class ReadBackpressureTest {
    private val limits = relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to "1048576", "MAX_CONTENT_LENGTH" to "1048576"))
    private val signer = NostrSignerSync()

    /** 36 MB in 60 EVENTs: fewer commands than reads pause at, more characters, more than the socket buffers hold. */
    private val events = (0 until 60).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "$it".padEnd(600_000, 'z')) }

    /** Takes an event every 100 ms: the store a publisher outruns. */
    private class SlowIngest : EventIngest {
        val taken = AtomicInteger()

        override suspend fun submit(
            event: Event,
            skipVerify: Boolean,
            onComplete: (IEventStore.InsertOutcome) -> Unit,
        ) {
            delay(100)
            taken.incrementAndGet()
            onComplete(IEventStore.InsertOutcome.Accepted)
        }

        override fun hasBacklog() = false

        override fun close() {}
    }

    @Test
    fun `a publisher that outruns its commands is held back, then read to the end`() {
        for (native in listOf(true, false)) {
            val ingest = SlowIngest()
            withRawRelay(native, limits, ingest = ingest, soTimeoutMs = 30_000) { ws ->
                val written = AtomicInteger()
                val writer =
                    Thread {
                        for (e in events) {
                            ws.text("""["EVENT",${e.toJson()}]""")
                            written.incrementAndGet()
                        }
                    }.apply { start() }
                Thread.sleep(2_000)
                // Twenty commands run by now; a relay reading ahead would have taken all sixty.
                val heldBack = writer.isAlive
                val texts = ws.textsUntil { seen -> seen.count { it.startsWith("[\"OK\"") } == events.size }
                writer.join(5_000)
                val transport = if (native) "native" else "ktor"
                assertTrue(heldBack, "$transport: all ${written.get()} EVENTs were taken in 2 s with ${ingest.taken.get()} run")
                assertTrue(texts.count { it.startsWith("[\"OK\"") && ",true," in it } == events.size, "$transport: every EVENT answered")
            }
        }
    }
}
