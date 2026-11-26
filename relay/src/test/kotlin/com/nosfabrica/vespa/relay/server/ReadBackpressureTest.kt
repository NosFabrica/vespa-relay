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
import kotlinx.coroutines.CompletableDeferred
import kotlin.test.Test
import kotlin.test.assertTrue

/** A connection whose commands run slower than they arrive stops being read, on either transport, and is read again. */
class ReadBackpressureTest {
    private val limits = relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to "1048576", "MAX_CONTENT_LENGTH" to "1048576"))
    private val signer = NostrSignerSync()

    /** 36 MB in 60 EVENTs: fewer commands than reads pause at, more characters, more than the socket buffers hold. */
    private val events = (0 until 60).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "$it".padEnd(600_000, 'z')) }

    /** Holds every write until [open]: the store a publisher outruns, for as long as a test needs. */
    private class GatedIngest : EventIngest {
        private val gate = CompletableDeferred<Unit>()

        fun open() = gate.complete(Unit)

        override suspend fun submit(
            event: Event,
            skipVerify: Boolean,
            onComplete: (IEventStore.InsertOutcome) -> Unit,
        ) {
            gate.await()
            onComplete(IEventStore.InsertOutcome.Accepted)
        }

        override fun hasBacklog() = false

        override fun close() {}
    }

    @Test
    fun `a publisher that outruns its commands is held back, then read to the end`() {
        for (native in listOf(true, false)) {
            val transport = if (native) "native" else "ktor"
            val pauses = InboundReads.pauses.get()
            val resumes = InboundReads.resumes.get()
            val ingest = GatedIngest()
            withRawRelay(native, limits, ingest = ingest, soTimeoutMs = 60_000) { ws ->
                val writer = Thread { runCatching { for (e in events) ws.text("""["EVENT",${e.toJson()}]""") } }.apply { start() }
                // 36M characters against a 16M-character budget, none run: reads must pause however fast either side is.
                val until = System.nanoTime() + 60_000_000_000L
                while (InboundReads.pauses.get() == pauses && System.nanoTime() < until) Thread.sleep(50)
                assertTrue(InboundReads.pauses.get() > pauses, "$transport: reads never paused")
                ingest.open()
                val texts = ws.textsUntil { seen -> seen.count { it.startsWith("[\"OK\"") } == events.size }
                writer.join(10_000)
                assertTrue(InboundReads.resumes.get() > resumes, "$transport: reads never resumed")
                assertTrue(texts.count { it.startsWith("[\"OK\"") && ",true," in it } == events.size, "$transport: every EVENT answered")
            }
        }
    }
}
