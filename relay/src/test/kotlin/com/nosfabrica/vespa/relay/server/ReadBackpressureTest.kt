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
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlin.test.Test
import kotlin.test.assertTrue

/** A connection whose commands run slower than they arrive stops being read, on either transport, and is read again. */
class ReadBackpressureTest {
    private val limits = relayLimitsFromEnv(mapOf("MAX_MESSAGE_LENGTH" to "1048576", "MAX_CONTENT_LENGTH" to "1048576"))
    private val signer = NostrSignerSync()

    /** 36 MB in 60 EVENTs: fewer commands than reads pause at, more characters, more than the socket buffers hold. */
    private val events = (0 until 60).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "$it".padEnd(600_000, 'z')) }

    @Test
    fun `a publisher that outruns its commands is held back, then read to the end`() {
        for (native in listOf(true, false)) {
            val pauses = InboundReads.pauses.get()
            val resumes = InboundReads.resumes.get()
            withRawRelay(native, limits, ingest = SlowIngest(100), soTimeoutMs = 30_000) { ws ->
                val writer = Thread { for (e in events) ws.text("""["EVENT",${e.toJson()}]""") }.apply { start() }
                val texts = ws.textsUntil { seen -> seen.count { it.startsWith("[\"OK\"") } == events.size }
                writer.join(5_000)
                val transport = if (native) "native" else "ktor"
                // 36M characters against a 16M-character budget: reads paused at least once, and were resumed, whatever the kernel buffered.
                assertTrue(InboundReads.pauses.get() > pauses, "$transport: reads never paused")
                assertTrue(InboundReads.resumes.get() > resumes, "$transport: reads never resumed")
                assertTrue(texts.count { it.startsWith("[\"OK\"") && ",true," in it } == events.size, "$transport: every EVENT answered")
            }
        }
    }
}
