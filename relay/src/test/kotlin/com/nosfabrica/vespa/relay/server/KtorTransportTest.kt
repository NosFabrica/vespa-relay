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

import com.nosfabrica.vespa.relay.server.config.messageBytes
import com.nosfabrica.vespa.relay.server.config.relayLimitsFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlin.test.Test
import kotlin.test.assertEquals

/** The Ktor transport (NATIVE_WEBSOCKET=false): what it bounds, and that its bounds never starve a client's pongs. */
class KtorTransportTest {
    private val signer = NostrSignerSync()

    @Test
    fun `a publisher whose commands back up still has its pongs heard`() {
        val events = (0 until 40).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "$it".padEnd(5_000, 'k')) }
        for (native in listOf(false, true)) {
            // Twelve seconds of commands against a three-second silence timeout, the client answering every ping.
            withRawRelay(native, ingest = SlowIngest(300), keepalive = Keepalive(pingPeriodMs = 1_000, silenceTimeoutMs = 3_000), soTimeoutMs = 30_000) { ws ->
                for (e in events) ws.text("""["EVENT",${e.toJson()}]""")
                val texts = ws.textsUntil(answerPings = true) { seen -> seen.count { it.startsWith("[\"OK\"") } == events.size }
                assertEquals(events.size, texts.count { it.startsWith("[\"OK\"") && ",true," in it }, if (native) "native" else "ktor")
            }
        }
    }

    @Test
    fun `Ktor refuses a frame past the engine's length from its header, before holding it`() {
        val limits = relayLimitsFromEnv(emptyMap())
        // Ktor's reader allocates a frame's declared length when its header arrives: past the engine's length it must refuse instead.
        withRawRelay(native = false, limits = limits) { ws ->
            ws.send("[\"EVENT\"".toByteArray(), declared = messageBytes(limits) + 1)
            assertEquals(1009, ws.untilClose(withinMs = 5_000).last().closeCode)
        }
    }

    @Test
    fun `Ktor closes on a message past the engine's length, whole or fragmented`() {
        val big = """["EVENT",{"id":"${"6".repeat(64)}","content":"${"m".repeat(900_000)}"}]""".toByteArray()
        withRawRelay(native = false) { ws ->
            ws.send(big)
            assertEquals(1009, ws.untilClose(withinMs = 5_000).last().closeCode)
        }
        withRawRelay(native = false) { ws ->
            runCatching { ws.fragmented(big, chunk = 100_000) }
            assertEquals(1009, ws.untilClose(withinMs = 5_000).last().closeCode)
        }
    }
}
