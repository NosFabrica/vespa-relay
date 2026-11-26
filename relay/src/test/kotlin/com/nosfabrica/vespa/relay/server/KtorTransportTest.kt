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
import com.vitorpamplona.quartz.nip01Core.relay.server.FanoutExecutor
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import java.net.SocketTimeoutException
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

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
    fun `a Ktor connection's live events are delivered by its fan-out thread, not by the ingest`() {
        // The connection's one executor runs nothing until the test lets it.
        val released = CountDownLatch(1)
        val thread = FanoutThread("held-fanout")
        val held =
            object : FanoutExecutor {
                override fun execute(task: () -> Unit) =
                    thread.execute {
                        released.await()
                        task()
                    }
            }
        val note = signer.sign<Event>(System.currentTimeMillis() / 1000, 1, emptyArray(), "a live note")
        withRawRelay(native = false, ktorFanout = KtorFanout(listOf(held)), soTimeoutMs = 2_000) { ws ->
            ws.text("""["REQ","live",{"kinds":[1],"search":"include:spam"}]""")
            ws.textsUntil { seen -> seen.any { it.startsWith("""["EOSE","live"]""") } }
            ws.text("""["EVENT",${note.toJson()}]""")
            ws.textsUntil { seen -> seen.any { it.startsWith("""["OK","${note.id}",true""") } }
            // Inline, the ingest would have written the frame before the OK's batch moved on.
            assertFailsWith<SocketTimeoutException>("the live frame went out before its fan-out thread ran") { ws.next() }
            released.countDown()
            val live = ws.textsUntil { seen -> seen.any { it.startsWith("""["EVENT","live",""") } }
            assertEquals(1, live.count { note.id in it })
        }
        thread.close()
    }

    @Test
    fun `connections share the fan-out's executors, so quartz keeps one partition per thread`() {
        val fanout = KtorFanout.perCore()
        val given = List(1_000) { fanout.next() }
        assertEquals(Runtime.getRuntime().availableProcessors(), given.toSet().size)
        fanout.close()
    }

    @Test
    fun `a fan-out thread runs its tasks in order, off the caller, and queues one it is handed from itself`() {
        val thread = FanoutThread("ordered-fanout")
        val ran = Collections.synchronizedList(mutableListOf<String>())
        val done = CountDownLatch(1)
        val caller = Thread.currentThread()
        thread.execute {
            assertTrue(Thread.currentThread() !== caller)
            thread.execute {
                ran += "queued from itself"
                done.countDown()
            }
            ran += "first"
        }
        thread.execute { ran += "second" }
        assertTrue(done.await(5, TimeUnit.SECONDS))
        assertEquals(listOf("first", "second", "queued from itself"), ran.toList())
        thread.close()
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
        // Ktor refuses from a header, so the rest of a send may meet a closed socket.
        withRawRelay(native = false) { ws ->
            runCatching { ws.send(big) }
            assertEquals(1009, ws.untilClose(withinMs = 5_000).last().closeCode)
        }
        withRawRelay(native = false) { ws ->
            runCatching { ws.fragmented(big, chunk = 100_000) }
            assertEquals(1009, ws.untilClose(withinMs = 5_000).last().closeCode)
        }
    }
}
