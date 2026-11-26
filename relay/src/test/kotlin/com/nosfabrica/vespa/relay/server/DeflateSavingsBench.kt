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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * What permessage-deflate saves a client's data plan: the bytes a relay sends one client through a session of
 * reads and a held live feed, counted on the TCP stream, with compression off and on over the same corpus.
 * Selected by `-DdeflateBench=<dir>`, a directory of `*.jsonl` files, one event a line (relay/tools/fetch-slice.mjs
 * or a NIP-FE read writes them). Prints a table and asserts only that both arms received the same events.
 */
class DeflateSavingsBench {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /** One step of the session: a read answered to its EOSE, or the live feed's share of [LIVE_EVENTS]. */
    private class Step(
        val name: String,
        val filter: String,
    )

    private val reads =
        listOf(
            Step("home feed (notes, reposts)", """{"kinds":[1,6],"limit":500}"""),
            Step("notifications (reactions, zaps)", """{"kinds":[7,9735],"limit":500}"""),
            Step("profiles", """{"kinds":[0],"limit":500}"""),
            Step("follow and relay lists", """{"kinds":[3,10002],"limit":300}"""),
        )
    private val live = Step("live feed (notes, reactions)", """{"kinds":[1,7],"limit":0}""")

    /** What one arm measured per step: bytes on the wire downstream, and the EVENT frames that carried them. */
    private class Measured(
        val bytes: Long,
        val events: Int,
    )

    @Test
    fun `what compression saves a client`() {
        val dir = System.getProperty("deflateBench") ?: return println("DEFLATE-BENCH skipped — run with -DdeflateBench=<dir of *.jsonl>")
        val events =
            File(dir)
                .listFiles { f -> f.name.endsWith(".jsonl") }
                .orEmpty()
                .sortedBy { it.name }
                .flatMap { file -> file.readLines().filter { it.isNotBlank() }.map { Event.fromJson(it) } }
                .distinctBy { it.id }
        if (events.isEmpty()) fail("no events in $dir")
        // Held back from the store and published once the live feed is open: notes and reactions, not future-dated.
        val now = System.currentTimeMillis() / 1000
        val held = events.filter { (it.kind == 1 || it.kind == 7) && it.createdAt <= now }.shuffled(kotlin.random.Random(1)).take(LIVE_EVENTS)
        val stored = events - held.toSet()

        val plain = session(compression = false, stored, held)
        val deflated = session(compression = true, stored, held)
        val steps = reads + live
        println("DEFLATE-BENCH ${stored.size} stored events, ${held.size} published live, over one client socket")
        println("DEFLATE-BENCH %-32s %7s %12s %12s %7s".format("step", "events", "plain", "deflated", "saved"))
        for ((i, step) in steps.withIndex()) {
            assertEquals(plain[i].events, deflated[i].events, "both arms received the same ${step.name}")
            println("DEFLATE-BENCH %-32s %7d %12s %12s %6.1f%%".format(step.name, plain[i].events, kib(plain[i].bytes), kib(deflated[i].bytes), saved(plain[i].bytes, deflated[i].bytes)))
        }
        val plainTotal = plain.sumOf { it.bytes }
        val deflatedTotal = deflated.sumOf { it.bytes }
        println("DEFLATE-BENCH %-32s %7d %12s %12s %6.1f%%".format("whole session", plain.sumOf { it.events }, kib(plainTotal), kib(deflatedTotal), saved(plainTotal, deflatedTotal)))
        println("DEFLATE-BENCH per 1,000 events: %s plain, %s deflated".format(kib(plainTotal * 1000 / plain.sumOf { it.events }), kib(deflatedTotal * 1000 / plain.sumOf { it.events })))
    }

    /** One client's session against a relay serving [stored], with [held] published while its live feed is open. */
    private fun session(
        compression: Boolean,
        stored: List<Event>,
        held: List<Event>,
    ): List<Measured> {
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
        runBlocking { stored.chunked(1_000).forEach { store.batchInsert(it) } }
        val relay = NostrRelayServer(store, relayUrl)
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), websocketCompression = compression, wait = false)
        val http = OkHttpClient()
        try {
            val port =
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                }
            CountingProxy(port).use { proxy ->
                val client = Client(http, proxy.port)
                check((client.agreed != null) == compression) { "compression=$compression, agreed ${client.agreed}" }
                val measured = mutableListOf<Measured>()
                for ((i, step) in reads.withIndex()) {
                    val before = proxy.down.get()
                    client.send("""["REQ","r$i",${step.filter.withLens()}]""")
                    val count = client.eventsUntilEose("r$i")
                    measured += Measured(proxy.settled() - before, count)
                }
                client.send("""["REQ","live",${live.filter.withLens()}]""")
                client.eventsUntilEose("live")
                val before = proxy.settled()
                val publisher = Client(http, port)
                held.forEach { publisher.send("""["EVENT",${it.toJson()}]""") }
                val count = client.liveEvents("live", held.size)
                measured += Measured(proxy.settled() - before, count)
                publisher.close()
                client.close()
                return measured
            }
        } finally {
            http.dispatcher.executorService.shutdown()
            server.stop(0, 0)
            relay.close()
        }
    }

    /** An anonymous read waives the ranking lens the relay asks of one (LensRequiredPolicy). */
    private fun String.withLens() = dropLast(1) + ""","search":"include:spam"}"""

    /** An OkHttp socket, which offers permessage-deflate as browsers do. */
    private class Client(
        http: OkHttpClient,
        port: Int,
    ) : WebSocketListener() {
        private val opened = CompletableFuture<Response>()
        private val frames = LinkedBlockingQueue<String>()
        private val socket = http.newWebSocket(Request.Builder().url("ws://127.0.0.1:$port/").build(), this)
        val agreed: String? = opened.get(10, TimeUnit.SECONDS).header("Sec-WebSocket-Extensions")

        fun send(text: String) = check(socket.send(text))

        fun close() = socket.close(1000, null)

        /** EVENT frames for [sub] until its EOSE; anything that ends it otherwise fails the bench. */
        fun eventsUntilEose(sub: String): Int {
            var events = 0
            while (true) {
                val frame = next()
                when {
                    frame.startsWith("""["EVENT","$sub",""") -> events++
                    frame.startsWith("""["EOSE","$sub"""") -> return events
                    frame.startsWith("""["CLOSED","$sub"""") -> fail(frame)
                }
            }
        }

        /** Live EVENT frames for [sub] until [expected] arrived or the socket has been quiet for 10s. */
        fun liveEvents(
            sub: String,
            expected: Int,
        ): Int {
            var events = 0
            while (events < expected) {
                val frame = frames.poll(10, TimeUnit.SECONDS) ?: break
                if (frame.startsWith("""["EVENT","$sub",""")) events++
            }
            return events
        }

        private fun next(): String = frames.poll(60, TimeUnit.SECONDS) ?: fail("no frame within 60s")

        override fun onOpen(
            webSocket: WebSocket,
            response: Response,
        ) {
            opened.complete(response)
        }

        override fun onMessage(
            webSocket: WebSocket,
            text: String,
        ) {
            frames.add(text)
        }

        override fun onFailure(
            webSocket: WebSocket,
            t: Throwable,
            response: Response?,
        ) {
            opened.completeExceptionally(t)
            frames.add("""["CLOSED","*","$t"]""")
        }
    }

    /** A TCP relay between a client and the relay's port that counts the bytes each way, handshake and framing included. */
    private class CountingProxy(
        private val target: Int,
    ) : AutoCloseable {
        private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
        val port: Int = server.localPort
        val down = AtomicLong()
        val up = AtomicLong()

        init {
            thread(isDaemon = true, name = "proxy-accept") {
                while (!server.isClosed) {
                    val client = runCatching { server.accept() }.getOrNull() ?: break
                    val upstream = Socket(InetAddress.getLoopbackAddress(), target)
                    pump(client.getInputStream(), upstream.getOutputStream(), up, client, upstream)
                    pump(upstream.getInputStream(), client.getOutputStream(), down, client, upstream)
                }
            }
        }

        /** [down] once it has stopped moving: a frame read by the client was counted before it, but its neighbours may not be. */
        fun settled(): Long {
            var last = -1L
            while (true) {
                val now = down.get()
                if (now == last) return now
                last = now
                Thread.sleep(200)
            }
        }

        private fun pump(
            input: InputStream,
            output: OutputStream,
            counter: AtomicLong,
            vararg sockets: Socket,
        ) = thread(isDaemon = true, name = "proxy-pump") {
            val buffer = ByteArray(64 * 1024)
            try {
                while (true) {
                    val read = input.read(buffer)
                    if (read < 0) break
                    counter.addAndGet(read.toLong())
                    output.write(buffer, 0, read)
                    output.flush()
                }
            } catch (_: IOException) {
            } finally {
                sockets.forEach { runCatching { it.close() } }
            }
        }

        override fun close() = server.close()
    }

    private fun kib(bytes: Long) = "%,.1f KiB".format(bytes / 1024.0)

    private fun saved(
        plain: Long,
        deflated: Long,
    ) = 100.0 * (plain - deflated) / plain

    private companion object {
        const val LIVE_EVENTS = 300
    }
}
