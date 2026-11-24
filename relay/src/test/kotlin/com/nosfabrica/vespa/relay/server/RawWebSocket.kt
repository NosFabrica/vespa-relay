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
import com.nosfabrica.vespa.relay.server.config.defaultMaxFrameBytes
import com.nosfabrica.vespa.relay.server.config.relayLimitsFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.io.DataInputStream
import java.io.EOFException
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertTrue
import kotlin.test.fail

/** A frame the server sent: its opcode and payload; a close's code and reason are read off the payload. */
internal class ServerFrame(
    val opcode: Int,
    val payload: ByteArray,
) {
    val text: String get() = payload.toString(Charsets.UTF_8)
    val closeCode: Int get() = if (payload.size < 2) 1005 else ((payload[0].toInt() and 0xFF) shl 8) or (payload[1].toInt() and 0xFF)
    val closeReason: String get() = if (payload.size < 2) "" else payload.copyOfRange(2, payload.size).toString(Charsets.UTF_8)
}

/**
 * A websocket client spoken by hand over [socket], already upgraded: a client library would refuse
 * to send what most of these tests send.
 */
internal class RawWebSocket(
    val socket: Socket,
    val port: Int,
) {
    private val out: OutputStream = socket.getOutputStream()
    private val input = DataInputStream(socket.getInputStream())

    /** A masked frame whose header declares [declared] bytes; only [payload] follows it. Whole, whichever thread sends. */
    @Synchronized
    fun send(
        payload: ByteArray,
        opcode: Int = 0x1,
        fin: Boolean = true,
        declared: Long = payload.size.toLong(),
        rsv: Int = 0,
        masked: Boolean = true,
    ) {
        out.write((if (fin) 0x80 else 0) or (rsv shl 4) or opcode)
        val maskBit = if (masked) 0x80 else 0
        when {
            declared < 126 -> {
                out.write(maskBit or declared.toInt())
            }

            declared <= 0xFFFF -> {
                out.write(maskBit or 126)
                out.write((declared shr 8).toInt() and 0xFF)
                out.write(declared.toInt() and 0xFF)
            }

            else -> {
                out.write(maskBit or 127)
                for (shift in 56 downTo 0 step 8) out.write((declared shr shift).toInt() and 0xFF)
            }
        }
        if (masked) {
            val mask = byteArrayOf(0x37, 0x5A, 0x11, 0x7E)
            out.write(mask)
            out.write(ByteArray(payload.size) { (payload[it].toInt() xor mask[it % 4].toInt()).toByte() })
        } else {
            out.write(payload)
        }
        out.flush()
    }

    fun text(text: String) = send(text.toByteArray(Charsets.UTF_8))

    /** [payload] as one text message split into frames of at most [chunk] bytes. */
    fun fragmented(
        payload: ByteArray,
        chunk: Int,
    ) {
        val parts = (payload.indices step chunk).map { payload.copyOfRange(it, minOf(it + chunk, payload.size)) }
        parts.forEachIndexed { i, part -> send(part, opcode = if (i == 0) 0x1 else 0x0, fin = i == parts.lastIndex) }
    }

    /** The next frame from the server, or null when the connection ended (EOF or reset). */
    fun next(): ServerFrame? =
        try {
            val opcode = input.readUnsignedByte() and 0x0F
            val len =
                when (val short = input.readUnsignedByte() and 0x7F) {
                    126 -> input.readUnsignedShort().toLong()
                    127 -> input.readLong()
                    else -> short.toLong()
                }
            ServerFrame(opcode, ByteArray(len.toInt()).also { input.readFully(it) })
        } catch (_: EOFException) {
            null
        } catch (_: SocketException) {
            null
        }

    /** Frames up to and including the server's close; fails past [withinMs], or on EOF before a close. */
    fun untilClose(withinMs: Long = 15_000): List<ServerFrame> {
        val seen = mutableListOf<ServerFrame>()
        val deadline = System.nanoTime() + withinMs * 1_000_000
        while (true) {
            if (System.nanoTime() > deadline) fail("no close frame within $withinMs ms; got ${seen.size} frames")
            val frame =
                try {
                    next()
                } catch (_: SocketTimeoutException) {
                    fail("no close frame within the socket timeout; got ${seen.map { it.opcode }}")
                } ?: fail("the connection ended without a close frame; got ${seen.map { if (it.opcode == 1) it.text.take(80) else it.opcode }}")
            seen += frame
            if (frame.opcode == 0x8) return seen
        }
    }

    /** True once the server ends the connection; fails if it is still open after the socket timeout. */
    fun awaitEnd() {
        while (true) {
            try {
                next() ?: return
            } catch (_: SocketTimeoutException) {
                fail("the server kept the connection open")
            }
        }
    }

    /** Reads text frames until [done] holds over the ones seen; a close or EOF first fails. */
    fun textsUntil(done: (List<String>) -> Boolean): List<String> {
        val texts = mutableListOf<String>()
        while (!done(texts)) {
            val frame =
                try {
                    next()
                } catch (_: SocketTimeoutException) {
                    fail("timed out; got ${texts.map { it.take(120) }}")
                } ?: fail("the connection ended; got ${texts.map { it.take(120) }}")
            if (frame.opcode == 0x8) fail("closed ${frame.closeCode} ${frame.closeReason}; got ${texts.map { it.take(120) }}")
            if (frame.opcode == 0x1) texts += frame.text
        }
        return texts
    }

    /** The connection still answers: a REQ gets a frame on its sub id. */
    fun stillServes() {
        text("""["REQ","after",{"ids":["${"0".repeat(64)}"],"search":"include:spam"}]""")
        textsUntil { seen -> seen.any { "\"after\"" in it } }
    }
}

private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

/** A relay on an ephemeral port over an in-memory store, with one upgraded raw socket to it. */
internal fun <T> withRawRelay(
    native: Boolean,
    limits: RelayLimits = relayLimitsFromEnv(emptyMap()),
    maxFrameBytes: Int = defaultMaxFrameBytes(limits),
    ingest: EventIngest? = null,
    openMessageDeadlineMs: Long = OPEN_MESSAGE_DEADLINE_MS,
    keepalive: Keepalive = Keepalive(),
    soTimeoutMs: Int = 5_000,
    receiveBufferBytes: Int? = null,
    block: (RawWebSocket) -> T,
): T {
    val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl), relayUrl, limits = limits, ingest = ingest)
    val server =
        serveRelay(
            relay = relay,
            port = 0,
            nip11 = Nip11Info(),
            limits = limits,
            maxFrameBytes = maxFrameBytes,
            openMessageDeadlineMs = openMessageDeadlineMs,
            keepalive = keepalive,
            nativeWebSocket = native,
            wait = false,
        )
    return try {
        val port =
            runBlocking {
                server.engine
                    .resolvedConnectors()
                    .first()
                    .port
            }
        Socket().use { socket ->
            receiveBufferBytes?.let { socket.receiveBufferSize = it }
            socket.connect(InetSocketAddress("127.0.0.1", port), 5_000)
            socket.soTimeout = soTimeoutMs
            upgrade(socket, port)
            block(RawWebSocket(socket, port))
        }
    } finally {
        server.stop(0, 0)
        relay.close()
    }
}

/** Ktor's websocket, then the native one, each with a socket of its own. */
internal fun bothTransports(
    limits: RelayLimits = relayLimitsFromEnv(emptyMap()),
    maxFrameBytes: Int = defaultMaxFrameBytes(limits),
    block: (RawWebSocket, Boolean) -> Unit,
) {
    for (native in listOf(false, true)) withRawRelay(native, limits, maxFrameBytes) { block(it, native) }
}

internal fun upgrade(
    socket: Socket,
    port: Int,
) {
    socket.getOutputStream().apply {
        write(
            (
                "GET / HTTP/1.1\r\nHost: localhost:$port\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
            ).toByteArray(),
        )
        flush()
    }
    // Byte by byte, so nothing after the headers is consumed by a buffer.
    val input = socket.getInputStream()
    val head = StringBuilder()
    while (!head.endsWith("\r\n\r\n")) head.append(input.read().takeIf { it >= 0 }?.toChar() ?: fail("closed during the upgrade"))
    assertTrue(head.startsWith("HTTP/1.1 101"), "upgraded: $head")
}

/** Takes an event every [delayMs] and answers it as written: the store a publisher outruns. */
internal class SlowIngest(
    private val delayMs: Long,
) : EventIngest {
    val taken = AtomicInteger()

    override suspend fun submit(
        event: Event,
        skipVerify: Boolean,
        onComplete: (IEventStore.InsertOutcome) -> Unit,
    ) {
        delay(delayMs)
        taken.incrementAndGet()
        onComplete(IEventStore.InsertOutcome.Accepted)
    }

    override fun hasBacklog() = false

    override fun close() {}
}

/** Takes an event at once and answers it [delayMs] later from another thread, as IngestPipeline answers from a batch. */
internal class LateIngest(
    private val delayMs: Long,
) : EventIngest {
    private val timer = Executors.newSingleThreadScheduledExecutor()

    override suspend fun submit(
        event: Event,
        skipVerify: Boolean,
        onComplete: (IEventStore.InsertOutcome) -> Unit,
    ) {
        timer.schedule({ onComplete(IEventStore.InsertOutcome.Accepted) }, delayMs, TimeUnit.MILLISECONDS)
    }

    override fun hasBacklog() = false

    override fun close() = timer.shutdown()
}
