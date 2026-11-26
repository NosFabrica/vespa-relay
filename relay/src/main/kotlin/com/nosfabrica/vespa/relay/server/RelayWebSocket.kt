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

import com.nosfabrica.vespa.relay.server.config.defaultRelayLimits
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.server.FanoutExecutor
import com.vitorpamplona.quartz.nip01Core.relay.server.SessionSink
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import io.ktor.server.routing.Route
import io.ktor.server.websocket.webSocketRaw
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.FrameTooBigException
import io.ktor.websocket.ProtocolViolationException
import io.ktor.websocket.WebSocketSession
import io.ktor.websocket.close
import io.ktor.websocket.readReason
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/** Outbound frames a connection may queue on top of one whole stored page before it is treated as a slow consumer. */
internal const val MAX_OUTGOING_BUFFER = 8192

/**
 * One connection's frame bound: the largest stored page [limits] let one REQ ask for, plus [MAX_OUTGOING_BUFFER].
 * The store builds a page whole before a row is sent, so a page the relay agreed to serve never trips it.
 */
internal fun outboundFrameCap(limits: RelayLimits?): Int {
    val filters = (limits?.maxFilters ?: defaultRelayLimits().maxFilters ?: 1).toLong()
    val perFilter = (limits?.maxLimit ?: defaultRelayLimits().maxLimit ?: 0).toLong()
    return (filters * perFilter + MAX_OUTGOING_BUFFER).coerceAtMost(MAX_OUTGOING_FRAMES.toLong()).toInt()
}

/** The most frames any connection may queue, whatever the limits allow one REQ: raised limits do not unbound it. */
internal const val MAX_OUTGOING_FRAMES = 1 shl 20

/**
 * Characters queued to EVERY connection of the process, against one budget. No one page the relay agreed to
 * serve is cut for its size ([outboundFrameCap]), but many slow readers each holding a large page would
 * together exhaust the heap: while the process holds more than [limitChars] queued, a connection holding more
 * than [MAX_OUTGOING_CHARS] of it is a slow consumer ([QueuedChars.overBudget]).
 */
internal class OutboundBudget(
    val limitChars: Long,
) {
    private val queued = AtomicLong()

    fun add(chars: Long) {
        queued.addAndGet(chars)
    }

    fun queued(): Long = queued.get()

    fun exhausted(): Boolean = queued.get() > limitChars
}

/** The process's [OutboundBudget]: an eighth of the heap in characters, a quarter of it in bytes at worst (UTF-16). */
internal val PROCESS_OUTBOUND = OutboundBudget(Runtime.getRuntime().maxMemory() / 8)

/**
 * One connection's queued characters, counted into [budget] as well. Locked, not atomic, so that [release] at
 * close hands back exactly what the connection still held while a writer may be subtracting: a leak here would
 * count against every connection for the life of the process.
 */
internal class QueuedChars(
    private val budget: OutboundBudget,
) {
    private var held = 0L
    private var released = false

    @Synchronized
    fun add(chars: Long) {
        if (released) return
        held += chars
        budget.add(chars)
    }

    @Synchronized
    fun held(): Long = held

    /** Over [own] while the process is over its [budget]: the share of the shortage this connection must give back. */
    fun overBudget(own: Long): Boolean = held() > own && budget.exhausted()

    /** At close: whatever is still queued leaves the process budget, once. */
    @Synchronized
    fun release() {
        if (released) return
        released = true
        budget.add(-held)
        held = 0
    }
}

/** Outbound characters a connection may hold queued without question; past it, only a stalled writer is a slow consumer. */
internal const val MAX_OUTGOING_CHARS = 16L * 1024 * 1024

/** How long the writer may wait on one frame before a connection over [MAX_OUTGOING_CHARS] is a slow consumer. */
internal const val WRITER_STALL_MS = 30_000L

/** How often a connection's writer is checked for a stall. */
private const val STALL_CHECK_MS = 1_000L

/** How long a slow consumer's close frame gets before the session is torn down anyway. */
internal const val CLOSE_GRACE_MS = 5_000L

/** The ping that makes a peer that walked off NAT detectable, and the silence after which its session is reaped. */
internal const val PING_PERIOD_MS = 30_000L
internal const val SILENCE_TIMEOUT_MS = 60_000L

/** How often a connection is pinged, and how long it may say nothing before it is reaped; tests shorten both. */
data class Keepalive(
    val pingPeriodMs: Long = PING_PERIOD_MS,
    val silenceTimeoutMs: Long = SILENCE_TIMEOUT_MS,
)

/**
 * [Frame.Text]'s text as Ktor's `readText` reads it, malformed bytes replaced, without its copy
 * into a packet and its growing builder: one decode into an exact-size string.
 */
internal fun Frame.Text.utf8Text(): String {
    require(fin) { "Text could be only extracted from non-fragmented frame" }
    return String(data, Charsets.UTF_8)
}

/** Encoded by the JDK's String.getBytes; Ktor's Frame.Text(String) runs a CharsetEncoder a character at a time. */
internal fun textFrame(text: String): Frame.Text = Frame.Text(true, text.toByteArray(Charsets.UTF_8))

/** Commands queued from one connection before its reads pause, and the depth at which they resume; either transport. */
internal const val PAUSE_READS_AT = 64
internal const val RESUME_READS_AT = 16

/** The same bound in characters, since one message may be megabytes: 64 messages at the engine's default length. */
internal const val PAUSE_READS_AT_CHARS = 16L * 1024 * 1024
internal const val RESUME_READS_AT_CHARS = PAUSE_READS_AT_CHARS / 4

/** Every time a connection's reads paused for its own backlog, and resumed; counted for tests and diagnosis. */
internal object InboundReads {
    val pauses = AtomicLong()
    val resumes = AtomicLong()
}

/**
 * The relay websocket on `/` on Ktor's transport, raw: every frame is read as it comes, so a pong is
 * heard however far the connection's commands are behind, and what a backlog holds back is the client's
 * next commands, through an inbox with the native transport's budgets. Ktor's own pinger is off, since
 * it hears pongs only behind the commands; [keepalive] is kept here instead. Messages are bounded by the
 * plugin's `maxFrameSize`, whole or fragmented. Live events are delivered on the connection's [fanout] thread.
 */
fun Route.nostrRelay(
    server: NostrRelayServer,
    keepalive: Keepalive = Keepalive(),
    fanout: KtorFanout,
) {
    webSocketRaw("/") {
        // One writer drains a bounded queue; a slow consumer is disconnected rather than having frames
        // dropped, which would break NIP-01.
        val outQueue = OutboundQueue(maxFrames = outboundFrameCap(server.limits))
        val writer = launch { outQueue.drain { text -> outgoing.send(textFrame(text)) } }
        val disconnecting = AtomicBoolean()

        fun disconnectSlowConsumer() {
            outQueue.close()
            if (!disconnecting.compareAndSet(false, true)) return
            launch {
                // The close frame queues behind the congestion that tripped this; cancelling the
                // session closes the socket regardless.
                runCatching {
                    withTimeoutOrNull(CLOSE_GRACE_MS) {
                        close(CloseReason(CloseReason.Codes.TRY_AGAIN_LATER, "slow consumer: outbound buffer full"))
                    }
                }
                this@webSocketRaw.cancel()
            }
        }
        // A client that stops reading may send nothing more, so the stall is also looked for between offers.
        val watchdog =
            launch {
                while (isActive && !outQueue.slowConsumer()) delay(STALL_CHECK_MS)
                if (isActive) disconnectSlowConsumer()
            }
        val inbox = KtorInbox()
        val reader =
            launch {
                try {
                    readFrames(inbox, maxFrameSize)
                } finally {
                    inbox.close()
                }
            }
        val pinger =
            launch {
                while (isActive) {
                    delay(keepalive.pingPeriodMs)
                    if (inbox.silentFor() >= keepalive.silenceTimeoutMs) {
                        this@webSocketRaw.cancel()
                        break
                    }
                    outgoing.send(Frame.Ping(ByteArray(0)))
                }
            }
        val send = { text: String -> if (!outQueue.offer(text)) disconnectSlowConsumer() }
        try {
            server.serve(
                sink = KtorSink(send, fanout.next()),
                incoming = { session -> inbox.forEach { session.receive(it) } },
            )
        } finally {
            outQueue.close()
            watchdog.cancel()
            pinger.cancel()
            reader.cancel()
            writer.cancel()
        }
    }
}

/** Every frame to [send] as wire JSON, live events from [liveExecutor] rather than from the thread that accepted them. */
private class KtorSink(
    private val send: (String) -> Unit,
    override val liveExecutor: FanoutExecutor,
) : SessionSink {
    override fun message(message: Message) = send(message.toJson())

    override fun raw(json: String) = send(json)
}

/**
 * Reads this connection's frames into [inbox] until the client closes: pings answered, pongs heard,
 * data frames joined into messages of at most [maxBytes] (a binary one read and dropped). A close is
 * answered with its own reason; a message past [maxBytes] closes with 1009.
 */
private suspend fun WebSocketSession.readFrames(
    inbox: KtorInbox,
    maxBytes: Long,
) {
    var parts: MutableList<ByteArray>? = null
    var size = 0L
    var text = false
    try {
        for (frame in incoming) {
            inbox.heard()
            when (frame) {
                is Frame.Ping -> {
                    outgoing.send(Frame.Pong(frame.data))
                }

                is Frame.Pong -> {
                    Unit
                }

                is Frame.Close -> {
                    outgoing.send(Frame.Close(frame.readReason() ?: CloseReason(CloseReason.Codes.NORMAL, "")))
                    return
                }

                else -> {
                    if (parts == null) {
                        parts = mutableListOf()
                        size = 0
                        text = frame is Frame.Text
                    }
                    size += frame.data.size
                    if (size > maxBytes) {
                        outgoing.send(Frame.Close(CloseReason(CloseReason.Codes.TOO_BIG, "message over $maxBytes bytes")))
                        return
                    }
                    if (text) parts.add(frame.data)
                    if (frame.fin) {
                        val joined = parts.singleOrNull() ?: ByteArray(size.toInt()).also { all -> parts.fold(0) { at, p -> p.copyInto(all, at).let { at + p.size } } }
                        parts = null
                        if (text) inbox.put(String(joined, Charsets.UTF_8))
                    }
                }
            }
        }
    } catch (_: ClosedReceiveChannelException) {
    } catch (_: FrameTooBigException) {
    } catch (_: ProtocolViolationException) {
        // Ktor's reader has sent the close for both.
    }
}

/**
 * One Ktor connection's commands, read and not yet run, under the native transport's budgets: [put]
 * suspends the reader while the inbox is over them, which is when the connection counts as heard from,
 * since what it sends waits on this relay.
 */
private class KtorInbox {
    private val commands = Channel<String>(Channel.UNLIMITED)
    private val depth = AtomicInteger()
    private val chars = AtomicLong()
    private val resumed = Channel<Unit>(Channel.CONFLATED)

    @Volatile
    private var paused = false

    @Volatile
    private var lastHeard = System.nanoTime()

    fun heard() {
        lastHeard = System.nanoTime()
    }

    fun silentFor(): Long = if (paused) 0 else (System.nanoTime() - lastHeard) / 1_000_000

    suspend fun put(command: String) {
        chars.addAndGet(command.length.toLong())
        depth.incrementAndGet()
        commands.send(command)
        if (depth.get() < PAUSE_READS_AT && chars.get() < PAUSE_READS_AT_CHARS) return
        paused = true
        InboundReads.pauses.incrementAndGet()
        while (depth.get() > RESUME_READS_AT || chars.get() > RESUME_READS_AT_CHARS) resumed.receive()
        paused = false
        heard()
        InboundReads.resumes.incrementAndGet()
    }

    suspend fun forEach(run: suspend (String) -> Unit) {
        for (command in commands) {
            chars.addAndGet(-command.length.toLong())
            depth.decrementAndGet()
            resumed.trySend(Unit)
            run(command)
        }
    }

    fun close() {
        commands.close()
        resumed.close()
    }
}

/**
 * One connection's outbound frames. Past [maxFrames] (see [outboundFrameCap]), or past [maxChars] while the writer has waited
 * on one frame for [stallMs], the client is a slow consumer: characters alone never close one still reading, unless the
 * process as a whole is over its [OutboundBudget].
 */
internal class OutboundQueue(
    maxFrames: Int = MAX_OUTGOING_BUFFER,
    private val maxChars: Long = MAX_OUTGOING_CHARS,
    private val stallMs: Long = WRITER_STALL_MS,
    private val nowMs: () -> Long = { System.nanoTime() / 1_000_000 },
    budget: OutboundBudget = PROCESS_OUTBOUND,
) {
    private val frames = Channel<String>(maxFrames)
    private val queuedChars = QueuedChars(budget)

    /** When the writer began waiting on the frame it holds, or [IDLE] while it holds none. */
    private val writingSince = AtomicLong(IDLE)

    @Volatile
    private var closed = false

    /** False only when [text] makes an open queue a slow consumer; a closed queue drops it, its connection is ending. */
    fun offer(text: String): Boolean {
        if (closed) return true
        val chars = text.length.toLong()
        queuedChars.add(chars)
        if (slowConsumer() || !frames.trySend(text).isSuccess) {
            queuedChars.add(-chars)
            return closed
        }
        return true
    }

    /** [stalled], or holding more than its character budget while the process is over its own ([OutboundBudget]). */
    fun slowConsumer(): Boolean = !closed && (stalled() || queuedChars.overBudget(maxChars))

    /** True when the queue holds more than its character budget and the writer has made no progress for the stall window. */
    fun stalled(): Boolean {
        if (closed || queuedChars.held() <= maxChars) return false
        val since = writingSince.get()
        return since != IDLE && nowMs() - since >= stallMs
    }

    /** Hands each frame to [write] in order until the queue is closed. */
    suspend fun drain(write: suspend (String) -> Unit) {
        for (text in frames) {
            writingSince.set(nowMs())
            write(text)
            writingSince.set(IDLE)
            queuedChars.add(-text.length.toLong())
        }
    }

    fun close() {
        closed = true
        frames.close()
        queuedChars.release()
    }

    private companion object {
        const val IDLE = Long.MIN_VALUE
    }
}
