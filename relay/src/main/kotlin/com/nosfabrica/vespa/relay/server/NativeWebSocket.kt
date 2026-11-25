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

import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.OkMessage
import com.vitorpamplona.quartz.nip01Core.relay.server.FanoutExecutor
import com.vitorpamplona.quartz.nip01Core.relay.server.LiveFrameSink
import com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession
import com.vitorpamplona.quartz.nip01Core.relay.server.SessionSink
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelPipeline
import io.netty.channel.EventLoop
import io.netty.channel.socket.DuplexChannel
import io.netty.handler.codec.http.DefaultFullHttpRequest
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.EmptyHttpHeaders
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.QueryStringDecoder
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus
import io.netty.handler.codec.http.websocketx.WebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory
import io.netty.util.ReferenceCountUtil
import io.netty.util.concurrent.ScheduledFuture
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/** Ktor's HTTP/1.1 handlers, in the order its channel initializer adds them; an upgraded socket keeps none. */
private val KTOR_HTTP1_HANDLERS = listOf("readTimeout", "continue", "timeout", "http1")
private const val KTOR_HTTP1 = "http1"

/** Where the handshake puts its frame decoder, which [InboundFrameDecoder] replaces. */
private const val WS_DECODER = "wsdecoder"

/** Live bodies up to this size are copied into a socket's batch; larger ones are written as a shared duplicate. */
private const val LIVE_COPY_MAX_BYTES = 4_096

/** The keepalive tick, and the ping period in ticks; the slow-consumer and silence rules are the Ktor path's (RelayWebSocket.kt). */
private const val TICK_MS = 1_000L

/** The least a closing connection waits for answers to the commands it already read. */
private const val ANSWER_SETTLE_MS = 1_000L

/** How long a message may take to arrive while its bytes are held. */
internal const val OPEN_MESSAGE_DEADLINE_MS = 120_000L

private val inboundScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

/**
 * Serves the relay's websocket on Netty directly, ahead of Ktor's HTTP/1.1 handler: an upgrade on `/` is
 * taken here and every other request goes on to Ktor. HTTP/2 connections are left to Ktor entirely. With
 * [compression], a client that offers permessage-deflate (RFC 7692) is answered compressed.
 */
internal fun ChannelPipeline.installNativeWebSocket(
    relay: NostrRelayServer,
    inbound: NativeInbound,
    onionLocation: () -> String?,
    compression: Boolean = true,
) {
    if (get(KTOR_HTTP1) != null) addBefore(KTOR_HTTP1, "nostrUpgrade", NativeWebSocketUpgrade(relay, inbound, onionLocation, compression))
}

/**
 * How the native transport reads: [frames] bounds a message, [maxMessageLength] is the engine's, which a
 * streamed message is padded past so quartz refuses it, and [openMessageDeadlineMs] bounds a held one.
 */
internal class NativeInbound(
    val frames: InboundLimits,
    val maxMessageLength: Int?,
    val openMessageDeadlineMs: Long,
    val keepalive: Keepalive = Keepalive(),
) {
    /** Spaces past the engine's length, built once: a streamed message's head plus these is one string to build. */
    val padding: String by lazy { " ".repeat((maxMessageLength ?: 0) + 1) }
}

private class NativeWebSocketUpgrade(
    private val relay: NostrRelayServer,
    private val inbound: NativeInbound,
    private val onionLocation: () -> String?,
    private val compression: Boolean,
) : ChannelInboundHandlerAdapter() {
    override fun channelRead(
        ctx: ChannelHandlerContext,
        msg: Any,
    ) {
        if (msg !is HttpRequest || !isUpgrade(msg)) {
            ctx.fireChannelRead(msg)
            return
        }
        // Built before anything is released: a GET upgrade carries no body, and the handshake wants a whole request.
        val request = DefaultFullHttpRequest(msg.protocolVersion(), msg.method(), msg.uri(), Unpooled.EMPTY_BUFFER, msg.headers(), EmptyHttpHeaders.INSTANCE)
        ReferenceCountUtil.release(msg)
        val host = msg.headers()[HttpHeaderNames.HOST].orEmpty()
        val deflate = if (compression) negotiateDeflate(msg.headers().getAll(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS)) else null
        // RSV bits are a protocol error on a socket that agreed no extension, so the decoder allows them only here.
        val handshaker =
            WebSocketServerHandshakerFactory("ws://$host/", null, deflate != null).newHandshaker(request)
                ?: return WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel()).let { }
        // Every check the handshake makes is behind us before the pipeline changes or a session connects.
        val pipeline = ctx.pipeline()
        for (name in KTOR_HTTP1_HANDLERS) if (pipeline.get(name) != null) pipeline.remove(name)
        val decoder = InboundFrameDecoder(inbound.frames, if (deflate != null) EventLoopSessions.of(ctx.channel().eventLoop()).deflate.inflaters else null)
        pipeline.replace(this, "nostrSocket", NativeWebSocketSession(relay, handshaker, decoder, inbound, deflate))
        val headers = DefaultHttpHeaders()
        deflate?.let { headers.set(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS, it.response) }
        if (!host.substringBefore(':').endsWith(".onion", ignoreCase = true)) onionLocation()?.let { headers.set("Onion-Location", it) }
        handshaker.handshake(ctx.channel(), request, headers, ctx.channel().newPromise()).addListener { if (!it.isSuccess) ctx.close() }
        // The handshake installs Netty's decoder before it returns, on this loop, so no frame has been read by it.
        if (pipeline.get(WS_DECODER) != null) pipeline.replace(WS_DECODER, WS_DECODER, decoder)
    }

    /** A whole RFC 6455 upgrade on `/`; anything less is Ktor's to answer, as it was before this handler. */
    private fun isUpgrade(request: HttpRequest): Boolean {
        val headers = request.headers()
        return request.method() == HttpMethod.GET &&
            headers.containsValue(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true) &&
            headers.containsValue(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE, true) &&
            !headers[HttpHeaderNames.SEC_WEBSOCKET_KEY].isNullOrBlank() &&
            QueryStringDecoder(request.uri()).path() == "/"
    }
}

/**
 * The connections served by one event loop. Those with frames waiting are drained by one task on the loop however
 * many a fan-out touched, and one timer a second ticks them all, so neither costs a task per connection.
 * [sessions] is the loop's own: touched only on the loop thread.
 */
private class EventLoopSessions(
    private val loop: EventLoop,
) : FanoutExecutor {
    private val dirty = ConcurrentLinkedQueue<NativeWebSocketSession>()
    private val scheduled = AtomicBoolean()
    private val drainTask = Runnable { drain() }
    private val sessions = LinkedHashSet<NativeWebSocketSession>()
    private var ticker: ScheduledFuture<*>? = null

    fun join(session: NativeWebSocketSession) {
        sessions.add(session)
        if (ticker == null) ticker = loop.scheduleAtFixedRate({ tick() }, TICK_MS, TICK_MS, TimeUnit.MILLISECONDS)
    }

    fun leave(session: NativeWebSocketSession) {
        sessions.remove(session)
    }

    fun mark(session: NativeWebSocketSession) {
        dirty.add(session)
        if (scheduled.compareAndSet(false, true)) loop.execute(drainTask)
    }

    private fun drain() {
        scheduled.set(false)
        while (true) dirty.poll()?.let { isolated(it) { drain() } } ?: break
    }

    /** Sessions written to since the last flush; this loop's own. */
    private val written = ArrayList<NativeWebSocketSession>()
    private var flushQueued = false
    private val flushTask = Runnable { flushWritten() }

    /** Quartz's fan-out for this loop's subscriptions, run here. */
    override fun execute(task: () -> Unit) = loop.execute(task)

    /**
     * Flushes [session] from a task queued behind the work already on the loop: on an idle loop that runs next, and
     * under load it waits out the fan-outs queued ahead of it, so a socket those events reach takes them in one write.
     */
    fun wrote(session: NativeWebSocketSession) {
        written.add(session)
        if (!flushQueued) {
            flushQueued = true
            loop.execute(flushTask)
        }
    }

    private fun flushWritten() {
        flushQueued = false
        for (session in written) isolated(session) { flushWritten() }
        written.clear()
    }

    private var lastBody: String? = null
    private var lastBodyBytes: ByteBuf? = null

    /**
     * [body] and the frame's closing `]` as bytes, encoded once per event however many of this loop's sockets
     * it reaches: fan-out hands every subscription the same string, so identity finds it. The loop's own buffer,
     * valid until the next event: copy it or take a retained duplicate, never keep it.
     */
    fun sharedBody(
        body: String,
        alloc: ByteBufAllocator,
    ): ByteBuf {
        if (body !== lastBody) {
            lastBodyBytes?.release()
            val bytes = alloc.directBuffer(ByteBufUtil.utf8Bytes(body) + 1)
            ByteBufUtil.writeUtf8(bytes, body)
            bytes.writeByte(']'.code)
            lastBody = body
            lastBodyBytes = bytes
        }
        return lastBodyBytes!!
    }

    /** This loop's compression, made the first time a socket on it agrees to permessage-deflate. */
    val deflate by lazy(LazyThreadSafetyMode.NONE) { LoopDeflate() }

    private var lastDeflatedBody: String? = null
    private var lastDeflatedBytes: ByteBuf? = null

    /** [sharedBody] deflated on an empty window, once per event for every compressed socket here; held as it is. */
    fun sharedDeflatedBody(
        body: String,
        alloc: ByteBufAllocator,
    ): ByteBuf {
        if (body !== lastDeflatedBody) {
            lastDeflatedBytes?.release()
            lastDeflatedBytes = null
            val plain = sharedBody(body, alloc)
            val bytes = alloc.directBuffer(plain.readableBytes() / 2 + 64)
            try {
                deflate.fresh.deflate(plain, bytes, keepContext = false)
            } catch (e: Throwable) {
                bytes.release()
                throw e
            }
            lastDeflatedBody = body
            lastDeflatedBytes = bytes
        }
        return lastDeflatedBytes!!
    }

    // A copy, since a tick can close a connection and so leave this set mid-walk.
    private fun tick() = sessions.toTypedArray().forEach { isolated(it) { tick() } }

    /** One session's failure ends that connection; it never escapes into the shared task, which a periodic one would not survive. */
    private inline fun isolated(
        session: NativeWebSocketSession,
        block: NativeWebSocketSession.() -> Unit,
    ) {
        try {
            session.block()
        } catch (e: Throwable) {
            session.fail(e)
        }
    }

    companion object {
        private val byLoop = ConcurrentHashMap<EventLoop, EventLoopSessions>()

        fun of(loop: EventLoop): EventLoopSessions = byLoop.computeIfAbsent(loop, ::EventLoopSessions)
    }
}

/**
 * [text] as one server-to-client websocket text frame (RFC 6455 §5.2: final, unmasked) in a pooled buffer: the
 * header and the UTF-8 written once, with no frame object or encoder between it and the socket.
 */
internal fun encodeTextFrame(
    alloc: ByteBufAllocator,
    text: CharSequence,
): ByteBuf {
    val length = ByteBufUtil.utf8Bytes(text)
    val frame = alloc.ioBuffer(frameHeaderSize(length) + length)
    writeFrameHeader(frame, length)
    ByteBufUtil.writeUtf8(frame, text)
    return frame
}

/**
 * A live frame appended to [out]: its header, [prefix], and a copy of [body], the event's shared bytes with the
 * closing `]`. One buffer whole, so a frame costs the socket one write, not a head and a body to track and release.
 * [deflated] when [body] is that compressed once for every socket ([encodeLiveFrameHead] says how the two join).
 */
internal fun appendLiveFrame(
    out: ByteBuf,
    prefix: String,
    body: ByteBuf,
    deflated: Boolean = false,
) {
    val prefixBytes = ByteBufUtil.utf8Bytes(prefix)
    val length = liveFrameLength(prefixBytes, body.readableBytes(), deflated)
    out.ensureWritable(frameHeaderSize(length) + length)
    writeLiveFrameHead(out, prefix, prefixBytes, length, deflated)
    out.writeBytes(body, body.readerIndex(), body.readableBytes())
}

/**
 * The header and [prefix] of a live frame whose body, `]` included, is [bodyBytes] long and written after it. A
 * [deflated] prefix rides uncompressed as a stored block, so the body after it can be deflated once per event.
 */
internal fun encodeLiveFrameHead(
    alloc: ByteBufAllocator,
    prefix: String,
    bodyBytes: Int,
    deflated: Boolean = false,
): ByteBuf {
    val prefixBytes = ByteBufUtil.utf8Bytes(prefix)
    val length = liveFrameLength(prefixBytes, bodyBytes, deflated)
    val head = alloc.ioBuffer(frameHeaderSize(length) + length - bodyBytes)
    writeLiveFrameHead(head, prefix, prefixBytes, length, deflated)
    return head
}

/** [appendLiveFrame] into a buffer of its own. */
internal fun encodeLiveFrame(
    alloc: ByteBufAllocator,
    prefix: String,
    body: ByteBuf,
    deflated: Boolean = false,
): ByteBuf = alloc.ioBuffer(prefix.length + body.readableBytes() + 16).also { appendLiveFrame(it, prefix, body, deflated) }

private fun liveFrameLength(
    prefixBytes: Int,
    bodyBytes: Int,
    deflated: Boolean,
): Int = prefixBytes + bodyBytes + if (deflated) STORED_BLOCK_HEADER else 0

private fun writeLiveFrameHead(
    frame: ByteBuf,
    prefix: String,
    prefixBytes: Int,
    length: Int,
    deflated: Boolean,
) {
    writeFrameHeader(frame, length, deflated)
    if (deflated) writeStoredBlockHeader(frame, prefixBytes)
    ByteBufUtil.writeUtf8(frame, prefix)
}

/** The longest header RFC 6455 §5.2 gives a server's frame: the 64-bit length form. */
internal const val MAX_FRAME_HEADER = 10

internal fun frameHeaderSize(length: Int): Int =
    when {
        length < 126 -> 2
        length <= 0xFFFF -> 4
        else -> 10
    }

private fun writeFrameHeader(
    frame: ByteBuf,
    length: Int,
    compressed: Boolean = false,
) {
    val at = frame.writerIndex()
    frame.ensureWritable(frameHeaderSize(length))
    frame.writerIndex(at + setFrameHeader(frame, at, length, compressed))
}

/** A final text frame's header at [at], RSV1 set when [compressed] (RFC 7692 §6); its size in bytes. */
internal fun setFrameHeader(
    frame: ByteBuf,
    at: Int,
    length: Int,
    compressed: Boolean,
): Int {
    frame.setByte(at, if (compressed) 0xC1 else 0x81)
    val size = frameHeaderSize(length)
    when (size) {
        2 -> {
            frame.setByte(at + 1, length)
        }

        4 -> {
            frame.setByte(at + 1, 126)
            frame.setShort(at + 2, length)
        }

        else -> {
            frame.setByte(at + 1, 127)
            frame.setLong(at + 2, length.toLong())
        }
    }
    return size
}

/** A live `EVENT` frame queued in its two parts; [body] is shared by every subscription it reaches. */
private class LiveFrame(
    val prefix: String,
    val body: String,
)

/**
 * One relay connection over an upgraded channel. Live frames from its own loop's fan-out join a batch its
 * [EventLoopSessions] flushes; frames from any other thread join a lock-free queue it drains while the socket takes
 * writes. Commands are read in order by one coroutine per connection.
 */
private class NativeWebSocketSession(
    private val relay: NostrRelayServer,
    private val handshaker: WebSocketServerHandshaker,
    private val decoder: InboundFrameDecoder,
    private val inbound: NativeInbound,
    private val deflateTerms: DeflateTerms?,
) : ChannelInboundHandlerAdapter() {
    private lateinit var context: ChannelHandlerContext
    private lateinit var channel: io.netty.channel.Channel
    private lateinit var session: RelaySession
    private lateinit var reader: Job

    private val outbox = ConcurrentLinkedQueue<Any>()
    private val maxFrames = outboundFrameCap(relay.limits)
    private val queuedFrames = AtomicInteger()
    private val queuedChars = QueuedChars(PROCESS_OUTBOUND)
    private val drainScheduled = AtomicBoolean()
    private lateinit var loop: EventLoopSessions

    /** Set when the handshake agreed permessage-deflate; the event loop's alone. */
    private var deflate: SocketDeflate? = null

    @Volatile
    private var unwritableSince = IDLE

    @Volatile
    private var closing = false

    /** Set on the loop once this connection takes no more input: what it still sends is read and dropped. */
    private var inputEnded = false

    /** Since when the commands already read have had to be answered, and the close that follows; [IDLE] if not ending. */
    private var endingSince = IDLE
    private var endingClose: CloseWebSocketFrame? = null
    private val ended = AtomicBoolean()

    /** Commands read and not yet run: a String, or an [OversizedMessage]. */
    private val inbox = Channel<Any>(Channel.UNLIMITED)
    private val inboxDepth = AtomicInteger()
    private val inboxChars = AtomicLong()

    /** OKs this connection is owed: an EVENT or AUTH read and not yet answered, which a store may answer late. */
    private val oksOwed = AtomicInteger()
    private var lastHeard = System.nanoTime()
    private var ticks = 0

    override fun handlerAdded(ctx: ChannelHandlerContext) {
        context = ctx
        channel = ctx.channel()
        loop = EventLoopSessions.of(channel.eventLoop())
        deflate = deflateTerms?.let { SocketDeflate(it, loop.deflate) }
        batchLimit = channel.config().writeBufferHighWaterMark
        session =
            relay.connect(
                object : SessionSink, LiveFrameSink {
                    override val liveExecutor: FanoutExecutor get() = loop

                    override fun message(message: Message) {
                        if (message is OkMessage) oksOwed.decrementAndGet()
                        raw(message.toJson())
                    }

                    override fun raw(json: String) = offer(json, json.length)

                    override fun liveEvent(
                        framePrefix: String,
                        body: String,
                    ) {
                        // On this socket's own loop, with nothing queued ahead, the frame joins the batch the loop flushes.
                        if (!closing && channel.eventLoop().inEventLoop() && outbox.isEmpty() && channel.isWritable) {
                            // Quartz logs and skips a sink that throws; this connection ends instead of losing the event.
                            try {
                                writeLive(framePrefix, body)
                            } catch (e: Throwable) {
                                fail(e)
                            }
                        } else {
                            offer(LiveFrame(framePrefix, body), framePrefix.length + body.length + 1)
                        }
                    }
                },
            )
        reader =
            inboundScope.launch {
                try {
                    for (item in inbox) {
                        val length = charsOf(item)
                        val chars = inboxChars.addAndGet(-length.toLong())
                        val crossedChars = chars <= RESUME_READS_AT_CHARS && chars + length > RESUME_READS_AT_CHARS
                        if (inboxDepth.decrementAndGet() == RESUME_READS_AT || crossedChars) channel.eventLoop().execute(resumeReads)
                        session.receive(if (item is OversizedMessage) refusable(item) else item as String)
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    // As on the Ktor path, where the throw leaves serve(): a connection that cannot answer its commands ends.
                    fail(e)
                }
            }
        // Closed only once the reader is done, as serve() does: a close from the loop could land between a REQ starting
        // and its registering, and leave that subscription live, unreachable, in the fan-out index for good.
        reader.invokeOnCompletion { session.close() }
        loop.join(this)
        // Ktor reads a request at a time with autoRead off; from here the socket is read as fast as commands drain.
        channel.config().isAutoRead = true
        ctx.read()
    }

    override fun channelRead(
        ctx: ChannelHandlerContext,
        msg: Any,
    ) {
        if (inputEnded) return ReferenceCountUtil.release(msg).let { }
        if (msg is OversizedMessage) return queue(msg)
        if (msg !is WebSocketFrame) {
            // The upgrade request's empty trailer, decoded before the codec was swapped out.
            ReferenceCountUtil.release(msg)
            return
        }
        lastHeard = System.nanoTime()
        when (msg) {
            is TextWebSocketFrame -> {
                val text = msg.text()
                msg.release()
                queue(text)
            }

            is PingWebSocketFrame -> {
                ctx.writeAndFlush(PongWebSocketFrame(msg.content()))
            }

            is CloseWebSocketFrame -> {
                handshaker.close(channel, msg)
            }

            else -> {
                msg.release()
            }
        }
    }

    /** A command for the reader, counted before it is queued: the reader may take it, and count it out, at once. */
    private fun queue(item: Any) {
        lastHeard = System.nanoTime()
        val length = charsOf(item)
        val owesOk = owesOk(if (item is OversizedMessage) item.head else item as String)
        val chars = inboxChars.addAndGet(length.toLong())
        val depth = inboxDepth.incrementAndGet()
        if (owesOk) oksOwed.incrementAndGet()
        if (inbox.trySend(item).isFailure) {
            inboxChars.addAndGet(-length.toLong())
            inboxDepth.decrementAndGet()
            if (owesOk) oksOwed.decrementAndGet()
        } else if (depth >= PAUSE_READS_AT || chars >= PAUSE_READS_AT_CHARS) {
            pauseReads()
        }
    }

    /** Since when this connection's reads have been paused for its own backlog, or [IDLE]; the loop's own. */
    private var pausedSince = IDLE

    private fun pauseReads() {
        channel.config().isAutoRead = false
        if (pausedSince == IDLE) {
            pausedSince = System.nanoTime()
            InboundReads.pauses.incrementAndGet()
        }
    }

    /** Reads on again, and the time they were paused taken off the held message's clock: it was ours, not the client's. */
    private fun resumeReadsNow() {
        channel.config().isAutoRead = true
        val since = pausedSince
        if (since != IDLE) {
            decoder.resumedAfter(since)
            InboundReads.resumes.incrementAndGet()
        }
        pausedSince = IDLE
    }

    /** An EVENT or AUTH, which NIP-01 and NIP-42 owe an OK; one that never gets one is waited for until the grace ends. */
    private fun owesOk(command: String): Boolean {
        var i = 0
        while (i < command.length && command[i].isWhitespace()) i++
        if (i >= command.length || command[i] != '[') return false
        i++
        while (i < command.length && command[i].isWhitespace()) i++
        return command.startsWith("\"EVENT\"", i) || command.startsWith("\"AUTH\"", i)
    }

    private fun charsOf(item: Any): Int = if (item is OversizedMessage) item.head.length else (item as String).length

    /**
     * The head of a message too long for the engine, padded past its length: quartz refuses it on length
     * and reads the id or sub id off the head, so the answer is quartz's own. Remove with [FrameIdScanner].
     */
    private fun refusable(message: OversizedMessage): String = message.head + inbound.padding

    /** On the event loop, where reads are also paused, so the two never cross: reads resume once the backlog is short. */
    private val resumeReads =
        Runnable {
            if (!closing && inboxDepth.get() <= RESUME_READS_AT && inboxChars.get() <= RESUME_READS_AT_CHARS) resumeReadsNow()
        }

    /** Ends this connection after an unexpected failure in its own work. */
    fun fail(cause: Throwable) {
        System.err.println("websocket: closing a connection after ${cause::class.simpleName}: ${cause.message}")
        channel.close()
    }

    /** From any thread: queues a frame of [chars] characters, or ends a connection that has stopped taking what it is sent. */
    private fun offer(
        frame: Any,
        chars: Int,
    ) {
        if (closing) return
        if (queuedFrames.incrementAndGet() > maxFrames || stalled() || queuedChars.overBudget(MAX_OUTGOING_CHARS)) {
            queuedFrames.decrementAndGet()
            disconnectSlowConsumer()
            return
        }
        queuedChars.add(chars.toLong())
        outbox.add(frame)
        if (drainScheduled.compareAndSet(false, true)) loop.mark(this)
    }

    /** This socket's live frames since its loop last flushed, in one buffer not yet handed to the socket. */
    private var pendingLive: ByteBuf? = null

    /** Set when this socket was written to since its loop last flushed. */
    private var flushOwed = false

    /** The channel's write high-water mark, read once: past it a batch goes to the socket without waiting for the flush. */
    private var batchLimit = 0

    /**
     * On the event loop, inside a fan-out task: the frame joins this socket's batch, and the flush is left to the loop.
     * A batch past the socket's high-water mark, or a body too large to copy, is handed to the socket now, so its
     * writability, and with it the outbox's slow-consumer limits, govern what follows.
     */
    private fun writeLive(
        prefix: String,
        body: String,
    ) {
        val deflated = deflatesLive(prefix)
        val shared = liveBody(body, deflated)
        if (shared.readableBytes() > LIVE_COPY_MAX_BYTES) {
            writePendingLive()
            writeLiveFrame(prefix, shared, deflated)
        } else {
            val pending = pendingLive
            if (pending == null) {
                pendingLive = encodeLiveFrame(context.alloc(), prefix, shared, deflated)
            } else {
                appendLiveFrame(pending, prefix, shared, deflated)
                if (pending.readableBytes() >= batchLimit) writePendingLive()
            }
        }
        if (!flushOwed) {
            flushOwed = true
            loop.wrote(this)
        }
    }

    fun flushWritten() {
        if (!flushOwed) return
        flushOwed = false
        if (closing) return
        writePendingLive()
        context.flush()
        if (!channel.isWritable && unwritableSince == IDLE) unwritableSince = nowMs()
    }

    /** One live frame to the socket, unflushed: a small body copied in with its head, a large one shared after it. */
    private fun writeLiveFrame(
        prefix: String,
        shared: ByteBuf,
        deflated: Boolean,
    ) {
        if (shared.readableBytes() > LIVE_COPY_MAX_BYTES) {
            context.write(encodeLiveFrameHead(context.alloc(), prefix, shared.readableBytes(), deflated), context.voidPromise())
            context.write(shared.retainedDuplicate(), context.voidPromise())
        } else {
            context.write(encodeLiveFrame(context.alloc(), prefix, shared, deflated), context.voidPromise())
        }
    }

    /** Whether a live frame for [prefix] goes out compressed: its prefix must fit the stored block it rides in. */
    private fun deflatesLive(prefix: String): Boolean = deflate != null && prefix.length <= MAX_STORED_PREFIX_CHARS

    /** The event's bytes as this socket sends them, shared with every other socket on the loop. */
    private fun liveBody(
        body: String,
        deflated: Boolean,
    ): ByteBuf {
        if (!deflated) return loop.sharedBody(body, context.alloc())
        deflate?.bypassed()
        return loop.sharedDeflatedBody(body, context.alloc())
    }

    /** Hands the socket the live frames held for the loop's flush, ahead of anything queued after them; false if none. */
    private fun writePendingLive(): Boolean {
        val pending = pendingLive ?: return false
        pendingLive = null
        context.write(pending, context.voidPromise())
        return true
    }

    /** On the event loop: writes the live batch, then what is queued while the socket takes it, then flushes once. */
    fun drain() {
        drainScheduled.set(false)
        var wrote = writePendingLive()
        while (channel.isWritable) {
            val frame = outbox.poll() ?: break
            queuedFrames.decrementAndGet()
            // From this handler's place in the pipeline: past the tail, and past the frame encoder a buffer skips.
            if (frame is LiveFrame) {
                queuedChars.add(-(frame.prefix.length + frame.body.length + 1).toLong())
                val deflated = deflatesLive(frame.prefix)
                writeLiveFrame(frame.prefix, liveBody(frame.body, deflated), deflated)
            } else {
                val text = frame as String
                queuedChars.add(-text.length.toLong())
                context.write(deflate?.encodeText(context.alloc(), text) ?: encodeTextFrame(context.alloc(), text), context.voidPromise())
            }
            wrote = true
        }
        if (wrote) context.flush()
        if (!channel.isWritable) {
            if (unwritableSince == IDLE) unwritableSince = nowMs()
            // Held until the socket takes writes again, when channelWritabilityChanged drains: offers schedule nothing meanwhile.
            drainScheduled.set(true)
        }
    }

    override fun channelWritabilityChanged(ctx: ChannelHandlerContext) {
        if (channel.isWritable) {
            unwritableSince = IDLE
            drain()
        } else if (unwritableSince == IDLE) {
            unwritableSince = nowMs()
        }
        ctx.fireChannelWritabilityChanged()
    }

    /** A message whose bytes are held has been arriving for longer than the deadline, pings or not. */
    private fun heldTooLong(): Boolean {
        val since = decoder.holdingSince
        if (since == InboundFrameDecoder.IDLE || pausedSince != IDLE) return false
        return (System.nanoTime() - since) / 1_000_000 >= inbound.openMessageDeadlineMs
    }

    private fun stalled(): Boolean {
        if (queuedChars.held() <= MAX_OUTGOING_CHARS) return false
        val since = unwritableSince
        return since != IDLE && nowMs() - since >= WRITER_STALL_MS
    }

    private fun disconnectSlowConsumer() {
        if (closing) return
        closing = true
        channel.eventLoop().execute {
            outbox.clear()
            releasePendingLive()
            channel.writeAndFlush(CloseWebSocketFrame(WebSocketCloseStatus.TRY_AGAIN_LATER, "slow consumer: outbound buffer full"))
            channel.eventLoop().schedule({ channel.close() }, CLOSE_GRACE_MS, TimeUnit.MILLISECONDS)
        }
    }

    /** Once a second on the event loop: catches a queue a writability change missed, a stall, paused reads and a silent peer. */
    fun tick() {
        if (closing) return
        if (stalled() || queuedChars.overBudget(MAX_OUTGOING_CHARS)) return disconnectSlowConsumer()
        deflate?.tick()
        if (endingSince != IDLE) return closeWhenAnswered()
        if (heldTooLong()) {
            val seconds = inbound.openMessageDeadlineMs / 1_000
            return endInput(
                NoticeMessage("error: a message took over $seconds s to arrive; closing").toJson(),
                CloseWebSocketFrame(WebSocketCloseStatus.POLICY_VIOLATION, "message not finished within $seconds s"),
            )
        }
        if (!outbox.isEmpty() && channel.isWritable) drain()
        // Paused reads are our backlog, not the peer's silence: its pongs wait unread behind its commands.
        if (!channel.config().isAutoRead) {
            lastHeard = System.nanoTime()
            resumeReads.run()
        }
        // Bytes of a message still arriving are the client heard from, as much as a whole frame.
        val heard = maxOf(lastHeard, decoder.lastRead)
        if ((System.nanoTime() - heard) / 1_000_000 >= inbound.keepalive.silenceTimeoutMs) return channel.close().let { }
        if (++ticks % (inbound.keepalive.pingPeriodMs / TICK_MS).coerceAtLeast(1) == 0L) channel.writeAndFlush(PingWebSocketFrame())
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        end()
        ctx.fireChannelInactive()
    }

    override fun handlerRemoved(ctx: ChannelHandlerContext) = end()

    override fun exceptionCaught(
        ctx: ChannelHandlerContext,
        cause: Throwable,
    ) {
        val maxBytes = inbound.frames.maxMessageBytes
        when {
            // A message past the cap, judged from the header of the frame that passes it.
            cause is CorruptedWebSocketFrameException && cause.closeStatus() == WebSocketCloseStatus.MESSAGE_TOO_BIG -> {
                endInput(
                    NoticeMessage("error: a message over $maxBytes bytes is past this relay's websocket limit; closing").toJson(),
                    CloseWebSocketFrame(WebSocketCloseStatus.MESSAGE_TOO_BIG, "message over $maxBytes bytes"),
                )
            }

            // A client that breaks the protocol is owed nothing more.
            cause is CorruptedWebSocketFrameException -> {
                stopInput()
                sendClose(CloseWebSocketFrame(cause.closeStatus(), cause.message ?: cause.closeStatus().reasonText()))
            }

            else -> {
                ctx.close()
            }
        }
    }

    /** On the loop: reads go on, so a close never meets unread input (which would reset it), but nothing is decoded. */
    private fun stopInput() {
        inputEnded = true
        decoder.discard()
        resumeReadsNow()
    }

    /**
     * Takes no more input and ends the connection: [notice] goes out behind the answers already queued, and
     * [close] once the commands already read are answered, or after [CLOSE_GRACE_MS] (checked each tick).
     */
    private fun endInput(
        notice: String,
        close: CloseWebSocketFrame,
    ) {
        if (closing || endingSince != IDLE) return close.release().let { }
        stopInput()
        endingSince = nowMs()
        endingClose = close
        offer(notice, notice.length)
    }

    /** Answers can trail their commands (an EVENT's OK waits on its write), so the close waits at least a tick. */
    private fun closeWhenAnswered() {
        val waited = nowMs() - endingSince
        val answered = inboxDepth.get() == 0 && oksOwed.get() <= 0 && outbox.isEmpty() && pendingLive == null && waited >= ANSWER_SETTLE_MS
        if (answered || waited >= CLOSE_GRACE_MS) endingClose?.let { sendClose(it) }
    }

    /**
     * [close], then the output side shut and input still read and dropped: the client gets a FIN behind the
     * close rather than a reset that could discard it. The socket closes on the client's EOF, or after the grace.
     */
    private fun sendClose(close: CloseWebSocketFrame) {
        if (closing) return close.release().let { }
        closing = true
        endingClose = null
        outbox.clear()
        releasePendingLive()
        context.writeAndFlush(close).addListener {
            val duplex = channel as? DuplexChannel
            if (it.isSuccess && duplex != null) duplex.shutdownOutput() else channel.close()
        }
        channel.eventLoop().schedule({ channel.close() }, CLOSE_GRACE_MS, TimeUnit.MILLISECONDS)
    }

    private fun end() {
        if (!ended.compareAndSet(false, true)) return
        closing = true
        loop.leave(this)
        outbox.clear()
        queuedChars.release()
        releasePendingLive()
        endingClose?.release()
        endingClose = null
        deflate?.end()
        inbox.close()
        reader.cancel()
    }

    private fun releasePendingLive() {
        pendingLive?.release()
        pendingLive = null
    }

    private fun nowMs() = System.nanoTime() / 1_000_000

    private companion object {
        const val IDLE = Long.MIN_VALUE
    }
}
