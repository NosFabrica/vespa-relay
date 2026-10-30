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
import io.netty.handler.codec.http.DefaultFullHttpRequest
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.EmptyHttpHeaders
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.QueryStringDecoder
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus
import io.netty.handler.codec.http.websocketx.WebSocketFrame
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator
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

/** The keepalive tick, and the ping period in ticks; the slow-consumer and silence rules are the Ktor path's (RelayWebSocket.kt). */
private const val TICK_MS = 1_000L
private const val PING_EVERY_TICKS = (PING_PERIOD_MS / TICK_MS).toInt()

/** Commands queued from one connection before its reads pause, and the depth at which they resume. */
private const val PAUSE_READS_AT = 64
private const val RESUME_READS_AT = 16

private val inboundScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

/**
 * Serves the relay's websocket on Netty directly, ahead of Ktor's HTTP/1.1 handler: an upgrade on `/` is
 * taken here and every other request goes on to Ktor. HTTP/2 connections are left to Ktor entirely.
 */
internal fun ChannelPipeline.installNativeWebSocket(
    relay: NostrRelayServer,
    maxFrameBytes: Int,
    onionLocation: () -> String?,
) {
    if (get(KTOR_HTTP1) != null) addBefore(KTOR_HTTP1, "nostrUpgrade", NativeWebSocketUpgrade(relay, maxFrameBytes, onionLocation))
}

private class NativeWebSocketUpgrade(
    private val relay: NostrRelayServer,
    private val maxFrameBytes: Int,
    private val onionLocation: () -> String?,
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
        val handshaker =
            WebSocketServerHandshakerFactory("ws://$host/", null, false, maxFrameBytes).newHandshaker(request)
                ?: return WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel()).let { }
        // Every check the handshake makes is behind us before the pipeline changes or a session connects.
        val pipeline = ctx.pipeline()
        for (name in KTOR_HTTP1_HANDLERS) if (pipeline.get(name) != null) pipeline.remove(name)
        pipeline.replace(this, "nostrSocket", NativeWebSocketSession(relay, handshaker))
        pipeline.addBefore("nostrSocket", "wsAggregator", WebSocketFrameAggregator(maxFrameBytes))
        val headers = DefaultHttpHeaders()
        if (!host.substringBefore(':').endsWith(".onion", ignoreCase = true)) onionLocation()?.let { headers.set("Onion-Location", it) }
        handshaker.handshake(ctx.channel(), request, headers, ctx.channel().newPromise()).addListener { if (!it.isSuccess) ctx.close() }
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

    /** Sessions fan-out tasks wrote to since the last flush; this loop's own. */
    private val written = ArrayList<NativeWebSocketSession>()
    private var flushQueued = false
    private val flushTask = Runnable { flushWritten() }

    /**
     * Quartz's fan-out for this loop's subscriptions, run here. The sockets it wrote to are flushed by a task queued
     * behind it: on an idle loop that runs next, and under load it waits out the fan-outs already queued, so a socket
     * those events reach takes them in one write rather than one per event.
     */
    override fun execute(task: () -> Unit) {
        loop.execute {
            try {
                task()
            } finally {
                if (written.isNotEmpty() && !flushQueued) {
                    flushQueued = true
                    loop.execute(flushTask)
                }
            }
        }
    }

    private fun flushWritten() {
        flushQueued = false
        for (session in written) isolated(session) { flushWritten() }
        written.clear()
    }

    fun wrote(session: NativeWebSocketSession) {
        written.add(session)
    }

    private var lastBody: String? = null
    private var lastBodyBytes: ByteBuf? = null

    /**
     * [body] and the frame's closing `]` as bytes, encoded once per event however many of this loop's sockets
     * it reaches: fan-out hands every subscription the same string, so identity finds it. The loop's own buffer,
     * valid until the next event: copy from it, never keep it.
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
 */
internal fun appendLiveFrame(
    out: ByteBuf,
    prefix: String,
    body: ByteBuf,
) {
    val length = ByteBufUtil.utf8Bytes(prefix) + body.readableBytes()
    out.ensureWritable(frameHeaderSize(length) + length)
    writeFrameHeader(out, length)
    ByteBufUtil.writeUtf8(out, prefix)
    out.writeBytes(body, body.readerIndex(), body.readableBytes())
}

/** [appendLiveFrame] into a buffer of its own. */
internal fun encodeLiveFrame(
    alloc: ByteBufAllocator,
    prefix: String,
    body: ByteBuf,
): ByteBuf = alloc.ioBuffer(prefix.length + body.readableBytes() + 10).also { appendLiveFrame(it, prefix, body) }

private fun frameHeaderSize(length: Int): Int =
    when {
        length < 126 -> 2
        length <= 0xFFFF -> 4
        else -> 10
    }

private fun writeFrameHeader(
    frame: ByteBuf,
    length: Int,
) {
    frame.writeByte(0x81)
    when (frameHeaderSize(length)) {
        2 -> {
            frame.writeByte(length)
        }

        4 -> {
            frame.writeByte(126)
            frame.writeShort(length)
        }

        else -> {
            frame.writeByte(127)
            frame.writeLong(length.toLong())
        }
    }
}

/** A live `EVENT` frame queued in its two parts; [body] is shared by every subscription it reaches. */
private class LiveFrame(
    val prefix: String,
    val body: String,
)

/**
 * One relay connection over an upgraded channel. Frames from any thread join a lock-free queue that its event
 * loop's [EventLoopSessions] drains while the socket takes writes, so a fan-out costs an enqueue, never a coroutine.
 * Commands are read in order by one coroutine per connection.
 */
private class NativeWebSocketSession(
    private val relay: NostrRelayServer,
    private val handshaker: WebSocketServerHandshaker,
) : ChannelInboundHandlerAdapter() {
    private lateinit var context: ChannelHandlerContext
    private lateinit var channel: io.netty.channel.Channel
    private lateinit var session: RelaySession
    private lateinit var reader: Job

    private val outbox = ConcurrentLinkedQueue<Any>()
    private val queuedFrames = AtomicInteger()
    private val queuedChars = AtomicLong()
    private val drainScheduled = AtomicBoolean()
    private lateinit var loop: EventLoopSessions

    @Volatile
    private var unwritableSince = IDLE

    @Volatile
    private var closing = false
    private val ended = AtomicBoolean()

    private val inbox = Channel<String>(Channel.UNLIMITED)
    private val inboxDepth = AtomicInteger()
    private var lastHeard = System.nanoTime()
    private var ticks = 0

    override fun handlerAdded(ctx: ChannelHandlerContext) {
        context = ctx
        channel = ctx.channel()
        loop = EventLoopSessions.of(channel.eventLoop())
        session =
            relay.connect(
                object : SessionSink, LiveFrameSink {
                    override val liveExecutor: FanoutExecutor get() = loop

                    override fun message(message: Message) = raw(message.toJson())

                    override fun raw(json: String) = offer(json, json.length)

                    override fun liveEvent(
                        framePrefix: String,
                        body: String,
                    ) {
                        // On this socket's own loop, with nothing queued ahead, the frame goes straight to the socket.
                        if (!closing && channel.eventLoop().inEventLoop() && outbox.isEmpty() && channel.isWritable) {
                            writeLive(framePrefix, body)
                        } else {
                            offer(LiveFrame(framePrefix, body), framePrefix.length + body.length + 1)
                        }
                    }
                },
            )
        reader =
            inboundScope.launch {
                try {
                    for (text in inbox) {
                        if (inboxDepth.decrementAndGet() == RESUME_READS_AT) channel.eventLoop().execute(resumeReads)
                        session.receive(text)
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
                inbox.trySend(text)
                if (inboxDepth.incrementAndGet() >= PAUSE_READS_AT) channel.config().isAutoRead = false
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

    /** On the event loop, where reads are also paused, so the two never cross: reads resume once the backlog is short. */
    private val resumeReads = Runnable { if (!closing && inboxDepth.get() <= RESUME_READS_AT) channel.config().isAutoRead = true }

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
        if (queuedFrames.incrementAndGet() > MAX_OUTGOING_BUFFER || stalled()) {
            queuedFrames.decrementAndGet()
            disconnectSlowConsumer()
            return
        }
        queuedChars.addAndGet(chars.toLong())
        outbox.add(frame)
        if (drainScheduled.compareAndSet(false, true)) loop.mark(this)
    }

    /** This socket's live frames from the fan-out tasks since its loop last flushed, written as one buffer. */
    private var pendingLive: ByteBuf? = null

    /** On the event loop, inside a fan-out task: the frame joins this socket's buffer; the write is left to the loop's flush. */
    private fun writeLive(
        prefix: String,
        body: String,
    ) {
        val shared = loop.sharedBody(body, context.alloc())
        val pending = pendingLive
        if (pending != null) {
            appendLiveFrame(pending, prefix, shared)
        } else {
            pendingLive = encodeLiveFrame(context.alloc(), prefix, shared)
            loop.wrote(this)
        }
    }

    fun flushWritten() {
        if (!writePendingLive()) return
        context.flush()
        if (!channel.isWritable && unwritableSince == IDLE) unwritableSince = nowMs()
    }

    /** Hands the socket the live frames held for the loop's flush, ahead of anything queued after them; false if none. */
    private fun writePendingLive(): Boolean {
        val pending = pendingLive ?: return false
        pendingLive = null
        context.write(pending, context.voidPromise())
        return true
    }

    /** On the event loop: writes what is queued while the socket takes it, then flushes once. */
    fun drain() {
        drainScheduled.set(false)
        var wrote = writePendingLive()
        while (channel.isWritable) {
            val frame = outbox.poll() ?: break
            queuedFrames.decrementAndGet()
            // From this handler's place in the pipeline: past the tail, and past the frame encoder a buffer skips.
            if (frame is LiveFrame) {
                queuedChars.addAndGet(-(frame.prefix.length + frame.body.length + 1).toLong())
                context.write(encodeLiveFrame(context.alloc(), frame.prefix, loop.sharedBody(frame.body, context.alloc())), context.voidPromise())
            } else {
                val text = frame as String
                queuedChars.addAndGet(-text.length.toLong())
                context.write(encodeTextFrame(context.alloc(), text), context.voidPromise())
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

    private fun stalled(): Boolean {
        if (queuedChars.get() <= MAX_OUTGOING_CHARS) return false
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
        if (stalled()) return disconnectSlowConsumer()
        if (!outbox.isEmpty() && channel.isWritable) drain()
        // Paused reads are our backlog, not the peer's silence: its pongs wait unread behind its commands.
        if (!channel.config().isAutoRead) {
            lastHeard = System.nanoTime()
            resumeReads.run()
        }
        if ((System.nanoTime() - lastHeard) / 1_000_000 >= SILENCE_TIMEOUT_MS) return channel.close().let { }
        if (++ticks % PING_EVERY_TICKS == 0) channel.writeAndFlush(PingWebSocketFrame())
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
        ctx.close()
    }

    private fun end() {
        if (!ended.compareAndSet(false, true)) return
        closing = true
        loop.leave(this)
        outbox.clear()
        releasePendingLive()
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
