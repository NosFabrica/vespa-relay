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

/** Frames and characters one connection may hold queued, and how long past the latter its socket may refuse writes. */
private const val MAX_QUEUED_FRAMES = 8192
private const val MAX_QUEUED_CHARS = 16L * 1024 * 1024
private const val STALL_MS = 30_000L
private const val CLOSE_GRACE_MS = 5_000L

/** The keepalive tick, the ping period in ticks, and the silence after which a peer is reaped. */
private const val TICK_MS = 1_000L
private const val PING_EVERY_TICKS = 30
private const val SILENCE_MS = 60_000L

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
        val pipeline = ctx.pipeline()
        for (name in KTOR_HTTP1_HANDLERS) if (pipeline.get(name) != null) pipeline.remove(name)
        pipeline.replace(this, "nostrSocket", NativeWebSocketSession(relay, handshaker))
        pipeline.addBefore("nostrSocket", "wsAggregator", WebSocketFrameAggregator(maxFrameBytes))
        val headers = DefaultHttpHeaders()
        if (!host.substringBefore(':').endsWith(".onion", ignoreCase = true)) onionLocation()?.let { headers.set("Onion-Location", it) }
        handshaker.handshake(ctx.channel(), request, headers, ctx.channel().newPromise()).addListener { if (!it.isSuccess) ctx.close() }
    }

    private fun isUpgrade(request: HttpRequest): Boolean =
        request.method() == HttpMethod.GET &&
            request.headers().containsValue(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true) &&
            QueryStringDecoder(request.uri()).path() == "/"
}

/**
 * The connections served by one event loop. Those with frames waiting are drained by one task on the loop however
 * many a fan-out touched, and one timer a second ticks them all, so neither costs a task per connection.
 * [sessions] is the loop's own: touched only on the loop thread.
 */
private class EventLoopSessions(
    private val loop: EventLoop,
) {
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
        while (true) dirty.poll()?.drain() ?: break
    }

    // A copy, since a tick can close a connection and so leave this set mid-walk.
    private fun tick() = sessions.toTypedArray().forEach { it.tick() }

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
    val header =
        when {
            length < 126 -> 2
            length <= 0xFFFF -> 4
            else -> 10
        }
    val frame = alloc.ioBuffer(header + length)
    frame.writeByte(0x81)
    when (header) {
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
    ByteBufUtil.writeUtf8(frame, text)
    return frame
}

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

    private val outbox = ConcurrentLinkedQueue<String>()
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
        session = relay.connect(SessionSink.of(::offer))
        reader =
            inboundScope.launch {
                for (text in inbox) {
                    if (inboxDepth.decrementAndGet() == RESUME_READS_AT) channel.config().isAutoRead = true
                    session.receive(text)
                }
            }
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
                if (inboxDepth.incrementAndGet() == PAUSE_READS_AT) channel.config().isAutoRead = false
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

    /** From any thread: queues [text], or ends a connection that has stopped taking what it is sent. */
    private fun offer(text: String) {
        if (closing) return
        if (queuedFrames.incrementAndGet() > MAX_QUEUED_FRAMES || stalled()) {
            queuedFrames.decrementAndGet()
            disconnectSlowConsumer()
            return
        }
        queuedChars.addAndGet(text.length.toLong())
        outbox.add(text)
        if (drainScheduled.compareAndSet(false, true)) loop.mark(this)
    }

    /** On the event loop: writes what is queued while the socket takes it, then flushes once. */
    fun drain() {
        drainScheduled.set(false)
        var wrote = false
        while (channel.isWritable) {
            val text = outbox.poll() ?: break
            queuedFrames.decrementAndGet()
            queuedChars.addAndGet(-text.length.toLong())
            // From this handler's place in the pipeline: past the tail, and past the frame encoder a buffer skips.
            context.write(encodeTextFrame(context.alloc(), text), context.voidPromise())
            wrote = true
        }
        if (wrote) context.flush()
        if (!channel.isWritable && unwritableSince == IDLE) unwritableSince = nowMs()
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
        if (queuedChars.get() <= MAX_QUEUED_CHARS) return false
        val since = unwritableSince
        return since != IDLE && nowMs() - since >= STALL_MS
    }

    private fun disconnectSlowConsumer() {
        if (closing) return
        closing = true
        channel.eventLoop().execute {
            outbox.clear()
            channel.writeAndFlush(CloseWebSocketFrame(WebSocketCloseStatus.TRY_AGAIN_LATER, "slow consumer: outbound buffer full"))
            channel.eventLoop().schedule({ channel.close() }, CLOSE_GRACE_MS, TimeUnit.MILLISECONDS)
        }
    }

    /** Once a second on the event loop: catches a queue a writability change missed, a stall, and a silent peer. */
    fun tick() {
        if (closing) return
        if (stalled()) return disconnectSlowConsumer()
        if (!outbox.isEmpty() && channel.isWritable) drain()
        if (++ticks % PING_EVERY_TICKS != 0) return
        if ((System.nanoTime() - lastHeard) / 1_000_000 >= SILENCE_MS) {
            channel.close()
        } else {
            channel.writeAndFlush(PingWebSocketFrame())
        }
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
        inbox.close()
        reader.cancel()
        session.close()
    }

    private fun nowMs() = System.nanoTime() / 1_000_000

    private companion object {
        const val IDLE = Long.MIN_VALUE
    }
}
