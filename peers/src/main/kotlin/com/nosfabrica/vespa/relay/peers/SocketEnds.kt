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
package com.nosfabrica.vespa.relay.peers

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * How each upstream socket ended and the frames we sent on it, recorded around quartz's socket
 * because `RelayConnectionListener.onDisconnected` drops both. Remove when quartz's disconnect carries them.
 */
class SocketEnds(
    private val clock: () -> Long = System::currentTimeMillis,
) {
    /** Which side ended a socket. */
    enum class Side(
        val says: String,
    ) {
        /** The relay sent a close frame. */
        RELAY("the relay closed it"),

        /** This process closed it: a release, an eviction, quartz's own reconnect. */
        US("we closed it"),

        /** No close frame: a reset, an EOF, a failed dial or ping. */
        TRANSPORT("the transport failed"),
    }

    /** One frame we sent: its size in UTF-8 bytes and its verb (`REQ`, `NEG-OPEN`, ...). */
    class Sent(
        val bytes: Int,
        val verb: String,
    ) {
        override fun toString() = "${fmtBytes(bytes)} $verb"
    }

    /** How one socket ended, and what we had sent on it by then. */
    class End(
        val side: Side,
        /** The close frame's code, or the HTTP status of a refused upgrade. */
        val code: Int?,
        /** The close frame's reason, the HTTP message, or the exception's class and message. */
        val reason: String?,
        /** For [Side.US], who asked for the close: the deciding frame, and ours beneath it when that differs. */
        val by: String?,
        val atMs: Long,
        /** When the socket opened; null when it never did. */
        val openedAtMs: Long?,
        val sent: SentSoFar,
        /** Orders this end against a [Mark], which a millisecond clock cannot. */
        internal val seq: Long,
    ) {
        /** A short key to count by: the side, then the code, the caller or the exception class. */
        val cause: String
            get() =
                when (side) {
                    Side.RELAY -> "relay closed ${code ?: "?"}" + (reason?.takeIf { it.isNotBlank() }?.let { " ${foldDigits(it).take(CAUSE_REASON_CHARS)}" } ?: "")
                    Side.US -> "we closed (${by ?: "unknown"})"
                    Side.TRANSPORT -> "transport ${reason?.substringBefore(':')?.take(CAUSE_REASON_CHARS) ?: "failure"}" + (code?.let { " http $it" } ?: "")
                }

        override fun toString(): String =
            side.says +
                (code?.let { " code $it" } ?: "") +
                (reason?.takeIf { it.isNotBlank() }?.let { " \"$it\"" } ?: "") +
                (by?.let { " from $it" } ?: "") +
                (openedAtMs?.let { " after ${atMs - it}ms open" } ?: " before it opened") +
                "; $sent"
    }

    /** The frames sent on one socket: how many, the largest and the last. */
    class SentSoFar(
        val frames: Long,
        val largest: Sent?,
        val last: Sent?,
    ) {
        override fun toString() = if (frames == 0L) "nothing sent" else "$frames frame(s) sent, largest $largest, last $last"
    }

    /** What a caller reads back about a url since its [Mark]. */
    class Since(
        /** The socket's end, if one landed after the mark. */
        val end: End?,
        /** The ended socket's frames, else the live one's. */
        val sent: SentSoFar?,
        /** NEG-MSG frames the relay sent for the first NEG-OPEN after the mark; null when none went out. */
        val negFramesIn: Long?,
        val elapsedMs: Long,
    )

    /** Taken before an ask, so what is read back after it belongs to it. */
    class Mark internal constructor(
        val url: NormalizedRelayUrl,
        val atMs: Long,
        internal val seq: Long,
    )

    /** One NEG-OPEN we sent, and the NEG-MSG frames the relay answered it with. */
    private class NegSub(
        val subId: String,
        val seq: Long,
    ) {
        val framesIn = AtomicLong()
    }

    private class Track {
        @Volatile var live: Session? = null

        @Volatile var lastEnd: End? = null

        /** The latest NEG-OPENs on this url, oldest first, bounded by [NEG_SUBS_KEPT]. */
        val negSubs = ArrayDeque<NegSub>()

        fun opened(sub: NegSub) =
            synchronized(negSubs) {
                if (negSubs.size >= NEG_SUBS_KEPT) negSubs.removeFirst()
                negSubs.addLast(sub)
            }

        fun answered(subId: String) = synchronized(negSubs) { negSubs.lastOrNull { it.subId == subId } }?.framesIn?.incrementAndGet()

        fun firstOpenedAfter(seq: Long) = synchronized(negSubs) { negSubs.firstOrNull { it.seq > seq } }
    }

    private val seq = AtomicLong()
    private val tracks = ConcurrentHashMap<NormalizedRelayUrl, Track>()

    private fun track(url: NormalizedRelayUrl) = tracks.computeIfAbsent(url) { Track() }

    /** [inner] with every socket it builds recorded here. */
    fun around(inner: WebsocketBuilder): WebsocketBuilder =
        object : WebsocketBuilder {
            override fun build(
                url: NormalizedRelayUrl,
                out: WebSocketListener,
            ): WebSocket {
                val session = Session(track(url), out)
                session.socket = inner.build(url, session.listener)
                return session
            }

            override fun canConnect(url: NormalizedRelayUrl) = inner.canConnect(url)
        }

    fun mark(url: NormalizedRelayUrl): Mark = Mark(url, clock(), seq.get())

    fun since(mark: Mark): Since {
        val track = tracks[mark.url]
        val end = track?.lastEnd?.takeIf { it.seq > mark.seq }
        return Since(
            end = end,
            sent = end?.sent ?: track?.live?.sentSoFar(),
            negFramesIn = track?.firstOpenedAfter(mark.seq)?.framesIn?.get(),
            elapsedMs = clock() - mark.atMs,
        )
    }

    /** The last end recorded for [url], whenever it was. */
    fun lastEnd(url: NormalizedRelayUrl): End? = tracks[url]?.lastEnd

    /**
     * One socket session. Quartz reports exactly one terminal callback per session, and a
     * [disconnect] reports its `onClosed` synchronously, so the first end recorded is the true one.
     */
    private inner class Session(
        private val track: Track,
        private val out: WebSocketListener,
    ) : WebSocket {
        lateinit var socket: WebSocket

        @Volatile private var openedAtMs: Long? = null
        private var ended = false
        private var frames = 0L
        private var largest: Sent? = null
        private var last: Sent? = null

        init {
            track.live = this
        }

        @Synchronized
        fun sentSoFar() = SentSoFar(frames, largest, last)

        @Synchronized
        private fun sent(msg: String) {
            val bytes = utf8Length(msg)
            val frame = Sent(bytes, verbOf(msg))
            frames++
            last = frame
            if (bytes > (largest?.bytes ?: -1)) largest = frame
        }

        /** True for the first end only; every later report of the same session is dropped. */
        @Synchronized
        private fun claimEnd(): Boolean {
            if (ended) return false
            ended = true
            return true
        }

        private fun ended(
            side: Side,
            code: Int?,
            reason: String?,
            by: String? = null,
        ) {
            track.lastEnd = End(side, code, reason?.take(REASON_CHARS), by, clock(), openedAtMs, sentSoFar(), seq.incrementAndGet())
            if (track.live === this) track.live = null
            if (tracks.size > MAX_TRACKED) tracks.entries.removeIf { it.value.live == null && it.value !== track }
        }

        val listener =
            object : WebSocketListener {
                override fun onOpen(
                    pingMillis: Int,
                    compression: Boolean,
                ) {
                    openedAtMs = clock()
                    out.onOpen(pingMillis, compression)
                }

                override suspend fun onMessage(text: String) {
                    if (text.startsWith(NEG_MSG_PREFIX)) subIdOf(text)?.let { track.answered(it) }
                    out.onMessage(text)
                }

                override fun onClosed(
                    code: Int,
                    reason: String,
                ) {
                    // Before delegating: quartz's handler is what fails the reconcile reading this.
                    if (claimEnd()) ended(Side.RELAY, code, reason)
                    out.onClosed(code, reason)
                }

                override fun onFailure(
                    t: Throwable,
                    code: Int?,
                    response: String?,
                ) {
                    if (claimEnd()) ended(Side.TRANSPORT, code, "${t::class.simpleName}: ${t.message ?: ""}" + (response?.let { " ($it)" } ?: ""))
                    out.onFailure(t, code, response)
                }
            }

        override fun needsReconnect() = socket.needsReconnect()

        override fun connect() = socket.connect()

        override fun disconnect() {
            // The stack walk only for a close not already reported, which quartz's own cleanup is not.
            if (claimEnd()) ended(Side.US, null, null, by = ourCaller())
            socket.disconnect()
        }

        override fun send(msg: String): Boolean {
            sent(msg)
            if (msg.startsWith(NEG_OPEN_PREFIX)) subIdOf(msg)?.let { track.opened(NegSub(it, seq.incrementAndGet())) }
            return socket.send(msg)
        }
    }

    companion object {
        /** Tracks kept for urls with no live socket before the idle ones are dropped. */
        private const val MAX_TRACKED = 50_000
        private const val NEG_SUBS_KEPT = 16
        private const val REASON_CHARS = 160
        private const val CAUSE_REASON_CHARS = 60
        private const val NEG_MSG_PREFIX = "[\"NEG-MSG\",\""
        private const val NEG_OPEN_PREFIX = "[\"NEG-OPEN\",\""
        private const val VERB_CHARS = 16
        private const val SUB_ID_CHARS = 128
        private const val OURS = "com.nosfabrica."

        /** The socket layer and the runtime: never who decided a close. */
        private val PLUMBING =
            listOf(
                "${SocketEnds::class.java.name}$",
                "com.vitorpamplona.quartz.nip01Core.relay.sockets.",
                "com.vitorpamplona.quartz.nip01Core.relay.client.single.",
                "java.",
                "jdk.",
                "sun.",
                "kotlin.",
                "kotlinx.",
                "okhttp3.",
            )
        private val DIGITS = Regex("[0-9]+")

        /** [s] with every run of digits as `#`, so a reason carrying counts or addresses is one cause. */
        fun foldDigits(s: String): String = DIGITS.replace(s, "#")

        /** The verb of a NIP-01 frame, `["VERB", ...]`, or `?` for anything else. */
        internal fun verbOf(msg: String): String {
            if (!msg.startsWith("[\"")) return "?"
            val close = msg.indexOf('"', 2)
            return if (close in 3..(2 + VERB_CHARS)) msg.substring(2, close) else "?"
        }

        /** The subscription id of a `["VERB","<id>",...]` frame. */
        internal fun subIdOf(msg: String): String? {
            val open = msg.indexOf("\",\"")
            if (open < 0) return null
            val from = open + 3
            val close = msg.indexOf('"', from)
            return if (close in from..(from + SUB_ID_CHARS)) msg.substring(from, close) else null
        }

        internal fun utf8Length(s: String): Int {
            var bytes = 0
            var i = 0
            while (i < s.length) {
                val c = s[i]
                bytes +=
                    when {
                        c.code < 0x80 -> {
                            1
                        }

                        c.code < 0x800 -> {
                            2
                        }

                        Character.isHighSurrogate(c) && i + 1 < s.length && Character.isLowSurrogate(s[i + 1]) -> {
                            i++
                            4
                        }

                        else -> {
                            3
                        }
                    }
                i++
            }
            return bytes
        }

        internal fun fmtBytes(bytes: Int): String =
            when {
                bytes < 1024 -> "${bytes}B"
                bytes < 1024 * 1024 -> "%.1fKiB".format(bytes / 1024.0)
                else -> "%.1fMiB".format(bytes / (1024.0 * 1024.0))
            }

        /** `Class.method` for a frame, with a lambda or coroutine body named after the function it is written in. */
        internal fun frameName(
            className: String,
            methodName: String,
        ): String {
            val parts = className.substringAfterLast('.').split('$').filter { it.isNotEmpty() && !it.all(Char::isDigit) && !it.startsWith("Lambda") }
            val method = methodName.substringBefore('$')
            val inner = parts.drop(1)
            return when {
                // A coroutine or lambda class: `Outer$fn$1.invokeSuspend` is `fn`'s body.
                inner.isNotEmpty() && method in LAMBDA_METHODS -> "${parts.first()}.${inner.joinToString(".")}"

                else -> (parts + method).joinToString(".")
            }
        }

        private val LAMBDA_METHODS = setOf("invoke", "invokeSuspend", "run", "call", "accept")

        /**
         * Who asked for the close: the first frame outside the socket layer, which may be quartz
         * deciding inside a call of ours, and then the first frame of ours beneath it.
         */
        private fun ourCaller(): String =
            StackWalker.getInstance().walk { frames ->
                var decider: String? = null
                var ours: String? = null
                val walk = frames.iterator()
                while (walk.hasNext() && ours == null) {
                    val f = walk.next()
                    if (PLUMBING.any { prefix -> f.className.startsWith(prefix) }) continue
                    val name = frameName(f.className, f.methodName)
                    if (decider == null) decider = name
                    if (f.className.startsWith(OURS)) ours = name
                }
                when {
                    decider == null -> "unknown"
                    ours == null || ours == decider -> decider
                    else -> "$decider via $ours"
                }
            }
    }
}
