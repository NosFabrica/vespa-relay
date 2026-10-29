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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.peers.TorTransport
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.EOFException
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.util.concurrent.ConcurrentHashMap
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLException
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory

/**
 * Whether the TCP pre-probe measures the route the dial will take. The pre-probe connects
 * directly from this box, so for anything routed through Tor only the websocket dial counts.
 */
internal fun shouldPreProbe(
    url: NormalizedRelayUrl,
    tor: TorTransport?,
): Boolean = tor?.routes(url) != true

/** What the guard in front of a dial learned. Only [PROVED_UNREACHABLE] is a fact about the relay. */
enum class Reach {
    REACHABLE,

    /** The relay's host refused or does not exist, on a cause [Unreachability] accepts. */
    PROVED_UNREACHABLE,

    /** Our own transport cannot carry the dial; nothing was learned about the relay. */
    TRANSPORT_DOWN,

    /** The lookup failed with no reason left to read; the dial would hit the same cached answer. */
    UNEXPLAINED,

    /** Every address had the whole connect window and none answered: no proof, but a dial would wait out its own. */
    UNANSWERED,
}

/** What our own direct handshake says about a dial's TLS failure. */
enum class TlsAnswer {
    /** It succeeded under our default trust, so the dial's failure was our transport's. */
    ACCEPTED,

    /** The server's certificate failed our trust, or the server refused the handshake. */
    REJECTED,

    /** No handshake could be made or read from here; nothing was learned. */
    UNCHECKED,
}

/** Can we open a socket at all: the guard in front of every dial, shared so one url is judged one way. */
internal class ReachabilityProbe(
    private val tor: TorTransport?,
    /** Its own threads, sized to the dials it guards, so blocking connects never queue on the shared IO pool. */
    threads: Int = AliasFolding.DEFAULT_DIAL_CONCURRENCY,
    private val resolve: (String) -> Array<InetAddress> = InetAddress::getAllByName,
    /** Opens a socket to one address within the timeout, or throws why not. */
    private val connect: (InetSocketAddress, Int) -> Socket = ::openSocket,
    private val clock: () -> Long = System::currentTimeMillis,
    /** Our default trust; the dial's client trusts the same roots. */
    private val tlsFactory: SSLSocketFactory = SSLContext.getDefault().socketFactory,
    /** How long a [REUSABLE] answer stands; no longer than a sweep, so each sweep's fold asks afresh. */
    private val reuseMs: Long = REUSE_MS,
) {
    private val io: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(threads)

    /** Host:port -> an answer no claim is signed on, and when it was had; every pass of a sweep asks the same hosts. */
    private val lately = ConcurrentHashMap<String, Pair<Reach, Long>>()

    /**
     * Whether to dial, and when not, whose side the reason is on. One fresh lookup and connect:
     * the JVM caches a failed lookup without its reason, so a second attempt could not tell our
     * resolver failing from a name that does not exist. Only [REUSABLE] answers are reused, for [reuseMs].
     */
    suspend fun reach(url: NormalizedRelayUrl): Reach {
        if (tor?.routes(url) == true) return if (withContext(Dispatchers.IO) { tor.socksAnswers() }) Reach.REACHABLE else Reach.TRANSPORT_DOWN
        if (!shouldPreProbe(url, tor)) return Reach.REACHABLE
        val target = Target.of(url) ?: return Reach.REACHABLE
        val key = "${target.host}:${target.port}"
        lately[key]?.let { (answer, atMs) -> if (clock() - atMs < reuseMs) return answer }
        return probe(target).also { answer -> if (answer in REUSABLE) remember(key, answer) }
    }

    /** Past [MAX_REMEMBERED] the lapsed answers go, and all of them if none had: a forgotten host is only asked again. */
    private fun remember(
        key: String,
        answer: Reach,
    ) {
        if (lately.size >= MAX_REMEMBERED) {
            lately.entries.removeIf { clock() - it.value.second >= reuseMs }
            if (lately.size >= MAX_REMEMBERED) lately.clear()
        }
        lately[key] = answer to clock()
    }

    /** One lookup and connect, on the probe's own threads. */
    private suspend fun probe(target: Target): Reach =
        withContext(io) {
            val addresses =
                try {
                    resolve(target.host)
                } catch (e: IOException) {
                    return@withContext lookupFailed(e)
                }
            val attempt = open(target, addresses)
            attempt.socket?.close()
            if (attempt.socket != null) Reach.REACHABLE else judge(attempt.failures, attempt.complete, attempt.fullWindows)
        }

    /**
     * A direct handshake to [url] under our default trust, on the pre-probe's route, to check a
     * dial's TLS failure. A url routed through Tor cannot be checked from here.
     */
    suspend fun tls(url: NormalizedRelayUrl): TlsAnswer {
        if (!shouldPreProbe(url, tor) || !url.url.startsWith("wss://", ignoreCase = true)) return TlsAnswer.UNCHECKED
        val target = Target.of(url) ?: return TlsAnswer.UNCHECKED
        return withContext(io) {
            val addresses =
                try {
                    resolve(target.host)
                } catch (_: IOException) {
                    return@withContext TlsAnswer.UNCHECKED
                }
            val socket = open(target, addresses).socket ?: return@withContext TlsAnswer.UNCHECKED
            socket.use { handshake(it, target) }
        }
    }

    private fun handshake(
        socket: Socket,
        target: Target,
    ): TlsAnswer =
        try {
            socket.soTimeout = TLS_READ_TIMEOUT_MS
            (tlsFactory.createSocket(socket, target.host, target.port, true) as SSLSocket).use { ssl ->
                // The dial's client checks the name on the certificate, so this check must too.
                ssl.sslParameters = ssl.sslParameters.apply { endpointIdentificationAlgorithm = "HTTPS" }
                ssl.startHandshake()
                TlsAnswer.ACCEPTED
            }
        } catch (e: SSLException) {
            if (refusedByServer(e)) TlsAnswer.REJECTED else TlsAnswer.UNCHECKED
        } catch (_: IOException) {
            TlsAnswer.UNCHECKED
        }

    /** Where a url's socket goes. */
    private class Target(
        val host: String,
        val port: Int,
    ) {
        companion object {
            fun of(url: NormalizedRelayUrl): Target? {
                val uri = runCatching { java.net.URI(url.url) }.getOrNull() ?: return null
                val host = uri.host ?: return null
                val port =
                    when {
                        uri.port > 0 -> uri.port
                        url.url.startsWith("wss://", ignoreCase = true) -> 443
                        else -> 80
                    }
                return Target(host, port)
            }
        }
    }

    /** The first socket any address opened, or each tried address's failure; [complete] when none went untried. */
    private class Attempt(
        val socket: Socket?,
        val failures: List<Exception>,
        val complete: Boolean,
        /** Every address tried was handed the whole [PROBE_TIMEOUT_MS], so a timeout on it means that window. */
        val fullWindows: Boolean = true,
    )

    /**
     * The addresses in order under one deadline, stopping at the first socket or once the
     * failures so far can only judge [Reach.REACHABLE].
     */
    private fun open(
        target: Target,
        addresses: Array<InetAddress>,
    ): Attempt {
        val deadline = clock() + TOTAL_TIMEOUT_MS
        val failures = ArrayList<Exception>()
        var fullWindows = true
        for (address in addresses) {
            val left = deadline - clock()
            if (left <= 0) return Attempt(null, failures, complete = false)
            // A shortened window cannot make the timeouts so far `UNANSWERED`, so it is not spent on them.
            if (left < PROBE_TIMEOUT_MS && failures.isNotEmpty() && failures.all { it is SocketTimeoutException }) {
                return Attempt(null, failures, complete = false)
            }
            val window = minOf(PROBE_TIMEOUT_MS.toLong(), left).toInt()
            if (window < PROBE_TIMEOUT_MS) fullWindows = false
            try {
                return Attempt(connect(InetSocketAddress(address, target.port), window), failures, complete = true)
            } catch (e: IOException) {
                failures += e
                if (judge(failures, complete = true, fullWindows) == Reach.REACHABLE) return Attempt(null, failures, complete = false)
            }
        }
        return Attempt(null, failures, complete = true, fullWindows)
    }

    companion object {
        private const val PROBE_TIMEOUT_MS = 5_000

        /**
         * The answers a later ask may reuse: each still ends in a dial or in no claim. Proof is
         * never reused, since `dead` is signed on it, nor our own side, which is a moment.
         */
        private val REUSABLE = setOf(Reach.REACHABLE, Reach.UNANSWERED)

        /** The longest a [REUSABLE] answer stands, whatever the sweep. */
        internal const val REUSE_MS = 60L * 60 * 1000

        /** Hosts held at once; past it, the lapsed answers are dropped. */
        private const val MAX_REMEMBERED = 50_000

        /** Per read while the direct handshake runs. */
        private const val TLS_READ_TIMEOUT_MS = 5_000

        /** Across every address of one host; an address left untried proves nothing. */
        internal const val TOTAL_TIMEOUT_MS = 8_000L

        /**
         * A host's verdict over its addresses: proof only when every address failed with proof,
         * our side only when every address failed on ours, unanswered only when every address
         * timed out on a whole window, and any mix or gap left to the dial.
         */
        internal fun judge(
            failures: List<Exception>,
            complete: Boolean,
            fullWindows: Boolean = true,
        ): Reach =
            when {
                !complete || failures.isEmpty() -> Reach.REACHABLE
                failures.all { Unreachability.ourSide(it) } -> Reach.TRANSPORT_DOWN
                failures.all { Unreachability.proves(it) } -> Reach.PROVED_UNREACHABLE
                fullWindows && failures.all { it is SocketTimeoutException } -> Reach.UNANSWERED
                else -> Reach.REACHABLE
            }

        /** A handshake the server failed or refused; a connection dropped or stalled under it is neither. */
        private fun refusedByServer(e: SSLException): Boolean =
            (e is SSLHandshakeException || e is SSLPeerUnverifiedException) &&
                generateSequence(e.cause) { it.cause }.none { it is EOFException || it is SocketException || it is java.io.InterruptedIOException }

        /** A lookup that failed; one carrying no words we can place is the JVM's cached answer. */
        private fun lookupFailed(e: IOException): Reach =
            when {
                Unreachability.ourSide(e) -> Reach.TRANSPORT_DOWN
                Unreachability.proves(e) -> Reach.PROVED_UNREACHABLE
                e is UnknownHostException -> Reach.UNEXPLAINED
                else -> Reach.REACHABLE
            }

        private fun openSocket(
            address: InetSocketAddress,
            timeoutMs: Int,
        ): Socket {
            val socket = Socket()
            try {
                socket.connect(address, timeoutMs)
            } catch (e: IOException) {
                socket.close()
                throw e
            }
            return socket
        }
    }
}
