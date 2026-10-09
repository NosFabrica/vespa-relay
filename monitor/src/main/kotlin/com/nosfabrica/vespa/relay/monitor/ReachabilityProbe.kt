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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

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
}

/** Can we open a socket at all: the guard in front of every dial, shared so one url is judged one way. */
internal class ReachabilityProbe(
    private val tor: TorTransport?,
) {
    /**
     * Whether to dial, and when not, whose side the reason is on. One fresh lookup and connect:
     * the JVM caches a failed lookup without its reason, so a second attempt could not tell our
     * resolver failing from a name that does not exist.
     */
    suspend fun reach(url: NormalizedRelayUrl): Reach {
        if (tor?.routes(url) == true) return if (withContext(Dispatchers.IO) { tor.socksAnswers() }) Reach.REACHABLE else Reach.TRANSPORT_DOWN
        if (!shouldPreProbe(url, tor)) return Reach.REACHABLE
        val failure = failure(url) ?: return Reach.REACHABLE
        return when {
            Unreachability.ourSide(failure) -> Reach.TRANSPORT_DOWN

            Unreachability.proves(failure) -> Reach.PROVED_UNREACHABLE

            // An unplaced failure is left to the dial, which reports what the relay said.
            else -> Reach.REACHABLE
        }
    }

    /** Our transport can carry it and something answers. */
    suspend fun canDial(url: NormalizedRelayUrl): Boolean = reach(url) == Reach.REACHABLE

    /** Null when some address of the host takes the connect, or the url has no host. */
    private suspend fun failure(url: NormalizedRelayUrl): Exception? =
        withContext(Dispatchers.IO) {
            val uri = runCatching { java.net.URI(url.url) }.getOrNull() ?: return@withContext null
            val host = uri.host ?: return@withContext null
            val port =
                when {
                    uri.port > 0 -> uri.port
                    url.url.startsWith("wss://", ignoreCase = true) -> 443
                    else -> 80
                }
            val addresses =
                try {
                    java.net.InetAddress.getAllByName(host)
                } catch (e: java.io.IOException) {
                    return@withContext e
                }
            var last: Exception? = null
            for (address in addresses) {
                try {
                    java.net.Socket().use { it.connect(java.net.InetSocketAddress(address, port), PROBE_TIMEOUT_MS) }
                    return@withContext null
                } catch (e: java.io.IOException) {
                    last = e
                }
            }
            last
        }

    companion object {
        private const val PROBE_TIMEOUT_MS = 5_000
    }
}
