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
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.TcpProber
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Whether the TCP pre-probe measures the route the dial will take. [TcpProber] connects
 * directly from this box, so for anything routed through Tor only the websocket dial counts.
 */
internal fun shouldPreProbe(
    url: NormalizedRelayUrl,
    tor: TorTransport?,
): Boolean = tor?.routes(url) != true

/** Can we open a socket at all: the guard in front of every dial, shared so one url is judged one way. */
internal class ReachabilityProbe(
    private val tor: TorTransport?,
) {
    /**
     * Short-circuits only on proof: [TcpProber] folds refusal and timeout into one Boolean, so a
     * failure is re-run for its cause and believed only for what [Unreachability] accepts.
     * Publishes nothing; the fitness pass writes the `dead` verdict.
     */
    suspend fun reachable(url: NormalizedRelayUrl): Boolean {
        if (!shouldPreProbe(url, tor)) return true
        if (runCatching { TcpProber.tcpReachable(url) }.getOrDefault(true)) return true
        return cause(url)?.let { !Unreachability.proves(it) } ?: true
    }

    /** Our transport can carry it and something answers. */
    suspend fun canDial(url: NormalizedRelayUrl): Boolean = (tor?.routes(url) != true || tor.socksAnswers()) && reachable(url)

    /** Null when the retry succeeds or the url has no host. */
    private suspend fun cause(url: NormalizedRelayUrl): Exception? =
        withContext(Dispatchers.IO) {
            val uri = runCatching { java.net.URI(url.url) }.getOrNull() ?: return@withContext null
            val host = uri.host ?: return@withContext null
            val port =
                when {
                    uri.port > 0 -> uri.port
                    url.url.startsWith("wss://", ignoreCase = true) -> 443
                    else -> 80
                }
            try {
                java.net.Socket().use { it.connect(java.net.InetSocketAddress(host, port), PROBE_TIMEOUT_MS) }
                null
            } catch (e: java.io.IOException) {
                e
            }
        }

    companion object {
        private const val PROBE_TIMEOUT_MS = 5_000
    }
}
