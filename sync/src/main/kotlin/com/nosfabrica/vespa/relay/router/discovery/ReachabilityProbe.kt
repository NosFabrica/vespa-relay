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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.relay.router.TorTransport
import com.nosfabrica.vespa.relay.router.shouldPreProbe
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.RelayMonitor
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.TcpProber
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * CAN WE OPEN A SOCKET AT ALL — the cheap guard in front of every dial, and the
 * only thing that ever looks at most discovered urls.
 *
 * Shared by the fan-out and the probe passes so one url is judged one way.
 */
internal class ReachabilityProbe(
    private val tor: TorTransport?,
    private val monitor: RelayMonitor?,
) {
    /**
     * Only a NEGATIVE result is published, and only for a cause that proves it.
     *
     * [TcpProber] answers with a Boolean, so a refusal and a timeout arrive as
     * the same value and they are not the same claim: a refusal proves nobody is
     * listening, a timeout is as likely to be our own socket budget. Publishing
     * on the Boolean signed 5,001 unreachable records in an hour, of which 732
     * urls across 423 hosts answered a REQ perfectly well. So a failure is
     * re-run once for its cause and published only for what [Unreachability]
     * accepts — the extra connect is paid on the failing path alone.
     */
    suspend fun reachable(url: NormalizedRelayUrl): Boolean {
        if (!shouldPreProbe(url, tor)) return true
        val ok = runCatching { TcpProber.tcpReachable(url) }.getOrDefault(true)
        if (!ok) {
            cause(url)?.takeIf { Unreachability.proves(it) }?.let {
                monitor?.observer?.record(url, reachable = false, error = "tcp: ${it.javaClass.simpleName}")
            }
        }
        return ok
    }

    /** Our transport can carry it AND something answers — what a probe pass asks. */
    suspend fun canDial(url: NormalizedRelayUrl): Boolean = (tor?.routes(url) != true || tor.socksAnswers()) && reachable(url)

    /**
     * Null when it unexpectedly succeeds — the pre-probe's budget is tight and a
     * merely slow host is itself a reason not to have published — or when the
     * url has no host to dial.
     */
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
        /** Short: the answer wanted here is "refused or not", not "how slow". */
        private const val PROBE_TIMEOUT_MS = 5_000
    }
}
