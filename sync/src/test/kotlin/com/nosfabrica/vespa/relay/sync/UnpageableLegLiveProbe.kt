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
package com.nosfabrica.vespa.relay.sync

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * The mirror's catch-up leg (the stream's kinds with `since` at the band's edge) through quartz's
 * own pager, against relays staging files as `abortedUnpageable`. Prints each page's cursor and
 * the ending; asserts nothing. Selected by `-DunpageableProbe=true`;
 * `-DunpageableLegs='wss://relay.example=<coveredTo>,...'` picks the legs.
 */
class UnpageableLegLiveProbe {
    @Test
    fun theCatchUpLegAtTheBandEdge() {
        if (System.getProperty("unpageableProbe") != "true") {
            println("[skip] UnpageableLegLiveProbe — set -DunpageableProbe=true to dial the public internet")
            return
        }
        val legs =
            (System.getProperty("unpageableLegs") ?: DEFAULT_LEGS)
                .split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .map { spec -> spec.substringBefore('=') to spec.substringAfter('=').toLong() }
        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val scope = CoroutineScope(SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
        println("=".repeat(90))
        println("The catch-up leg at the band edge, through fetchAllPages")
        println("=".repeat(90))
        try {
            runBlocking {
                for ((raw, coveredTo) in legs) {
                    val url = RelayUrlNormalizer.normalize(raw)
                    println("== ${url.url}  coveredTo=$coveredTo")
                    val shapes =
                        listOf(
                            "since=edge, kinds 0/10002/10040" to Filter(kinds = PROFILE_KINDS, since = coveredTo),
                            "since=edge, kinds 10002 only" to Filter(kinds = listOf(10002), since = coveredTo),
                            "since=edge+1, kinds 0/10002/10040" to Filter(kinds = PROFILE_KINDS, since = coveredTo + 1),
                        )
                    for ((label, filter) in shapes) {
                        val pages = mutableListOf<Long>()
                        val delivered = mutableListOf<String>()
                        val walked =
                            client.fetchAllPages(url, listOf(filter), IDLE_MS, onNewPage = { pages += it }) { event ->
                                delivered += "k${event.kind}@${event.createdAt}"
                            }
                        println(
                            "   $label -> end=${walked.end} downloaded=${walked.downloaded} " +
                                "pages-after-first=${pages.size}${if (pages.isNotEmpty()) " untils=$pages" else ""}" +
                                (if (delivered.isNotEmpty()) " delivered=${delivered.take(6)}" else ""),
                        )
                    }
                }
            }
        } finally {
            runCatching { client.disconnect() }
            scope.cancel()
        }
        println("=".repeat(90))
    }

    companion object {
        private val PROFILE_KINDS = listOf(0, 10002, 10040)
        private const val IDLE_MS = 10_000L

        /** Staging's unpageable rows, with their `coveredTo`. */
        private const val DEFAULT_LEGS =
            "wss://nostr.lopp.social=1765993162," +
                "wss://n.ka.st=1736768033," +
                "wss://nostr-relay.algotech.io=1734984802," +
                "wss://nostr-relay.dont-panic.dev=1767558630," +
                "wss://cfrelay.haorendashu.workers.dev=1770878459"
    }
}
