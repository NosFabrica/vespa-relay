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

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test

/**
 * Pulls live kind-10040 declarations into a real Vespa, runs [RelayDiscovery.discover]
 * over `monitor.conf.example`'s sources, and re-answers through the tags projection
 * so the two can be compared. Asserts nothing. Selected by `-DliveListProbe=true`;
 * `-DliveListRelay`, `-DliveListVespa` and `-DliveListKind` override the defaults.
 */
class RelayListLiveProbe {
    private val enabled = System.getProperty("liveListProbe") == "true"
    private val source = System.getProperty("liveListRelay") ?: "wss://search-staging.brainstorm.world"
    private val vespa = System.getProperty("liveListVespa") ?: "http://localhost:8080"
    private val kind = System.getProperty("liveListKind")?.toIntOrNull() ?: 10040

    @Test
    fun `real declarations, real engine, the operator's own config`() =
        runBlocking {
            if (!enabled) return@runBlocking println("LIVE-LIST skipped — pass -DliveListProbe=true")

            val pulled = pull(source, kind)
            println("LIVE-LIST pulled ${pulled.size} kind-$kind event(s) from $source")
            if (pulled.isEmpty()) return@runBlocking println("LIVE-LIST nothing to read — stopping")

            VespaEventStore.open(vespa, autoDeploy = true).use { store ->
                var fed = 0
                for (event in pulled) {
                    runCatching { store.insert(event) }.onSuccess { fed++ }
                }
                println("LIVE-LIST fed $fed of ${pulled.size}; store holds ${store.count(Filter(kinds = listOf(kind)))}")

                fun at(name: String) = requireNotNull(listOf(File("../$name"), File(name)).firstOrNull { it.isFile }) { "missing $name" }
                val conf =
                    RouterConfigLoader.parse(
                        at("sync.conf.example").readText(),
                        monitorHocon = at("monitor.conf.example").readText(),
                    )
                val monitor = requireNotNull(conf.monitor) { "the example has no monitor config" }
                val sources = monitor.sources.orEmpty().filter { it.filter.kinds?.contains(kind) == true }
                val selects = sources.flatMap { it.selects }
                println("LIVE-LIST config: ${sources.size} source(s) for kind $kind, ${selects.size} select(s)")

                val discovery = RelayDiscoveryConfig(sources = sources, refreshSeconds = monitor.sweepSeconds, exclude = monitor.exclude)

                val at = System.nanoTime()
                val found = RelayDiscovery.discover(store, discovery)
                val took = System.nanoTime() - at
                println("LIVE-LIST discover: ${found.size} relay(s) in ${secs(took)} — the path that ships")
                found.take(15).forEach { println("    ${it.url.url}${if (it.bindings.isEmpty()) "" else "  ${it.bindings}"}") }
                if (found.size > 15) println("    …and ${found.size - 15} more")

                // One corpus visit per tag name; read the ratio of the two clocks.
                val viaVisit = LinkedHashSet<String>()
                val visitAt = System.nanoTime()
                for (select in selects.filter { it.tag != null && it.bindings.isEmpty() }) {
                    store.store
                        .distinctTagValues(
                            filter = Filter(kinds = listOf(kind)),
                            tagName = select.tag!!,
                            valueIndex = select.urlIndex,
                            where = { tag -> select.where.isEmpty() || select.where.any { it.matches(tag.toTypedArray()) } },
                        ).forEach(viaVisit::add)
                }
                val visitTook = System.nanoTime() - visitAt
                println("LIVE-LIST projection: ${viaVisit.size} raw value(s) in ${secs(visitTook)} over ${selects.size} corpus walk(s)")

                // The projection is unfiltered, so only a url the scan found and it missed is a disagreement.
                val scanned = found.map { it.url.url }.toSet()
                val onlyScan = scanned.filterNot { s -> viaVisit.any { it.trimEnd('/') == s.trimEnd('/') } }
                println("LIVE-LIST agreement: ${scanned.size} scanned, ${viaVisit.size} projected, ${onlyScan.size} the projection missed")
                onlyScan.take(10).forEach { println("    ONLY-SCAN $it") }
                val dropped = viaVisit.filterNot { v -> scanned.any { it.trimEnd('/') == v.trimEnd('/') } }
                println("LIVE-LIST the url rules refused ${dropped.size} raw value(s) the projection returned")
                dropped.take(10).forEach { println("    REFUSED $it") }
            }
        }

    /** One REQ collected to EOSE, answering NIP-42 with a throwaway key; every control frame is printed. */
    private fun pull(
        url: String,
        kind: Int,
    ): List<Event> {
        val client = OkHttpClient.Builder().readTimeout(90, TimeUnit.SECONDS).build()
        val events = java.util.Collections.synchronizedList(ArrayList<Event>())
        val done = CountDownLatch(1)
        val signer = NostrSignerSync()
        val asked =
            java.util.concurrent.atomic
                .AtomicInteger()
        val unranked =
            java.util.concurrent.atomic
                .AtomicBoolean(false)

        // Under the relay's trust lens a throwaway key gets nothing; `include:spam` is the unranked ask.
        fun req(
            ws: WebSocket,
            unranked: Boolean,
        ) {
            val sub = "probe${asked.incrementAndGet()}"
            val filter = if (unranked) """{"kinds":[$kind],"search":"include:spam"}""" else """{"kinds":[$kind]}"""
            ws.send("""["REQ","$sub",$filter]""")
        }

        val socket =
            client.newWebSocket(
                Request.Builder().url(url).build(),
                object : WebSocketListener() {
                    override fun onOpen(
                        ws: WebSocket,
                        response: Response,
                    ) = req(ws, unranked = false)

                    override fun onMessage(
                        ws: WebSocket,
                        text: String,
                    ) {
                        val frame = runCatching { Json.parseToJsonElement(text).jsonArray }.getOrNull() ?: return
                        when (val verb = frame.firstOrNull()?.jsonPrimitive?.content) {
                            "EVENT" -> {
                                frame.getOrNull(2)?.let { eventOf(it.jsonObject) }?.let(events::add)
                            }

                            "AUTH" -> {
                                val challenge = frame.getOrNull(1)?.jsonPrimitive?.content ?: return
                                println("LIVE-LIST relay asked for NIP-42; answering with a throwaway key")
                                val auth =
                                    signer.sign<Event>(
                                        System.currentTimeMillis() / 1000,
                                        22242,
                                        arrayOf(arrayOf("relay", url), arrayOf("challenge", challenge)),
                                        "",
                                    )
                                ws.send("""["AUTH",${auth.toJson()}]""")
                                // Re-ask under the authenticated session: the first REQ may already have been refused.
                                req(ws, unranked = false)
                            }

                            // An empty first answer is the trust lens, not an empty corpus: one retry, unranked.
                            "EOSE" -> {
                                if (events.isEmpty() && unranked.compareAndSet(false, true)) {
                                    println("LIVE-LIST empty under the trust lens — re-asking with include:spam")
                                    req(ws, unranked = true)
                                } else {
                                    done.countDown()
                                }
                            }

                            // Not a finish line: a refused subscription is followed by the next one being served.
                            "CLOSED", "NOTICE", "OK" -> {
                                println("LIVE-LIST relay said: $text".take(300))
                            }

                            else -> {
                                println("LIVE-LIST unhandled frame '$verb'")
                            }
                        }
                    }

                    override fun onFailure(
                        ws: WebSocket,
                        t: Throwable,
                        response: Response?,
                    ) {
                        System.err.println("LIVE-LIST websocket failed: ${t.message}")
                        done.countDown()
                    }
                },
            )
        if (!done.await(90, TimeUnit.SECONDS)) println("LIVE-LIST no EOSE inside 90s — taking what arrived")
        socket.close(1000, null)
        client.dispatcher.executorService.shutdown()
        return events.toList()
    }

    /** Built field by field rather than through a deserializer, so a silently dropped tag cannot pass. */
    private fun eventOf(o: kotlinx.serialization.json.JsonObject): Event? =
        runCatching {
            Event(
                o["id"]!!.jsonPrimitive.content,
                o["pubkey"]!!.jsonPrimitive.content,
                o["created_at"]!!.jsonPrimitive.content.toLong(),
                o["kind"]!!.jsonPrimitive.content.toInt(),
                (o["tags"] as JsonArray).map { tag -> tag.jsonArray.map { it.jsonPrimitive.content }.toTypedArray() }.toTypedArray(),
                o["content"]?.jsonPrimitive?.content.orEmpty(),
                o["sig"]!!.jsonPrimitive.content,
            )
        }.getOrNull()

    private fun secs(nanos: Long) = "%.4fs".format(nanos / 1_000_000_000.0)
}
