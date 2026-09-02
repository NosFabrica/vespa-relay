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
 * THE WHOLE READ AGAINST REAL DATA: real kind-10040 declarations, pulled off a
 * real relay, fed into a real Vespa, read back through the config an operator
 * actually deploys.
 *
 * Everything else that covers this path is either an in-memory index (which
 * cannot have a document API, so it cannot show the difference #182 is about)
 * or a bench that prices one query. This runs `RelayDiscovery.discover` over
 * `router.conf.example`'s own `monitor { sources }` block — all 38 delegation
 * tags — and then re-answers the same question through the tags projection the
 * fix removed, so the two paths can be compared on the ONE thing that matters
 * besides the clock: whether they name the same relays.
 *
 * Asserts nothing. A probe against a live relay cannot pin a number that
 * somebody else's corpus decides.
 *
 * Needs a Vespa and network. Off by default:
 *
 *     DOCKER_MIN_API_VERSION=1.24 dockerd &
 *     docker run -d --name vespa -p 8080:8080 -p 19071:19071 vespaengine/vespa
 *     ./gradlew :peers:test --tests '*RelayListLiveProbe*' -DliveListProbe=true --rerun -i
 *
 * `--rerun` is load-bearing: the task is up-to-date-checked, so a second
 * identical run is SKIPPED and prints nothing, which reads as a silent pass.
 *
 *     -DliveListRelay=wss://…   the relay to pull declarations from
 *     -DliveListVespa=http://…  the engine to feed and read
 *     -DliveListKind=10002      another relay-list kind
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

                // THE OPERATOR'S OWN CONFIG, not a hand-built one: the 38
                // delegation tags as `router.conf.example` names them.
                val conf =
                    RouterConfigLoader.parse(
                        requireNotNull(
                            listOf(File("../router.conf.example"), File("router.conf.example")).firstOrNull { it.isFile },
                        ) { "missing router.conf.example" }.readText(),
                    )
                val monitor = requireNotNull(conf.monitor) { "the example has no monitor block" }
                val sources = monitor.sources.filter { it.filter.kinds?.contains(kind) == true }
                val selects = sources.flatMap { it.selects }
                println("LIVE-LIST config: ${sources.size} source(s) for kind $kind, ${selects.size} select(s)")

                val discovery = RelayDiscoveryConfig(sources = sources, refreshSeconds = monitor.sweepSeconds, exclude = monitor.exclude)

                val at = System.nanoTime()
                val found = RelayDiscovery.discover(store, discovery)
                val took = System.nanoTime() - at
                println("LIVE-LIST discover: ${found.size} relay(s) in ${secs(took)} — the path that ships")
                found.take(15).forEach { println("    ${it.url.url}${if (it.bindings.isEmpty()) "" else "  ${it.bindings}"}") }
                if (found.size > 15) println("    …and ${found.size - 15} more")

                // THE PATH THAT WAS REMOVED, for the same question. One corpus
                // visit per tag name, and this store is tiny — so read the
                // RATIO of the two clocks, never the visit's absolute number:
                // a visit's cost is the corpus, and a laptop corpus of a few
                // hundred events is the one size at which it looks cheap.
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

                // THE ANSWERS, COMPARED. The scan applies the router's url
                // rules and the projection hands back raw tag values, so the
                // projection's set is the SUPERSET the scan filters — anything
                // the scan found that the projection did not is a real
                // disagreement, and the other direction is the rules working.
                val scanned = found.map { it.url.url }.toSet()
                val onlyScan = scanned.filterNot { s -> viaVisit.any { it.trimEnd('/') == s.trimEnd('/') } }
                println("LIVE-LIST agreement: ${scanned.size} scanned, ${viaVisit.size} projected, ${onlyScan.size} the projection missed")
                onlyScan.take(10).forEach { println("    ONLY-SCAN $it") }
                val dropped = viaVisit.filterNot { v -> scanned.any { it.trimEnd('/') == v.trimEnd('/') } }
                println("LIVE-LIST the url rules refused ${dropped.size} raw value(s) the projection returned")
                dropped.take(10).forEach { println("    REFUSED $it") }
            }
        }

    /**
     * One REQ, collected to EOSE — through NIP-42 if the relay asks, which
     * `search-staging.brainstorm.world` does the moment the socket opens.
     *
     * The auth key is a THROWAWAY generated per run. This reads public relay
     * lists and writes nothing, so the identity only has to exist; borrowing
     * the deployment's own would put its pubkey in a stranger's logs for a read
     * anybody may make.
     *
     * Every control frame is printed. A probe whose whole job is "what does the
     * real relay do" must not swallow the sentence where it says no.
     */
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

        // UNRANKED, BY THE RELAY'S OWN INSTRUCTION. This deployment serves
        // through a web of trust, and a throwaway key has no kind 10040 — so a
        // plain REQ is answered with `restricted: no kind 10040 for you here —
        // ranked search will be empty` and an empty EOSE. Its `auth-required`
        // notice names the way out verbatim: "ask for the whole corpus unranked
        // with `include:spam`". That token is the whole search field, so the
        // read stays plain NIP-01 recall of the kind.
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
                                // Re-ask under the authenticated session: the
                                // first REQ may already have been refused.
                                req(ws, unranked = false)
                            }

                            // An EMPTY first answer is this relay's lens, not
                            // an empty corpus — see [req]. One retry, unranked.
                            "EOSE" -> {
                                if (events.isEmpty() && unranked.compareAndSet(false, true)) {
                                    println("LIVE-LIST empty under the trust lens — re-asking with include:spam")
                                    req(ws, unranked = true)
                                } else {
                                    done.countDown()
                                }
                            }

                            // NOT a finish line. A relay that refuses the
                            // unauthenticated REQ closes that subscription and
                            // then serves the next one; treating CLOSED as the
                            // end is how this probe first reported "0 events"
                            // against a relay holding hundreds.
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

    /**
     * The wire object as a quartz [Event]. Built field by field rather than
     * through a deserializer: this probe is about what the READ does with real
     * tags, so a parse that silently dropped one would be the worst possible
     * way to pass.
     */
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
