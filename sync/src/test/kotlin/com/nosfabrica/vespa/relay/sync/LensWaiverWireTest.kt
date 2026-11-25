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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.SyncUpstream
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * What reaches the socket on the reads that do not go through [RelayReads]: the NEG-OPEN and the
 * by-id fetches of a reconcile, the retraction audit's two reads, and the upstream push's
 * reconcile. A waiving relay sees `include:spam` in every filter; any other sees no `search`.
 */
class LensWaiverWireTest {
    private val vespa = RelayUrlNormalizer.normalize("wss://vespa.example")
    private val strfry = RelayUrlNormalizer.normalize("wss://strfry.example")
    private val waivers = LensWaivers(log = {}).also { it.learn(vespa, "auth-required: this relay answers through a web of trust; ask with `include:spam`.") }

    /** Every filter each relay was sent, by the command that carried it. */
    private class ScriptedRelays(
        private val scope: CoroutineScope,
    ) : WebsocketBuilder {
        val sent = CopyOnWriteArrayList<Pair<NormalizedRelayUrl, Pair<String, JsonObject>>>()

        override fun build(
            url: NormalizedRelayUrl,
            out: WebSocketListener,
        ): WebSocket =
            object : WebSocket {
                @Volatile
                private var open = false

                override fun needsReconnect() = false

                override fun connect() {
                    open = true
                    scope.launch { out.onOpen(0, false) }
                }

                override fun disconnect() {
                    if (open) {
                        open = false
                        out.onClosed(1000, "")
                    }
                }

                override fun send(msg: String): Boolean {
                    val frame = Json.parseToJsonElement(msg).jsonArray
                    val cmd = frame[0].jsonPrimitive.content
                    val subId = frame.getOrNull(1)?.jsonPrimitive?.content
                    if (cmd == "REQ" || cmd == "NEG-OPEN" || cmd == "COUNT") {
                        frame.drop(2).filterIsInstance<JsonObject>().forEach { sent += url to (cmd to it) }
                    }
                    val reply =
                        when (cmd) {
                            "REQ" -> Json.parseToJsonElement("""["EOSE","$subId"]""")
                            "NEG-OPEN" -> Json.parseToJsonElement("""["NEG-ERR","$subId","blocked: scripted"]""")
                            else -> null
                        }
                    if (reply != null) scope.launch { out.onMessage(reply.toString()) }
                    return true
                }
            }
    }

    private fun filtersTo(
        relays: ScriptedRelays,
        url: NormalizedRelayUrl,
    ) = relays.sent.filter { it.first == url }.map { it.second }

    private fun waives(f: JsonObject) = SearchQuery.parse(f["search"]?.jsonPrimitive?.content).includeSpam

    /** Runs [reads] against both relays over a scripted client, and checks what each was sent. */
    private fun assertWaivedOnlyWhereTaken(reads: suspend (NostrClient, NormalizedRelayUrl) -> Unit) =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                val relays = ScriptedRelays(scope)
                val client = NostrClient(relays, scope)
                client.connect()
                for (url in listOf(vespa, strfry)) withTimeoutOrNull(5_000) { runCatching { reads(client, url) } }
                val toVespa =
                    filtersTo(relays, vespa).filter { (cmd, f) ->
                        cmd != "REQ" || f["ids"]
                            ?.jsonArray
                            ?.singleOrNull()
                            ?.jsonPrimitive
                            ?.content != KEEP_ALIVE
                    }
                assertTrue(toVespa.isNotEmpty(), "the read reached the waiving relay")
                assertTrue(toVespa.all { waives(it.second) }, "every filter to the waiving relay carries include:spam: $toVespa")
                val toStrfry = filtersTo(relays, strfry)
                assertTrue(toStrfry.isNotEmpty(), "the read reached the other relay")
                assertTrue(toStrfry.none { "search" in it.second }, "no filter to it carries search: $toStrfry")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a negentropy window opens with the waiver, and its fallback page asks with it`() =
        assertWaivedOnlyWhereTaken { client, url ->
            val sync = ClientWindowSync(client, FilterWidths(), idleTimeoutMs = 2_000, waivers = waivers)
            val window = Filter(kinds = listOf(1), since = 100, until = 200)
            val local = NegentropyLocalIndex.of(NostrSemanticsStore(InMemoryEventIndex()))
            runCatching { sync.reconcile(url, window, local, 1_000, null, {}, {}) }
            sync.page(url, window) {}
        }

    @Test
    fun `the retraction audit reconciles and fetches with the waiver`() =
        assertWaivedOnlyWhereTaken { client, url ->
            val reads = ClientRetractionReads(client, waivers)
            runCatching { reads.reconcileIds(url, Filter(kinds = listOf(30382)), emptyList()) }
            reads.fetchIds(url, listOf("d".repeat(64))) {}
        }

    @Test
    fun `the upstream push reconciles with the waiver`() =
        assertWaivedOnlyWhereTaken { client, url ->
            val scope = CoroutineScope(SupervisorJob())
            try {
                val push = UpstreamPush(client, NostrSemanticsStore(InMemoryEventIndex()), 60, Semaphore(1), scope, waivers)
                push.pass(listOf(SyncUpstream("out", url, Filter(kinds = listOf(1)), trusted = false)))
            } finally {
                scope.cancel()
            }
        }

    private companion object {
        /** Quartz holds a reconcile's socket open with a REQ for this id; it is the client's, not the read's. */
        val KEEP_ALIVE = "f".repeat(64)
    }
}
