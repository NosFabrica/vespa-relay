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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.ScoredSearch
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.http.contentType
import io.ktor.server.routing.routing
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** The relay's ORE-05 search backend, driven in-process over a fake store function. */
class OpenRankingRouteTest {
    private val house = "0e".repeat(32)
    private val pov = "0b".repeat(32)
    private val service = "5e".repeat(32)
    private val alice = "a1".repeat(32)
    private val bob = "b2".repeat(32)

    private data class Ask(
        val q: String,
        val observer: String,
        val limit: Int,
    )

    private val asked = mutableListOf<Ask>()

    private fun answering(vararg byObserver: Pair<String, ScoredSearch>): ScoredProfileSearch {
        val answers = byObserver.toMap()
        return { q, o, n ->
            asked += Ask(q, o, n)
            answers[o] ?: noList
        }
    }

    private val noList = ScoredSearch(emptyList(), null)

    private fun ready(vararg hits: ScoredSearch.Hit) = ScoredSearch(hits.toList(), service)

    private val logged = mutableListOf<String>()

    private fun ore(
        search: ScoredProfileSearch,
        concurrency: Int = 4,
        slotWaitMillis: Long = SLOT_WAIT_MILLIS,
        bodyWaitMillis: Long = BODY_WAIT_MILLIS,
        pressure: ServingPressure? = null,
    ) = OpenRanking(
        search,
        OpenRankingConfig(concurrency = concurrency),
        slotWaitMillis = slotWaitMillis,
        bodyWaitMillis = bodyWaitMillis,
        log = { logged += it },
        pressure = pressure,
    )

    private fun personal(p: String = pov) = """{"query":"alice","algorithm":"relevance-pov","pov":"$p"}"""

    private fun ApplicationTestBuilder.serving(ore: OpenRanking) = application { routing { openRanking(ore) } }

    private suspend fun HttpClient.search(body: String): HttpResponse =
        post("/search/pubkeys") {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private fun json(text: String): JsonElement = Json.parseToJsonElement(text)

    @Test
    fun `a global search ranks through the House Observer with each hit's own score, highest first`() =
        testApplication {
            serving(ore(answering(house to ready(ScoredSearch.Hit(alice, 812.5), ScoredSearch.Hit(bob, 3.0)))))
            val res = client.search("""{"observer":"$house","query":"alice"}""")
            assertEquals(HttpStatusCode.OK, res.status)
            assertEquals(json("""{"results":[{"pubkey":"$alice","rank":812.5},{"pubkey":"$bob","rank":3.0}],"ttl":300}"""), json(res.bodyAsText()))
            assertEquals(listOf(Ask("alice", house, 100)), asked, "limit defaults to 100")
        }

    @Test
    fun `a global search ignores any pov, even one that is not a pubkey`() =
        testApplication {
            serving(ore(answering(house to ready(ScoredSearch.Hit(alice, 1.0)))))
            for (p in listOf("\"$pov\"", "\"nonsense\"", "42")) {
                assertEquals(HttpStatusCode.OK, client.search("""{"observer":"$house","query":"alice","algorithm":"relevance","pov":$p}""").status, p)
            }
            assertEquals(List(3) { Ask("alice", house, 100) }, asked)
        }

    private suspend fun assertRefused(
        client: HttpClient,
        status: HttpStatusCode,
        body: String,
    ) {
        val res = client.search(body)
        assertEquals(status, res.status, body)
        val reason = assertNotNull(res.headers["X-Reason"], "every refusal says why: $body")
        assertEquals(json("""{"error":${Json.encodeToString(reason)}}"""), json(res.bodyAsText()), body)
    }

    @Test
    fun `a body that is not a JSON object is a 400`() =
        testApplication {
            serving(ore(answering(house to ready())))
            for (body in listOf("", "{", "not json", "[]", "\"alice\"", "null")) assertRefused(client, HttpStatusCode.BadRequest, body)
            assertEquals(emptyList(), asked)
        }

    @Test
    fun `every invalid field is a 422 with a reason, and never reaches the store`() =
        testApplication {
            serving(ore(answering(house to ready(), pov to ready())))
            val bad =
                listOf(
                    """{}""",
                    """{"observer":"$house","query":5}""",
                    """{"observer":"$house","query":""}""",
                    """{"observer":"$house","query":"  \u0000\u001f\u007f "}""",
                    """{"observer":"$house","query":"${"a".repeat(513)}"}""",
                    """{"observer":"$house","query":"alice","limit":0}""",
                    """{"observer":"$house","query":"alice","limit":-1}""",
                    """{"observer":"$house","query":"alice","limit":401}""",
                    """{"observer":"$house","query":"alice","limit":"ten"}""",
                    """{"observer":"$house","query":"alice","limit":1.5}""",
                    """{"observer":"$house","query":"alice","limit":true}""",
                    """{"observer":"$house","query":"alice","algorithm":"pagerank"}""",
                    """{"observer":"$house","query":"alice","algorithm":5}""",
                    """{"observer":"$house","query":"alice","algorithm":"relevance-pov"}""",
                    """{"observer":"$house","query":"alice","algorithm":"relevance-pov","pov":null}""",
                    """{"observer":"$house","query":"alice","algorithm":"relevance-pov","pov":"${"0b".repeat(31)}"}""",
                    """{"observer":"$house","query":"alice","algorithm":"relevance-pov","pov":"${"zz".repeat(32)}"}""",
                    """{"observer":"$house","query":"alice","algorithm":"relevance-pov","pov":42}""",
                    """{"observer":"$house","query":"alice","algorithm":"relevance-pov","pov":"${pov}zz"}""",
                    """{"observer":"${house}0e","query":"alice"}""",
                )
            for (body in bad) assertRefused(client, HttpStatusCode.UnprocessableEntity, body)
            assertEquals(emptyList(), asked)
        }

    @Test
    fun `an unknown algorithm is echoed bounded and without control characters, so the refusal stays a 422`() =
        testApplication {
            serving(ore(answering(house to ready())))
            for (algorithm in listOf("page\\nrank", "page\\r\\nX-Injected: 1", "\\u0000\\u0085", "x".repeat(15_000))) {
                val body = """{"observer":"$house","query":"alice","algorithm":"$algorithm"}"""
                assertRefused(client, HttpStatusCode.UnprocessableEntity, body)
                val reason = assertNotNull(client.search(body).headers["X-Reason"])
                assertTrue(reason.none { it.isISOControl() }, reason)
                assertTrue(reason.length < 200, "${reason.length} characters")
            }
            // Cut by character, never inside a surrogate pair; the header's latin-1 copy differs, so the body is read.
            val emoji = client.search("""{"observer":"$house","query":"alice","algorithm":"${"\\uD83D\\uDE00".repeat(100)}"}""")
            assertEquals(HttpStatusCode.UnprocessableEntity, emoji.status)
            assertEquals(json("""{"error":"Algorithm '${"\uD83D\uDE00".repeat(64)}...' is not supported by /search/pubkeys"}"""), json(emoji.bodyAsText()))
            assertEquals(emptyList(), asked)
        }

    @Test
    fun `a body over the cap is a 413 and never reaches the store, one at the cap is read`() =
        testApplication {
            serving(ore(answering(house to ready())))
            val prefix = """{"observer":"$house","query":"alice","pad":""""
            val atCap = prefix + "x".repeat(16 * 1024 - prefix.length - 2) + "\"}"
            assertEquals(HttpStatusCode.OK, client.search(atCap).status)
            assertRefused(client, HttpStatusCode.PayloadTooLarge, atCap.replace("\"}", "x\"}"))
            // Control characters are stripped after the read, so 18 KiB of them as JSON escapes still counts here.
            assertRefused(client, HttpStatusCode.PayloadTooLarge, """{"observer":"$house","query":"${"\\u0000".repeat(3_000)}alice"}""")
            assertEquals(listOf(Ask("alice", house, 100)), asked)
        }

    @Test
    fun `the query reaches the store without control characters, and the bounds are inclusive`() =
        testApplication {
            serving(ore(answering(house to ready(), pov to ready())))
            assertEquals(HttpStatusCode.OK, client.search("""{"observer":"$house","query":"al\u0000ice\n","limit":1}""").status)
            val longest = "\uD83D\uDE00".repeat(512)
            assertEquals(HttpStatusCode.OK, client.search("""{"observer":"$house","query":"$longest","limit":400}""").status, "512 characters, not 512 UTF-16 units")
            val upper = pov.uppercase()
            assertEquals(HttpStatusCode.OK, client.search("""{"observer":"$house","query":"bob","algorithm":"relevance-pov","pov":"$upper"}""").status)
            assertEquals(
                listOf(Ask("alice", house, 1), Ask(Json.decodeFromString<String>("\"$longest\""), house, 400), Ask("bob", pov, 100)),
                asked,
            )
        }

    @Test
    fun `results stop at limit even when the store answers more`() =
        testApplication {
            serving(ore(answering(house to ready(ScoredSearch.Hit(alice, 9.0), ScoredSearch.Hit(bob, 8.0), ScoredSearch.Hit(pov, 7.0)))))
            val res = client.search("""{"observer":"$house","query":"alice","limit":2}""")
            assertEquals(json("""{"results":[{"pubkey":"$alice","rank":9.0},{"pubkey":"$bob","rank":8.0}],"ttl":300}"""), json(res.bodyAsText()))
        }

    @Test
    fun `control characters do not count toward the query's length`() =
        testApplication {
            serving(ore(answering(house to ready())))
            val longest = "a".repeat(512)
            assertEquals(HttpStatusCode.OK, client.search("""{"observer":"$house","query":"\u0000$longest\u007f\n"}""").status)
            assertEquals(listOf(Ask(longest, house, 100)), asked)
        }

    @Test
    fun `a global search needs the proxy's House Observer, a valid pubkey`() =
        testApplication {
            serving(ore(answering(house to ready())))
            assertRefused(client, HttpStatusCode.UnprocessableEntity, """{"query":"alice"}""")
            assertRefused(client, HttpStatusCode.UnprocessableEntity, """{"query":"alice","observer":"npub1nope"}""")
            assertEquals(emptyList(), asked)
        }

    @Test
    fun `a personalized search needs no House Observer`() =
        testApplication {
            serving(ore(answering(pov to ready(ScoredSearch.Hit(alice, 1.0)))))
            assertEquals(HttpStatusCode.OK, client.search(personal()).status)
        }

    @Test
    fun `a personalized search ranks through the pov's own lens`() =
        testApplication {
            serving(ore(answering(house to ready(ScoredSearch.Hit(bob, 9.0)), pov to ready(ScoredSearch.Hit(alice, 7.25)))))
            val res = client.search(personal())
            assertEquals(HttpStatusCode.OK, res.status)
            assertEquals(json("""{"results":[{"pubkey":"$alice","rank":7.25}],"ttl":300}"""), json(res.bodyAsText()))
            assertEquals(listOf(Ask("alice", pov, 100)), asked)
        }

    @Test
    fun `a pov with no trust provider list is a 422, never the House Observer's view`() =
        testApplication {
            serving(ore(answering(house to ready(ScoredSearch.Hit(bob, 9.0)))))
            val res = client.search(personal())
            assertEquals(HttpStatusCode.UnprocessableEntity, res.status)
            assertContains(assertNotNull(res.headers["X-Reason"]), "trust provider list")
            assertEquals(listOf(Ask("alice", pov, 100)), asked)
        }

    @Test
    fun `a pov whose list names a rank service is its ranking, even with nothing scored yet - an empty 200, never a 202`() =
        testApplication {
            serving(ore(answering(pov to ready())))
            val res = client.search(personal())
            assertEquals(HttpStatusCode.OK, res.status)
            assertEquals(json("""{"results":[],"ttl":300}"""), json(res.bodyAsText()))
        }

    @Test
    fun `a global search whose House Observer has no list is a 503, never an empty 200`() =
        testApplication {
            serving(ore(answering(house to noList)))
            val res = client.search("""{"observer":"$house","query":"alice"}""")
            assertEquals(HttpStatusCode.ServiceUnavailable, res.status)
            assertContains(assertNotNull(res.headers["X-Reason"]), "House Observer")
        }

    @Test
    fun `a store that fails is a 503 with a reason, and the failure is logged without what was searched`() =
        testApplication {
            serving(ore({ _, _, _ -> throw IllegalArgumentException("vespa search 400: {\"errors\":[\"userQuery(alice)\"]}") }))
            val res = client.search("""{"observer":"$house","query":"alice"}""")
            assertEquals(HttpStatusCode.ServiceUnavailable, res.status)
            assertNotNull(res.headers["X-Reason"])
            val line = logged.single()
            assertContains(line, "vespa search 400")
            assertTrue("alice" !in line, "what was searched stays out of the log: $line")
        }

    @Test
    fun `a search is a read the mirror throttles on, an answered one a sample and a failed one none`() =
        testApplication {
            val pressure = ServingPressure()
            var fail = false
            serving(ore({ _, _, _ -> if (fail) error("vespa down") else ready() }, pressure = pressure))
            assertEquals(HttpStatusCode.OK, client.search("""{"observer":"$house","query":"alice"}""").status)
            assertEquals(1, pressure.sampleCount())
            fail = true
            assertEquals(HttpStatusCode.ServiceUnavailable, client.search("""{"observer":"$house","query":"alice"}""").status)
            assertEquals(1, pressure.sampleCount())
        }

    @Test
    fun `a body that stops arriving is a 408 and never reaches the store`() =
        testApplication {
            serving(ore(answering(house to ready()), bodyWaitMillis = 200))
            val res =
                client.post("/search/pubkeys") {
                    contentType(ContentType.Application.Json)
                    setBody(
                        object : OutgoingContent.WriteChannelContent() {
                            override val contentLength = 64L

                            override suspend fun writeTo(channel: ByteWriteChannel) {
                                channel.writeStringUtf8("""{"observer":""")
                                channel.flush()
                                delay(2_000)
                            }
                        },
                    )
                }
            assertEquals(HttpStatusCode.RequestTimeout, res.status)
            assertNotNull(res.headers["X-Reason"])
            assertEquals(emptyList(), asked)
        }

    @Test
    fun `past the total a search waits for a slot, and gets one that frees in time`() =
        testApplication {
            val running = Channel<Unit>(Channel.UNLIMITED)
            val release = CompletableDeferred<Unit>()
            serving(
                ore({ _, _, _ ->
                    running.send(Unit)
                    release.await()
                    ready()
                }, concurrency = 2),
            )
            coroutineScope {
                val held = List(2) { async { client.search("""{"observer":"$house","query":"alice"}""") } }
                repeat(2) { running.receive() }
                val queued = async { client.search("""{"observer":"$house","query":"bob"}""") }
                delay(100)
                release.complete(Unit)
                held.forEach { assertEquals(HttpStatusCode.OK, it.await().status) }
                assertEquals(HttpStatusCode.OK, queued.await().status, "the queued search ran once a slot freed")
            }
        }

    @Test
    fun `a search still waiting for a slot past the wait is a 503`() =
        testApplication {
            val running = Channel<Unit>(Channel.UNLIMITED)
            val release = CompletableDeferred<Unit>()
            serving(
                ore({ _, _, _ ->
                    running.send(Unit)
                    release.await()
                    ready()
                }, concurrency = 2, slotWaitMillis = 100),
            )
            coroutineScope {
                val held = List(2) { async { client.search("""{"observer":"$house","query":"alice"}""") } }
                repeat(2) { running.receive() }
                val busy = client.search("""{"observer":"$house","query":"bob"}""")
                assertEquals(HttpStatusCode.ServiceUnavailable, busy.status)
                assertNotNull(busy.headers["X-Reason"])
                release.complete(Unit)
                held.forEach { assertEquals(HttpStatusCode.OK, it.await().status) }
            }
            assertEquals(HttpStatusCode.OK, client.search("""{"observer":"$house","query":"carol"}""").status, "a finished search frees its slot")
        }

    private fun serve(
        ore: OpenRanking?,
        block: (port: Int) -> Unit,
    ) {
        val clearnet = RelayUrlNormalizer.normalize("ws://localhost:7777")
        val relay = NostrRelayServer(NostrSemanticsStore(InMemoryEventIndex(), relay = clearnet), clearnet)
        val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), openRanking = ore, wait = false)
        try {
            block(
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                },
            )
        } finally {
            server.stop(0, 0)
            relay.close()
        }
    }

    private val http =
        java.net.http.HttpClient
            .newHttpClient()

    private fun send(req: HttpRequest.Builder) = http.send(req.build(), BodyHandlers.ofString())

    @Test
    fun `serveRelay serves only the search, only when enabled, and exposes no reason header cross-origin`() {
        val post = { port: Int ->
            HttpRequest
                .newBuilder(URI("http://localhost:$port/search/pubkeys"))
                .POST(HttpRequest.BodyPublishers.ofString("""{"observer":"$house","query":"alice"}"""))
                .header("Origin", "https://client.example")
                .header("Content-Type", "application/json")
        }
        serve(ore(answering(house to noList))) { port ->
            assertEquals(404, send(HttpRequest.newBuilder(URI("http://localhost:$port/.well-known/open-ranking.json"))).statusCode(), "no capability document: the server is the provider")
            val refused = send(post(port))
            assertEquals(503, refused.statusCode(), "the House Observer has no list here")
            val exposed =
                refused
                    .headers()
                    .allValues("Access-Control-Expose-Headers")
                    .flatMap { it.split(',') }
                    .map { it.trim().lowercase() }
            assertTrue("retry-after" in exposed && "x-reason" !in exposed, "only NIP-FE's hints, nothing for a cross-origin ORE caller: $exposed")
        }
        serve(null) { port -> assertEquals(404, send(post(port)).statusCode()) }
    }
}
