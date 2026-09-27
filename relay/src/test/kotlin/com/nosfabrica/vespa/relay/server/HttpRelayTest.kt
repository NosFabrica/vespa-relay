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
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip86RelayManagement.server.BanStore
import com.vitorpamplona.quartz.nip98HttpAuth.HTTPAuthorizationEvent
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Collections
import java.util.zip.GZIPInputStream
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

/** NIP-FE over a real Netty server: one client frame posted to the relay's URL, the socket's own frames back, nothing left open. */
class HttpRelayTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val origin = "https://relay.example"
    private val onion = "http://${"n".repeat(56)}.onion"

    /** Records whose lens each ranked read ran through; a query for kind 7 never answers. */
    private class RecordingIndex : EventIndex {
        val inner = InMemoryEventIndex()
        val observers = Collections.synchronizedList(mutableListOf<String?>())

        override suspend fun get(id: String) = inner.get(id)

        override suspend fun put(doc: EventDoc) = inner.put(doc)

        override suspend fun remove(id: String) = inner.remove(id)

        override suspend fun search(query: EventQuery): List<EventDoc> {
            if (query.kinds?.contains(STALLED_KIND) == true) awaitCancellation()
            if (query.search != null || query.ranking != null) observers += query.observer
            return inner.search(query)
        }

        override suspend fun count(query: EventQuery) = inner.count(query)

        override suspend fun countByAuthor(query: EventQuery) = inner.countByAuthor(query)

        override fun close() {}
    }

    /** Answers what it holds, then never finishes a read that asks for [TRICKLE_KIND]: a store mid-page. */
    private class TrickleStore(
        private val inner: IEventStore,
    ) : IEventStore by inner {
        override suspend fun rawQuery(
            filters: List<Filter>,
            onEach: (RawEvent) -> Unit,
        ) {
            inner.rawQuery(filters, onEach)
            if (filters.any { it.kinds?.contains(TRICKLE_KIND) == true }) awaitCancellation()
        }
    }

    private val index = RecordingIndex()
    private val relay = NostrRelayServer(TrickleStore(NostrSemanticsStore(index, relay = relayUrl)), relayUrl)
    private val alice = NostrSignerSync()
    private val client = HttpClient.newHttpClient()

    private fun serving(
        gate: HttpRelayGate = HttpRelayGate(0, 0),
        deadlineMs: Long = 10_000,
        clients: ClientAddresses = ClientAddresses(),
        commands: Boolean = true,
        admin: Nip86Admin? = null,
        bodyTimeoutMs: Long = HttpRelay.DEFAULT_BODY_TIMEOUT_MS,
        block: (base: String) -> Unit,
    ) {
        val server =
            serveRelay(
                relay = relay,
                port = 0,
                nip11 = Nip11Info(),
                httpRelay = if (commands) HttpRelay(gate, deadlineMs, origins = { listOf(origin, onion) }, clients = clients, bodyTimeoutMs = bodyTimeoutMs) else null,
                admin = admin,
                wait = false,
            )
        try {
            val port =
                runBlocking {
                    server.engine
                        .resolvedConnectors()
                        .first()
                        .port
                }
            block("http://127.0.0.1:$port")
        } finally {
            server.stop(0, 0)
        }
    }

    @AfterTest
    fun tearDown() {
        relay.close()
    }

    private fun publish(vararg notes: Event) {
        val out = Collections.synchronizedList(mutableListOf<String>())
        val session = relay.connect { out.add(it) }
        try {
            runBlocking { notes.forEach { session.receive("""["EVENT",${it.toJson()}]""") } }
            val deadline = System.currentTimeMillis() + 10_000
            while (synchronized(out) { out.count { it.startsWith("""["OK"""") } } < notes.size) {
                if (System.currentTimeMillis() > deadline) fail("the notes were never stored: $out")
                Thread.sleep(20)
            }
        } finally {
            session.close()
        }
    }

    private fun note(content: String) = alice.sign<Event>(1_700_000_000L, 1, emptyArray(), content)

    /** One HTTP answer, whole. */
    private class Answer(
        val status: Int,
        val headers: java.net.http.HttpHeaders,
        val body: String,
    ) {
        fun header(name: String): String? = headers.firstValue(name).orElse(null)

        val lines: List<String> get() = body.lines().filter { it.isNotEmpty() }
    }

    private fun post(
        url: String,
        body: String,
        authorization: String? = null,
        forwardedFor: List<String> = emptyList(),
        contentType: String? = null,
        from: String? = null,
    ): Answer {
        val request =
            HttpRequest
                .newBuilder(URI(url))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .apply { contentType?.let { header("Content-Type", it) } }
                .apply { from?.let { header("Origin", it) } }
                .apply { authorization?.let { header("Authorization", it) } }
                .apply { forwardedFor.forEach { header("X-Forwarded-For", it) } }
                .build()
        val raw = client.send(request, HttpResponse.BodyHandlers.ofString())
        return Answer(raw.statusCode(), raw.headers(), raw.body())
    }

    /** A NIP-98 token for [body], signed at the relay's URL [at]. */
    private fun token(
        body: String,
        at: String = origin,
        by: NostrSignerSync = alice,
    ): String =
        by
            .sign(HTTPAuthorizationEvent.build(at, "POST", body.encodeToByteArray(), System.currentTimeMillis() / 1000) {})
            .toAuthToken()

    /** The REQ frame for [filters], one filter or a JSON array of them, under the id [SUB]. */
    private fun req(filters: String) = """["REQ","$SUB",${filters.trim().removeSurrounding("[", "]")}]"""

    private fun count(filters: String) = """["COUNT","$SUB",${filters.trim().removeSurrounding("[", "]")}]"""

    private fun event(json: String) = """["EVENT",$json]"""

    @Test
    fun `a REQ answers the stored events and ends at EOSE`() {
        val first = note("first")
        val second = note("second")
        publish(first, second)
        serving { base ->
            val response = post(base, req("""{"kinds":[1],"search":"include:spam"}"""))
            assertEquals(200, response.status, response.body)
            assertEquals("application/x-ndjson", response.header("Content-Type")!!.substringBefore(';'))
            val lines = response.lines
            assertEquals("""["EOSE","$SUB"]""", lines.last(), "the answer ends on its EOSE: $lines")
            val events = lines.dropLast(1)
            assertTrue(events.all { it.startsWith("""["EVENT",""") }, "every other line is an EVENT frame: $lines")
            assertEquals(setOf(first.id, second.id), events.map { l -> listOf(first, second).first { it.id in l }.id }.toSet())
        }
    }

    @Test
    fun `an array body is the REQ's filters, ORed`() {
        val first = note("first")
        publish(first)
        serving { base ->
            val response = post(base, req("""[{"ids":["${first.id}"],"search":"include:spam"},{"kinds":[30000],"search":"include:spam"}]"""))
            assertEquals(200, response.status, response.body)
            assertEquals(2, response.lines.size, response.body)
            assertTrue(first.id in response.lines.first())
        }
    }

    @Test
    fun `a COUNT answers one COUNT frame`() {
        publish(note("a"), note("b"))
        serving { base ->
            val response = post(base, count("""{"kinds":[1],"search":"include:spam"}"""))
            assertEquals(200, response.status, response.body)
            val lines = response.lines
            assertEquals(1, lines.size, response.body)
            assertTrue(lines.single().startsWith("""["COUNT",""") && "\"count\":2" in lines.single(), response.body)
        }
    }

    @Test
    fun `an anonymous read with no lens is told to sign`() {
        serving { base ->
            val response = post(base, req("""{"kinds":[1]}"""))
            assertEquals(401, response.status)
            assertEquals("Nostr", response.header("WWW-Authenticate"))
            assertTrue(response.lines.single().startsWith("""["CLOSED","$SUB","auth-required:"""), response.body)
        }
    }

    @Test
    fun `a NIP-98 read needs no lens and ranks through the signer`() {
        publish(note("hello world"))
        serving { base ->
            val body = """{"kinds":[1],"search":"hello"}"""
            val response = post(base, req(body), token(req(body)))
            assertEquals(200, response.status, response.body)
            assertEquals("""["EOSE","$SUB"]""", response.lines.last())
            assertEquals(listOf<String?>(alice.pubKey), index.observers.distinct(), "the signer is the lens")
        }
    }

    @Test
    fun `a token signed at the onion address verifies there`() {
        serving { base ->
            val body = """{"kinds":[1]}"""
            val response = post(base, req(body), token(req(body), at = onion))
            assertEquals(200, response.status, response.body)
        }
    }

    @Test
    fun `a token for another url or another body is refused, and one may be sent again`() {
        serving { base ->
            val body = """{"kinds":[1]}"""
            val elsewhere = post(base, req(body), token(req(body), at = "https://other.example"))
            assertEquals(401, elsewhere.status, elsewhere.body)
            assertTrue("url mismatch" in elsewhere.body, elsewhere.body)

            val otherBody = post(base, req("""{"kinds":[0]}"""), token(req(body)))
            assertEquals(401, otherBody.status, otherBody.body)
            assertTrue("payload" in otherBody.body, otherBody.body)

            val signed = token(req(body))
            assertEquals(200, post(base, req(body), signed).status)
            val again = post(base, req(body), signed)
            assertEquals(200, again.status, "any instance may answer it, so none remembers it: ${again.body}")
        }
    }

    @Test
    fun `a body that is not one REQ, COUNT or EVENT frame is a 400 NOTICE and one over the message cap a 413`() {
        serving { base ->
            // Refused before any session opens: not a frame, a bare filter (NIP-FE's old body), or a
            // command HTTP does not carry.
            for (bad in listOf("", "not json", "[]", "\"REQ\"", """{"kinds":[1]}""", """["REQ","q",1]""", """["CLOSE","q"]""", """["AUTH",{}]""")) {
                val response = post(base, bad)
                assertEquals(400, response.status, "'$bad' -> ${response.body}")
                assertTrue(response.lines.single().startsWith("""["NOTICE","invalid:"""), response.body)
            }
            val huge = req("""{"search":"include:spam ${"x".repeat(300_000)}"}""")
            val tooBig = post(base, huge)
            assertEquals(413, tooBig.status)
            assertTrue(tooBig.lines.single().startsWith("""["NOTICE","invalid:"""), tooBig.body)
        }
    }

    @Test
    fun `the subscription id is the client's, and every frame carries it back`() {
        val first = note("picked")
        publish(first)
        serving { base ->
            val response = post(base, """["REQ","mine-42",{"ids":["${first.id}"],"search":"include:spam"}]""")
            assertEquals(200, response.status, response.body)
            assertTrue(response.lines.first().startsWith("""["EVENT","mine-42",{"""), response.body)
            assertEquals("""["EOSE","mine-42"]""", response.lines.last())
            val counted = post(base, """["COUNT","c7",{"ids":["${first.id}"],"search":"include:spam"}]""")
            assertTrue(counted.lines.single().startsWith("""["COUNT","c7",{"""), counted.body)
        }
    }

    @Test
    fun `a command needs no Content-Type, and a NIP-86 one on the relay URL is not a command`() {
        serving { base ->
            val bare = post(base, req("""{"kinds":[1],"search":"include:spam"}"""))
            assertEquals(200, bare.status, "no Content-Type at all: ${bare.body}")
            val typed = post(base, req("""{"kinds":[1],"search":"include:spam"}"""), contentType = "text/plain;charset=UTF-8")
            assertEquals(200, typed.status, "a browser's no-preflight type: ${typed.body}")
            // This server mounts no admin rpc, so the call finds nothing rather than being run as a frame.
            val rpc = post(base, """{"method":"supportedmethods","params":[]}""", contentType = "application/nostr+json+rpc")
            assertEquals(404, rpc.status, rpc.body)
        }
    }

    private fun admin() = Nip86Admin(BanStore(), setOf(alice.pubKey), origin, purge = {})

    @Test
    fun `the relay URL's POST is NIP-86 for its Content-Type and a command for any other`() {
        serving(admin = admin()) { base ->
            val rpc = post(base, """{"method":"supportedmethods","params":[]}""", contentType = "application/nostr+json+rpc; charset=utf-8")
            assertEquals(401, rpc.status, "the admin rpc answered it, asking for its own NIP-98: ${rpc.body}")
            assertEquals("Missing NIP-98 Authorization", rpc.body)
            val command = post(base, req("""{"kinds":[1],"search":"include:spam"}"""))
            assertEquals(200, command.status, command.body)
            assertEquals("""["EOSE","$SUB"]""", command.lines.last())
        }
        // With the commands off every POST is the rpc's, as it was before NIP-FE.
        serving(commands = false, admin = admin()) { base ->
            val frame = post(base, req("""{"kinds":[1],"search":"include:spam"}"""))
            assertEquals(401, frame.status, frame.body)
            assertEquals("Missing NIP-98 Authorization", frame.body)
        }
    }

    @Test
    fun `a filter the relay refuses keeps the relay's own reason`() {
        serving { base ->
            // Over MAX_FILTERS: quartz's LimitsPolicy, reached through the same session as the websocket.
            val filters = (1..25).joinToString(",", "[", "]") { """{"kinds":[$it],"search":"include:spam"}""" }
            val response = post(base, req(filters))
            assertEquals(400, response.status, response.body)
            assertEquals("""["CLOSED","$SUB","invalid: too many filters (max 20)"]""", response.lines.single())
        }
    }

    @Test
    fun `events are streamed as the store finds them, and a deadline mid-answer ends in CLOSED`() {
        val first = note("found")
        publish(first)
        serving(deadlineMs = 1_500) { base ->
            val response = post(base, req("""{"kinds":[1,$TRICKLE_KIND],"search":"include:spam"}"""))
            assertEquals(200, response.status, response.body)
            assertTrue(first.id in response.lines.first(), "the event went out before the store finished: ${response.body}")
            assertTrue(response.lines.last().startsWith("""["CLOSED","$SUB","error: the answer ran past"""), response.body)
            assertEquals(2, response.lines.size, response.body)
        }
    }

    @Test
    fun `the first line reaches the client long before the answer ends, gzipped or not`() {
        publish(note("early"))
        serving(deadlineMs = 2_000) { base ->
            for (gzip in listOf(false, true)) {
                val request =
                    HttpRequest
                        .newBuilder(URI(base))
                        .POST(HttpRequest.BodyPublishers.ofString(req("""{"kinds":[1,$TRICKLE_KIND],"search":"include:spam"}""")))
                        .apply { if (gzip) header("Accept-Encoding", "gzip") }
                        .build()
                val startedMs = System.currentTimeMillis()
                val raw = client.send(request, HttpResponse.BodyHandlers.ofInputStream())
                assertEquals(if (gzip) "gzip" else null, raw.headers().firstValue("Content-Encoding").orElse(null))
                // Read by chunk, not through a Reader: a Reader over GZIPInputStream waits for more than one flush.
                val body = if (gzip) GZIPInputStream(raw.body()) else raw.body()
                val text = java.io.ByteArrayOutputStream()
                var firstLineMs: Long? = null
                val chunk = ByteArray(8192)
                body.use {
                    while (true) {
                        val n = it.read(chunk)
                        if (n < 0) break
                        text.write(chunk, 0, n)
                        if (firstLineMs == null && '\n' in text.toString()) firstLineMs = System.currentTimeMillis() - startedMs
                    }
                }
                val endMs = System.currentTimeMillis() - startedMs
                val lines = text.toString().lines().filter { it.isNotEmpty() }
                assertTrue(lines.first().startsWith("""["EVENT","""), lines.toString())
                assertTrue(lines.last().startsWith("""["CLOSED""""), lines.toString())
                val first = assertNotNull(firstLineMs)
                assertTrue(first < 1_000 && endMs >= 2_000, "gzip=$gzip: first line at ${first}ms, end at ${endMs}ms")
            }
        }
    }

    @Test
    fun `a read that finds nothing before the deadline is a 503`() {
        serving(deadlineMs = 1_000) { base ->
            val response = post(base, req("""{"kinds":[$STALLED_KIND],"search":"include:spam"}"""))
            assertEquals(503, response.status, response.body)
            assertEquals("1", response.header("Retry-After"))
        }
    }

    @Test
    fun `a client over its share is refused while another client is served`() {
        val stall = """{"kinds":[$STALLED_KIND],"search":"include:spam"}"""
        val quick = """{"kinds":[1],"search":"include:spam"}"""
        val gate = HttpRelayGate(perClient = 1, total = 0)
        val viaProxy = ClientAddresses("X-Forwarded-For", listOf(Cidr.parse("127.0.0.1")!!))
        serving(gate = gate, deadlineMs = 3_000, clients = viaProxy) { base ->
            // The proxy's own hop is the last entry across every line; what the client wrote before it is its own claim.
            val stalled = Thread { post(base, req(stall), forwardedFor = listOf("10.0.0.9", "10.0.0.1")) }.also { it.start() }
            awaitInFlight(gate, 1)

            val refused = post(base, req(quick), forwardedFor = listOf("10.0.0.7, 10.0.0.1"))
            assertEquals(429, refused.status, refused.body)
            assertTrue(refused.lines.single().startsWith("""["NOTICE","rate-limited:"""), refused.body)
            assertEquals(200, post(base, req(quick), forwardedFor = listOf("10.0.0.2")).status, "another address has its own share")

            stalled.join()
            assertEquals(200, post(base, req(quick), forwardedFor = listOf("10.0.0.1")).status, "the slot frees when the command ends")
        }
    }

    @Test
    fun `behind two trusted proxies the client is the address before them`() {
        val gate = HttpRelayGate(perClient = 1, total = 0)
        val twoHops = ClientAddresses("X-Forwarded-For", listOf(Cidr.parse("127.0.0.1")!!, Cidr.parse("10.0.0.0/8")!!))
        val stall = req("""{"kinds":[$STALLED_KIND],"search":"include:spam"}""")
        val quick = req("""{"kinds":[1],"search":"include:spam"}""")
        serving(gate = gate, deadlineMs = 3_000, clients = twoHops) { base ->
            // The load balancer (10.0.0.5) appended the client; nginx (the socket's peer) appended the balancer.
            val stalled = Thread { post(base, stall, forwardedFor = listOf("203.0.113.5, 10.0.0.5")) }.also { it.start() }
            awaitInFlight(gate, 1)
            assertEquals(200, post(base, quick, forwardedFor = listOf("198.51.100.7, 10.0.0.5")).status, "another client behind the same balancer")
            assertEquals(429, post(base, quick, forwardedFor = listOf("203.0.113.5, 10.0.0.5")).status, "the same client is still the same")
            stalled.join()
        }
    }

    @Test
    fun `an IPv6 client is one client across its 64`() {
        val gate = HttpRelayGate(perClient = 1, total = 0)
        val viaProxy = ClientAddresses("X-Forwarded-For", listOf(Cidr.parse("127.0.0.1")!!))
        val quick = req("""{"kinds":[1],"search":"include:spam"}""")
        serving(gate = gate, deadlineMs = 3_000, clients = viaProxy) { base ->
            val stalled = Thread { post(base, req("""{"kinds":[$STALLED_KIND],"search":"include:spam"}"""), forwardedFor = listOf("2001:db8:1:2::1")) }.also { it.start() }
            awaitInFlight(gate, 1)
            assertEquals(429, post(base, quick, forwardedFor = listOf("2001:db8:1:2::abcd")).status, "a fresh address in the same /64 is the same client")
            assertEquals(200, post(base, quick, forwardedFor = listOf("2001:db8:1:3::1")).status, "the next /64 is another")
            stalled.join()
        }
    }

    @Test
    fun `a slow upload holds its client's slot and is dropped at the body timeout`() {
        val gate = HttpRelayGate(perClient = 1, total = 0)
        serving(gate = gate, bodyTimeoutMs = 700) { base ->
            java.net.Socket("127.0.0.1", URI(base).port).use { socket ->
                socket.soTimeout = 10_000
                // Promises 200 bytes and sends 10: admitted first, then the upload trickles.
                socket.getOutputStream().apply { write("POST / HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 200\r\n\r\n[\"REQ\",\"q\"".encodeToByteArray()) }.flush()
                awaitInFlight(gate, 1)
                val busy = post(base, req("""{"kinds":[1],"search":"include:spam"}"""))
                assertEquals(429, busy.status, "the upload holds the slot, so it counts against the cap: ${busy.body}")
                val input = java.io.BufferedInputStream(socket.getInputStream())
                val head = generateSequence { readLine(input) }.takeWhile { it.isNotEmpty() }.toList()
                assertTrue(head.first().contains(" 408 "), head.toString())
            }
            assertEquals(200, post(base, req("""{"kinds":[1],"search":"include:spam"}""")).status, "and frees it at the timeout")
        }
    }

    @Test
    fun `a cross-origin page can read the back-off and auth hints`() {
        serving { base ->
            val unlensed = post(base, req("""{"kinds":[1]}"""), from = "https://app.example")
            assertEquals(401, unlensed.status)
            val exposed = unlensed.header("Access-Control-Expose-Headers").orEmpty()
            assertTrue("WWW-Authenticate" in exposed && "Retry-After" in exposed, "exposed: '$exposed'")
        }
    }

    @Test
    fun `a forwarded-for header from a peer that is not a trusted proxy is ignored`() {
        val gate = HttpRelayGate(perClient = 1, total = 0)
        val trustsElsewhere = ClientAddresses("X-Forwarded-For", listOf(Cidr.parse("10.9.9.9")!!))
        serving(gate = gate, deadlineMs = 3_000, clients = trustsElsewhere) { base ->
            val stalled = Thread { post(base, req("""{"kinds":[$STALLED_KIND],"search":"include:spam"}"""), forwardedFor = listOf("10.0.0.1")) }.also { it.start() }
            awaitInFlight(gate, 1)
            val spoofed = post(base, req("""{"kinds":[1],"search":"include:spam"}"""), forwardedFor = listOf("10.0.0.2"))
            assertEquals(429, spoofed.status, "a made-up address is still the socket's peer: ${spoofed.body}")
            stalled.join()
        }
    }

    @Test
    fun `a refusal at the gate does not spend the caller's token`() {
        val gate = HttpRelayGate(perClient = 1, total = 0)
        serving(gate = gate, deadlineMs = 3_000) { base ->
            val stalled = Thread { post(base, req("""{"kinds":[$STALLED_KIND],"search":"include:spam"}""")) }.also { it.start() }
            awaitInFlight(gate, 1)
            val body = """{"kinds":[1]}"""
            val signed = token(req(body))
            assertEquals(429, post(base, req(body), signed).status)
            stalled.join()
            val retried = post(base, req(body), signed)
            assertEquals(200, retried.status, "the same token, once the slot is free: ${retried.body}")
        }
    }

    @Test
    fun `an Authorization header in another scheme is not addressed to the relay`() {
        publish(note("basic"))
        serving { base ->
            val response = post(base, req("""{"kinds":[1],"search":"include:spam"}"""), authorization = "Basic dXNlcjpwYXNz")
            assertEquals(200, response.status, response.body)
            val unlensed = post(base, req("""{"kinds":[1]}"""), authorization = "Bearer abc")
            assertEquals(401, unlensed.status, "and it signs nobody in: ${unlensed.body}")
        }
    }

    @Test
    fun `an event posted over HTTP is answered with its OK and then served`() {
        val posted = note("posted over http")
        serving { base ->
            val response = post(base, event(posted.toJson()))
            assertEquals(200, response.status, response.body)
            assertEquals("""["OK","${posted.id}",true,""]""", response.lines.single())

            val again = post(base, event(posted.toJson()))
            assertEquals(200, again.status, "a duplicate is still accepted: ${again.body}")

            val read = post(base, req("""{"ids":["${posted.id}"],"search":"include:spam"}"""))
            assertTrue(posted.id in read.lines.first(), read.body)
        }
    }

    @Test
    fun `a forged event is refused with the relay's OK and its status`() {
        val real = note("genuine")
        val forged = Event(real.id, real.pubKey, real.createdAt, real.kind, real.tags, "tampered", real.sig)
        serving { base ->
            val response = post(base, event(forged.toJson()))
            assertEquals(400, response.status, response.body)
            val ok = response.lines.single()
            assertTrue(ok.startsWith("""["OK","${forged.id}",false,"invalid:"""), ok)
            for (bad in listOf("""["EVENT"]""", """["EVENT",[${real.toJson()}]]""", """["EVENT","x"]""")) {
                assertEquals(400, post(base, bad).status, "'$bad' is not one event")
            }
        }
    }

    /** [n] notes of random content: it does not compress, so the answer really is megabytes on the wire. */
    private fun bigNotes(n: Int): List<Event> {
        val random = java.security.SecureRandom()
        return (1..n).map { i -> alice.sign<Event>(1_700_000_000L + i, 1, emptyArray(), ByteArray(4096).also(random::nextBytes).toHexKey()) }
    }

    /**
     * POSTs [body] on a raw socket with a small window and reads nothing until [whileStalled] returns:
     * java.net.http reads a body eagerly and is never a slow reader. Answers the decoded lines.
     */
    private fun postStalled(
        base: String,
        body: String,
        gzip: Boolean,
        whileStalled: () -> Unit,
    ): List<String> =
        java.net.Socket().use { socket ->
            socket.receiveBufferSize = 16 * 1024
            socket.connect(java.net.InetSocketAddress("127.0.0.1", URI(base).port), 5_000)
            socket.soTimeout = 30_000
            val request =
                "POST / HTTP/1.1\r\nHost: 127.0.0.1\r\n" + (if (gzip) "Accept-Encoding: gzip\r\n" else "") +
                    "Content-Type: application/json\r\nContent-Length: ${body.length}\r\nConnection: close\r\n\r\n$body"
            socket.getOutputStream().apply { write(request.encodeToByteArray()) }.flush()
            whileStalled()
            val input = java.io.BufferedInputStream(socket.getInputStream())
            val head = generateSequence { readLine(input) }.takeWhile { it.isNotEmpty() }.toList()
            assertTrue(head.first().contains(" 200 "), head.toString())
            assertEquals(gzip, head.any { it.equals("Content-Encoding: gzip", ignoreCase = true) }, head.toString())
            val bytes = unchunked(input)
            val text = if (gzip) GZIPInputStream(bytes.inputStream()).use { it.readBytes() } else bytes
            text.decodeToString().lines().filter { it.isNotEmpty() }
        }

    @Test
    fun `a reader that stops reading holds the relay's writes, not its heap`() {
        val big = bigNotes(4000)
        publish(*big.toTypedArray())
        val gate = HttpRelayGate(0, 0)
        serving(gate = gate, deadlineMs = 30_000) { base ->
            for (gzip in listOf(false, true)) {
                val lines =
                    postStalled(base, req("""{"kinds":[1],"search":"include:spam"}"""), gzip) {
                        Thread.sleep(2_500)
                        // Tens of megabytes cannot fit the socket's buffers, so a relay that is still writing is waiting on it.
                        assertEquals(1, gate.inFlight, "gzip=$gzip: the relay finished an answer nobody read, so it holds it in memory")
                    }
                assertEquals("""["EOSE","$SUB"]""", lines.last(), "gzip=$gzip")
                assertEquals(big.size, lines.count { it.startsWith("""["EVENT",""") && it.endsWith("}]") }, "gzip=$gzip")
            }
        }
    }

    @Test
    fun `a gzip answer past its deadline while writes are blocked still decodes to its CLOSED line`() {
        val big = bigNotes(4000)
        publish(*big.toTypedArray())
        serving(deadlineMs = 1_000) { base ->
            // The socket fills, the relay's writes block, and the deadline passes during one.
            val lines = postStalled(base, req("""{"kinds":[1],"search":"include:spam"}"""), gzip = true) { Thread.sleep(2_000) }
            assertTrue(lines.size < big.size, "the deadline must have cut the answer: ${lines.size} lines")
            assertTrue(lines.dropLast(1).all { it.startsWith("""["EVENT",""") && it.endsWith("}]") }, "every line before the last is a whole frame")
            assertTrue(lines.last().startsWith("""["CLOSED","$SUB","error: the answer ran past"""), "ended on ${lines.last().take(80)} after ${lines.size} lines")
        }
    }

    /** One CRLF-terminated line of an HTTP head or chunk header. */
    private fun readLine(input: java.io.InputStream): String {
        val line = StringBuilder()
        while (true) {
            val c = input.read()
            if (c < 0 || c == '\n'.code) return line.toString().trimEnd('\r')
            line.append(c.toChar())
        }
    }

    /** A chunked body, reassembled. */
    private fun unchunked(input: java.io.InputStream): ByteArray {
        val out = java.io.ByteArrayOutputStream()
        while (true) {
            val size = readLine(input).substringBefore(';').trim().toInt(16)
            if (size == 0) return out.toByteArray()
            out.write(input.readNBytes(size))
            readLine(input)
        }
    }

    private fun awaitInFlight(
        gate: HttpRelayGate,
        n: Int,
    ) {
        val deadline = System.currentTimeMillis() + 5_000
        while (gate.inFlight < n) {
            if (System.currentTimeMillis() > deadline) fail("the stalled command never took its slot")
            Thread.sleep(10)
        }
    }

    private companion object {
        /** The subscription id every test REQ and COUNT picks. */
        const val SUB = "q"
        const val STALLED_KIND = 7
        const val TRICKLE_KIND = 8
    }
}
