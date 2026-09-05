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
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * `t` tags reach the search index on a 30382 Contact Card and on no other NIP-85 kind, because
 * only `ContactCardEvent` is a `SearchableEvent` upstream. Driven over the wire so `EventFactory`
 * does the typing.
 */
class AssertionTopicSearchTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7778")
    private val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
    private val server = NostrRelayServer(store, relayUrl)
    private val service = NostrSignerSync()
    private val subject = NostrSignerSync()
    private val topic = "permaculture"

    @AfterTest
    fun tearDown() {
        server.close()
    }

    @Test
    fun `a topic on a contact card is searchable and pulls the profile with it`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // No petname, no summary: the `t` tag is the only thing that can make this card a hit.
                val card =
                    service.sign<Event>(
                        1_700_000_000L,
                        30382,
                        arrayOf(arrayOf("d", subject.pubKey), arrayOf("rank", "9.0"), arrayOf("t", topic)),
                        "",
                    )
                val profile = subject.sign<Event>(1_700_000_010L, 0, arrayOf(), """{"name":"Subject"}""")
                publish(session, out, card, profile)

                // Signed by the observer itself, so it clears the trust gate without a 10040 enrolment.
                session.receive(
                    """["REQ","topic",{"kinds":[0,30382],"search":"$topic include:spam observer:${service.pubKey}"}]""",
                )
                awaitMessage(out) { it.startsWith("""["EOSE","topic"]""") }
                val served = synchronized(out) { out.filter { it.startsWith("""["EVENT","topic",""") } }
                assertTrue(served.any { card.id in it }, "the topic must find the card that carries it: $served")
                assertTrue(served.any { profile.id in it }, "the card's subject profile must ride along: $served")
            } finally {
                session.close()
            }
        }

    @Test
    fun `the same topic on the other assertion kinds indexes nothing`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val onEvent = service.sign<Event>(1_700_000_000L, 30383, arrayOf(arrayOf("d", "ab".repeat(32)), arrayOf("t", topic)), "")
                val onAddress = service.sign<Event>(1_700_000_001L, 30384, arrayOf(arrayOf("d", "30023:${subject.pubKey}:post"), arrayOf("t", topic)), "")
                val onExternal = service.sign<Event>(1_700_000_002L, 30385, arrayOf(arrayOf("d", "isbn:1234567890"), arrayOf("t", topic)), "")
                publish(session, out, onEvent, onAddress, onExternal)

                session.receive("""["REQ","quiet",{"kinds":[30383,30384,30385],"search":"$topic include:spam"}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","quiet"]""") }
                assertTrue(
                    synchronized(out) { out.none { it.startsWith("""["EVENT","quiet",""") } },
                    "no assertion kind but 30382 may be reachable by its topic: $out",
                )

                // The control: `#t` recall still serves them; only the search index skips them.
                session.receive("""["REQ","bytag",{"kinds":[30383,30384,30385],"#t":["$topic"],"search":"include:spam"}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","bytag"]""") }
                val byTag = synchronized(out) { out.filter { it.startsWith("""["EVENT","bytag",""") } }
                for (stored in listOf(onEvent, onAddress, onExternal)) {
                    assertTrue(byTag.any { stored.id in it }, "tag recall must still serve ${stored.kind}: $byTag")
                }
            } finally {
                session.close()
            }
        }

    private suspend fun publish(
        session: RelaySession,
        out: List<String>,
        vararg events: Event,
    ) {
        for (event in events) {
            session.receive("""["EVENT",${event.toJson()}]""")
            awaitMessage(out) { it.startsWith("""["OK","${event.id}",true""") }
        }
    }

    private fun awaitMessage(
        out: List<String>,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }
}
