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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.server.config.relayIngestFromEnv
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * NIP-70: an event carrying `["-"]` may only be published by its author. "The default behavior of a
 * relay MUST be to reject any event that contains `["-"]`"; a relay that accepts them must first
 * require NIP-42 AUTH and accept only when the authenticated pubkey is the event's author, answering
 * an unauthenticated publish `auth-required:`. Driven through the production stack: the relay's
 * policies and its default ingest pipeline over NostrSemanticsStore.
 */
class ProtectedEventTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val index = InMemoryEventIndex()
    private val store = NostrSemanticsStore(index, relay = relayUrl)
    private val scope = CoroutineScope(Job())
    private val pipeline = IngestPipeline(store, relayIngestFromEnv(emptyMap()).tuning!!, audit = null, servingPressure = null, scope = scope, knownIds = index::existingIds).also { it.start() }
    private val server = NostrRelayServer(store, relayUrl, requireReadLens = false, ingest = pipeline)

    private val author = NostrSignerSync()

    private fun protectedNote(text: String): Event = author.sign(System.currentTimeMillis() / 1000, 1, arrayOf(arrayOf("-")), text)

    @AfterTest
    fun tearDown() {
        server.close()
        pipeline.closeIntake()
        scope.cancel()
        pipeline.close()
    }

    private suspend fun signIn(
        session: RelaySession,
        out: List<String>,
        as_: NostrSignerSync,
    ) {
        val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
        val auth = as_.sign(RelayAuthEvent.build(relayUrl, challenge))
        session.receive("""["AUTH",${auth.toJson()}]""")
        awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }
    }

    // Reproduces a defect not fixed yet; fails against main 2f8ae4df. The fix removes this @Ignore.
    @Ignore
    @Test
    fun `an unauthenticated connection's protected event is refused auth-required`() =
        runBlocking<Unit> {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val event = protectedNote("republished by anyone")
                session.receive("""["EVENT",${event.toJson()}]""")
                val ok = awaitMessage(out) { it.startsWith("""["OK","${event.id}",""") }
                val stored = store.count(Filter(ids = listOf(event.id)))
                assertTrue(
                    ok.startsWith("""["OK","${event.id}",false,"auth-required:"""),
                    "NIP-70: a [\"-\"] event from an unauthenticated connection must be refused auth-required; the relay answered $ok (stored: $stored)",
                )
                assertEquals(0, stored, "a refused protected event must not be stored")
            } finally {
                session.close()
            }
        }

    // Reproduces a defect not fixed yet; fails against main 2f8ae4df. The fix removes this @Ignore.
    @Ignore
    @Test
    fun `a protected event published by someone signed in as another key is refused`() =
        runBlocking<Unit> {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                signIn(session, out, NostrSignerSync())
                val event = protectedNote("republished by a third party")
                session.receive("""["EVENT",${event.toJson()}]""")
                val ok = awaitMessage(out) { it.startsWith("""["OK","${event.id}",""") }
                assertTrue(
                    ok.startsWith("""["OK","${event.id}",false,"""),
                    "NIP-70: a [\"-\"] event may only be published by its author, not by another authenticated key; the relay answered $ok",
                )
            } finally {
                session.close()
            }
        }

    /** CONTROL (expected to pass): the author, signed in, publishes its own protected event. */
    @Test
    fun `control - the authenticated author's protected event is accepted`() =
        runBlocking<Unit> {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                signIn(session, out, author)
                val event = protectedNote("published by its author")
                session.receive("""["EVENT",${event.toJson()}]""")
                val ok = awaitMessage(out) { it.startsWith("""["OK","${event.id}",""") }
                assertTrue(ok.startsWith("""["OK","${event.id}",true"""), "the author's own protected event: $ok")
            } finally {
                session.close()
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
