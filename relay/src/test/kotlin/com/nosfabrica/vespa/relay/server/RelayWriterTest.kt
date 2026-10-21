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
import com.nosfabrica.vespa.relay.ingest.RelayWriter
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import java.net.ServerSocket
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The sync's writes over a real socket: signed in as the relay's key on an address the relay does
 * not know itself by, onto the mirror lane, with the OKs read back as the store's own outcomes.
 */
class RelayWriterTest {
    // What the relay calls itself is not what the writer dials, as on compose (ws://relay:7777).
    private val relayUrl = RelayUrlNormalizer.normalize("wss://relay.example.com/")
    private val relayKey = NostrSignerInternal(KeyPair())
    private val someone = NostrSignerSync()
    private val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
    private val mirrored: MutableList<String> = Collections.synchronizedList(mutableListOf())

    /** The mirror lane's ingest: writes through the store and remembers what reached it. */
    private val mirror =
        object : EventIngest {
            override suspend fun submit(
                event: Event,
                skipVerify: Boolean,
                onComplete: (IEventStore.InsertOutcome) -> Unit,
            ) {
                mirrored += event.id
                onComplete(store.batchInsert(listOf(event)).single())
            }

            override fun hasBacklog() = false

            override fun close() = Unit
        }

    private val relay =
        NostrRelayServer(store, relayUrl, kindDeny = setOf(7), mirrorLane = MirrorLane(relayKey.pubKey, mirror)).also {
            it.eventCommandHandler = MirrorDeleteHandler(relayKey.pubKey, purge = { filter -> store.delete(filter) })
        }
    private val server = serveRelay(relay = relay, port = 0, nip11 = Nip11Info(), wait = false)
    private val port =
        runBlocking {
            server.engine
                .resolvedConnectors()
                .first()
                .port
        }
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val writer = RelayWriter(RelayUrlNormalizer.normalize("ws://127.0.0.1:$port"), relayUrl, relayKey, scope)

    @AfterTest
    fun stop() {
        writer.close()
        scope.cancel()
        server.stop(0, 0)
        relay.close()
    }

    private fun note(n: Int): Event = someone.sign(1_700_000_000L + n, 1, emptyArray(), "note $n")

    private fun stored(): Set<String> = runBlocking { store.query<Event>(Filter(kinds = listOf(1))).map { it.id }.toSet() }

    @Test
    fun `a batch lands on the mirror lane, and a resend comes back as the store's duplicate`() {
        val notes = (1..5).map(::note)
        val first = runBlocking { writer.batchInsert(notes) }
        assertEquals(List(5) { IEventStore.InsertOutcome.Accepted }, first)
        assertTrue(writer.signedIn(), "signed in for the relay's own address, though dialled on another")
        assertEquals(0L, writer.unsignedBatches.get())
        assertEquals(notes.map { it.id }.toSet(), mirrored.toSet(), "every event through the mirror lane")
        assertEquals(notes.map { it.id }.toSet(), stored())

        val again = runBlocking { writer.batchInsert(notes) }
        assertTrue(again.all { it is IEventStore.InsertOutcome.Rejected && it.reason.startsWith("duplicate:") }, "a resend: $again")
    }

    @Test
    fun `a policy refusal is the store's refusal, and insert throws on it`() {
        val reaction = someone.sign<Event>(1_700_000_000L, 7, emptyArray(), "+")
        val outcome = runBlocking { writer.batchInsert(listOf(reaction)) }.single()
        assertTrue(outcome is IEventStore.InsertOutcome.Rejected && outcome.reason.startsWith("blocked:"), "denied kind: $outcome")
        assertFailsWith<IllegalStateException> { runBlocking { writer.insert(reaction) } }
        runBlocking { writer.insert(note(9)) }
        assertTrue(note(9).id in stored())
    }

    @Test
    fun `a refused event leaves the writer's outbox, so a long-lived socket holds nothing it was answered`() {
        // Quartz used to keep an OK-false event pending until three refusals, which only reconnects produce.
        val reactions = (1..50).map { someone.sign<Event>(1_700_000_000L + it, 7, emptyArray(), "+") }
        val outcomes = runBlocking { writer.batchInsert(reactions) }
        assertTrue(outcomes.all { it is IEventStore.InsertOutcome.Rejected }, "all refused: $outcomes")
        assertEquals(0, writer.pendingInOutbox())
    }

    @Test
    fun `the socket outlives the quiet between two batches, so the next one needs no new sign-in`() {
        runBlocking { writer.batchInsert(listOf(note(1))) }
        assertTrue(writer.signedIn())
        // Well past Quartz's 300ms pool sample, which drops a relay nothing holds; a redial would
        // reset the NIP-42 state with the socket.
        Thread.sleep(1_500)
        assertTrue(writer.signedIn(), "still the same signed-in socket")
        runBlocking { writer.batchInsert(listOf(note(2))) }
        assertEquals(0L, writer.unsignedBatches.get())
    }

    @Test
    fun `a delete goes as a signed request and the relay purges exactly its ids`() {
        val notes = (1..3).map(::note)
        runBlocking { writer.batchInsert(notes) }
        runBlocking { writer.delete(Filter(kinds = listOf(1), ids = listOf(notes[0].id, notes[2].id))) }
        assertEquals(setOf(notes[1].id), stored())
    }

    @Test
    fun `a relay that does not answer fails the batch rather than refusing it`() {
        val closed = ServerSocket(0).use { it.localPort }
        val dead = RelayWriter(RelayUrlNormalizer.normalize("ws://127.0.0.1:$closed"), relayUrl, relayKey, scope, authWaitMs = 100, idleTimeoutMs = 1_000)
        try {
            val outcomes = runBlocking { dead.batchInsert(listOf(note(1), note(2))) }
            assertTrue(outcomes.all { it is IEventStore.InsertOutcome.Failed }, "a down relay is a failure, which withholds the band: $outcomes")
            assertEquals(1L, dead.unsignedBatches.get())
        } finally {
            dead.close()
        }
    }
}
