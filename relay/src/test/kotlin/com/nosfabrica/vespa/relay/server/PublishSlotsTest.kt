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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.EventCmd
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * A connection at its bound of unanswered publishes waits for an answer, and is never refused for it; another
 * connection's publish goes straight on, and the mirror lane and refused publishes take no slot.
 */
class PublishSlotsTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val relayKey = NostrSignerSync()
    private val someone = NostrSignerSync()

    /** Holds every outcome until [acceptAll]: a lane whose batch is still being written. */
    private class Holding : EventIngest {
        val pending: MutableList<Pair<String, (IEventStore.InsertOutcome) -> Unit>> = Collections.synchronizedList(mutableListOf())

        override suspend fun submit(
            event: Event,
            skipVerify: Boolean,
            onComplete: (IEventStore.InsertOutcome) -> Unit,
        ) {
            pending += event.id to onComplete
        }

        fun ids(): List<String> = synchronized(pending) { pending.map { it.first } }

        fun acceptAll() = synchronized(pending) { pending.toList().also { pending.clear() } }.forEach { it.second(IEventStore.InsertOutcome.Accepted) }

        override fun hasBacklog() = pending.isNotEmpty()

        override fun close() = Unit
    }

    private val clients = Holding()
    private val mirror = Holding()

    private fun server(kindDeny: Set<Int> = emptySet()) =
        NostrRelayServer(
            NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl),
            relayUrl,
            requireReadLens = false,
            kindDeny = kindDeny,
            ingest = clients,
            mirrorLane = MirrorLane(relayKey.pubKey, mirror),
            publishSlots = PublishSlots(perConnection = 2),
        )

    private fun connect(
        server: NostrRelayServer,
        signer: NostrSignerSync? = null,
    ): Pair<RelaySession, MutableList<String>> {
        val out = Collections.synchronizedList(mutableListOf<String>())
        val session = server.connect { out.add(it) }
        if (signer != null) {
            val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
            val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
            runBlocking { session.receive("""["AUTH",${auth.toJson()}]""") }
            awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }
        }
        return session to out
    }

    private fun note(
        n: Int,
        kind: Int = 1,
    ): Event = someone.sign(1_700_000_000L + n, kind, emptyArray(), "note $n")

    @Test
    fun `a connection at its bound waits for an answer while another connection's publish goes on`() =
        runBlocking<Unit> {
            val server = server()
            try {
                val flooder = connect(server)
                val other = connect(server)
                val flood = (1..3).map { note(it) }
                val reader = launch(Dispatchers.Default) { for (e in flood) flooder.first.receive("""["EVENT",${e.toJson()}]""") }
                awaitTrue("the flooder's reader waits at its bound") { clients.ids().size == 2 && server.publishGate.held == 1 }

                val mine = note(100)
                withTimeout(2_000) { other.first.receive("""["EVENT",${mine.toJson()}]""") }
                assertEquals(flood.take(2).map { it.id } + mine.id, clients.ids(), "the other connection's publish is not held behind the flooder")

                clients.acceptAll()
                awaitTrue("an answer frees the flooder's slot") { clients.ids() == listOf(flood[2].id) }
                clients.acceptAll()
                withTimeout(2_000) { reader.join() }
                for (e in flood) assertTrue(""",true""" in awaitMessage(flooder.second) { it.startsWith("""["OK","${e.id}"""") }, "held, never refused")
                assertEquals(3, flooder.second.count { it.startsWith("""["OK",""") }, "one answer per publish: ${flooder.second}")
                assertEquals(0, server.publishGate.held)
            } finally {
                server.close()
            }
        }

    @Test
    fun `the mirror lane's connection is not held at the clients' bound`() =
        runBlocking<Unit> {
            val server = server()
            try {
                val sync = connect(server, relayKey)
                val mirrored = (1..5).map { note(it) }
                withTimeout(2_000) { for (e in mirrored) sync.first.receive("""["EVENT",${e.toJson()}]""") }
                assertEquals(mirrored.map { it.id }, mirror.ids())
                assertEquals(0L, server.publishGate.waited)
            } finally {
                server.close()
            }
        }

    @Test
    fun `a refused publish takes no slot`() =
        runBlocking<Unit> {
            val server = server(kindDeny = setOf(7))
            try {
                val publisher = connect(server)
                val refused = (1..4).map { note(it, kind = 7) }
                val accepted = (5..6).map { note(it) }
                withTimeout(2_000) { for (e in refused + accepted) publisher.first.receive("""["EVENT",${e.toJson()}]""") }
                assertEquals(accepted.map { it.id }, clients.ids())
                assertEquals(0L, server.publishGate.waited)
            } finally {
                server.close()
            }
        }

    /**
     * A publish marked at the policy stage that never reaches the clients' lane (its connection
     * dropped in between, or the mirror lane took its id) left its mark in the shared map for good:
     * quartz's policies hear no disconnect. Marks are swept by age every few marks instead.
     */
    @Test
    fun `marks that never reach the clients' lane are swept`() {
        val slots = PublishSlots(perConnection = 2, staleNanos = 0, sweepEvery = 4)
        val policy = slots.policy()!!
        (1..3).forEach { policy.accept(EventCmd(note(it))) }
        assertEquals(3, slots.pending, "three marks, none claimed")
        Thread.sleep(2)
        policy.accept(EventCmd(note(4)))
        assertEquals(1, slots.pending, "the fourth mark sweeps the three stale ones and keeps itself")
        assertEquals(3L, slots.strays)
    }

    @Test
    fun `two connections publishing one id are each held to their own slots`() =
        runBlocking<Unit> {
            val slots = PublishSlots(perConnection = 1)
            val lane = slots.over(clients)
            val flooder = slots.policy()!!
            val other = slots.policy()!!
            val first = note(1)
            flooder.accept(EventCmd(first))
            lane.submit(first, false) {}
            val shared = note(2)
            val othersCopy = Event.fromJson(shared.toJson())
            val floodersCopy = Event.fromJson(shared.toJson())
            other.accept(EventCmd(othersCopy))
            flooder.accept(EventCmd(floodersCopy))

            assertSettles(slots, othersCopy, lane, held = false, "the other connection's copy is not held behind the flooder's bound")
            val floods = assertSettles(slots, floodersCopy, lane, held = true, "the flooder's copy waits at its own bound")
            assertEquals(listOf(first.id, shared.id), clients.ids())
            clients.acceptAll()
            withTimeout(2_000) { floods.join() }
            assertEquals(listOf(shared.id), clients.ids(), "an answer frees the flooder's slot for its copy")
            assertEquals(0, slots.pending, "every mark claimed by its own copy")
        }

    @Test
    fun `an unmarked copy of a marked id takes no connection's slot`() =
        runBlocking<Unit> {
            val slots = PublishSlots(perConnection = 1)
            val lane = slots.over(clients)
            val publisher = slots.policy()!!
            val first = note(1)
            publisher.accept(EventCmd(first))
            lane.submit(first, false) {}
            val shared = note(2)
            val publishersCopy = Event.fromJson(shared.toJson())
            publisher.accept(EventCmd(publishersCopy))

            assertSettles(slots, Event.fromJson(shared.toJson()), lane, held = false, "a copy no policy marked, as the mirror lane's, goes straight on")
            val publishes = assertSettles(slots, publishersCopy, lane, held = true, "the publisher's copy waits at its bound")
            clients.acceptAll()
            withTimeout(2_000) { publishes.join() }
            assertEquals(0, slots.pending, "the mark claimed by its own copy")
        }

    /** Submits [event] on its own coroutine, waits until it is through or held, and asserts which. */
    private fun CoroutineScope.assertSettles(
        slots: PublishSlots,
        event: Event,
        lane: EventIngest,
        held: Boolean,
        what: String,
    ): Job {
        val heldBefore = slots.held
        val submit = launch(Dispatchers.Default) { lane.submit(event, false) {} }
        awaitTrue("the submit is through or held") { submit.isCompleted || slots.held > heldBefore }
        assertEquals(held, !submit.isCompleted, what)
        return submit
    }

    private fun awaitTrue(
        what: String,
        condition: () -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + 10_000
        while (!condition()) {
            if (System.currentTimeMillis() > deadline) fail("timed out: $what")
            Thread.sleep(10)
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
