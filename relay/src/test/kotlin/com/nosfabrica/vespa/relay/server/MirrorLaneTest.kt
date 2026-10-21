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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * What a connection signed in as the relay's own key publishes goes to the mirror lane, and
 * everything else to the publishes' lane; both under one policy stack, both fanned out.
 */
class MirrorLaneTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val relayKey = NostrSignerSync()
    private val someone = NostrSignerSync()

    /** An ingest that accepts everything and remembers what it was handed. */
    private class Recording : EventIngest {
        val ids: MutableList<String> = Collections.synchronizedList(mutableListOf())

        override suspend fun submit(
            event: Event,
            skipVerify: Boolean,
            onComplete: (IEventStore.InsertOutcome) -> Unit,
        ) {
            ids += event.id
            onComplete(IEventStore.InsertOutcome.Accepted)
        }

        override fun hasBacklog() = false

        override fun close() = Unit
    }

    private val clients = Recording()
    private val mirror = Recording()
    private val lane = MirrorLane(relayKey.pubKey, mirror)

    private fun server(
        kindDeny: Set<Int> = emptySet(),
        requireReadLens: Boolean = true,
    ) = NostrRelayServer(
        NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl),
        relayUrl,
        kindDeny = kindDeny,
        requireReadLens = requireReadLens,
        ingest = clients,
        mirrorLane = lane,
    )

    /** A connection, signed in as [signer] when one is given. */
    private fun connect(
        server: NostrRelayServer,
        signer: NostrSignerSync?,
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

    private fun publish(
        session: Pair<RelaySession, MutableList<String>>,
        event: Event,
    ): String {
        runBlocking { session.first.receive("""["EVENT",${event.toJson()}]""") }
        return awaitMessage(session.second) { it.startsWith("""["OK","${event.id}"""") }
    }

    private fun note(
        signer: NostrSignerSync,
        content: String,
        kind: Int = 1,
    ): Event = signer.sign(1_700_000_000L, kind, emptyArray(), content)

    @Test
    fun `the relay's own key publishes into the mirror lane, everyone else into the publishes'`() {
        val server = server()
        try {
            val sync = connect(server, relayKey)
            val anonymous = connect(server, null)
            val other = connect(server, someone)

            val mirrored = note(someone, "mirrored from an upstream")
            assertTrue(",true" in publish(sync, mirrored))
            val published = note(someone, "published by a client")
            assertTrue(",true" in publish(anonymous, published))
            val signedIn = note(someone, "published by a signed-in client")
            assertTrue(",true" in publish(other, signedIn))

            assertEquals(listOf(mirrored.id), mirror.ids.toList())
            assertEquals(listOf(published.id, signedIn.id), clients.ids.toList())
        } finally {
            server.close()
        }
    }

    @Test
    fun `the mirror lane meets the same policies, and a refused event leaves no tag`() {
        val server = server(kindDeny = setOf(7))
        try {
            val sync = connect(server, relayKey)
            val refused = note(someone, "+", kind = 7)
            assertTrue(",false" in publish(sync, refused), "a denied kind is denied on the mirror lane too")
            assertTrue(mirror.ids.isEmpty() && clients.ids.isEmpty(), "and reaches no lane")
            assertFalse(lane.claim(refused), "the tagger runs last, so a refusal tags nothing")
        } finally {
            server.close()
        }
    }

    @Test
    fun `an event the mirror lane writes reaches a live subscriber`() {
        // An anonymous unranked read, so the subscriber needs no lens of its own.
        val server = server(requireReadLens = false)
        try {
            val reader = connect(server, null)
            runBlocking { reader.first.receive("""["REQ","live",{"kinds":[1],"since":1699999999}]""") }
            awaitMessage(reader.second) { it.startsWith("""["EOSE","live"""") }
            val sync = connect(server, relayKey)
            val mirrored = note(someone, "backfilled history")
            publish(sync, mirrored)
            awaitMessage(reader.second) { it.startsWith("""["EVENT","live",""") && mirrored.id in it }
        } finally {
            server.close()
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
