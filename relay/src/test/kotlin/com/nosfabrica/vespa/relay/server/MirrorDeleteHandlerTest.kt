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
import com.nosfabrica.vespa.relay.ingest.MirrorDelete
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The sync process's delete request: honoured from the relay's own key alone, fresh, with its
 * ids named, and never stored itself; everything else the hook lets pass.
 */
class MirrorDeleteHandlerTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val relayKey = NostrSignerSync()
    private val someone = NostrSignerSync()
    private val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
    private val server =
        NostrRelayServer(store, relayUrl).also {
            it.eventCommandHandler = MirrorDeleteHandler(relayKey.pubKey, purge = { filter -> store.delete(filter) })
        }

    @AfterTest
    fun stop() = server.close()

    private val notes = (1..3).map { someone.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "note $it") }

    private fun stored(): Set<String> = runBlocking { store.query<Event>(Filter(kinds = listOf(1))).map { it.id }.toSet() }

    private fun send(event: Event): String {
        val out = Collections.synchronizedList(mutableListOf<String>())
        val session = server.connect { out.add(it) }
        runBlocking { session.receive("""["EVENT",${event.toJson()}]""") }
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull { it.startsWith("""["OK","${event.id}"""") } }?.let {
                session.close()
                return it
            }
            Thread.sleep(20)
        }
        fail("no OK for ${event.id}: $out")
    }

    private fun request(
        signer: NostrSignerSync,
        filter: Filter,
        createdAt: Long = System.currentTimeMillis() / 1000,
    ): Event = signer.sign(MirrorDelete.template(filter, createdAt))

    @Test
    fun `the relay's own key deletes the ids it names, and the request is not stored`() {
        runBlocking { notes.forEach { store.insert(it) } }
        val ok = send(request(relayKey, Filter(ids = notes.take(2).map { it.id }, authors = listOf(someone.pubKey))))
        assertTrue(ok.contains(",true,"), ok)
        assertEquals(setOf(notes[2].id), stored(), "the two named are gone, the third stays")
        assertEquals(0, runBlocking { store.count(Filter(kinds = listOf(MirrorDelete.KIND))) }, "the request itself is never stored")
    }

    @Test
    fun `anyone else, a stale request, or one without ids is refused and deletes nothing`() {
        runBlocking { notes.forEach { store.insert(it) } }
        val all = notes.map { it.id }
        val refusals =
            mapOf(
                "another key" to request(someone, Filter(ids = all)),
                "stale" to request(relayKey, Filter(ids = all), createdAt = System.currentTimeMillis() / 1000 - 3_600),
                "no ids" to request(relayKey, Filter(authors = listOf(someone.pubKey))),
                "too many ids" to request(relayKey, Filter(ids = List(MirrorDelete.MAX_IDS + 1) { "%064x".format(it) })),
            )
        for ((why, event) in refusals) {
            val ok = send(event)
            assertTrue(ok.contains(",false,") && ok.contains("restricted:"), "$why must be refused: $ok")
        }
        assertEquals(all.toSet(), stored(), "nothing was deleted")
    }

    @Test
    fun `every other event passes the hook untouched`() {
        val ok = send(notes[0])
        assertTrue(ok.contains(",true,"), ok)
        assertEquals(setOf(notes[0].id), stored())
    }
}
