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
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * SQL on the serving relay: gated like a REQ (the read lens wants a signed-in connection), and
 * answered from the store underneath the lens, which is what lets the mirror and the monitor
 * read the raw corpus through it.
 */
class RelaySqlTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val authors = List(3) { NostrSignerSync() }
    private val reader = NostrSignerSync()

    private fun serverWithNotes(): NostrRelayServer {
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
        runBlocking {
            authors.forEachIndexed { i, a ->
                repeat(2) { n -> store.insert(a.sign<Event>(1_700_000_000L + i * 10 + n, 1, emptyArray(), "note $i.$n")) }
            }
        }
        return NostrRelayServer(store, relayUrl)
    }

    @Test
    fun `an anonymous connection is asked to sign in`() =
        runBlocking {
            val server = serverWithNotes()
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                session.receive("""["SQL","q1","SELECT count(*) FROM events"]""")
                val closed = awaitMessage(out) { it.startsWith("""["CLOSED","q1"""") }
                assertTrue("auth-required:" in closed, closed)
            } finally {
                session.close()
                server.close()
            }
        }

    @Test
    fun `a signed-in connection reads every author, untouched by the lens`() =
        runBlocking {
            val server = serverWithNotes()
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')
                val auth = reader.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                session.receive("""["SQL","q2","SELECT pubkey, count(*) FROM events WHERE kind = 1 GROUP BY pubkey ORDER BY pubkey"]""")
                val rows = awaitMessage(out) { it.startsWith("""["SQL-ROWS","q2"""") }
                for (a in authors) assertTrue("""["${a.pubKey}",2]""" in rows, "every author's notes, lens or not: $rows")
                assertTrue(rows.endsWith(""","done"]"""), rows)

                // Math functions, answered by the store's pushdown: 6 notes over 3 authors.
                session.receive("""["SQL","q3","SELECT sqrt(count(*) * 6), pow(2, count(DISTINCT pubkey)), floor(log10(1000)) FROM events WHERE kind = 1"]""")
                val math = awaitMessage(out) { it.startsWith("""["SQL-ROWS","q3"""") }
                assertTrue("""[[6.0,8.0,3.0]]""" in math, math)
            } finally {
                session.close()
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
