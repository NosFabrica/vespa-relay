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
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * THE PROPERTY: one connection's ranked reads reach the engine one at a time,
 * in arrival order; its plain reads, and another connection's reads, never
 * wait behind them; and the lane is released at EOSE, not at the end of a REQ
 * that parks at its live tail.
 *
 * Driven through the real server and a real session — the gate sits in the
 * backend the session calls, and what the property is about is what a CLIENT
 * sees on the wire — over an index whose searches can be HELD, so "still
 * queued" is observable rather than a race against a fast in-memory answer.
 */
class SearchGateTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /**
     * An index whose ranked searches park on [holds] until their TERM is
     * released; plain recall answers at once.
     *
     * Keyed by term, not by arrival: one searching REQ reaches the index more
     * than once — the store runs the reference expansion's companion queries
     * (labels, assertions, lists naming the same words) beside the filter's
     * own — so "the search for `alpha`" is several index calls carrying
     * `alpha`, and the gate's unit is the REQ, not the call.
     */
    private class HoldingIndex : EventIndex {
        val inner = InMemoryEventIndex()
        val holds = Collections.synchronizedList(mutableListOf<Pair<String, CompletableDeferred<Unit>>>())
        val started = Collections.synchronizedList(mutableListOf<String>())
        val plainReads = AtomicInteger(0)

        /** The distinct terms the engine has been asked for, in first-arrival order. */
        fun terms(): List<String> = synchronized(started) { started.distinct() }

        override suspend fun get(id: String) = inner.get(id)

        override suspend fun put(doc: EventDoc) = inner.put(doc)

        override suspend fun remove(id: String) = inner.remove(id)

        override suspend fun search(query: EventQuery): List<EventDoc> {
            val term = query.search
            if (term == null && query.ranking == null) {
                plainReads.incrementAndGet()
                return inner.search(query)
            }
            val key = term ?: query.ranking!!
            started += key
            val hold = CompletableDeferred<Unit>()
            holds += key to hold
            hold.await()
            return inner.search(query)
        }

        override suspend fun count(query: EventQuery) = inner.count(query)

        override suspend fun countByAuthor(query: EventQuery) = inner.countByAuthor(query)

        override fun close() {}
    }

    private val index = HoldingIndex()
    private val store = NostrSemanticsStore(index, relay = relayUrl)
    private val server = NostrRelayServer(store, relayUrl)

    @AfterTest
    fun tearDown() {
        server.close()
    }

    @Test
    fun `a connection's ranked reads queue behind each other, its plain reads do not, and EOSE frees the lane`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                session.receive("""["REQ","a",{"kinds":[1],"search":"alpha include:spam","limit":5}]""")
                session.receive("""["REQ","b",{"kinds":[1],"search":"beta include:spam","limit":5}]""")
                // A plain recall on the same socket answers while both searches are outstanding.
                session.receive("""["REQ","p",{"kinds":[1],"search":"include:spam","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","p"]""") }
                assertEquals(1, index.plainReads.get(), "a plain read is not held")

                settle()
                assertEquals(listOf("alpha"), index.terms(), "the second ranked read waits for the first to finish")
                assertFalse(out.any { it.startsWith("""["EOSE","a"]""") }, "the first is still in the engine")
                assertEquals(1, server.searchLanesOpen, "one lane, for the one connection that searched")

                // A second connection is another lane: its search runs beside the held one.
                val out2 = Collections.synchronizedList(mutableListOf<String>())
                val other = server.connect { out2.add(it) }
                try {
                    other.receive("""["REQ","c",{"kinds":[1],"search":"gamma include:spam","limit":5}]""")
                    settle()
                    assertEquals(listOf("alpha", "gamma"), index.terms(), "another connection never waits behind this one")
                    assertEquals(2, server.searchLanesOpen)
                } finally {
                    other.close()
                }
                settle()
                assertEquals(1, server.searchLanesOpen, "a closed connection's lane is dropped")

                // Releasing the first search answers it and admits the second —
                // and the first REQ is still OPEN (it parks at its live tail),
                // which is the whole reason the lane is freed at EOSE.
                release("alpha")
                awaitMessage(out) { it.startsWith("""["EOSE","a"]""") }
                awaitStarted("beta")
                assertFalse(out.any { it.startsWith("""["CLOSED","a"""") }, "the first subscription stays open at its live tail")

                release("beta")
                awaitMessage(out) { it.startsWith("""["EOSE","b"]""") }
            } finally {
                session.close()
            }
            // A Unit last expression: JUnit silently SKIPS a test method that
            // returns a value, and `runBlocking { try … finally }` returns the
            // try block's last expression — the two session tests here ran
            // zero times under that shape and reported green.
            assertEquals(0, server.searchLanesOpen, "no lane outlives its connection")
        }

    @Test
    fun `a ranked read is one with terms, a phrase, or a sort — a lens alone is plain recall`() {
        assertTrue(Filter(search = "bitcoin include:spam").isRankedRead())
        assertTrue(Filter(search = "\"lightning network\" observer:${"a".repeat(64)}").isRankedRead())
        assertTrue(Filter(search = "sort:rank include:spam").isRankedRead(), "a trust-ordered match-all ranks the whole corpus")
        assertFalse(Filter(search = "include:spam").isRankedRead())
        assertFalse(Filter(search = "observer:${"a".repeat(64)}").isRankedRead())
        assertFalse(Filter(search = "-spam include:spam").isRankedRead(), "an exclusion alone is recall minus a word, not a ranking")
        assertFalse(Filter(kinds = listOf(1)).isRankedRead())
        assertFalse(Filter(search = "  ").isRankedRead())
    }

    @Test
    fun `a gate of zero holds nothing`() =
        runBlocking {
            val zero = NostrRelayServer(store, relayUrl, searchConcurrencyPerConnection = 0)
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = zero.connect { out.add(it) }
            try {
                session.receive("""["REQ","a",{"kinds":[1],"search":"alpha include:spam","limit":5}]""")
                session.receive("""["REQ","b",{"kinds":[1],"search":"beta include:spam","limit":5}]""")
                awaitStarted("beta")
                // A set: with nothing gating them the two run TOGETHER, and which
                // reaches the index first is the scheduler's business.
                assertEquals(setOf("alpha", "beta"), index.terms().toSet())
                assertEquals(0, zero.searchLanesOpen)
                release("alpha")
                release("beta")
                awaitMessage(out) { it.startsWith("""["EOSE","a"]""") }
                awaitMessage(out) { it.startsWith("""["EOSE","b"]""") }
            } finally {
                session.close()
                zero.close()
            }
            assertEquals(0, zero.searchLanesOpen, "a gate of zero opens no lane at all")
        }

    /** Long enough for a read that WOULD have started to have started. */
    private fun settle() = Thread.sleep(300)

    /**
     * Let every index call carrying [term] answer — the ones parked now AND
     * the ones the same REQ makes after them (the expansion asks again once
     * the filter's own search returns), for as long as the REQ is answering.
     */
    private fun release(term: String) {
        val deadline = System.currentTimeMillis() + 2_000
        var freed = 0
        while (System.currentTimeMillis() < deadline) {
            val mine = synchronized(index.holds) { index.holds.filter { it.first == term }.also { index.holds.removeAll(it) } }
            mine.forEach { it.second.complete(Unit) }
            freed += mine.size
            if (freed > 0 && mine.isEmpty()) return
            Thread.sleep(20)
        }
        if (freed == 0) fail("nothing was parked on '$term'")
    }

    private fun awaitStarted(term: String) {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            if (term in index.started) return
            Thread.sleep(20)
        }
        fail("the search for '$term' never reached the index; started: ${index.started}")
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
