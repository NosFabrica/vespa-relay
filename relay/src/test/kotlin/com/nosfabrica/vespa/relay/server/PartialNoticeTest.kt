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
import com.nosfabrica.vespa.eventstore.engine.Ranked
import com.nosfabrica.vespa.eventstore.engine.ServedDegraded
import com.nosfabrica.vespa.eventstore.engine.client.PartialAnswer
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PassThroughPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyStack
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail

/** A REQ served from part of the index is told so once, before its EOSE; COUNT and a healthy read are not. */
class PartialNoticeTest {
    /** A cluster with a node down: strict reads refused, partial ones served and marked. */
    private class Outage(
        private val inner: EventIndex,
    ) : EventIndex by inner {
        @Volatile var down = false

        private suspend fun check(q: EventQuery) {
            if (!down) return
            if (!q.acceptsPartial) throw PartialAnswer("vespa searched only 75% of the corpus (non-ideal-state)", "non-ideal-state")
            ServedDegraded.markCurrent()
        }

        override suspend fun search(query: EventQuery): List<EventDoc> = check(query).let { inner.search(query) }

        override suspend fun searchRanked(query: EventQuery): List<Ranked<EventDoc>> = check(query).let { inner.searchRanked(query) }

        override suspend fun rawSearch(query: EventQuery): List<RawEvent> = check(query).let { inner.rawSearch(query) }

        override suspend fun rawSearchRanked(query: EventQuery): List<Ranked<RawEvent>> = check(query).let { inner.rawSearchRanked(query) }

        override suspend fun count(query: EventQuery): Int = check(query).let { inner.count(query) }
    }

    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val outage = Outage(InMemoryEventIndex())
    private val store = NostrSemanticsStore(outage, relay = relayUrl)
    private val server = NostrRelayServer(store, relayUrl, requireReadLens = false)

    @AfterTest
    fun tearDown() = server.close()

    @Test
    fun `a REQ served partial is told once, before its EOSE`() =
        runBlocking {
            store.insert(TextNoteEvent("1".repeat(64), "a1".repeat(32), 1_000L, emptyArray(), "hello", ""))
            outage.down = true
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                session.receive("""["REQ","a",{"kinds":[1]}]""")
                val eose = awaitIndex(out) { it.startsWith("""["EOSE","a"]""") }
                val notice = out.indexOfFirst { it.startsWith("""["NOTICE","partial:""") }
                assertTrue(notice in 0 until eose, "the notice comes before the EOSE: $out")
                assertTrue(out.any { it.startsWith("""["EVENT","a",""") }, "and the page is served: $out")

                session.receive("""["REQ","b",{"kinds":[1]}]""")
                awaitIndex(out) { it.startsWith("""["EOSE","b"]""") }
                session.receive("""["COUNT","c",{"kinds":[1]}]""")
                awaitIndex(out) { it.startsWith("""["CLOSED","c",""") || it.startsWith("""["COUNT","c",""") }
                assertEquals(1, out.count { it.startsWith("""["NOTICE","partial:""") }, "once per connection, and never for a COUNT: $out")
                assertTrue(out.none { it.startsWith("""["COUNT","c",""") }, "a COUNT over part of the corpus is still refused: $out")
            } finally {
                session.close()
            }
        }

    @Test
    fun `a complete REQ is not told anything`() =
        runBlocking {
            store.insert(TextNoteEvent("1".repeat(64), "a1".repeat(32), 1_000L, emptyArray(), "hello", ""))
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                session.receive("""["REQ","a",{"kinds":[1]}]""")
                awaitIndex(out) { it.startsWith("""["EOSE","a"]""") }
                assertTrue(out.none { it.startsWith("""["NOTICE",""") }, "a healthy read says nothing: $out")
            } finally {
                session.close()
            }
        }

    @Test
    fun `the notice is sent at most once per interval, and found through the policy stack`() {
        var now = 0L
        val sent = mutableListOf<Message>()
        val notice = PartialNotice(everyMillis = 1_000L, now = { now })
        val stack = PolicyStack(PassThroughPolicy(), notice)
        stack.onConnect(ctx(stack)) { sent += it }
        assertSame(notice, PartialNotice.of(stack))
        assertSame(notice, PartialNotice.of(PolicyStack(PassThroughPolicy(), stack)), "nested, as quartz stacks its limits")

        notice.tell()
        now = 999L
        notice.tell()
        assertEquals(1, sent.size, "inside the interval")
        now = 1_000L
        notice.tell()
        assertEquals(2, sent.size, "re-armed after it")
        assertEquals(PartialNotice.MESSAGE, (sent.last() as NoticeMessage).message)
    }

    private fun ctx(policy: IRelayPolicy) =
        object : RequestContext {
            override val connectionId = 1L
            override val policy = policy
            override val authenticatedUsers = emptySet<String>()
        }

    private fun awaitIndex(
        out: List<String>,
        match: (String) -> Boolean,
    ): Int {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.indexOfFirst(match) }.takeIf { it >= 0 }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }
}
