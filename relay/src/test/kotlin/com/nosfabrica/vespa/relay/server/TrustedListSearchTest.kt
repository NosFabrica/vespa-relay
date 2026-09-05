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
 * A Trusted List's title is searchable, which holds only while the quartz this repo forces on the
 * store carries the `EventFactory` branch for kinds 30392-30395; an older pin fails here only.
 */
class TrustedListSearchTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
    private val server = NostrRelayServer(store, relayUrl)
    private val signer = NostrSignerSync()
    private val member = "c3".repeat(32)

    @AfterTest
    fun tearDown() {
        server.close()
    }

    /** A kind-30392 list of pubkeys: `title` names it, `metric` names the computation behind it. */
    private fun trustedList() =
        signer.sign<Event>(
            1_700_000_000L,
            30392,
            arrayOf(
                arrayOf("d", "podcasters"),
                arrayOf("title", "Podcaster Trust List"),
                arrayOf("metric", "influence"),
                arrayOf("p", member),
            ),
            """{"members":["$member"]}""",
        )

    @Test
    fun `the title is searchable, the metric and the membership are not`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val list = trustedList()
                session.receive("""["EVENT",${list.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${list.id}",true""") }

                session.receive("""["REQ","title",{"kinds":[30392],"search":"podcaster include:spam"}]""")
                awaitMessage(out) { it.startsWith("""["EVENT","title",""") && list.id in it }

                // `metric` names a computation; a common word must not return every list that ran it.
                assertNoEventBeforeEose(session, out, "metric", """{"kinds":[30392],"search":"influence include:spam"}""")

                // The membership is hex ids and a JSON echo of them: out of the search text, served by tag.
                assertNoEventBeforeEose(session, out, "hex", """{"kinds":[30392],"search":"$member include:spam"}""")
                session.receive("""["REQ","tag",{"kinds":[30392],"#p":["$member"],"search":"include:spam"}]""")
                val byTag = awaitMessage(out) { it.startsWith("""["EVENT","tag",""") }
                assertTrue(list.id in byTag, "tag recall must still serve the list: $byTag")
            } finally {
                session.close()
            }
        }

    @Test
    fun `a titleless list stores cleanly rather than throwing`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // `indexableContent()` runs on the insert path, so a titleless list must index "" not throw.
                val untitled = signer.sign<Event>(1_700_000_100L, 30393, arrayOf(arrayOf("d", "untitled")), "")
                session.receive("""["EVENT",${untitled.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${untitled.id}",true""") }

                session.receive("""["REQ","by-kind",{"kinds":[30393],"search":"include:spam"}]""")
                val stored = awaitMessage(out) { it.startsWith("""["EVENT","by-kind",""") }
                assertTrue(untitled.id in stored, "the titleless list must still be stored and served: $stored")
            } finally {
                session.close()
            }
        }

    /** Asserts the EOSE arrives with no EVENT on [subId] before it. */
    private suspend fun assertNoEventBeforeEose(
        session: RelaySession,
        out: List<String>,
        subId: String,
        filter: String,
    ) {
        session.receive("""["REQ","$subId",$filter]""")
        awaitMessage(out) { it.startsWith("""["EOSE","$subId"]""") }
        assertTrue(
            synchronized(out) { out.none { it.startsWith("""["EVENT","$subId",""") } },
            "$subId matched something it should not have: $out",
        )
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
