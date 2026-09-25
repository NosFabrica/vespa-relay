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
package com.nosfabrica.vespa.relay.peers

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RelayDiscoveryConfig
import com.nosfabrica.vespa.relay.config.RelayExcludes
import com.nosfabrica.vespa.relay.config.RelaySelect
import com.nosfabrica.vespa.relay.config.RelaySource
import com.nosfabrica.vespa.relay.config.TagCondition
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.NostrServer
import com.vitorpamplona.quartz.nip01Core.relay.server.inprocess.InProcessWebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The mirror's and the monitor's reads, answered through the relay as SQL, against the same
 * reads made of the store directly. The relay is quartz's server over a Vespa-shaped store,
 * so the SQL runs through the store's own pushdown, refusals included.
 */
class RelaySqlEventStoreTest {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
    private val server = NostrServer(store)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val client =
        NostrClient(
            object : WebsocketBuilder {
                override fun build(
                    url: NormalizedRelayUrl,
                    out: WebSocketListener,
                ): WebSocket = InProcessWebSocket(server, out)
            },
            scope,
        )
    private val reads = RelaySqlEventStore(client, relayUrl, store, idleTimeoutMs = 10_000)

    private val authors = List(4) { NostrSignerSync() }
    private val monitor = NostrSignerSync()

    init {
        runBlocking {
            authors.forEachIndexed { i, a ->
                repeat(3) { n -> store.insert(a.sign<Event>(1_700_000_000L + i * 10 + n, 1, arrayOf(arrayOf("t", "tag$n")), "note $i.$n")) }
                // A replaceable, twice: the store keeps the newer one.
                store.insert(a.sign<Event>(1_700_000_100L, 0, emptyArray(), """{"name":"old$i"}"""))
                store.insert(a.sign<Event>(1_700_000_200L, 0, emptyArray(), """{"name":"u$i"}"""))
                store.insert(
                    a.sign<Event>(
                        1_700_000_300L + i,
                        10002,
                        arrayOf(arrayOf("r", "wss://shared.example"), arrayOf("r", "wss://own$i.example", if (i % 2 == 0) "write" else "read")),
                        "",
                    ),
                )
            }
            listOf("wss://a.example/", "wss://b.example/", "wss://c.example/").forEachIndexed { i, url ->
                store.insert(
                    monitor.sign<Event>(
                        1_700_000_400L + i,
                        30166,
                        arrayOf(arrayOf("d", url), arrayOf("l", if (i == 1) "dead" else "ok", "relay.fitness")),
                        "",
                    ),
                )
            }
        }
    }

    @AfterTest
    fun close() {
        client.close()
        server.close()
        scope.cancel()
    }

    private val filters =
        listOf(
            Filter(kinds = listOf(1)),
            Filter(kinds = listOf(1), limit = 5),
            Filter(kinds = listOf(1), authors = listOf(authors[1].pubKey), since = 1_700_000_011L),
            Filter(kinds = listOf(1), tags = mapOf("t" to listOf("tag0", "tag2"))),
            Filter(kinds = listOf(0), authors = authors.map { it.pubKey }),
            Filter(kinds = listOf(10002), until = 1_700_000_301L),
            Filter(kinds = listOf(30166), authors = listOf(monitor.pubKey), tags = mapOf("d" to listOf("wss://b.example/", "wss://c.example/"))),
            Filter(kinds = listOf(30166), tags = mapOf("l" to listOf("dead"))),
            Filter(kinds = listOf(7)),
        )

    @Test
    fun queriesMatchTheStore() =
        runBlocking {
            for (f in filters) {
                val direct = store.query<Event>(f).map { it.toJson() }.sorted()
                val viaRelay = reads.query<Event>(f).map { it.toJson() }.sorted()
                assertEquals(direct, viaRelay, "query ${f.toJson()}")
                assertEquals(store.count(f), reads.count(f), "count ${f.toJson()}")
            }
            assertTrue(reads.query<Event>(Filter(kinds = listOf(1))).size == 12)
            // A count honours its filter's limit, and <= 0 matches nothing; several filters count the union.
            for (limit in listOf(0, 3, 50)) {
                val f = Filter(kinds = listOf(1), limit = limit)
                assertEquals(store.count(f), reads.count(f), "limit $limit")
            }
            val overlapping = listOf(Filter(kinds = listOf(1), limit = 5), Filter(kinds = listOf(1), authors = listOf(authors[3].pubKey)))
            assertEquals(store.count(overlapping), reads.count(overlapping))
        }

    @Test
    fun negentropySnapshotsMatchTheStore() =
        runBlocking {
            val window = listOf(Filter(kinds = listOf(1, 0), since = 1_700_000_005L, until = 1_700_000_150L))
            assertEquals(
                store.snapshotIdsForNegentropy(window).sortedBy { it.id },
                reads.snapshotIdsForNegentropy(window).sortedBy { it.id },
            )
        }

    @Test
    fun theIngestGatesReadTheRelay() =
        runBlocking {
            val notes = store.query<Event>(Filter(kinds = listOf(1))).map { it.id }
            assertEquals(notes.take(3).toSet(), reads.existingIds(notes.take(3) + "f".repeat(64)))

            val versions = reads.newestVersions(0, authors.map { it.pubKey } + monitor.pubKey)
            assertEquals(authors.map { it.pubKey }.toSet(), versions.keys)
            for (a in authors) {
                val stored = store.query<Event>(Filter(kinds = listOf(0), authors = listOf(a.pubKey))).single()
                assertEquals(stored.createdAt to stored.id, versions.getValue(a.pubKey).let { it.createdAt to it.id })
            }
        }

    @Test
    fun theRelayListRollUpMatchesTheEngineGrouping() =
        runBlocking {
            val f = Filter(kinds = listOf(10002))
            assertEquals(store.distinctTagValues(f, "r", unconditional = true), reads.distinctTagValues(f, "r"))
            val verdicts = Filter(kinds = listOf(30166), tags = mapOf("l" to listOf("dead")))
            assertEquals(setOf("wss://b.example/"), reads.distinctTagValues(verdicts, "d"))
        }

    @Test
    fun discoveryFindsTheSameRelaysEitherWay() =
        runBlocking {
            fun config(select: RelaySelect) =
                RelayDiscoveryConfig(
                    sources = listOf(RelaySource(selects = listOf(select), filter = Filter(kinds = listOf(10002)))),
                    refreshSeconds = 3_600,
                    exclude = RelayExcludes.parse(emptyList()),
                )
            // The roll-up (a plain tag) and the paged walk (a positional condition).
            val rollUp = config(RelaySelect(kind = null, tag = "r", urlIndex = 1))
            val paged = config(RelaySelect(kind = null, tag = "r", urlIndex = 1, where = listOf(TagCondition(index = 2, equals = "write"))))
            for (c in listOf(rollUp, paged)) {
                assertEquals(RelayDiscovery.discover(store, c), RelayDiscovery.discover(reads, c))
            }
            // The shared relay and each author's own.
            assertEquals(5, RelayDiscovery.discover(reads, rollUp).size)
        }

    @Test
    fun writesLandInTheStore() =
        runBlocking {
            val e = authors[0].sign<Event>(1_700_000_500L, 1, emptyArray(), "written through")
            reads.insert(e)
            assertEquals(listOf(e.id), store.query<Event>(Filter(ids = listOf(e.id))).map { it.id })
            assertEquals(listOf(e.id), reads.query<Event>(Filter(ids = listOf(e.id))).map { it.id })
        }
}
