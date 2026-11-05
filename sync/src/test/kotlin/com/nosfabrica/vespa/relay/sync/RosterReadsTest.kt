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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.fold.StoredFold
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** A rebuild reads the verdicts once for every stream, and builds the roster it built reading them stream by stream. */
class RosterReadsTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val ours = NostrSignerInternal(KeyPair())

    private fun url(raw: String): NormalizedRelayUrl = RelayUrlNormalizer.normalize(raw)

    private val config =
        """
        streams {
            graded { dir = "down", filter = { "kinds": [1] }
                relaySource = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ] }
            listed { dir = "down", filter = { "kinds": [0] }
                relaySource = [ { select = [ { tag = "r", relay = 1, authors = "pubkey" } ], filter = { "kinds": [10002] } } ] }
            declared { dir = "down", filter = { "kinds": [3] }, urls = [ "wss://declared.example/" ] }
        }
        """

    /** Folds, a cleared path, an undecided server, NIP-77 answers both ways, and a fold onto a url no source names. */
    private suspend fun seeded(): NostrSemanticsStore {
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
        val record = RelayVerdictRecord(store, ours)
        val prime = listOf("wss://a.example/", "wss://a.example/x", "wss://b.example/", "wss://b.example/p", "wss://b.example/q", "wss://c.example/", "wss://d.example/one", "wss://d.example/two")
        for ((i, u) in prime.withIndex()) record.publishFitness(url(u), "prime", "answered", pageable = null, nip77 = (i % 2 == 0) to "measured")
        record.publish(url("wss://a.example/x"), url("wss://a.example/"), 500, 480)
        record.publishDistinct(url("wss://b.example/p"), 500, "2 compared endpoint(s) on this host", 3)
        record.publish(url("wss://c.example/"), url("wss://c.example:8443/"), 500, 470)
        record.publishFitness(url("wss://declared.example/"), "prime", "answered", pageable = null, nip77 = true to "measured")
        val writer = NostrSignerSync(KeyPair())
        store.insert(writer.sign<Event>(1_700_000_000L, 10002, arrayOf(arrayOf("r", "wss://a.example/x"), arrayOf("r", "wss://e.example/")), ""))
        return store
    }

    private fun perCall(store: NostrSemanticsStore): RosterBuilder {
        val fold = StoredFold.over(store, listOf(ours.pubKey), allowOnion = false)
        val verdicts = RelayVerdictRecord(store, signer = null, readFrom = listOf(ours.pubKey))
        return RosterBuilder(store, RouterConfigLoader.parse(config.trimIndent()).streams, SyncBands(null), foldedAway = fold::foldedAway, verdicts = verdicts::load, watching = true)
    }

    private fun once(store: NostrSemanticsStore): RosterBuilder {
        val fold = StoredFold.over(store, listOf(ours.pubKey), allowOnion = false)
        return RosterBuilder(
            store,
            RouterConfigLoader.parse(config.trimIndent()).streams,
            SyncBands(null),
            watching = true,
            reads = {
                val held = fold.reads()
                object : RosterBuilder.VerdictReads {
                    override suspend fun prefetch(urls: List<NormalizedRelayUrl>) = held.prefetch(urls)

                    override suspend fun foldedAway(urls: List<NormalizedRelayUrl>) = held.foldedAway(urls)

                    override suspend fun verdicts(urls: List<NormalizedRelayUrl>) = held.verdicts(urls)
                }
            },
        )
    }

    private fun RosterBuilder.Roster.shape() =
        listOf(
            asks.mapValues { (_, byStream) -> byStream.mapValues { (_, unit) -> unit.identity } },
            sharedAuthors,
            speaksNegentropy,
            measured,
            watching,
            declared,
        )

    private fun verdictReads(calls: StoreCalls): Long =
        calls
            .snapshot()
            .callers
            .firstOrNull { it.caller == StoreCalls.CALLER_MONITOR_VERDICTS }
            ?.issued ?: 0

    @Test
    fun `one read a rebuild builds the roster that a read per stream built`(): Unit =
        runBlocking {
            val store = seeded()
            val perCallCalls = StoreCalls()
            val expected = withContext(perCallCalls) { perCall(store).rebuild() }
            val onceCalls = StoreCalls()
            val actual = withContext(onceCalls) { once(store).rebuild() }

            assertEquals(expected.shape(), actual.shape())
            val keys = expected.asks.keys
            assertTrue(url("wss://a.example/x") !in keys && keys.count { it.url.startsWith("wss://d.example/") } == 1, "the fixture folds nothing: $keys")
            assertEquals(3L, verdictReads(perCallCalls), "a read per discovering stream's fold and one for the roster's own verdicts")
            assertEquals(1L, verdictReads(onceCalls), "the rebuild read the verdicts more than once")
        }
}
