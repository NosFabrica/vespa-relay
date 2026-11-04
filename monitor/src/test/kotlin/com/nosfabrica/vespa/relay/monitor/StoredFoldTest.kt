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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.fold.RelayAliases
import com.nosfabrica.vespa.relay.fold.StoredFold
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The fold read from our records alone answers the roster as the running monitor does. */
class StoredFoldTest {
    private val self = RelayUrlNormalizer.normalize("wss://ours.example")
    private val ours = NostrSignerInternal(KeyPair())

    private fun url(raw: String): NormalizedRelayUrl = RelayUrlNormalizer.normalize(raw)

    private val canonical = url("wss://folded.example/")
    private val alias = url("wss://folded.example/a")
    private val undecided = url("wss://folded.example/b")
    private val root = url("wss://paths.example/")
    private val one = url("wss://paths.example/one")
    private val two = url("wss://paths.example/two")
    private val dead = url("wss://paths.example/three")
    private val solo = url("wss://solo.example/")
    private val gone = url("wss://gone.example/")
    private val goneAlias = url("wss://gone.example/x")
    private val everyUrl = listOf(canonical, alias, undecided, root, one, two, dead, solo, gone, goneAlias)
    private val deadUrls = listOf(dead, gone)

    private val sources =
        RouterConfigLoader
            .parse(
                """
                streams { none { dir = "down", filter = { "kinds": [1] }, urls = [] } }
                monitor { sources = [ { select = [ { kind = 10002, tag = "r" } ], filter = { "kinds": [10002] } } ] }
                """.trimIndent(),
            ).monitorSources()

    /** Every url named by a relay list and recorded by us: two folds, one onto a dead url, undecided servers, a dead path. */
    private suspend fun gradedStore(): NostrSemanticsStore {
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
        store.insert(NostrSignerSync().sign<Event>(1_700_000_000L, 10002, everyUrl.map { arrayOf("r", it.url) }.toTypedArray(), ""))
        val record = RelayVerdictRecord(store, ours)
        record.publish(alias, canonical, 500, 500)
        record.publish(goneAlias, gone, 500, 500)
        for (u in everyUrl - alias - goneAlias - deadUrls.toSet()) record.publishFitness(u, "prime", "answered at a settled anchor", pageable = null, nip77 = null)
        for (u in deadUrls) record.publishFitness(u, "dead", "refused every connect", pageable = null, nip77 = null)
        return store
    }

    /** What the roster reads from a monitor in its own process: the fold over the monitor's derived world. */
    private suspend fun monitorsAnswer(
        store: IEventStore,
        urls: List<NormalizedRelayUrl>,
    ): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        val world =
            StreamWorld(
                store = store,
                sources = sources,
                probe = ReachabilityProbe(null),
                monitorAuthors = MonitorEngine.deadVerdictSigners(ours.pubKey, sources),
                self = ours.pubKey,
                tor = null,
                sockets = Sockets.NONE,
                onProbeEvent = { error("nothing here dials") },
            )
        world.candidates()
        val folding = AliasFolding(RelayAliases(), RelayVerdictRecord(store, ours), AliasProbe(fetch = { _, _, _, _ -> error("the roster never dials") }))
        return folding.applyVerdicts(urls, world.known).let { it.aliases + it.standIns + it.waiting }
    }

    private fun storedFold(store: IEventStore) = StoredFold.over(store, listOf(ours.pubKey), allowOnion = false)

    @Test
    fun `the stored fold answers the roster as the running monitor does`(): Unit =
        runBlocking {
            val store = gradedStore()
            // The whole set, and a stream naming only some of each server's urls.
            for (asked in listOf(everyUrl, listOf(alias, undecided, one, solo, goneAlias))) {
                val expected = monitorsAnswer(store, asked)
                assertEquals(expected, storedFold(store).foldedAway(asked), "asked $asked")
                assertTrue(alias in expected && expected.values.any { it in listOf(root, one, two) }, "the fixture folds and holds: $expected")
            }
        }

    @Test
    fun `a server waits only on a url we hold a record about`(): Unit =
        runBlocking {
            val store = gradedStore()
            // Named by nobody and recorded by nobody: the monitor grades nothing here yet.
            val fresh = url("wss://paths.example/")
            val folded = storedFold(store).foldedAway(listOf(url("wss://paths.example/four"), fresh, two))
            assertTrue(folded.values.all { it in everyUrl - deadUrls.toSet() }, "a stand-in nothing grades: $folded")
        }

    /** Answers until [refusing] is set, then refuses every query. */
    private class FlakyStore(
        inner: IEventStore,
    ) : IEventStore by inner {
        @Volatile var refusing = false
        private val inner = inner

        override suspend fun <T : Event> query(filter: Filter): List<T> = if (refusing) throw IllegalStateException("query refused") else inner.query(filter)
    }

    @Test
    fun `a store that cannot answer leaves the last fold standing`(): Unit =
        runBlocking {
            val store = FlakyStore(gradedStore())
            val fold = storedFold(store)
            val before = fold.foldedAway(everyUrl)
            assertTrue(alias in before)

            store.refusing = true
            assertEquals(before, fold.foldedAway(everyUrl), "a refused read un-hid a folded url")
        }

    @Test
    fun `the fold follows the signers it is given`(): Unit =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            val other = NostrSignerInternal(KeyPair())
            // Onto the path, where the stand-in rule alone would pick the root.
            RelayVerdictRecord(store, other).publish(canonical, alias, 500, 500)
            val asked = listOf(canonical, alias)

            assertEquals(mapOf(canonical to alias), StoredFold.over(store, listOf(other.pubKey), allowOnion = false).foldedAway(asked))
            assertTrue(canonical !in StoredFold.over(store, listOf(ours.pubKey), allowOnion = false).foldedAway(asked), "our fold took another monitor's verdict")
        }
}
