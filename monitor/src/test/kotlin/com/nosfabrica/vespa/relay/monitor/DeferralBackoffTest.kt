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
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** A relay that asks us to come back later is left alone that long, and its old `prime` does not outlive it. */
class DeferralBackoffTest {
    private val self = RelayUrlNormalizer.normalize("wss://ours.example")
    private val busy = RelayUrlNormalizer.normalize("wss://busy.example")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()
    private val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
    private val dials = AtomicInteger()

    private val tooMany = "cannot:WebSocket Failure: Expected HTTP 101 response but was '429 Too Many Requests'"

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /** [said] is the url's terminal transport word, or null for a relay that answers with [corpus]. */
    private fun fitness(
        store: NostrSemanticsStore,
        sweepMs: Long = AliasMonitor.DEFAULT_INTERVAL_MS,
        said: (NormalizedRelayUrl) -> String? = { if (it == busy) tooMany else null },
    ) = FitnessPass(
        record = RelayVerdictRecord(store, signer),
        probe =
            AliasProbe(
                fetch = { at, want, until, _ ->
                    dials.incrementAndGet()
                    val reason = said(at)
                    if (reason == null) AliasProbe.Page(corpus.filter { until == null || it.createdAt <= until }.take(want)) else AliasProbe.Page(events = null, reason = reason)
                },
                target = 40,
                page = 40,
                fallbackPage = 40,
                idleMs = { 20L },
            ),
        client = EmptyNostrClient(),
        progress = Processors().of("fitness"),
        reconcile = { _, _ -> },
        sweepMs = sweepMs,
    )

    private suspend fun FitnessPass.run(
        label: String,
        urls: List<NormalizedRelayUrl> = listOf(busy),
    ) = measure(label, urls, reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)

    @Test
    fun `a rate-limited url is not dialled again by the fast lane within its backoff`() =
        runBlocking {
            val pass = fitness(newStore())
            pass.run(AliasMonitor.FAST_LANE)
            val first = dials.get()
            assertTrue(first > 0)

            repeat(3) { pass.run(AliasMonitor.FAST_LANE) }

            assertEquals(first, dials.get(), "every lane tick dialled a relay that had just answered 429")
        }

    @Test
    fun `each way a relay defers backs off`() =
        runBlocking {
            val erroring = RelayUrlNormalizer.normalize("wss://erroring.example")
            val unknown = RelayUrlNormalizer.normalize("wss://unknown.example")
            val words =
                mapOf(
                    busy to tooMany,
                    erroring to "cannot:WebSocket Failure: Expected HTTP 101 response but was '503 Service Unavailable'",
                    unknown to "cannot: java.io.EOFException",
                )
            val pass = fitness(newStore(), said = { words[it] })

            pass.run(AliasMonitor.FAST_LANE, words.keys.toList())

            assertEquals(words.keys, pass.backingOff(words.keys))
        }

    @Test
    fun `the sweep dials it again once the backoff lapses`() =
        runBlocking {
            val pass = fitness(newStore(), sweepMs = 200L)
            pass.run(AliasMonitor.ALL_STREAMS)
            val first = dials.get()

            delay(300)
            pass.run(AliasMonitor.ALL_STREAMS)

            assertTrue(dials.get() > first, "a backoff of one sweep kept the url out of the sweep after it")
        }

    @Test
    fun `a relay's own Retry-After holds it out past the sweep's backoff`() =
        runBlocking {
            val pass = fitness(newStore(), sweepMs = 200L, said = { if (it == busy) "$tooMany; Retry-After: 3600" else null })
            pass.run(AliasMonitor.ALL_STREAMS)
            val first = dials.get()

            delay(300)
            pass.run(AliasMonitor.ALL_STREAMS)

            assertEquals(first, dials.get(), "the relay asked for an hour and was dialled again sooner")
        }

    @Test
    fun `a url that answers clears its backoff`() =
        runBlocking {
            var limited = true
            val pass = fitness(newStore(), sweepMs = 200L, said = { if (limited) tooMany else null })
            pass.run(AliasMonitor.ALL_STREAMS)
            assertEquals(setOf(busy), pass.backingOff(listOf(busy)))

            limited = false
            delay(300)
            pass.run(AliasMonitor.ALL_STREAMS)

            assertTrue(pass.backingOff(listOf(busy)).isEmpty())
        }

    @Test
    fun `a prime on a url that defers two sweeps in a row is withdrawn, and nothing negative is said`() =
        runBlocking {
            val store = newStore()
            RelayVerdictRecord(store, signer).publishFitness(busy, Verdict.PRIME.value, "answered 20 events", pageable = null, nip77 = null)
            val pass = fitness(store)

            pass.run(AliasMonitor.ALL_STREAMS)
            assertEquals(Verdict.PRIME.value, gradeOf(store, busy), "one deferral is a moment, not a reason to withdraw")

            // Still backing off, which is still deferring.
            pass.run(AliasMonitor.ALL_STREAMS)

            assertNull(gradeOf(store, busy), "a relay refusing to be measured kept its prime")
        }

    @Test
    fun `a prime older than two sweeps is withdrawn on the first deferral`() =
        runBlocking {
            val store = newStore()
            val sweepMs = AliasMonitor.DEFAULT_INTERVAL_MS
            val measuredAt = nowSeconds() - (FitnessPass.DEFERRED_SWEEPS + 1) * sweepMs / 1000
            RelayVerdictRecord(store, signer).publishFitness(busy, Verdict.PRIME.value, "answered 20 events", pageable = null, nip77 = null, measuredAt = measuredAt)
            assertEquals(Verdict.PRIME.value, gradeOf(store, busy), "the fixture needs the old grade to stand")

            fitness(store, sweepMs = sweepMs).run(AliasMonitor.ALL_STREAMS)

            assertNull(gradeOf(store, busy))
        }

    @Test
    fun `the fast lane never takes a prime back`() =
        runBlocking {
            val store = newStore()
            RelayVerdictRecord(store, signer).publishFitness(busy, Verdict.PRIME.value, "answered 20 events", pageable = null, nip77 = null)
            val pass = fitness(store, sweepMs = 1L)

            repeat(DEFERRED_RUNS) {
                pass.run(AliasMonitor.FAST_LANE)
                delay(5)
            }

            assertEquals(Verdict.PRIME.value, gradeOf(store, busy))
        }

    @Test
    fun `the lane's candidate set leaves out a url fitness is backing off from`() =
        runBlocking {
            val store = newStore()
            store.insert(events.sign(nowSeconds(), 10002, arrayOf(arrayOf("r", busy.url, "write"), arrayOf("r", "wss://fine.example", "write")), ""))
            val pass = fitness(store)
            val world =
                StreamWorld(
                    store = store,
                    sources = sources(),
                    probe = ReachabilityProbe(null),
                    monitorAuthors = listOf(signer.pubKey),
                    self = signer.pubKey,
                    tor = null,
                    sockets = Sockets.NONE,
                    onProbeEvent = {},
                    recentlyGraded = { urls -> StreamWorld.gradedSince(RelayVerdictRecord(store, signer), urls, nowSeconds() - 3600) },
                    backingOff = pass::backingOff,
                )
            val monitor =
                AliasMonitor(
                    passes = emptyList(),
                    scope = CoroutineScope(Dispatchers.Unconfined),
                    // The world's own pre-probe would resolve these names; the dial is the fixture's.
                    source =
                        object : AliasMonitor.CandidateSource by world {
                            override suspend fun reach(url: NormalizedRelayUrl) = Reach.REACHABLE
                        },
                    fastLaneEveryMs = 1_000L,
                    fastLanePasses = listOf(AliasMonitor.Pass { l, c, r, e, s -> pass.measure(l, c, r, e, s) }),
                )

            monitor.runFastLane(0)
            val first = dials.get()
            monitor.runFastLane(0)

            assertEquals(first, dials.get(), "the second tick dialled the relay that had just asked us to wait")
            assertEquals(setOf(busy), pass.backingOff(listOf(busy)), "the fixture needs the lane to have deferred it")
            // Left out before any lane pass sees it, so the stability gate ahead of fitness does not dial it either.
            assertTrue(busy !in world.candidatesSince(0))
        }

    private fun sources() =
        RouterConfigLoader
            .parse(
                """
                streams { none { dir = "down", filter = { "kinds": [1] }, urls = [] } }
                monitor {
                    sources = [
                        {
                            select = [ { kind = 10002, tag = "r", marker = "write" } ]
                            filter = { "kinds": [10002] }
                        }
                    ]
                }
                """.trimIndent(),
            ).monitorSources()

    private suspend fun gradeOf(
        store: NostrSemanticsStore,
        url: NormalizedRelayUrl,
    ): String? =
        store
            .query<Event>(
                Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(url.url))),
            ).flatMap { it.tags.toList() }
            .firstOrNull { it.size >= 3 && it[0] == "l" && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }
            ?.get(1)

    private companion object {
        const val DEFERRED_RUNS = 4
    }
}
