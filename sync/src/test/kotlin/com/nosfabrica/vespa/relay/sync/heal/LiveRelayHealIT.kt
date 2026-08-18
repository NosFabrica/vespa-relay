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
package com.nosfabrica.vespa.relay.sync.heal

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.sync.heal.HealQueue
import com.nosfabrica.vespa.relay.sync.refused.RouterRefusalSink
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.nio.file.Files
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The loop, against real relays, with no fake anywhere in it.
 *
 * Everything else in this suite drives the mechanism from constructed events;
 * this drives it from whatever the network actually serves. It exists because
 * the central claim — that a mirror is offered stale replaceable versions over
 * and over — is a claim about the live network, and a fixture cannot falsify
 * it.
 *
 * **Off unless `SYNC_LIVE_RELAYS` is set**, because a unit suite that dials
 * the internet fails for reasons that have nothing to do with the code. Run it
 * with:
 *
 *     SYNC_LIVE_RELAYS=wss://relay.damus.io,wss://nos.lol \
 *       ./gradlew :sync:test --tests '*LiveRelayHealIT*' -i
 *
 * Nothing here publishes. The healer's push path is deliberately NOT exercised
 * against production relays from a test: writing other people's events to
 * someone's server is not a thing to do by accident from CI.
 */
class LiveRelayHealIT {
    private val relays =
        System
            .getenv("SYNC_LIVE_RELAYS")
            ?.split(",")
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            .orEmpty()

    private fun store(): IEventStore = NostrSemanticsStore(InMemoryEventIndex(), relay = null)

    private class Harness {
        val dir = Files.createTempDirectory("live").toFile().also { it.deleteOnExit() }
        val refused = RefusedIds(dir, 90L * 24 * 60 * 60, 1_000_000)
        val queue = HealQueue()
        val sink = RouterRefusalSink(refused, queue, suppressionEnabled = true)
    }

    private fun client(scope: CoroutineScope): NostrClient {
        val http =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .pingInterval(Duration.ofSeconds(30))
                .build()
        return NostrClient(BasicOkHttpWebSocket.Builder { http }, scope)
    }

    @Test
    fun `three passes over the same live filter show the loop, the gate, and the suppression`() =
        runBlocking {
            if (relays.isEmpty()) {
                println("LiveRelayHealIT skipped — set SYNC_LIVE_RELAYS to run it")
                return@runBlocking
            }
            val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            val client = client(scope)
            val h = Harness()
            val store = store()
            client.connect()

            // Replaceable kinds across several relays: the exact shape the
            // proposal says never converges, because each relay serves its own
            // current version and only one of them can win in our store.
            val ask = Filter(kinds = listOf(0, 3, 10002), limit = 500)

            val replacedPerPass = mutableListOf<Int>()
            val suppressedPerPass = mutableListOf<Int>()
            var downloaded = 0

            repeat(3) { pass ->
                var replaced = 0
                var suppressed = 0
                for (raw in relays) {
                    val url = RelayUrlNormalizer.normalizeOrNull(raw) ?: continue
                    val batch = mutableListOf<Event>()
                    withTimeoutOrNull(90_000) {
                        client.fetchAllPages(url, listOf(ask), idleTimeoutMs = 20_000) { event ->
                            batch.add(event)
                        }
                    }
                    downloaded += batch.size

                    for (event in batch) {
                        // The suppression check sits exactly where
                        // IngestPipeline makes it: after the caller's band
                        // bookkeeping, before the store.
                        if (h.sink.isSuppressed(event)) {
                            suppressed++
                            continue
                        }
                        val outcome = store.batchInsert(listOf(event)).single()
                        if (outcome is IEventStore.InsertOutcome.Rejected) {
                            h.sink.onRefused(
                                event,
                                IngestOrigin(url, healContent = true, healRetractions = true),
                                outcome.reason,
                            )
                            if (outcome.reason.startsWith("replaced:")) replaced++
                        }
                    }
                    println("live: pass ${pass + 1} ${url.url} — ${batch.size} event(s)")
                }
                replacedPerPass += replaced
                suppressedPerPass += suppressed
            }

            println(
                "live: downloaded $downloaded; replaced per pass $replacedPerPass; " +
                    "suppressed per pass $suppressedPerPass; heal queue ${h.queue.size()}; ${h.refused.summary()}",
            )

            assertTrue(downloaded > 0, "no events came back from $relays — the run proves nothing")

            if (replacedPerPass[0] == 0) {
                println("live: no competing versions in this sample; the gate had nothing to act on")
                return@runBlocking
            }

            // The loop: the SAME versions come back on a second ask. This is the
            // claim the whole proposal rests on, measured rather than assumed.
            assertTrue(
                replacedPerPass[1] > 0,
                "pass 2 saw no repeat refusals — the loop this fixes did not reproduce",
            )
            // The gate: one refusal is a candidate only, so nothing may be
            // suppressed before the second.
            assertEquals(0, suppressedPerPass[0], "a first sighting must never suppress")
            // And by the third ask the twice-refused ids are gone from the
            // store's path entirely.
            assertTrue(
                suppressedPerPass[2] > 0,
                "by pass 3 the twice-refused ids should be suppressed; got $suppressedPerPass",
            )
            assertTrue(h.queue.size() > 0, "each stale copy should have queued a repair for the relay serving it")
            scope.coroutineContext[kotlinx.coroutines.Job]?.cancel()
        }

    @Test
    fun `a live relay's replaceable corpus really does hold one version per address`() =
        runBlocking {
            if (relays.isEmpty()) {
                println("LiveRelayHealIT skipped — set SYNC_LIVE_RELAYS to run it")
                return@runBlocking
            }
            // Pins the correction this proposal had to make to its own opening
            // claim: a compliant relay serves its CURRENT version per address,
            // not the whole edit history. If this ever fails against the live
            // network, the sizing arithmetic downstream of it is wrong.
            val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            val client = client(scope)
            client.connect()
            val url = RelayUrlNormalizer.normalizeOrNull(relays.first()) ?: return@runBlocking

            val seen = mutableListOf<Event>()
            withTimeoutOrNull(90_000) {
                client.fetchAllPages(url, listOf(Filter(kinds = listOf(0), limit = 500)), idleTimeoutMs = 20_000) {
                    seen.add(it)
                }
            }
            if (seen.isEmpty()) {
                println("live: no kind-0 events came back; nothing to conclude")
                return@runBlocking
            }
            val byAuthor = seen.groupBy { it.pubKey }
            val multi = byAuthor.filterValues { it.size > 1 }
            println("live: ${seen.size} kind-0 from ${byAuthor.size} author(s); ${multi.size} author(s) with >1 version")
            assertEquals(
                emptyMap(),
                multi
                    .mapValues { it.value.size }
                    .entries
                    .take(5)
                    .associate { it.key.take(8) to it.value },
                "a NIP-01-compliant relay keeps only the newest kind 0 per author",
            )
            scope.coroutineContext[kotlinx.coroutines.Job]?.cancel()
        }
}
