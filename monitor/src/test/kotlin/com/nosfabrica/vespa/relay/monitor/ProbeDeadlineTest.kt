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
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** One url must not hold a pass open, and a url the deadline cut carries no verdict. */
class ProbeDeadlineTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val wedged = RelayUrlNormalizer.normalize("wss://wedged.example")
    private val answering = RelayUrlNormalizer.normalize("wss://answers.example")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /** Deep enough to clear [RelayAliases.DEFAULT_MIN_SAMPLE] as one page. */
    private fun corpus(n: Int = 40): List<Event> = (0 until n).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }

    /** A relay that honours the cursor; one that ignores it is graded `unpageable` before any clock fires. */
    private fun paged(
        events: List<Event>,
        want: Int,
        until: Long?,
    ) = AliasProbe.Page(events.filter { until == null || it.createdAt <= until }.take(want))

    /** The deadline scales with the idle window, so a whole deadline fits in test time. */
    private val tinyIdleMs = 20L

    private fun deadlineMs() = AliasProbe.WINDOWS_PER_URL * tinyIdleMs

    /** Answers [answering] normally and parks [wedged] with no timer under it, as an unarmed idle window would. */
    private fun stalling(hits: AtomicInteger? = null): suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page =
        { url, want, until, _ ->
            hits?.incrementAndGet()
            if (url == wedged) {
                CompletableDeferred<Unit>().await()
                error("unreachable")
            } else {
                paged(corpus(), want, until)
            }
        }

    private fun probe(fetch: suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page) = AliasProbe(fetch = fetch, target = 40, page = 40, fallbackPage = 40, idleMs = { tinyIdleMs })

    @Test
    fun `a pass whose dials all come back empty publishes NOTHING, including the verdicts that looked fine`() =
        runBlocking {
            // When dialling breaks it breaks for the whole batch, so the few that answered go with the rest.
            val store = newStore()
            val blind = (0 until 90).map { RelayUrlNormalizer.normalize("wss://blind$it.example") }
            val fine = (0 until 10).map { RelayUrlNormalizer.normalize("wss://fine$it.example") }
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe =
                        probe { url, want, until, _ ->
                            if (url in fine) paged(corpus(), want, until) else AliasProbe.Page(events = null, reason = null)
                        },
                    client = EmptyNostrClient(),
                    progress = Processors().of("fitness"),
                )
            pass.measure("blind batch", blind + fine, reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)

            for (url in blind + fine) {
                assertNull(gradeOf(store, url), "a blind pass wrote a verdict for ${url.url}")
            }

            // A transport word beside servers that answered is a fact about that relay, and publishes.
            val refused = newStore()

            fun refusedCorpus(answering: List<NormalizedRelayUrl>) =
                FitnessPass(
                    record = RelayVerdictRecord(refused, signer),
                    probe =
                        probe { url, want, until, _ ->
                            if (url in answering) paged(corpus(), want, until) else AliasProbe.Page(events = null, reason = "cannot: Failed to connect to /1.2.3.4:443 (ConnectException)")
                        },
                    client = EmptyNostrClient(),
                    progress = Processors().of("fitness"),
                )
            refusedCorpus(fine).measure("refused corpus", blind + fine, reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)
            assertEquals(Verdict.SILENT.value, gradeOf(refused, blind.first()), "a transport word is evidence and must still publish")

            // The same words from every dial, with nothing answering, are our own network's.
            val dark = newStore()
            FitnessPass(
                record = RelayVerdictRecord(dark, signer),
                probe = probe { _, _, _, _ -> AliasProbe.Page(events = null, reason = "cannot: Failed to connect to /1.2.3.4:443 (ConnectException)") },
                client = EmptyNostrClient(),
                progress = Processors().of("fitness"),
            ).measure("dark corpus", blind, reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)
            assertNull(gradeOf(dark, blind.first()), "a batch where nothing reached a server publishes no `silent`")
        }

    @Test
    fun `a proxy that does not answer grades nothing, however many urls sit behind it`() =
        runBlocking {
            // Under SYNC_TOR_ALL a dead SOCKS port declines every url; none of that is about the relays.
            for (size in listOf(1, 60)) {
                val store = newStore()
                val urls = (0 until size).map { RelayUrlNormalizer.normalize("wss://behind$it.example") }
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = probe { _, want, until, _ -> paged(corpus(), want, until) },
                    client = EmptyNostrClient(),
                    progress = Processors().of("fitness"),
                ).measure("proxy down", urls, reach = { Reach.TRANSPORT_DOWN }, onEvent = {}, sockets = Sockets.NONE)
                for (url in urls) assertNull(gradeOf(store, url), "our proxy being down was signed onto ${url.url}")
            }
        }

    @Test
    fun `pre-probe proof is withheld from a batch where our network looks dark`() =
        runBlocking {
            // Our resolver or egress failing proves every host gone at once; a network does not go dark at once.
            suspend fun graded(
                gone: Int,
                answering: Int,
            ): Pair<List<String?>, List<String?>> {
                val store = newStore()
                val dead = (0 until gone).map { RelayUrlNormalizer.normalize("wss://gone$it.example") }
                val live = (0 until answering).map { RelayUrlNormalizer.normalize("wss://live$it.example") }
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = probe { _, want, until, _ -> paged(corpus(), want, until) },
                    client = EmptyNostrClient(),
                    progress = Processors().of("fitness"),
                ).measure(
                    "dark",
                    dead + live,
                    reach = { if (it in dead) Reach.PROVED_UNREACHABLE else Reach.REACHABLE },
                    onEvent = {},
                    sockets = Sockets.NONE,
                )
                return dead.map { gradeOf(store, it) } to live.map { gradeOf(store, it) }
            }

            val (outage, _) = graded(gone = 60, answering = 0)
            assertTrue(outage.all { it == null }, "a batch with no server reached signed `dead` onto ${outage.count { it != null }} url(s)")

            val (small, _) = graded(gone = 2, answering = 0)
            assertTrue(small.all { it == null }, "a fast-lane batch with no server reached must not sign `dead` either")

            val (mostly, mostlyLive) = graded(gone = 50, answering = 10)
            assertTrue(mostly.all { it == null }, "a pre-probe failing past the dead share must be doubted")
            assertTrue(mostlyLive.all { it == Verdict.PRIME.value }, "the relays that answered are still graded")

            val (some, _) = graded(gone = 20, answering = 40)
            assertTrue(some.all { it == Verdict.DEAD.value }, "proof beside a working network is published")
        }

    @Test
    fun `dead is published on proof or on the server's own refusal, never on a connect word the pre-probe contradicts`() =
        runBlocking {
            val url = RelayUrlNormalizer.normalize("wss://gone.example")

            suspend fun grade(
                reason: String,
                reachable: Boolean,
            ): String? {
                val store = newStore()
                // Beside a url that answers: a pre-probe proof is believed only where our network reaches others.
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = probe { at, want, until, _ -> if (at == answering) paged(corpus(), want, until) else AliasProbe.Page(events = null, reason = reason) },
                    client = EmptyNostrClient(),
                    progress = Processors().of("fitness"),
                ).measure(
                    "one",
                    listOf(url, answering),
                    reach = { if (it == url && !reachable) Reach.PROVED_UNREACHABLE else Reach.REACHABLE },
                    onEvent = {},
                    sockets = Sockets.NONE,
                )
                return gradeOf(store, url)
            }

            // The pre-probe resolved the name, so a dial that could not is our resolver and earns nothing.
            assertNull(grade("cannot: java.net.UnknownHostException: Temporary failure in name resolution", reachable = true))
            assertNull(grade("cannot: java.net.UnknownHostException: gone.example", reachable = true))
            // A routing or refusal word from the dial is at most `silent`.
            assertEquals(Verdict.SILENT.value, grade("cannot: connect: Network is unreachable", reachable = true))
            assertEquals(Verdict.SILENT.value, grade("cannot: java.net.ConnectException: Connection refused", reachable = true))
            // The server answered the connect and refused the protocol.
            assertEquals(Verdict.DEAD.value, grade("cannot: javax.net.ssl.SSLHandshakeException: PKIX path building failed", reachable = true))
            assertEquals(Verdict.DEAD.value, grade("cannot: Expected HTTP 101 response but was '404 Not Found'", reachable = true))
            // A server or CDN error is a moment, and an auth or payment wall is a live relay.
            assertNull(grade("cannot:WebSocket Failure: Expected HTTP 101 response but was '503 Service Unavailable'", reachable = true))
            assertNull(grade("cannot:WebSocket Failure: Expected HTTP 101 response but was '530 <none>'", reachable = true))
            assertEquals(Verdict.RESTRICTED.value, grade("cannot:WebSocket Failure: Expected HTTP 101 response but was '402 Payment Required'", reachable = true))
            // The typed pre-probe's proof stands on its own.
            assertEquals(Verdict.DEAD.value, grade("unused", reachable = false))
        }

    @Test
    fun `the deadline is a multiple of the very window it bounds`() {
        // Per url, not per process: a `.onion` is allowed its circuit on top of the clearnet budget.
        val clearnet = AliasProbe(fetch = stalling(), idleMs = { 20_000L })
        assertEquals(AliasProbe.WINDOWS_PER_URL * 20_000L, clearnet.deadlineMs(answering))

        val onion = AliasProbe(fetch = stalling(), idleMs = { if (it == wedged) 110_000L else 20_000L })
        assertTrue(
            onion.deadlineMs(wedged) > onion.deadlineMs(answering),
            "a url dialled through a longer window must get a longer deadline, or the fold never measures one",
        )
    }

    @Test
    fun `a fitness pass ends even though one url never answers, and grades it nothing`() =
        runBlocking {
            val store = newStore()
            val record = RelayVerdictRecord(store, signer)
            val processors = Processors()
            val pass =
                FitnessPass(
                    record = record,
                    probe = probe(stalling()),
                    client = EmptyNostrClient(),
                    progress = processors.of("fitness"),
                )

            // The assertion is the call completing at all.
            withTimeout(deadlineMs() * 20) {
                pass.measure(
                    "deadline",
                    listOf(wedged, answering),
                    reach = { Reach.REACHABLE },
                    onEvent = {},
                    sockets = Sockets.NONE,
                )
            }

            assertEquals(Verdict.PRIME.value, gradeOf(store, answering))
            assertNull(
                gradeOf(store, wedged),
                "a url the deadline cut must carry NO verdict: the clock is ours and the grade would be about the relay",
            )
        }

    @Test
    fun `a stability pass ends, and calls the url it abandoned exactly that`() =
        runBlocking {
            val processors = Processors()
            val handle = processors.of("consistency")
            val consistency = RelayConsistency()
            val pass =
                ConsistencyPass(
                    consistency = consistency,
                    record = RelayVerdictRecord(newStore(), signer),
                    probe = probe(stalling()),
                    progress = handle,
                )

            withTimeout(deadlineMs() * 20) {
                pass.measure("deadline", listOf(wedged, answering), canDial = { true })
            }

            // Its own reason, not the probe-threw bucket.
            val reasons =
                processors
                    .snapshot()
                    .single()
                    .work
                    .single()
                    .undecided
                    .map { it.reason }
            assertTrue(
                ConsistencyPass.Unmeasured.ABANDONED.reason in reasons,
                "the abandoned url must be named as abandoned, not as a probe failure: $reasons",
            )
            assertEquals(emptySet<NormalizedRelayUrl>(), consistency.unusable(listOf(wedged)).toSet())
        }

    @Test
    fun `a fold pass ends, and publishes nothing about the host it could not fingerprint`() =
        runBlocking {
            // Both urls of the group hang, so it has no yardstick.
            val a = RelayUrlNormalizer.normalize("wss://wedged.example/one")
            val b = RelayUrlNormalizer.normalize("wss://wedged.example/two")
            val aliases = RelayAliases()
            val hits = AtomicInteger()
            val fold =
                AliasFolding(
                    aliases = aliases,
                    record = RelayVerdictRecord(newStore(), signer),
                    probe =
                        AliasProbe(
                            fetch = { _, _, _, _ ->
                                hits.incrementAndGet()
                                CompletableDeferred<AliasProbe.Page>().await()
                            },
                            target = 40,
                            page = 40,
                            fallbackPage = 40,
                            idleMs = { tinyIdleMs },
                        ),
                )

            val learned =
                withTimeout(deadlineMs() * 40) {
                    fold.measure("deadline", listOf(a, b), canDial = { true })
                }

            assertEquals(0, learned, "a fold that never got a fingerprint must learn nothing")
            assertTrue(hits.get() > 0, "the pass has to have actually dialled, or this proves nothing")
            // Nothing written, so the group comes back next pass instead of carrying a fold signed off our timeout.
            assertTrue(aliases.unresolved(listOf(a, b)).isNotEmpty())
        }

    @Test
    fun `the abandoned url is named while it is still held, and released when it is cut`() =
        runBlocking {
            val processors = Processors()
            val handle = processors.of("fitness")
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(newStore(), signer),
                    probe = probe(stalling()),
                    client = EmptyNostrClient(),
                    progress = handle,
                )
            // A suspended coroutine has no stack frame to dump, so the held set is how a stall is diagnosed.
            val seen = mutableListOf<Processors.Holding.Held>()
            val watcher =
                Thread {
                    val until = System.currentTimeMillis() + deadlineMs() * 20
                    while (System.currentTimeMillis() < until) {
                        processors
                            .snapshot()
                            .single()
                            .inFlight
                            ?.relays
                            ?.let { seen += it }
                        Thread.sleep(5)
                    }
                }
            watcher.start()
            withTimeout(deadlineMs() * 20) {
                pass.measure("deadline", listOf(wedged), reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)
            }
            watcher.interrupt()
            watcher.join()

            val named = seen.firstOrNull { it.relay == wedged.url }
            assertNotNull(named, "a held url must be nameable from the snapshot; that is the whole point of the set")
            assertEquals(FitnessPass.STAGE_LADDER, named.stage, "…and it must say which step it is on")
            assertNull(processors.snapshot().single().inFlight)
        }

    @Test
    fun `a relay that connects and then says nothing is graded NOTHING AT ALL`() =
        runBlocking {
            // A window lapsing with no terminal reason is also what our own broken socket layer produces.
            val store = newStore()
            val pass =
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = probe { _, _, _, _ -> AliasProbe.Page(events = null, reason = null) },
                    client = EmptyNostrClient(),
                    progress = Processors().of("fitness"),
                )
            pass.measure("silence", listOf(wedged), reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)
            assertNull(gradeOf(store, wedged), "an instrument that learned nothing must not sign a verdict")

            // A relay that closes every rung has spoken and is read as a drain; `restricted` is unreachable
            // without a signal `AliasProbe.Page` lacks.
            val refusing = newStore()
            FitnessPass(
                record = RelayVerdictRecord(refusing, signer),
                probe = probe { _, _, _, _ -> AliasProbe.Page(events = emptyList(), reason = "closed: blocked: can't handle empty filters") },
                client = EmptyNostrClient(),
                progress = Processors().of("fitness"),
            ).measure("refusal", listOf(wedged), reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)
            assertEquals(Verdict.PRIME.value, gradeOf(refusing, wedged))
        }

    /** The fitness grade the store carries for [url], or null for no record; a roster cannot tell `dead` from absent. */
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
}
