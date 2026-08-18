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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.router.progress.Processors
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

/**
 * ONE URL MUST NOT BE ABLE TO HOLD A PASS OPEN, and what the pass says about
 * the url it gave up on.
 *
 * ## The failure being pinned
 *
 * A production fitness pass sat at `attempted: 12,373 of 12,374` for 74 minutes
 * and counting, on one url. Every outbound call the per-url job makes is
 * bounded — a 5s TCP pre-probe, a 10s NIP-11 document, an idle window per rung
 * of the ask ladder, a 10s NEG-OPEN — and the pass hung anyway, because
 * `coroutineScope` joins on every child and an IDLE WINDOW IS NOT A WALL CLOCK.
 * Quartz says so in its own header for the call these walks are made of: *"there
 * is no wall-clock ceiling parameter … a hard deadline composes at the call
 * site"*. Two paths inside that fetch loop are outside the window by
 * construction — a relay that never stops sending keeps the timeout disarmed,
 * and the suspending `onEvent` hook (which for this router is a bounded ingest
 * queue) is deliberately run outside the timeout scope.
 *
 * Because the streams admit a relay only on the grade that pass writes, the
 * whole dynamic mirror was down for as long as the one probe hung: `roster: 0`,
 * `tails: 0`, three streams reporting `0 certified relay(s)`.
 *
 * ## What each test drives
 *
 * A probe whose fetch NEVER RETURNS, through a real pass, with an idle window
 * small enough that [AliasProbe.WINDOWS_PER_URL] of them pass in test time. The
 * assertion is always the same pair: the pass RETURNS, and the url it abandoned
 * carries no verdict — a deadline is our instrument giving up, and publishing a
 * grade off it would sign our own timeout as a claim about somebody's server.
 */
class ProbeDeadlineTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val wedged = RelayUrlNormalizer.normalize("wss://wedged.example")
    private val answering = RelayUrlNormalizer.normalize("wss://answers.example")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /** Deep enough to clear [RelayAliases.DEFAULT_MIN_SAMPLE], as one page. */
    private fun corpus(n: Int = 40): List<Event> = (0 until n).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }

    /**
     * A window short enough that the whole deadline fits in a test.
     *
     * The point of deriving the deadline from the idle window rather than from
     * a constant is exactly that it scales, so a test can shrink it: 12 windows
     * of 20ms is a quarter of a second here and four minutes in production,
     * from the same line of code.
     */
    private val tinyIdleMs = 20L

    private fun deadlineMs() = AliasProbe.WINDOWS_PER_URL * tinyIdleMs

    /**
     * A fetch that answers [answering] normally and never answers [wedged].
     *
     * `CompletableDeferred` that nobody completes, rather than a long `delay`:
     * a delay is cancellable at a known point and would prove only that
     * `withTimeoutOrNull` cancels a sleeping coroutine. This parks the walk on
     * something with no timer under it at all, which is the shape of the
     * failure — a fetch loop whose idle window is never armed.
     */
    private fun stalling(hits: AtomicInteger? = null): suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page =
        { url, _, _, _ ->
            hits?.incrementAndGet()
            if (url == wedged) {
                CompletableDeferred<Unit>().await()
                error("unreachable")
            } else {
                AliasProbe.Page(corpus())
            }
        }

    private fun probe(fetch: suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page) = AliasProbe(fetch = fetch, target = 40, page = 40, fallbackPage = 40, idleMs = { tinyIdleMs })

    @Test
    fun `the deadline is a multiple of the very window it bounds`() {
        // Per URL, not per process — a `.onion` is allowed its circuit on top of
        // the clearnet budget, and a deadline sized from a constant would cut
        // every hidden service the monitor measures. So the probe is asked, and
        // it answers from the same lambda its fetch uses.
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
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = processors.of("fitness"),
                )

            // The bound this test is really about: BEFORE the deadline existed
            // this call did not return at all, so the assertion is the call
            // completing. Generous against the pass's own budget — the point is
            // "bounded", not "bounded to the millisecond" on a shared runner.
            withTimeout(deadlineMs() * 20) {
                pass.measure(
                    "deadline",
                    listOf(wedged, answering),
                    canDial = { true },
                    onEvent = {},
                    sockets = AliasFolding.Sockets.NONE,
                )
            }

            // The url that answered is graded; the url that hung is not
            // mentioned. `prime` is the whole admission decision, so a wrong
            // grade here is a relay wrongly in or out of every stream's list.
            assertEquals(FitnessPass.Verdict.PRIME.value, gradeOf(store, answering))
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

            // The reason is its own — not the probe-threw bucket beside it.
            // "The walk got an answer it could not use" and "the walk never got
            // an answer" want different responses, and only one of them is the
            // failure this deadline exists for.
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
            // …and nothing was learned about it either way, so the next pass
            // asks again rather than inheriting our timeout as a verdict.
            assertEquals(emptySet<NormalizedRelayUrl>(), consistency.unusable(listOf(wedged)).toSet())
        }

    @Test
    fun `a fold pass ends, and publishes nothing about the host it could not fingerprint`() =
        runBlocking {
            // Two urls on ONE host: a group, which is the fold's unit of work.
            // Both hang, so the group has no yardstick and must end undecided.
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
            // Nothing written down means the group comes back next pass — which
            // is the correct outcome and the one thing a published `same-as`
            // signed off our own timeout would have destroyed for a month.
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
                    foldedAway = { emptyMap() },
                    inconsistent = { emptySet() },
                    progress = handle,
                )
            // The held set is what made the 74-minute stall diagnosable at all:
            // the url was recoverable only from OkHttp thread names, because a
            // suspended coroutine has no stack frame to dump.
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
                pass.measure("deadline", listOf(wedged), canDial = { true }, onEvent = {}, sockets = AliasFolding.Sockets.NONE)
            }
            watcher.interrupt()
            watcher.join()

            val named = seen.firstOrNull { it.relay == wedged.url }
            assertNotNull(named, "a held url must be nameable from the snapshot; that is the whole point of the set")
            assertEquals(FitnessPass.STAGE_LADDER, named.stage, "…and it must say which step it is on")
            // And the row goes with the job. A row that outlived its pass would
            // be a fault report about a leg that is not there.
            assertNull(processors.snapshot().single().inFlight)
        }

    /**
     * The fitness grade the store now carries for [url], or null for NO RECORD
     * AT ALL — which is the distinction both fitness assertions rest on.
     *
     * Read off the record the pass signed rather than through [RelayDiscovery],
     * because "admitted" and "graded" are different questions: a `dead` verdict
     * is absent from a roster exactly as a missing record is, and the whole
     * claim here is that the deadline wrote nothing rather than that it wrote
     * something unadmitting.
     */
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
