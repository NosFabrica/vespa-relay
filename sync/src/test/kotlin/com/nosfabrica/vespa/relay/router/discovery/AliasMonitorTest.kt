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

import com.nosfabrica.vespa.relay.router.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The monitor is what keeps a probe pass off a sync cycle, so what matters
 * about it is bookkeeping rather than folding: what it holds between passes,
 * and what it does when one stream's pass blows up.
 */
class AliasMonitorTest {
    private val a = RelayUrlNormalizer.normalize("wss://nos.lol")
    private val b = RelayUrlNormalizer.normalize("wss://nos.lol/cipher-zulu")
    private val c = RelayUrlNormalizer.normalize("wss://nos.lol/delta-echo")

    /** Records what each pass was asked to measure, and can be told to fail. */
    private class Recording(
        private val throwOn: Map<String, () -> Throwable> = emptyMap(),
    ) : AliasMonitor.Pass {
        val passes = mutableListOf<Pair<String, List<NormalizedRelayUrl>>>()
        val calls = AtomicInteger()

        override suspend fun measure(
            label: String,
            candidates: List<NormalizedRelayUrl>,
            canDial: suspend (NormalizedRelayUrl) -> Boolean,
            onEvent: suspend (Event) -> Unit,
            sockets: AliasFolding.Sockets,
        ): Int {
            calls.incrementAndGet()
            passes += label to candidates
            throwOn[label]?.let { throw it() }
            return 0
        }
    }

    private fun monitor(
        pass: AliasMonitor.Pass,
        vararg urls: NormalizedRelayUrl,
    ) = sourced(pass, World(urls.toList()))

    /** A [AliasMonitor.CandidateSource] over a fixed world, standing in for the engine's derivation. */
    private class World(
        private val urls: List<NormalizedRelayUrl>,
        /** The row the derivation reports on, where a test is watching it. */
        override val progress: Processors.Handle? = null,
        /** Run INSIDE the derivation, which is the only moment its own phase is observable. */
        private val whileDeriving: () -> Unit = {},
        private val fail: (() -> Throwable)? = null,
    ) : AliasMonitor.CandidateSource {
        val derivations = AtomicInteger()

        override suspend fun candidates(): List<NormalizedRelayUrl> {
            derivations.incrementAndGet()
            whileDeriving()
            fail?.let { throw it() }
            return urls
        }

        override suspend fun canDial(url: NormalizedRelayUrl) = true

        override suspend fun onEvent(event: Event) = Unit

        override val sockets = AliasFolding.Sockets.NONE
    }

    private fun sourced(
        pass: AliasMonitor.Pass,
        source: AliasMonitor.CandidateSource,
    ) = AliasMonitor(listOf(pass), CoroutineScope(Dispatchers.Unconfined), source = source)

    @Test
    fun `a source measures every stream's world without waiting for any of them to submit`() =
        runBlocking {
            // THE RACE THIS EXISTS TO END, measured on staging: a 16-url stream
            // finished discovering in one second, the first pass ran against
            // those 16, and the two 17,499-url streams submitted 190 seconds
            // later — three minutes after the pass had gone to sleep for six
            // hours. Nothing had submitted here at all and the pass still
            // covers the whole world.
            val pass = Recording()
            val world = World(listOf(a, b, c))
            val m = sourced(pass, world)

            m.runPass()

            assertEquals(1, world.derivations.get(), "the world is derived once per pass, when the pass runs")
            assertEquals(listOf(AliasMonitor.ALL_STREAMS), pass.passes.map { it.first })
            assertEquals(listOf(a, b, c), pass.passes.single().second)
        }

    @Test
    fun `the derivation reports on a row of its own, bracketed like the passes it feeds`() =
        runBlocking {
            // THE FIVE MINUTES NOTHING COULD SEE. Deriving the candidate set
            // walks the whole store, and it happens at the head of a sweep,
            // before any pass has been given anything — so every row on the
            // monitor card read `idle` with no countdown, which is exactly what
            // they read when the monitor is asleep for six hours.
            val processors = Processors()
            val row = processors.of("aliasSource")
            var duringPhase: String? = null
            val world =
                World(
                    listOf(a, b, c),
                    progress = row,
                    whileDeriving = { duringPhase = processors.snapshot().single().phase },
                )

            sourced(Recording(), world).runPass()

            // Its own word, and not `measuring`: this opens no socket. A reader
            // told a pass is dialling looks at the relays, and there are none
            // to look at yet.
            assertEquals(Processors.COLLECTING, duringPhase, "the walk says what it is doing while it walks")
            val after = processors.snapshot().single()
            assertEquals(Processors.IDLE, after.phase, "and stands down when the passes take over")
            assertEquals(1L, after.passes, "one derivation is one pass on this row")
            assertTrue(after.lastPassAt != null, "a derivation that ended says when")
        }

    @Test
    fun `a derivation that throws still stamps its clock`() =
        runBlocking {
            // The same reason the passes stamp theirs from a `finally`: a walk
            // that fails every sweep and one that never returns are different
            // faults, and a frozen `lastPassAt` under a phase that never leaves
            // `collecting` is the only thing that tells them apart.
            val processors = Processors()
            val row = processors.of("aliasSource")
            val world = World(listOf(a, b, c), progress = row, fail = { IllegalStateException("the store is down") })

            sourced(Recording(), world).runPass()

            val after = processors.snapshot().single()
            assertEquals(Processors.IDLE, after.phase)
            assertEquals(1L, after.passes)
            assertTrue(after.lastPassAt != null, "a failed derivation is a pass that ran")
        }

    @Test
    fun `an empty world is an empty pass, not a measured one`() =
        runBlocking {
            // A cold store legitimately has no relay lists yet, which must read
            // as "nothing to do YET" and retry rather than as a pass that ran.
            val pass = Recording()
            val m = sourced(pass, World(emptyList()))

            m.runPass()

            assertEquals(0, pass.calls.get())
        }

    @Test
    fun `a world of ONE is measured, because only the fold needs a second url`() =
        runBlocking {
            // THIS USED TO BE SKIPPED, and skipping it took the mirror down.
            // The guard read `size < 2` from when this class ran the fold and
            // nothing else: one url cannot be held up against another. The
            // stability gate and the fitness pass joined the list since and
            // neither compares urls to anything — so a router discovering
            // exactly one relay never got a `prime` grade written for it, and
            // every stream selecting on that grade had a permanently empty
            // roster.
            //
            // The fold's own refusal is unaffected: it lives in
            // `AliasFolding.measure`, against the world it assembles rather
            // than against this pass's candidate list.
            val pass = Recording()
            val m = sourced(pass, World(listOf(a)))

            m.runPass()

            assertEquals(1, pass.calls.get())
            assertEquals(listOf(a), pass.passes.single().second)
        }

    @Test
    fun `one pass failing does not skip the passes after it`() =
        runBlocking {
            // A probe pass talks to arbitrary third-party relays; any of them
            // can fail in ways this cannot enumerate. Losing the rest of the
            // run to one of them is how the stability gate silently stops
            // measuring because the fold threw.
            val failing = Recording(throwOn = mapOf(AliasMonitor.ALL_STREAMS to { IllegalStateException("upstream went away") }))
            val after = Recording()
            val m = AliasMonitor(listOf(failing, after), CoroutineScope(Dispatchers.Unconfined), source = World(listOf(a, b, c)))

            m.runPass()

            assertEquals(1, after.calls.get(), "the run stopped at the failing pass")
        }

    @Test
    fun `cancellation ends the pass rather than being logged`() =
        runBlocking {
            // Swallowing this would keep the monitor grinding through every
            // remaining stream after the scope has been told to stop.
            val cancelling = Recording(throwOn = mapOf(AliasMonitor.ALL_STREAMS to { CancellationException("scope closing") }))
            val after = Recording()
            val m = AliasMonitor(listOf(cancelling, after), CoroutineScope(Dispatchers.Unconfined), source = World(listOf(a, b, c)))

            assertFailsWith<CancellationException> { m.runPass() }
            assertEquals(0, after.calls.get(), "the run continued past a cancellation")
        }

    @Test
    fun `a boot whose streams have not discovered yet retries soon, not next interval`() =
        runBlocking {
            // A cold store has no relay lists to derive from, so the first pass
            // legitimately finds nothing. Sleeping the full interval on that
            // would push the first measurement six hours out on exactly the
            // boot with the most to learn.
            val pass = Recording()
            val filling =
                object : AliasMonitor.CandidateSource {
                    val asked = AtomicInteger()

                    override suspend fun candidates() = if (asked.incrementAndGet() > 1) listOf(a, b) else emptyList()

                    override suspend fun canDial(url: NormalizedRelayUrl) = true

                    override suspend fun onEvent(event: Event) = Unit

                    override val sockets = AliasFolding.Sockets.NONE
                }
            val scope = CoroutineScope(Dispatchers.Default)
            val m =
                AliasMonitor(
                    listOf(pass),
                    scope,
                    intervalMs = 60_000,
                    startupDelayMs = 0,
                    emptyRetryMs = 20,
                    source = filling,
                ).start()

            withTimeout(2_000) { while (pass.calls.get() == 0) delay(10) }
            scope.cancel()

            assertEquals(listOf(a, b), pass.passes.single().second)
        }

    @Test
    fun `the generation moves only when a pass learns something`() =
        runBlocking {
            // It is a VERSION for the fold, read by streams holding a relay list
            // across cycles: a list built before a verdict was published goes on
            // dialling urls now known to be one relay. Bumping it on a pass that
            // learned nothing would throw that list away for no gain — a full
            // rediscovery, which is the cost the cache exists to avoid.
            var learn = 0
            val pass =
                AliasMonitor.Pass { _, _, _, _, _ -> learn }
            val m = monitor(pass, a, b, c)

            m.runPass()
            assertEquals(0L, m.generation(), "a pass that folded nothing changed the version")

            learn = 2
            m.runPass()
            assertEquals(2L, m.generation())

            learn = 1
            m.runPass()
            assertEquals(3L, m.generation(), "the version is not monotonic across passes")
        }

    @Test
    fun `the union's verdicts move one generation for every stream`() =
        runBlocking {
            // A verdict is about a URL, and two streams routinely discover the
            // same one. There is nothing per stream to version.
            val pass = AliasMonitor.Pass { label, _, _, _, _ -> if (label == AliasMonitor.ALL_STREAMS) 4 else 0 }
            val m = monitor(pass, a, b, c)

            m.runPass()

            assertEquals(4L, m.generation())
        }
}
