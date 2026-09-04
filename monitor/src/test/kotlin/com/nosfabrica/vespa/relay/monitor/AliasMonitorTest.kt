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

import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
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

/** The monitor's bookkeeping between passes: what it holds, and what it does when a pass throws. */
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
            sockets: Sockets,
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

    /** A [AliasMonitor.CandidateSource] over a fixed world. */
    private class World(
        private val urls: List<NormalizedRelayUrl>,
        override val progress: Processors.Handle? = null,
        /** Run inside the derivation, the only moment its own phase is observable. */
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

        override val sockets = Sockets.NONE
    }

    private fun sourced(
        pass: AliasMonitor.Pass,
        source: AliasMonitor.CandidateSource,
    ) = AliasMonitor(listOf(pass), CoroutineScope(Dispatchers.Unconfined), source = source)

    @Test
    fun `a source measures every stream's world without waiting for any of them to submit`() =
        runBlocking {
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

            // `collecting`, not `measuring`: the walk opens no socket.
            assertEquals(Processors.COLLECTING, duringPhase, "the walk says what it is doing while it walks")
            val after = processors.snapshot().single()
            assertEquals(Processors.IDLE, after.phase, "and stands down when the passes take over")
            assertEquals(1L, after.passes, "one derivation is one pass on this row")
            assertTrue(after.lastPassAt != null, "a derivation that ended says when")
        }

    @Test
    fun `a derivation that throws still stamps its clock`() =
        runBlocking {
            // A walk that fails every sweep and one that never returns are different
            // faults; a frozen clock under a phase stuck on `collecting` is what separates them.
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
            // A cold store has no relay lists yet; that is a retry, not a pass that ran.
            val pass = Recording()
            val m = sourced(pass, World(emptyList()))

            m.runPass()

            assertEquals(0, pass.calls.get())
        }

    @Test
    fun `a world of ONE is measured, because only the fold needs a second url`() =
        runBlocking {
            // The fold's own refusal lives in `AliasFolding.measure`, against the world it assembles.
            val pass = Recording()
            val m = sourced(pass, World(listOf(a)))

            m.runPass()

            assertEquals(1, pass.calls.get())
            assertEquals(listOf(a), pass.passes.single().second)
        }

    @Test
    fun `one pass failing does not skip the passes after it`() =
        runBlocking {
            val failing = Recording(throwOn = mapOf(AliasMonitor.ALL_STREAMS to { IllegalStateException("upstream went away") }))
            val after = Recording()
            val m = AliasMonitor(listOf(failing, after), CoroutineScope(Dispatchers.Unconfined), source = World(listOf(a, b, c)))

            m.runPass()

            assertEquals(1, after.calls.get(), "the run stopped at the failing pass")
        }

    @Test
    fun `cancellation ends the pass rather than being logged`() =
        runBlocking {
            // Swallowed, the monitor would grind through every remaining stream after the scope was told to stop.
            val cancelling = Recording(throwOn = mapOf(AliasMonitor.ALL_STREAMS to { CancellationException("scope closing") }))
            val after = Recording()
            val m = AliasMonitor(listOf(cancelling, after), CoroutineScope(Dispatchers.Unconfined), source = World(listOf(a, b, c)))

            assertFailsWith<CancellationException> { m.runPass() }
            assertEquals(0, after.calls.get(), "the run continued past a cancellation")
        }

    @Test
    fun `a boot whose streams have not discovered yet retries soon, not next interval`() =
        runBlocking {
            val pass = Recording()
            val filling =
                object : AliasMonitor.CandidateSource {
                    val asked = AtomicInteger()

                    override suspend fun candidates() = if (asked.incrementAndGet() > 1) listOf(a, b) else emptyList()

                    override suspend fun canDial(url: NormalizedRelayUrl) = true

                    override suspend fun onEvent(event: Event) = Unit

                    override val sockets = Sockets.NONE
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
            // A version for streams holding a relay list across cycles; bumping it on a
            // pass that learned nothing would force a full rediscovery for no gain.
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
            // A verdict is about a url, and two streams routinely discover the same one.
            val pass = AliasMonitor.Pass { label, _, _, _, _ -> if (label == AliasMonitor.ALL_STREAMS) 4 else 0 }
            val m = monitor(pass, a, b, c)

            m.runPass()

            assertEquals(4L, m.generation())
        }
}
