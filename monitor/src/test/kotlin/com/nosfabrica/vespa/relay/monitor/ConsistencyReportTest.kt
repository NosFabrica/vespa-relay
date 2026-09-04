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
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * What the stability gate says about the urls it could not decide: each way of
 * failing comes out under its own reason, and the candidate set divides exactly once.
 */
class ConsistencyReportTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val steady = RelayUrlNormalizer.normalize("wss://nos.lol")
    private val shuffler = RelayUrlNormalizer.normalize("wss://fiatjaf.com")
    private val thin = RelayUrlNormalizer.normalize("wss://quiet.example")
    private val duplicate = RelayUrlNormalizer.normalize("wss://nos.lol/alpha")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /** A corpus deep enough to clear [RelayAliases.DEFAULT_MIN_SAMPLE], as one page. */
    private fun corpus(n: Int = 40): List<Event> = (0 until n).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }

    /** A pass whose every url answers the same way, reporting into a real [Processors] handle. */
    private fun pass(
        handle: Processors.Handle,
        store: NostrSemanticsStore = newStore(),
        fetch: suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page,
    ) = ConsistencyPass(
        consistency = RelayConsistency(),
        record = RelayVerdictRecord(store, signer),
        probe = AliasProbe(fetch = fetch, target = 40, page = 40, fallbackPage = 40),
        progress = handle,
    )

    private fun row(processors: Processors) =
        processors
            .snapshot()
            .single()
            .work
            .single()

    private fun reasonOf(processors: Processors) = row(processors).undecided.single().reason

    @Test
    fun `a url our own transport will not carry is not a claim about the relay`() =
        runBlocking {
            // No socket is opened, so it must not count as a dial.
            val processors = Processors()
            val dials = AtomicInteger()
            val gate =
                pass(processors.of("consistency")) { _, _, _, _ ->
                    dials.incrementAndGet()
                    AliasProbe.Page(corpus())
                }

            assertEquals(0, gate.measure("t", listOf(steady), canDial = { false }))
            assertEquals(0, dials.get(), "a url the transport declined must never reach the probe")
            assertEquals("declined by our own transport", reasonOf(processors))
            assertEquals(0, row(processors).dialled, "no socket was opened, so nothing was dialled")
            assertEquals(1, row(processors).undecided.single().urls)
        }

    @Test
    fun `a probe that throws before the walk is ours, and says so`() =
        runBlocking {
            val processors = Processors()
            val gate = pass(processors.of("consistency")) { _, _, _, _ -> AliasProbe.Page(corpus()) }

            assertEquals(0, gate.measure("t", listOf(steady), canDial = { error("socket budget") }))
            assertEquals("the probe failed mid-walk", reasonOf(processors))
        }

    @Test
    fun `a url that never answers is silent, and one that answers once is not`() =
        runBlocking {
            // The row is the cause, nested under the reason it refines.
            val silent = Processors()
            val never =
                pass(silent.of("consistency")) { _, _, _, _ ->
                    AliasProbe.Page(null, reason = "cannot: java.net.ConnectException: Connection refused")
                }
            assertEquals(0, never.measure("t", listOf(steady), canDial = { true }))
            assertEquals("the connection was refused", reasonOf(silent))
            assertEquals(
                "never answered a REQ",
                silent
                    .snapshot()
                    .single()
                    .work
                    .single()
                    .undecided
                    .single()
                    .parent,
            )

            val mute = Processors()
            val quiet = pass(mute.of("consistency")) { _, _, _, _ -> AliasProbe.Page(null) }
            assertEquals(0, quiet.measure("t", listOf(steady), canDial = { true }))
            assertEquals("gave up for a reason we do not recognise", reasonOf(mute))
            assertEquals(
                "never answered a REQ",
                mute
                    .snapshot()
                    .single()
                    .work
                    .single()
                    .undecided
                    .single()
                    .parent,
            )

            // One of the two is a rate limiter's shape, not a dead host's.
            val half = Processors()
            val answers = AtomicInteger()
            val once =
                pass(half.of("consistency")) { _, _, _, _ ->
                    if (answers.getAndIncrement() == 0) AliasProbe.Page(corpus()) else AliasProbe.Page(null)
                }
            assertEquals(0, once.measure("t", listOf(steady), canDial = { true }))
            assertEquals("answered one of the two asks, not both", reasonOf(half))
        }

    @Test
    fun `a relay that answers with nothing is told from one that answers thinly`() =
        runBlocking {
            val refused = Processors()
            val empty = pass(refused.of("consistency")) { _, _, _, _ -> AliasProbe.Page(emptyList()) }
            assertEquals(0, empty.measure("t", listOf(steady), canDial = { true }))
            assertEquals("answered, but served no filter we know", reasonOf(refused))

            val short = Processors()
            val nine = pass(short.of("consistency")) { _, want, _, _ -> AliasProbe.Page(corpus(9).take(want)) }
            assertEquals(0, nine.measure("t", listOf(thin), canDial = { true }))
            assertEquals("too few events to judge on", reasonOf(short))
        }

    @Test
    fun `a credential refusal ends the ladder instead of paying for a second pair`() =
        runBlocking {
            // After a refused AUTH the relay answers nothing more on that socket, so every further rung waits out its window.
            val processors = Processors()
            val dials = AtomicInteger()
            val gate =
                pass(processors.of("consistency")) { _, _, _, _ ->
                    dials.incrementAndGet()
                    AliasProbe.Page(null, authRefused = true)
                }

            assertEquals(0, gate.measure("t", listOf(steady), canDial = { true }))
            assertEquals("refused our auth", reasonOf(processors))
            assertEquals(2, dials.get(), "the pair, and not the kinds fallback behind it")
        }

    @Test
    fun `the hosts under a reason are ranked, and do not pretend to be all of them`() =
        runBlocking {
            // One host with four dead urls, four hosts with one each: the rank tells a dead network spread
            // thin from a few servers wearing many urls.
            val processors = Processors()
            val wide = (1..4).map { RelayUrlNormalizer.normalize("wss://busy.example/$it") }
            val thin = (1..4).map { RelayUrlNormalizer.normalize("wss://lonely$it.example") }
            val gate = pass(processors.of("consistency")) { _, _, _, _ -> AliasProbe.Page(null) }

            gate.measure("t", wide + thin, canDial = { true })

            val row = row(processors).undecided.single()
            assertEquals(8, row.urls)
            assertEquals(5, row.hosts)
            assertEquals("busy.example" to 4, row.top.first().let { it.host to it.urls })
            // Sorted by host name within a count, so an unchanged network publishes the same document every tick.
            assertEquals(
                listOf("busy.example", "lonely1.example", "lonely2.example", "lonely3.example", "lonely4.example"),
                row.top.map { it.host },
            )
            assertEquals(emptyList(), row.examples, "names with counts, rather than the same names twice")
        }

    @Test
    fun `the named hosts are capped, and the reason still counts every url`() =
        runBlocking {
            // Sized off the constant: a literal case stopped testing anything when the cap grew past it.
            val processors = Processors()
            val hostCount = Processors.MAX_UNDECIDED_HOSTS + 20
            val many = (1..hostCount).map { RelayUrlNormalizer.normalize("wss://h$it.example") }
            val gate = pass(processors.of("consistency")) { _, _, _, _ -> AliasProbe.Page(null) }

            gate.measure("t", many, canDial = { true })

            val row = row(processors).undecided.single()
            assertEquals(Processors.MAX_UNDECIDED_HOSTS, row.top.size)
            assertEquals(hostCount, row.urls, "the count is over every url, not over the ones that fitted")
            assertEquals(hostCount, row.hosts)
            assertTrue(
                row.urls > row.top.sumOf { it.urls },
                "a ranked head that summed to its reason would hide the tail it was taken from",
            )
        }

    @Test
    fun `every row the gate can emit survives, because nothing cuts them any more`() {
        // A reason is an enum value, so the list is bounded by source, not by the network.
        // Every reason the gate can emit: each `Unmeasured` except the one `Silence` refines, plus every cause it splits into.
        val widest = (ConsistencyPass.Unmeasured.entries.size - 1) + Silence.entries.size
        val processors = Processors()
        processors.of("consistency").record(
            Processors.Work(
                stream = "all streams",
                candidates = widest,
                unmeasured = widest,
                dialled = widest,
                decided = 0,
                undecided = (0 until widest).map { Processors.Undecided(reason = "r$it", hosts = 1, urls = 1) },
            ),
        )

        val row =
            processors
                .snapshot()
                .single()
                .work
                .single()
        assertEquals(widest, row.undecided.size, "a list an enum bounds must not be cut by a number beside it")
        assertEquals(0, row.undecidedOmitted, "…and nothing is being dropped for the member to disclose")
        assertEquals(row.unmeasured, row.undecided.sumOf { it.urls }, "…so the rows still sum to what they account for")
    }

    @Test
    fun `the candidate set divides exactly once`() =
        runBlocking {
            //   candidates = foldedAway + consistent + inconsistent + unmeasured
            //   unmeasured = the undecided rows' urls
            val store = newStore()
            // A url the fold has already taken out counts once, as folded; it is never measured.
            RelayVerdictRecord(store, signer).publish(duplicate, steady, sampled = 40, shared = 40)

            val processors = Processors()
            val whole = corpus()
            val drift = AtomicInteger()
            val gate =
                pass(processors.of("consistency"), store) { at, want, _, _ ->
                    when (at) {
                        steady -> AliasProbe.Page(whole.take(want))

                        // A window that walks forward on every ask, so no two answers agree.
                        shuffler -> AliasProbe.Page(corpus(120).drop(drift.getAndAdd(40)).take(want))

                        else -> AliasProbe.Page(corpus(9).take(want))
                    }
                }

            val candidates = listOf(steady, shuffler, thin, duplicate)
            gate.measure("t", candidates, canDial = { true })

            val work = row(processors)
            assertEquals(4, work.candidates)
            // Nullable so a pass that measures no verdicts (the fold) publishes none rather than three zeroes.
            val foldedAway = assertNotNull(work.foldedAway, "the gate measures this and must publish it")
            val consistent = assertNotNull(work.consistent, "the gate measures this and must publish it")
            val inconsistent = assertNotNull(work.inconsistent, "the gate measures this and must publish it")
            assertEquals(1, foldedAway, "the folded url counts once, and as folded")
            assertEquals(1, consistent)
            assertEquals(1, inconsistent)
            assertEquals(1, work.unmeasured)
            assertEquals(
                work.candidates,
                foldedAway + consistent + inconsistent + work.unmeasured,
                "the four members must partition the candidate set",
            )
            assertEquals(
                work.unmeasured,
                work.undecided.sumOf { it.urls },
                "every url with no verdict must be under exactly one reason",
            )
            assertEquals(0, work.undecidedOmitted, "the reasons fit in the published cap")
            assertEquals(3, work.dialled)
            assertTrue(
                work.undecided.single().hosts == 1,
                "one thin relay is one host, beside the url count rather than instead of it",
            )
        }
}
