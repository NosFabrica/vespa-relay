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
 * WHAT THE STABILITY GATE SAYS ABOUT THE URLS IT COULD NOT DECIDE.
 *
 * The pass dials its whole candidate set every time, and on a discovered corpus
 * it decides a few hundred urls out of several thousand. For a long time the
 * other several thousand were one number — `unmeasured` — and one reason string
 * covering everything: a dead host, an auth wall, a relay holding nine events
 * and a url our own transport would not carry all arrived as "said too little
 * to judge". Those are four different problems with four different fixes, and
 * the ratio on the stats card was the same in every case, so the honest reading
 * ("this corpus is mostly dead urls") and the alarming one ("the gate is
 * stuck") could not be told apart.
 *
 * So each test below drives ONE way of failing all the way through a real pass
 * and pins the reason it comes out as, and the last one pins the property the
 * whole breakdown rests on: that the candidate set divides exactly once.
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

    /**
     * A pass whose every url answers the same way, reporting into a real
     * [Processors] handle — which is the half being asserted.
     */
    private fun pass(
        handle: Processors.Handle,
        store: NostrSemanticsStore = newStore(),
        fetch: suspend (NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page,
    ) = ConsistencyPass(
        consistency = RelayConsistency(),
        record = RelayAliasRecord(store, signer),
        probe = AliasProbe(fetch = fetch, target = 40, page = 40, fallbackPage = 40),
        progress = handle,
    )

    /** The single work row a pass publishes, by the label it was measured under. */
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
            // A `.onion` on a router with no Tor, or a host nothing is listening
            // on. No socket is opened, so it must not be counted as a dial —
            // `dialled` was `wanted.size` and reported work that never happened.
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
            // The TCP pre-probe opens a socket of its own and can fail on its
            // own terms. Reported as ours rather than as the relay saying
            // nothing, because the relay was never asked.
            val processors = Processors()
            val gate = pass(processors.of("consistency")) { _, _, _, _ -> AliasProbe.Page(corpus()) }

            assertEquals(0, gate.measure("t", listOf(steady), canDial = { error("socket budget") }))
            assertEquals("the probe failed mid-walk", reasonOf(processors))
        }

    @Test
    fun `a url that never answers is silent, and one that answers once is not`() =
        runBlocking {
            val silent = Processors()
            val never = pass(silent.of("consistency")) { _, _, _, _ -> AliasProbe.Page(null) }
            assertEquals(0, never.measure("t", listOf(steady), canDial = { true }))
            assertEquals("never answered a REQ", reasonOf(silent))

            // ONE OF THE TWO, which is its own finding: the relay was reachable
            // enough to serve one REQ and not the second one issued at the same
            // instant. Rolled into silence it reads as a dead host, and a dead
            // host is not what a rate limiter looks like.
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
            // An EOSE with nothing in it, on the bare filter and on the kinds
            // fallback: the relay is there and will not serve us. Distinct from
            // a relay that serves a real but short window, which is a small
            // relay and must never be refused for it.
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
            // Measured on `filter.nostr.wine`: the first ask is answered in 1.6s
            // with a refusal and every ask after it on that connection is
            // answered with nothing at all, so each one waits out the full idle
            // window. The kinds fallback used to run anyway — two more REQs into
            // a wall we had already been shown, per url, per pass — because the
            // refusal was flattened into "proved nothing" before the caller
            // could see it.
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
            // "3,902 urls on 2,201 hosts" is two opposite findings wearing one
            // shape — a dead network spread thin, or three servers wearing a
            // thousand urls each — and only the widest few can tell them apart.
            //
            // One host with four dead urls, four hosts with one each. The rank
            // is what says which of the two shapes this is.
            val processors = Processors()
            val wide = (1..4).map { RelayUrlNormalizer.normalize("wss://busy.example/$it") }
            val thin = (1..4).map { RelayUrlNormalizer.normalize("wss://lonely$it.example") }
            val gate = pass(processors.of("consistency")) { _, _, _, _ -> AliasProbe.Page(null) }

            gate.measure("t", wide + thin, canDial = { true })

            val row = row(processors).undecided.single()
            assertEquals(8, row.urls)
            assertEquals(5, row.hosts)
            assertEquals("busy.example" to 4, row.top.first().let { it.host to it.urls })
            // Sorted by host name within a count, so two passes over one
            // unchanged network publish the same document rather than a list
            // that reshuffles on every tick.
            assertEquals(
                listOf("busy.example", "lonely1.example", "lonely2.example", "lonely3.example", "lonely4.example"),
                row.top.map { it.host },
            )
            // A ranked head, not a partition: the reason's urls are NOT the sum
            // of what it names, and the card draws that difference as the tail
            // rather than closing the level over a list that was cut.
            assertEquals(emptyList(), row.examples, "names with counts, rather than the same names twice")
        }

    @Test
    fun `the named hosts are capped, and the reason still counts every url`() =
        runBlocking {
            // Ten hosts, six named. The cap is the point: what must NOT happen
            // is the row's own `urls` falling to what the cap could name, which
            // would make the funnel's fourth level close over a truncated list
            // and report the tail as if it did not exist.
            val processors = Processors()
            val many = (1..10).map { RelayUrlNormalizer.normalize("wss://h$it.example") }
            val gate = pass(processors.of("consistency")) { _, _, _, _ -> AliasProbe.Page(null) }

            gate.measure("t", many, canDial = { true })

            val row = row(processors).undecided.single()
            assertEquals(Processors.MAX_UNDECIDED_HOSTS, row.top.size)
            assertEquals(10, row.urls, "the count is over every url, not over the ones that fitted")
            assertEquals(10, row.hosts)
            assertTrue(
                row.urls > row.top.sumOf { it.urls },
                "a ranked head that summed to its reason would hide the tail it was taken from",
            )
        }

    @Test
    fun `the candidate set divides exactly once`() =
        runBlocking {
            // THE PROPERTY THE WHOLE BREAKDOWN RESTS ON:
            //
            //   candidates = foldedAway + consistent + inconsistent + unmeasured
            //   unmeasured = the undecided rows' urls
            //
            // Without it the funnel on the stats card is four numbers that
            // happen to be near each other, and a reader subdividing the fan-out
            // has no way to know when the arithmetic stopped closing.
            val store = newStore()
            // A url the FOLD has already taken out. It must count once, as
            // folded — it is never measured, so it can carry no verdict.
            RelayAliasRecord(store, signer).publish(duplicate, steady, sampled = 40, shared = 40)

            val processors = Processors()
            val whole = corpus()
            val drift = AtomicInteger()
            val gate =
                pass(processors.of("consistency"), store) { at, want, _, _ ->
                    when (at) {
                        // The same window every time: one verdict, consistent.
                        steady -> AliasProbe.Page(whole.take(want))

                        // A window that walks forward on every ask, so no two
                        // answers agree — the shape the gate exists to refuse.
                        shuffler -> AliasProbe.Page(corpus(120).drop(drift.getAndAdd(40)).take(want))

                        // Real, and far too short to judge on.
                        else -> AliasProbe.Page(corpus(9).take(want))
                    }
                }

            val candidates = listOf(steady, shuffler, thin, duplicate)
            gate.measure("t", candidates, canDial = { true })

            val work = row(processors)
            assertEquals(4, work.candidates)
            // Non-null on THIS pass, and the assertion is part of the point: the
            // three are nullable so that a pass which measures no verdicts —
            // the alias fold — publishes none rather than three zeroes, and the
            // gate is the pass that must always answer.
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
            assertEquals(0, work.undecidedOmitted, "seven reasons fit in the published cap")
            // The folded url is never dialled, so the fold's work is not paid
            // for twice — the reason `wanted` filters it out.
            assertEquals(3, work.dialled)
            assertTrue(
                work.undecided.single().hosts == 1,
                "one thin relay is one host, beside the url count rather than instead of it",
            )
        }
}
