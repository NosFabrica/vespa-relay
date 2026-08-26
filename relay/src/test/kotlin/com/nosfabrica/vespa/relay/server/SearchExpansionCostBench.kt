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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.SchemaDeployer
import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.trust.TrustProjection
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.runBlocking
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.fail

/**
 * WHAT THE REFERENCE EXPANSION COSTS A READ, measured through the whole serving
 * stack — a websocket session, quartz's engine, [ObserverBackend], the store —
 * by running the SAME REQ against two relays over ONE store and differing in
 * one thing: [SearchExpansionLimits.enabled].
 *
 * Both relays in one JVM against one corpus is the point. An absolute
 * milliseconds-per-REQ number off a laptop or a cloud sandbox says nothing
 * portable; the RATIO between two arms that shared a heap, a page cache and a
 * scheduler is the thing that survives the move to another machine.
 *
 * Three arms, because the change has three different prices:
 *
 *  1. **recall** — a termless `include:spam` read: a mirror's paging, a
 *     NIP-77 catch-up, the web page's plain filters. The expansion must not
 *     touch this at all, and the only thing it adds is the `isSearch` test.
 *     This is the arm that would have caught gating on "has a `search` field".
 *  2. **search, no kind can point** — a real text search that names `kinds`
 *     holding no Trusted List, Assertion or label. This is what most client
 *     searches look like, and `couldPoint` sends it down the untouched path,
 *     so it must come out at zero.
 *  3. **search, could point, none does** — the same search with `kinds`
 *     omitted, so any kind may come back and the relay has to look. Pays the
 *     whole buffering machinery — the flush coroutine, the gate, the per-row
 *     `sent` record — and NO extra store round trip, because no row's kind
 *     nominates anything. This is the arm that prices the machinery itself.
 *  4. **search, every hit a pointer** — a page of Trusted Lists. Pays the
 *     above plus the two extra recalls: the reader's 10040 once per REQ, and
 *     the subjects once per page.
 *
 * The store round trips are counted as well as timed. A count is a structural
 * fact that holds on any engine; a duration is a measurement of this one, and
 * against [InMemoryEventIndex] a "round trip" costs a hash lookup rather than
 * a network hop — which is exactly why the in-memory arm OVERSTATES the
 * relative cost of the relay-side work and UNDERSTATES the extra recalls. Point
 * it at a real engine to see the other bias:
 *
 *     ./gradlew :relay:test --tests '*SearchExpansionCostBench*' -DsearchExpansionBench -i
 *     BENCH_VESPA_URL=http://localhost:8080 ./gradlew :relay:test \
 *         --tests '*SearchExpansionCostBench*' -DsearchExpansionBench -i
 *
 * Off unless asked for by name: it writes a corpus and runs thousands of REQs,
 * which is not a unit test. Asserts nothing about durations — a timing
 * assertion on a shared CI box is a flake generator, and the numbers are for a
 * person to read.
 *
 * ## What it read when the expansion landed
 *
 * 2026-08-26, 4-core sandbox, single-node Vespa in Docker, 1,521-event corpus,
 * 201 rounds per arm, medians:
 *
 * | arm | page | off | on | |
 * |---|---|---|---|---|
 * | recall | 50 / 500 | 10.9 / 27.2ms | 10.9 / 27.3ms | -0.5% / +0.6% |
 * | search, no kind can point | 50 / 500 | 20.7 / 41.3ms | 20.8 / 40.1ms | +0.5% / -3.0% |
 * | search, could point, none does | 50 / 500 | 16.2 / 40.1ms | 16.3 / 39.1ms | +0.4% / -2.4% |
 * | search, every hit a pointer | 50 / 500 | 15.0 / 58.6ms | 21.5 / 66.8ms | +44% / +14% |
 *
 * Read it as: everything that does not expand is free, and a page that does
 * expand pays ONE extra round trip — the subjects — and nothing else. The
 * in-memory index prices that same round trip at well under a millisecond,
 * which is why its relative numbers look so different and why this table is the
 * real one.
 *
 * THREE THINGS THIS BENCH FOUND, none of them visible in the correctness tests:
 *
 *  - the flush originally ran as a child coroutine the REQ awaited, which put
 *    back the scheduler hop quartz's UNDISPATCHED REQ exists to avoid — a flat
 *    ~90us on EVERY search. It is launched undispatched from the EOSE callback
 *    now, and a page with nothing to expand never suspends at all.
 *  - the plan read every row's pointers before spending the budget, so a
 *    500-list page paid 450 tags-parses for rows it had already decided to take
 *    nothing from: 12.2ms -> 6.5ms on the in-memory pointer arm, and the Vespa
 *    pointer arm's absolute delta is flat across a 10x page because of it.
 *  - the reader's 10040 was re-read inside every REQ, a second round trip
 *    (+2.0 queries, 27.9ms at 50 hits) to re-fetch a document that changes when
 *    someone enrols a service. [EnrolledSigners] caches it per reader: +1.0
 *    queries, 21.5ms, and the arm went from +74% to +44%.
 *
 */
class SearchExpansionCostBench {
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /** Counts what a REQ costs the engine, so the report carries a number that is not a duration. */
    private class CountingIndex(
        val inner: EventIndex,
    ) : EventIndex {
        val searches = AtomicInteger(0)

        override suspend fun get(id: String) = inner.get(id)

        override suspend fun put(doc: EventDoc) = inner.put(doc)

        override suspend fun remove(id: String) = inner.remove(id)

        override suspend fun search(query: EventQuery): List<EventDoc> {
            searches.incrementAndGet()
            return inner.search(query)
        }

        override suspend fun count(query: EventQuery) = inner.count(query)

        override suspend fun countByAuthor(query: EventQuery) = inner.countByAuthor(query)

        override fun close() = inner.close()
    }

    private val reader = NostrSignerSync()
    private val curator = NostrSignerSync()

    /**
     * One arm's numbers. The MEDIAN and the p90, never the mean: a sandbox
     * hands you a handful of multi-millisecond GC pauses per thousand REQs, and
     * a mean is then mostly a report of which arm they landed in.
     */
    private class Timing(
        val label: String,
        samples: LongArray,
        val queriesPerReq: Double,
        val framesPerReq: Double,
    ) {
        val median: Long
        val p90: Long

        init {
            samples.sort()
            median = samples[samples.size / 2]
            p90 = samples[(samples.size * 9) / 10]
        }
    }

    @Test
    fun bench() {
        if (System.getProperty("searchExpansionBench") == null) {
            return println("EXPANSION-BENCH skipped — run with -DsearchExpansionBench")
        }
        val vespa = System.getenv("BENCH_VESPA_URL")
        val index = CountingIndex(if (vespa == null) InMemoryEventIndex() else vespaEngine(vespa))
        println(if (vespa == null) "EXPANSION-BENCH: in-memory index (set BENCH_VESPA_URL for a real engine)" else "EXPANSION-BENCH: real engine at $vespa")
        NostrSemanticsStore(index, relay = relayUrl).use { runBench(it, index) }
    }

    /**
     * The real engine, assembled the way `VespaEventStore.open` assembles it —
     * the ranked read goes through [TrustProjection], and a bench that skipped
     * it would be measuring a different query planner from the one that serves.
     * The counter wraps the whole thing, so what it counts is engine searches
     * including the ones trust resolution makes.
     */
    private fun vespaEngine(url: String): EventIndex {
        SchemaDeployer(System.getenv("BENCH_VESPA_CONFIG_URL") ?: url.replace(":8080", ":19071")).deployIfAbsent(url)
        val events = VespaEventIndex(url)
        return TrustProjection(events, VespaReputationIndex(url))
    }

    private fun runBench(
        store: IEventStore,
        index: CountingIndex,
    ) = runBlocking {
        val on = NostrRelayServer(store, relayUrl)
        val off = NostrRelayServer(store, relayUrl, searchExpansion = SearchExpansionLimits.Off)
        try {
            seed(on)
            val arms =
                listOf<Pair<String, (Int) -> String>>(
                    "recall" to { page -> """{"kinds":[1],"limit":$page,"search":"$lens"}""" },
                    "search, no kind can point" to { page -> """{"kinds":[1],"limit":$page,"search":"bramblecast $lens"}""" },
                    "search, could point, none does" to { page -> """{"limit":$page,"search":"bramblecast $lens"}""" },
                    "search, every hit a pointer" to { page -> """{"kinds":[0,30392],"limit":$page,"search":"roster $lens"}""" },
                )

            // Warm-up: the first REQ through a fresh JVM pays class loading,
            // the JIT and the store's first query plan, and it is easily 100x
            // the steady-state cost. Measuring it would drown every arm.
            repeat(WARMUP) {
                arms.forEach { (_, filter) ->
                    measure(on, filter(SMALL_PAGE))
                    measure(off, filter(SMALL_PAGE))
                }
            }

            println()
            println("arm                                  page   expansion    median       p90   queries   frames")
            println("------------------------------------------------------------------------------------------")
            for ((label, filter) in arms) {
                for (page in PAGES) {
                    // INTERLEAVED, one REQ each per round, rather than all of
                    // one arm and then all of the other: a GC pause or a JIT
                    // recompile that lands inside a whole arm is read as that
                    // arm being slower, and running them apart is how you get a
                    // 30% "regression" that reverses when you swap the order.
                    val offSamples = LongArray(ROUNDS)
                    val onSamples = LongArray(ROUNDS)
                    var offFrames = 0L
                    var onFrames = 0L
                    var offQueries = 0
                    var onQueries = 0
                    for (i in 0 until ROUNDS) {
                        var mark = index.searches.get()
                        var startedNs = System.nanoTime()
                        offFrames += measure(off, filter(page))
                        offSamples[i] = (System.nanoTime() - startedNs) / 1_000
                        offQueries += index.searches.get() - mark

                        mark = index.searches.get()
                        startedNs = System.nanoTime()
                        onFrames += measure(on, filter(page))
                        onSamples[i] = (System.nanoTime() - startedNs) / 1_000
                        onQueries += index.searches.get() - mark
                    }
                    val offArm = Timing("off", offSamples, offQueries.toDouble() / ROUNDS, offFrames.toDouble() / ROUNDS)
                    val onArm = Timing("on", onSamples, onQueries.toDouble() / ROUNDS, onFrames.toDouble() / ROUNDS)
                    row(label, page, offArm)
                    row("", page, onArm)
                    println(
                        "%-51s %+.1f%% on the median, %+.1f queries, %+.1f frames".format(
                            "",
                            (onArm.median - offArm.median) * 100.0 / offArm.median,
                            onArm.queriesPerReq - offArm.queriesPerReq,
                            onArm.framesPerReq - offArm.framesPerReq,
                        ),
                    )
                }
                println()
            }
        } finally {
            on.close()
            off.close()
        }
    }

    private fun row(
        label: String,
        page: Int,
        t: Timing,
    ) = println(
        "%-36s %5d   %-9s %7.3fms %7.3fms %8.2f %8.2f".format(
            label,
            page,
            t.label,
            t.median / 1000.0,
            t.p90 / 1000.0,
            t.queriesPerReq,
            t.framesPerReq,
        ),
    )

    /** One REQ, from the frame going in to the EOSE coming out. Returns the EVENT frames it produced. */
    private suspend fun measure(
        server: NostrRelayServer,
        filter: String,
    ): Int {
        val out = Collections.synchronizedList(mutableListOf<String>())
        val session = server.connect { out.add(it) }
        try {
            session.receive("""["REQ","b",$filter]""")
            await(out) { it.startsWith("""["EOSE","b"]""") }
            return synchronized(out) { out.count { it.startsWith("""["EVENT","b",""") } }
        } finally {
            session.close()
        }
    }

    /**
     * The corpus: notes to search, profiles for the lists to name, and lists
     * whose titles are the thing a search matches. Small enough to load on a
     * 4-core box, big enough that a page is a page.
     */
    private suspend fun seed(server: NostrRelayServer) {
        val out = Collections.synchronizedList(mutableListOf<String>())
        val session = server.connect { out.add(it) }
        try {
            val members = (0 until MEMBERS).map { NostrSignerSync() }
            val events = ArrayList<Event>()
            events += reader.sign<Event>(1_699_999_000L, 10040, arrayOf(arrayOf("30382:rank", curator.pubKey, "wss://provider.example")), "")
            members.forEachIndexed { i, m ->
                events += m.sign<Event>(1_700_000_000L + i, 0, emptyArray(), """{"name":"member $i","about":"nothing findable here"}""")
            }
            repeat(NOTES) { i ->
                events += members[i % MEMBERS].sign<Event>(1_700_100_000L + i, 1, emptyArray(), "bramblecast episode $i is up")
            }
            repeat(LISTS) { i ->
                events +=
                    curator.sign<Event>(
                        1_700_200_000L + i,
                        30392,
                        (arrayOf(arrayOf("d", "roster-$i"), arrayOf("title", "Podcaster Roster $i")) + members.map { arrayOf("p", it.pubKey) }),
                        "",
                    )
            }
            for (event in events) {
                session.receive("""["EVENT",${event.toJson()}]""")
                await(out) { it.startsWith("""["OK","${event.id}",true""") }
            }
            println("EXPANSION-BENCH corpus: ${events.size} events — $NOTES notes, $LISTS lists of $MEMBERS members, $MEMBERS profiles")
        } finally {
            session.close()
        }
    }

    private fun await(
        out: List<String>,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + 30_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(1)
        }
        fail("timed out; got ${out.size} messages")
    }

    private val lens get() = "include:spam observer:${reader.pubKey}"

    private companion object {
        const val ROUNDS = 201
        const val WARMUP = 30

        /**
         * TWO page sizes, because the change has a fixed cost and a per-row one
         * and a single size cannot tell them apart. The buffering pays a child
         * coroutine and a `CompletableDeferred` ONCE per REQ; the gate, the
         * `sent` record and the kind test are paid PER ROW. A delta that is
         * flat across a 10x page is the former and a bigger page is free; one
         * that scales with the page is the latter.
         */
        val PAGES = listOf(50, 500)
        const val SMALL_PAGE = 50

        const val NOTES = 1_000
        const val LISTS = 500
        const val MEMBERS = 20
    }
}
