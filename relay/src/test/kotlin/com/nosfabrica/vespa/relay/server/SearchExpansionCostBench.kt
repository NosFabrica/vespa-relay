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
import com.nosfabrica.vespa.eventstore.engine.InMemoryReputationIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaEventIndex
import com.nosfabrica.vespa.eventstore.engine.client.VespaReputationIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.search.SearchExpansionLimits
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
 *     touch this at all, and the only thing it adds is the store's "does any
 *     query carry TERMS" test. This is the arm that would have caught gating on
 *     "has a `search` field" — every anonymous read on a lens-requiring relay
 *     stamps `include:spam`, and a mirror's paging carries it too.
 *  2. **search, no kind can point** — a real text search that names `kinds`
 *     holding no Trusted List, Assertion or label. This is what most client
 *     searches look like, and the store's kind test — no `kinds` entry in
 *     `SearchReferences.KINDS` — sends it down the untouched path, so it must
 *     come out at zero.
 *  3. **search, could point, none does** — the same search with `kinds`
 *     omitted, so any kind may come back and the relay has to look. Pays for
 *     collecting the page before writing it out, and NO extra store round trip,
 *     because no row's kind nominates anything. This is the arm that prices the
 *     mechanism itself.
 *  4. **search, every hit a pointer** — a page of Trusted Lists. Pays the
 *     above plus the extra recall for the subjects. Their memberships OVERLAP,
 *     which is what a real roster does, so the splice converges on a handful of
 *     distinct pubkeys however long the page is.
 *  5. **search, every hit a label** — a page of NIP-32 labels, each naming ONE
 *     distinct event. This is the worst case the feature actually has in
 *     production, and the one the arm above understates: nothing dedupes, so
 *     the splice is as big as the page and the frame count doubles.
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
 * ## What it reads with the splice in the store
 *
 * 2026-08-29, 4-core sandbox, single-node Vespa in Docker, 2,521-event corpus,
 * 201 rounds per arm, medians:
 *
 * | arm | page | off | on | | frames |
 * |---|---|---|---|---|---|
 * | recall | 50 / 500 | 7.0 / 19.7ms | 7.0 / 19.8ms | -0.2% / +0.1% | unchanged |
 * | search, no kind can point | 50 / 500 | 12.1 / 29.8ms | 12.2 / 29.6ms | +0.2% / -0.7% | unchanged |
 * | search, could point, none does | 50 / 500 | 11.2 / 29.7ms | 11.3 / 29.4ms | +0.2% / -0.9% | unchanged |
 * | search, every hit a pointer | 50 / 500 | 10.1 / 46.0ms | 14.4 / 50.0ms | +42% / +9% | +20 |
 * | search, every hit a label | 50 / 500 | 13.4 / 34.0ms | 18.0 / 54.7ms | +35% / +61% | x2 |
 *
 * RE-TAKEN AFTER THE MOVE, and the pointer arm had to be, twice over. The gate
 * was the relay's before and flat across kinds, so a Treasure Map naming a
 * `30382:rank` service unpacked Trusted Lists too; it is per-kind now, and this
 * bench's corpus had to grow a bare `30392` entry — and its store a
 * `TrustProjection` — to keep that arm expanding at all. Without either it ran
 * at +0.0 queries and +0.0 frames while still reporting a ~+50-100% median,
 * which is noise attributed to a splice that never happened.
 *
 * The label arm was never gated (NIP-32 is ungated by design), and it is the
 * check that the box has not changed shape under the table: +61% at 500 both
 * times, against +15% -> +9% on the pointer arm.
 *
 * Everything that does not expand is free. A page that does expand pays ONE
 * extra round trip whatever its size, and then the subjects themselves.
 *
 * THE TWO EXPANDING ARMS DIVERGE, and the reason is the shape of the data
 * rather than anything in the code. Trusted Lists OVERLAP — a page of 500
 * rosters names the same 20 pubkeys — so the splice converges and the relative
 * cost FALLS as the page grows (+44% -> +15%). NIP-32 labels do not: each names
 * its own event, so the splice is as big as the page, the frame count DOUBLES,
 * and the cost RISES with it (+38% -> +61%). The marginal cost of a spliced
 * event on this box is ~48us — its share of the batched recall plus its frame.
 *
 * The label arm is the worst case the feature actually has in production, and
 * it is the one to look at first. Real labels carry a median of ONE nostr
 * target (405 of 433 sampled), with a thin tail up to 40, so a page of labels
 * really does splice about one event per hit. At that rate the default
 * `SEARCH_EXPAND_MAX_TOTAL` of 1,000 is spent by a page of roughly 400-500
 * labels — which is what bounds a REQ that names no `limit` at all and takes
 * the relay's 5,000 default.
 *
 * The in-memory index prices the extra round trip at well under a millisecond,
 * which is why its relative numbers look so different and why this table is the
 * real one.
 *
 * THINGS THIS BENCH FOUND, none of them visible in the correctness tests:
 *
 *  - the expansion originally lived in the session backend, where no delivery
 *    callback can suspend, so it awaited a child coroutine — putting back the
 *    scheduler hop quartz's UNDISPATCHED REQ exists to avoid, a flat ~90us on
 *    EVERY search. Chasing that number is what exposed the seam as wrong:
 *    moving it one layer down, into an `IEventStore` decorator, made the same
 *    calls ordinary suspend functions that return, and the coroutine went away
 *    entirely along with ~280 lines of machinery. (That decorator has since
 *    moved again, into the store itself — see AGENTS.md — so neither class this
 *    paragraph once linked to exists here any more.)
 *  - the plan read every row's pointers before spending the budget, so a
 *    500-list page paid 450 tags-parses for rows it had already decided to take
 *    nothing from: 12.2ms -> 6.5ms on the in-memory pointer arm, and the Vespa
 *    pointer arm's absolute delta is flat across a 10x page because of it.
 *  - the reader's 10040 was re-read inside every REQ, a second round trip
 *    (+2.0 queries, 27.9ms at 50 hits) to re-fetch a document that changes when
 *    someone enrols a service. Caching it per reader took that to +1.0 queries
 *    and 21.5ms, and the arm went from +74% to +44%. The cache is now the
 *    store's `ProviderMap` pass, which a 10040 write invalidates directly —
 *    the relay-side version needed a TTL because it could not see the sync
 *    process writing 10040s into the same index from another JVM, and that is
 *    one of the two reasons the feature moved.
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
        println(if (vespa == null) "EXPANSION-BENCH: in-memory index (set BENCH_VESPA_URL for a real engine)" else "EXPANSION-BENCH: real engine at $vespa")
        // COUNTER INNERMOST, PROJECTION OUTSIDE IT, on both arms. Two reasons,
        // and the move made both of them load-bearing:
        //
        //  - the gate reads the reader's Treasure Map off [TrustProjection], so
        //    a store assembled without one admits NO declaration and the
        //    Trusted List arm below silently prices a splice that never
        //    happens. It used to work over any store, because the enrolment
        //    cache was the relay's; it is the store's now.
        //  - the projection's own provider-list reads go to the index it
        //    wraps, so a counter placed OUTSIDE it never sees them — which is
        //    the opposite of what this bench is for. The counter is the
        //    innermost layer so that every engine search is counted, trust
        //    resolution's included.
        val counted = CountingIndex(if (vespa == null) InMemoryEventIndex() else vespaEvents(vespa))
        val index = TrustProjection(counted, if (vespa == null) InMemoryReputationIndex() else VespaReputationIndex(vespa))
        NostrSemanticsStore(index, relay = relayUrl).use { runBench(it, index, counted) }
    }

    /**
     * The real engine, deployed if absent. The [TrustProjection] that
     * `VespaEventStore.open` puts over it is assembled by the caller, around
     * the counter — a bench that skipped the projection would be measuring a
     * different query planner from the one that serves.
     */
    private fun vespaEvents(url: String): EventIndex {
        SchemaDeployer(System.getenv("BENCH_VESPA_CONFIG_URL") ?: url.replace(":8080", ":19071")).deployIfAbsent(url)
        return VespaEventIndex(url)
    }

    private fun runBench(
        store: IEventStore,
        index: EventIndex,
        counter: CountingIndex,
    ) = runBlocking {
        val on = NostrRelayServer(store, relayUrl)
        // The OFF arm is a second store over the same index, because the splice
        // moved into the store: "the same corpus without the expansion" is a
        // store opened without it, not a relay told to skip it. The SAME
        // projection instance, so both arms share one provider-map cache and
        // the difference between them is the splice alone.
        val off = NostrRelayServer(NostrSemanticsStore(index, relay = relayUrl, searchExpansion = SearchExpansionLimits.Off), relayUrl)
        try {
            seed(on)
            val arms =
                listOf<Pair<String, (Int) -> String>>(
                    "recall" to { page -> """{"kinds":[1],"limit":$page,"search":"$lens"}""" },
                    "search, no kind can point" to { page -> """{"kinds":[1],"limit":$page,"search":"bramblecast $lens"}""" },
                    "search, could point, none does" to { page -> """{"limit":$page,"search":"bramblecast $lens"}""" },
                    "search, every hit a pointer" to { page -> """{"kinds":[0,30392],"limit":$page,"search":"roster $lens"}""" },
                    "search, every hit a label" to { page -> """{"kinds":[1,1985],"limit":$page,"search":"$LABEL_VALUE $lens"}""" },
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
                        var mark = counter.searches.get()
                        var startedNs = System.nanoTime()
                        offFrames += measure(off, filter(page))
                        offSamples[i] = (System.nanoTime() - startedNs) / 1_000
                        offQueries += counter.searches.get() - mark

                        mark = counter.searches.get()
                        startedNs = System.nanoTime()
                        onFrames += measure(on, filter(page))
                        onSamples[i] = (System.nanoTime() - startedNs) / 1_000
                        onQueries += counter.searches.get() - mark
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
            // BOTH DELEGATION SHAPES, because the gate is per KIND and the
            // pointer arm below searches 30392: NIP-85's `<kind>:<metric>`
            // appoints a service to rank users, and the Tapestry ADR's generic
            // bare `<kind>` appoints a publisher to curate lists of them. A map
            // carrying only the first leaves every Trusted List here unexpanded
            // and the arm prices a splice that never happens — which is what it
            // did while the enrolment cache was the relay's and flat.
            events +=
                reader.sign<Event>(
                    1_699_999_000L,
                    10040,
                    arrayOf(
                        arrayOf("30382:rank", curator.pubKey, "wss://provider.example"),
                        arrayOf("30392", curator.pubKey, "wss://lists.example"),
                    ),
                    "",
                )
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
            // NIP-32 labels, in the shape production actually publishes them:
            // an `L` namespace, one `l` value shared by all of them (a language
            // tag is 243 of every 400 labels on the search relay), and ONE `e`
            // target each. The one-target-each part is what makes this the
            // worst case and the reason it is its own arm — a page of N labels
            // names N DISTINCT events, so nothing dedupes and the splice is the
            // same size as the page, where a page of Trusted Lists converges on
            // the handful of pubkeys they all name.
            val notes = events.filter { it.kind == 1 }
            repeat(minOf(LABELS, notes.size)) { i ->
                events +=
                    curator.sign<Event>(
                        1_700_300_000L + i,
                        1985,
                        arrayOf(arrayOf("L", "bench"), arrayOf("l", LABEL_VALUE, "bench"), arrayOf("e", notes[i].id)),
                        "",
                    )
            }
            for (event in events) {
                session.receive("""["EVENT",${event.toJson()}]""")
                await(out) { it.startsWith("""["OK","${event.id}",true""") }
            }
            println(
                "EXPANSION-BENCH corpus: ${events.size} events — $NOTES notes, $LISTS lists of $MEMBERS members, " +
                    "$MEMBERS profiles, ${minOf(LABELS, NOTES)} labels",
            )
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
        const val LABELS = 1_000

        /** The `l` value every bench label shares — a common label, which is the question. */
        const val LABEL_VALUE = "benchlabelvalue"
    }
}
