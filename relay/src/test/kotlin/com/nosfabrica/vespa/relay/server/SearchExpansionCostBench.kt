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
 * What the reference expansion costs a read, measured through the whole serving stack by running the
 * same REQ against two relays over one store that differ only in [SearchExpansionLimits.enabled]. Five
 * arms: a termless recall, a search no kind can point from, one that could but none does, a page of
 * Trusted Lists, a page of NIP-32 labels. Prints medians, p90s, store round trips and frames per REQ;
 * asserts nothing. Selected by `-DsearchExpansionBench`; `BENCH_VESPA_URL` points it at a real engine.
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

    /** One arm's numbers: the median and the p90, never the mean, which mostly reports where the GC pauses landed. */
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
        // Counter innermost, projection outside it: the gate reads the Map off the projection, and the
        // projection's own reads must be counted too.
        val counted = CountingIndex(if (vespa == null) InMemoryEventIndex() else vespaEvents(vespa))
        val index = TrustProjection(counted, if (vespa == null) InMemoryReputationIndex() else VespaReputationIndex(vespa))
        NostrSemanticsStore(index, relay = relayUrl).use { runBench(it, index, counted) }
    }

    /** The real engine, deployed if absent. The caller puts the [TrustProjection] around the counter, as `VespaEventStore.open` would. */
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
        // The off arm is a second store over the same projection instance, so both arms share one
        // provider-map cache and differ in the splice alone.
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

            // The first REQ through a fresh JVM pays class loading, the JIT and the first query plan.
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
                    // Interleaved, one REQ each per round: a GC pause inside a whole arm reads as that arm being slower.
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

    /** The corpus: notes to search, profiles for the lists to name, and lists whose titles a search matches. */
    private suspend fun seed(server: NostrRelayServer) {
        val out = Collections.synchronizedList(mutableListOf<String>())
        val session = server.connect { out.add(it) }
        try {
            val members = (0 until MEMBERS).map { NostrSignerSync() }
            val events = ArrayList<Event>()
            // Both delegation shapes: the gate is per kind, and a Map carrying only `30382:rank` leaves every
            // Trusted List unexpanded, so the pointer arm would price a splice that never happens.
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
            // Labels in production's shape: one shared `l` value and one `e` target each, so a page of N
            // labels names N distinct events and nothing dedupes.
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

        /** Two sizes, because the change has a fixed cost per REQ and a per-row one, and one size cannot tell them apart. */
        val PAGES = listOf(50, 500)
        const val SMALL_PAGE = 50

        const val NOTES = 1_000
        const val LISTS = 500
        const val MEMBERS = 20
        const val LABELS = 1_000

        /** The `l` value every bench label shares. */
        const val LABEL_VALUE = "benchlabelvalue"
    }
}
