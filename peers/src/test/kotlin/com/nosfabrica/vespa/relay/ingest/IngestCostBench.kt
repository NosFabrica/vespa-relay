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
package com.nosfabrica.vespa.relay.ingest

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.Test

/**
 * What one arriving event costs ingest, split by the verdict it ends on,
 * measured end to end through the real [IngestPipeline] against a live Vespa.
 * Asserts nothing; prints one `COST-BENCH` line per arm. Skipped unless
 * `BENCH_VESPA_URL` names an engine (see the Commands section of AGENTS.md).
 */
class IngestCostBench {
    private val url = System.getenv("BENCH_VESPA_URL")
    private val relayUrl = RelayUrlNormalizer.normalize("wss://bench.example")
    private val signer = NostrSignerSync()

    private fun notes(
        n: Int,
        gen: Int = 0,
    ): List<Event> = (0 until n).map { signer.sign<Event>(BASE_TIME + gen * 1_000_000L + it, 1, emptyArray(), "note $it gen $gen") }

    /** Kind-0 profiles for [n] distinct authors; a later [gen] is a newer version of the same address with a different id. */
    private fun profiles(
        n: Int,
        gen: Int,
    ): List<Event> =
        (0 until n).map {
            // A signer per author: kind 0's address is (kind, pubkey), so one
            // signer would make these n generations of one profile.
            authors[it].sign<Event>(BASE_TIME + gen * 1_000_000L, 0, emptyArray(), """{"name":"author $it","about":"gen $gen"}""")
        }

    private val authors by lazy { (0 until CORPUS).map { NostrSignerSync() } }

    private class Arm(
        val label: String,
        val events: List<Event>,
        val probe: Boolean,
        /** The pipeline shape to price this arm at. See [sweepShapes]. */
        val tuning: IngestTuning = IngestTuning(concurrency = 2, batch = 1000),
    )

    private fun run(
        store: VespaEventStore,
        arm: Arm,
    ) = runBlocking {
        val scope = CoroutineScope(Job())
        val pipeline =
            IngestPipeline(
                store,
                arm.tuning,
                audit = null,
                servingPressure = null,
                scope = scope,
                knownIds = if (arm.probe) store.eventIndex::existingIds else null,
                newestVersions =
                    if (!arm.probe) {
                        null
                    } else {
                        { kind, authors ->
                            store.eventIndex
                                .search(EventQuery(kinds = listOf(kind), authors = authors))
                                .groupBy { it.pubkey }
                                .mapValues { (_, docs) ->
                                    docs.maxWith(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id }).let { AddressVersion(it.createdAt, it.id) }
                                }
                        }
                    },
            )
        IngestStats.statusLine() // zero the deltas
        pipeline.start()
        val t0 = System.nanoTime()
        arm.events.forEach { pipeline.submit(it, skipVerify = false) }
        while (pipeline.accepted.get() + pipeline.rejected.get() < arm.events.size) delay(2)
        val dt = System.nanoTime() - t0
        val n = arm.events.size
        println(
            "COST-BENCH ${arm.label.padEnd(34)} probe=${if (arm.probe) "on " else "off"} " +
                "w=${arm.tuning.concurrency}x${arm.tuning.batch} " +
                "n=$n  ${"%.1f".format(dt / 1e6)}ms  ${"%.0f".format(n * 1e9 / dt)} ev/s  " +
                "${"%.0f".format(dt / 1e3 / n)}us/ev  accepted=${pipeline.accepted.get()} rejected=${pipeline.rejected.get()}" +
                pipeline.rejectionBreakdown(),
        )
        println("COST-BENCH   ${IngestStats.statusLine()}")
        scope.cancel()
        pipeline.close()
    }

    @Test
    fun bench() {
        val url = url ?: return println("COST-BENCH skipped — set BENCH_VESPA_URL")
        VespaEventStore.open(url, relay = relayUrl, autoDeploy = true).use { store ->
            val base = notes(CORPUS)
            val genOne = profiles(CORPUS, gen = 1)

            run(store, Arm("warm-up (ignore)", notes(CORPUS, gen = 9), probe = true))

            // 1. Fresh: nothing is known, every event is written, the probe is
            //    a guaranteed miss.
            run(store, Arm("fresh notes", base, probe = false))
            run(store, Arm("fresh profiles gen1", genOne, probe = false))

            // 2. Exact duplicates, with and without the probe.
            run(store, Arm("duplicate notes", base, probe = false))
            run(store, Arm("duplicate notes", base, probe = true))

            // 3. Stale replaceables arriving after gen1 is stored: new ids, same
            //    addresses, older. Only the version probe can see them.
            val genZero = profiles(CORPUS, gen = 0)
            run(store, Arm("stale replaceable, no version probe", genZero, probe = false))
            run(store, Arm("stale replaceable, version probe", genZero, probe = true))
            run(store, Arm("stale replaceable, no version probe (repeat)", genZero, probe = false))
            run(store, Arm("stale replaceable, version probe (repeat)", genZero, probe = true))

            // 4. Newer replaceables over gen1: same addresses, accepted, superseding.
            run(store, Arm("newer replaceable profiles", profiles(CORPUS, gen = 2), probe = true))

            // 5. The deciding pair again, interleaved, so a warming engine
            //    cannot be read as a difference between arms.
            run(store, Arm("duplicate notes (repeat)", base, probe = false))
            run(store, Arm("duplicate notes (repeat)", base, probe = true))

            // 6. The production mix: 98% held, 2% new, which is where the write
            //    and the writer lock come into play. A distinct generation per
            //    arm, because each arm stores its 2%; a fixed seed so runs compare.
            val keep = CORPUS * 98 / 100
            val add = CORPUS - keep
            run(store, Arm("98% dup / 2% fresh", (base.take(keep) + notes(add, gen = 5)).shuffled(Random(7)), probe = false))
            run(store, Arm("98% dup / 2% fresh", (base.take(keep) + notes(add, gen = 6)).shuffled(Random(7)), probe = true))
            run(store, Arm("98% dup / 2% fresh (repeat)", (base.take(keep) + notes(add, gen = 7)).shuffled(Random(7)), probe = true))

            // 7. The same 98/2 work through three pipeline shapes. See [sweepShapes].
            sweepShapes(store, base.take(keep))

            // 8. The same shapes on an all-fresh burst. See [sweepFreshShapes].
            sweepFreshShapes(store)

            // 9. What a supersession pre-filter would cost, priced against arm 3.
            priceVersionLookup(store, genZero)
            priceIdProbe(store, base)
        }
    }

    /**
     * A 100%-fresh burst through the same three shapes, forwards then backwards.
     * Every arm writes [CORPUS] new documents, so each meets a bigger index than
     * the last; the reversed pass puts that drift against the width. If the two
     * passes disagree about which shape wins, the ordering is corpus growth.
     */
    private fun sweepFreshShapes(store: VespaEventStore) {
        val shapes =
            listOf(
                IngestTuning(concurrency = 8, batch = 1024),
                IngestTuning(concurrency = 2, batch = 8192),
                IngestTuning(concurrency = 1, batch = 16384),
            )
        (shapes.map { "up" to it } + shapes.reversed().map { "down" to it }).forEachIndexed { i, (order, tuning) ->
            run(store, Arm("fresh burst sweep $order", notes(CORPUS, gen = 20 + i), probe = true, tuning = tuning))
        }
    }

    /**
     * The same 98/2 batch through three pipeline shapes. The store takes one
     * writer mutex per commit, so a lock hold is worth the survivors it writes,
     * `batch x (1 - dropRate)`; the shapes ask whether fewer, wider workers beat
     * more, narrower ones. [IngestPipeline] caps a batch at `capacity / workers`,
     * which is the reason for the third row.
     */
    private fun sweepShapes(
        store: VespaEventStore,
        dupes: List<Event>,
    ) {
        val add = CORPUS - dupes.size
        listOf(
            IngestTuning(concurrency = 8, batch = 1024),
            IngestTuning(concurrency = 2, batch = 8192),
            IngestTuning(concurrency = 1, batch = 16384),
        ).forEachIndexed { i, tuning ->
            // A distinct generation per shape: each one stores its 2%.
            val mix = (dupes + notes(add, gen = 10 + i)).shuffled(Random(7))
            run(store, Arm("98/2 shape sweep", mix, probe = true, tuning = tuning))
        }
    }

    /** The proposed pre-filter's query: current versions of each address, chunked by author. */
    private fun priceVersionLookup(
        store: VespaEventStore,
        events: List<Event>,
    ) = runBlocking {
        val authors = events.map { it.pubKey }.distinct()
        repeat(2) { pass ->
            val t0 = System.nanoTime()
            val found =
                authors
                    .chunked(CHECK_CHUNK)
                    .mapBounded(QUERY_FANOUT) { chunk -> store.eventIndex.search(EventQuery(kinds = listOf(0), authors = chunk)) }
                    .flatten()
            val dt = System.nanoTime() - t0
            if (pass > 0) {
                println(
                    "COST-BENCH version lookup (the pre-filter)   " +
                        "n=${authors.size} addresses  ${"%.1f".format(dt / 1e6)}ms  " +
                        "${"%.0f".format(dt / 1e3 / authors.size)}us/ev  ${found.size} versions read",
                )
            }
        }
    }

    /** The shipped probe's query, priced on the same corpus for comparison. */
    private fun priceIdProbe(
        store: VespaEventStore,
        events: List<Event>,
    ) = runBlocking {
        val ids = events.map { it.id }
        repeat(2) { pass ->
            val t0 = System.nanoTime()
            val hit = ids.chunked(DEDUP_CHUNK).mapBounded(QUERY_FANOUT) { store.eventIndex.existingIds(it) }.flatMapTo(HashSet()) { it }
            val dt = System.nanoTime() - t0
            if (pass > 0) {
                println(
                    "COST-BENCH id probe (the shipped one)        " +
                        "n=${ids.size} ids  ${"%.1f".format(dt / 1e6)}ms  " +
                        "${"%.0f".format(dt / 1e3 / ids.size)}us/ev  ${hit.size} held",
                )
            }
        }
    }

    private companion object {
        val CORPUS = System.getenv("BENCH_N")?.toIntOrNull() ?: 4_000
        const val BASE_TIME = 1_600_000_000L

        /** The store's own stage-C and stage-B widths, so the prices are comparable to production. */
        const val CHECK_CHUNK = 500
        const val DEDUP_CHUNK = 500
    }
}
