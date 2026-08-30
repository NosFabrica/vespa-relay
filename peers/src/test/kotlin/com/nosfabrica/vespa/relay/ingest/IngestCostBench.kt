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
 * What one arriving event COSTS ingest, split by the verdict it ends on —
 * measured end to end through the real [IngestPipeline] against a real Vespa.
 *
 * The mix of verdicts is a property of an operator's upstreams and is only
 * readable off their own stats line. The per-verdict cost is not: it is a
 * property of this code and this engine, and the two together are what say
 * whether hoisting a check ahead of `verify()` is worth building. This measures
 * the half that is ours.
 *
 * Skipped unless `BENCH_VESPA_URL` names a live engine — it deploys a schema
 * and writes a corpus, which is not a unit test:
 *
 *     BENCH_VESPA_URL=http://localhost:8080 ./gradlew :sync:test --tests '*IngestCostBench*' -i
 */
class IngestCostBench {
    private val url = System.getenv("BENCH_VESPA_URL")
    private val relayUrl = RelayUrlNormalizer.normalize("wss://bench.example")
    private val signer = NostrSignerSync()

    /** Regular (non-replaceable) notes — each one its own document forever. */
    private fun notes(
        n: Int,
        gen: Int = 0,
    ): List<Event> = (0 until n).map { signer.sign<Event>(BASE_TIME + gen * 1_000_000L + it, 1, emptyArray(), "note $it gen $gen") }

    /**
     * Kind-0 profiles for [n] distinct authors, at generation [gen]. A later
     * generation is a NEWER version of the SAME address with a DIFFERENT id —
     * which is exactly the arrival the id-existence probe cannot see.
     */
    private fun profiles(
        n: Int,
        gen: Int,
    ): List<Event> =
        (0 until n).map {
            // A signer per author: kind 0's address is (kind, pubkey), so one
            // signer would make these n generations of ONE profile.
            authors[it].sign<Event>(BASE_TIME + gen * 1_000_000L, 0, emptyArray(), """{"name":"author $it","about":"gen $gen"}""")
        }

    private val authors by lazy { (0 until CORPUS).map { NostrSignerSync() } }

    private class Arm(
        val label: String,
        val events: List<Event>,
        val probe: Boolean,
        /**
         * The pipeline shape to price this arm at. Default is the historical
         * one, so every existing arm reads exactly as it did.
         *
         * A parameter at all because at a mirror's real duplicate rate the
         * shape is the finding: the store serializes every commit on one
         * writer mutex, so what a batch is worth is how many SURVIVORS it
         * carries into that one lock hold — and survivors per commit is
         * `batch x (1 - dropRate)`. See [sweepShapes].
         */
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
            // The corpus this measures against: enough that a dedup query is a
            // real query, small enough to load in one pass on a 4-core box.
            val base = notes(CORPUS)
            val genOne = profiles(CORPUS, gen = 1)

            run(store, Arm("warm-up (ignore)", notes(CORPUS, gen = 9), probe = true))

            // 1. FRESH — nothing is known, every event is written. The probe is
            //    a guaranteed miss here, so this arm prices what it costs when
            //    it cannot help (the negentropy-backfill case).
            run(store, Arm("fresh notes", base, probe = false))
            run(store, Arm("fresh profiles gen1", genOne, probe = false))

            // 2. EXACT DUPLICATES — the same ids again. This is what the shipped
            //    change addresses; the pair prices it.
            run(store, Arm("duplicate notes", base, probe = false))
            run(store, Arm("duplicate notes", base, probe = true))

            // 3. STALE REPLACEABLE — gen0 profiles arriving AFTER gen1 is
            //    stored: new ids, same addresses, older. The id probe cannot
            //    see them; the version probe is what this pair prices, and it
            //    is the whole reason dropSuperseded exists.
            val genZero = profiles(CORPUS, gen = 0)
            run(store, Arm("stale replaceable, no version probe", genZero, probe = false))
            run(store, Arm("stale replaceable, version probe", genZero, probe = true))
            run(store, Arm("stale replaceable, no version probe (repeat)", genZero, probe = false))
            run(store, Arm("stale replaceable, version probe (repeat)", genZero, probe = true))

            // 4. NEWER REPLACEABLE — gen2 over gen1: same addresses, accepted,
            //    superseding. The write-side counterpart to arm 3.
            run(store, Arm("newer replaceable profiles", profiles(CORPUS, gen = 2), probe = true))

            // 5. Repeat the pair that decides the shipped change, interleaved,
            //    so a warming engine cannot be read as a difference between arms.
            run(store, Arm("duplicate notes (repeat)", base, probe = false))
            run(store, Arm("duplicate notes (repeat)", base, probe = true))

            // 6. THE PRODUCTION SHAPE, which none of the pure arms above can
            //    show: 98% already held, 2% new. A 100%-duplicate batch is
            //    dropped entirely by the probe — it never reaches verify, never
            //    reaches the write, and never takes the writer lock — so it
            //    prices the rejection path and nothing else. A mirror's real
            //    batch carries a couple of percent that must be WRITTEN, and
            //    the write is where the lock is held. The pair of arms above
            //    brackets this one; only this one says which side dominates.
            //
            //    Shuffled with a fixed seed so the new events are spread
            //    through the batch rather than sitting in one tail, and so two
            //    runs compare. Distinct generations per arm: the first arm
            //    STORES its 2%, and reusing them would make the second arm
            //    100% duplicate without saying so.
            val keep = CORPUS * 98 / 100
            val add = CORPUS - keep
            run(store, Arm("98% dup / 2% fresh", (base.take(keep) + notes(add, gen = 5)).shuffled(Random(7)), probe = false))
            run(store, Arm("98% dup / 2% fresh", (base.take(keep) + notes(add, gen = 6)).shuffled(Random(7)), probe = true))
            run(store, Arm("98% dup / 2% fresh (repeat)", (base.take(keep) + notes(add, gen = 7)).shuffled(Random(7)), probe = true))

            // 7. THE SHAPE SWEEP, and at a mirror's duplicate rate it is the
            //    one that decides throughput. Same 98/2 work, three pipeline
            //    shapes. See [sweepShapes].
            sweepShapes(store, base.take(keep))

            // 8. THE SAME SWEEP ON A BURST OF ALL-FRESH EVENTS, which is the
            //    OTHER regime and the one the 98/2 answer says nothing about.
            //    A mirror's steady state rejects almost everything and barely
            //    touches the writer lock; a burst of genuinely new events is
            //    100% write, and there the lock is held for essentially the
            //    whole wall clock. Whether that is the LOCK or the ENGINE is
            //    the question, and the shapes answer it: throughput that is
            //    flat across them means the engine is saturated and no amount
            //    of lock work helps, while throughput that climbs with width
            //    means the batching does.
            sweepFreshShapes(store)

            // 8. What a SUPERSESSION pre-filter would cost: the batched read
            //    that answers "do we hold a newer version of this address", in
            //    the shape stage C uses for its guards — chunked by author,
            //    bounded fan-out. Priced against arm 3's per-event cost, this
            //    is the whole business case for building it.
            priceVersionLookup(store, genZero)
            priceIdProbe(store, base)
        }
    }

    /**
     * A 100%-fresh burst through the same three shapes — the regime the 98/2
     * sweep cannot speak for.
     *
     * Every event here is written, so `lock.ingest.hold` covers a real write
     * rather than a near-empty commit, and the feed client's own pipelining is
     * in play: a wider batch is both fewer lock acquisitions AND a bigger
     * `putAll`. The two move together on purpose — the question this answers is
     * "what shape absorbs a burst fastest", not "which of the two is
     * responsible", and the flat-versus-climbing shape of the answer is what
     * says whether the engine or the pipeline is the ceiling.
     */
    private fun sweepFreshShapes(store: VespaEventStore) {
        listOf(
            IngestTuning(concurrency = 8, batch = 1024),
            IngestTuning(concurrency = 2, batch = 8192),
            IngestTuning(concurrency = 1, batch = 16384),
        ).forEachIndexed { i, tuning ->
            run(store, Arm("fresh burst sweep", notes(CORPUS, gen = 20 + i), probe = true, tuning = tuning))
        }
    }

    /**
     * The same 98/2 batch through three pipeline shapes — because at this
     * duplicate rate the shape, not the probe, is what moves the number.
     *
     * The store takes ONE writer mutex for the whole of `commit`, so writes
     * never run in parallel however many ingest workers there are: more
     * workers only lengthens the queue for it. What a lock hold is WORTH is
     * how many surviving events it writes, and that is `batch x (1 - dropRate)`
     * — at 98% dropped, a 1024-event batch carries about twenty. So the
     * hypothesis this prices is that FEWER, WIDER workers beat more, narrower
     * ones: same survivors, far fewer lock acquisitions.
     *
     * The production shape is first. Note `IngestPipeline` caps a batch at its
     * share of the queue (`capacity / workers`, and capacity itself at
     * MAX_INBOUND_QUEUE), so at eight workers a batch cannot exceed 2048
     * however high SYNC_INGEST_BATCH is set — the cap is derived from a queue
     * sized for MEMORY, and it lands on the number that decides write
     * efficiency. That interaction is the reason for the third row.
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
            // A distinct generation per shape: each one STORES its 2%, and
            // reusing them would quietly make the next shape 100% duplicate
            // and time a batch that never writes.
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

        /** The store's own stage-C width and stage-B width, so the prices are comparable to production. */
        const val CHECK_CHUNK = 500
        const val DEDUP_CHUNK = 500
    }
}
