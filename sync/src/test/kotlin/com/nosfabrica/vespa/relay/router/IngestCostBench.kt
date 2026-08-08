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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.QUERY_FANOUT
import com.nosfabrica.vespa.eventstore.engine.mapBounded
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
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
    )

    private fun run(
        store: VespaEventStore,
        arm: Arm,
    ) = runBlocking {
        val scope = CoroutineScope(Job())
        val pipeline =
            IngestPipeline(
                store,
                RouterConfig(connectionTimeoutSec = 10, streams = emptyList(), ingestConcurrency = 2, ingestBatch = 1000),
                audit = null,
                servingPressure = null,
                scope = scope,
                knownIds = if (arm.probe) store.eventIndex::existingIds else null,
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
            //    stored: new ids, same addresses, older. The probe cannot see
            //    them, so they pay full verification and are rejected as
            //    `replaced` by the store. This is the arm that says whether a
            //    supersession pre-filter is worth building.
            val genZero = profiles(CORPUS, gen = 0)
            run(store, Arm("stale replaceable profiles", genZero, probe = true))

            // 4. NEWER REPLACEABLE — gen2 over gen1: same addresses, accepted,
            //    superseding. The write-side counterpart to arm 3.
            run(store, Arm("newer replaceable profiles", profiles(CORPUS, gen = 2), probe = true))

            // 5. Repeat the pair that decides the shipped change, interleaved,
            //    so a warming engine cannot be read as a difference between arms.
            run(store, Arm("duplicate notes (repeat)", base, probe = false))
            run(store, Arm("duplicate notes (repeat)", base, probe = true))

            // 6. What a SUPERSESSION pre-filter would cost: the batched read
            //    that answers "do we hold a newer version of this address", in
            //    the shape stage C uses for its guards — chunked by author,
            //    bounded fan-out. Priced against arm 3's per-event cost, this
            //    is the whole business case for building it.
            priceVersionLookup(store, genZero)
            priceIdProbe(store, base)
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
