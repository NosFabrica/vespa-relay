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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.sun.management.ThreadMXBean
import com.vitorpamplona.negentropy.storage.IStorage
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySync
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip77Negentropy.NegentropyServerSession
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.lang.management.ManagementFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.test.Test

/**
 * The mirror's NIP-77 sweep against one relay, our Vespa store as the local side, timed and
 * heap-sampled. Off unless `-DnegBench=true`; `-DnegBenchUrl`, `-DnegBenchVespa`,
 * `-DnegBenchMode=pager|whole`, `-DnegBenchTarget`, `-DnegBenchRepeat`.
 */
class NegentropyBenchProbe {
    @Test
    fun sweep() {
        if (System.getProperty("negBench") != "true") {
            println("[skip] NegentropyBenchProbe — set -DnegBench=true")
            return
        }
        val url = RelayUrlNormalizer.normalize(System.getProperty("negBenchUrl", "ws://127.0.0.1:7101"))
        val vespa = System.getProperty("negBenchVespa", "http://localhost:8080")
        val mode = System.getProperty("negBenchMode", "pager")
        val target = System.getProperty("negBenchTarget", "100000").toInt()
        val repeat = System.getProperty("negBenchRepeat", "2").toInt()

        val sampler = HeapSampler().also { it.start() }
        VespaEventStore.open(vespa, relay = RelayUrlNormalizer.normalize("ws://bench.local"), autoDeploy = false, readOnly = true).use { store ->
            runBlocking {
                val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
                val okhttp =
                    OkHttpClient
                        .Builder()
                        .connectTimeout(Duration.ofSeconds(15))
                        .pingInterval(Duration.ofSeconds(120))
                        .build()
                val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
                client.connect()
                repeat(repeat) { run ->
                    val index = TimedIndex(StoreWindowIndex(store))
                    val got = AtomicInteger()
                    System.gc()
                    sampler.reset()
                    val alloc0 = allocated()
                    val t0 = System.nanoTime()
                    var summary: String
                    when (mode) {
                        "pager" -> {
                            val pager =
                                NegentropyPager(
                                    index,
                                    ClientWindowSync(client, FilterWidths(), refused = RefusedIds.disabled()),
                                    SweepState(null),
                                    NegPageTuning(target = target, minTarget = 1_000, maxTarget = 1_000_000, slackSeconds = 60),
                                )
                            val out = pager.sweep("bench", url, Filter(), Filter()) { got.incrementAndGet() }
                            summary = "reconciled=${out.reconciledWindows} paged=${out.pagedWindows} complete=${out.complete} failure=${out.failure?.message}"
                        }

                        else -> {
                            val r = client.negentropySync(url, Filter(), localIndex = index, targetWindow = if (mode == "whole") 0 else target) { got.incrementAndGet() }
                            summary = "need=${r.needCount} have=${r.haveCount} windows=${r.windows}"
                        }
                    }
                    val ms = (System.nanoTime() - t0) / 1_000_000
                    val mb = (allocated() - alloc0) / (1 shl 20)
                    println(
                        "NEGBENCH run=$run mode=$mode target=$target wall_ms=$ms downloaded=${got.get()} $summary " +
                            "store_count_calls=${index.counts.get()} store_count_ms=${index.countNs.get() / 1_000_000} " +
                            "store_snap_calls=${index.snaps.get()} store_snap_ms=${index.snapNs.get() / 1_000_000} store_snap_ids=${index.snapIds.get()} " +
                            "heap_peak_mb=${sampler.peak() / (1 shl 20)} allocated_mb=$mb",
                    )
                }
                client.disconnect()
            }
        }
        sampler.stop()
    }

    /** What one whole-filter NEG-OPEN costs our side: the walk, the seal, and what each holds. */
    @Test
    fun snapshot() {
        if (System.getProperty("negBench") != "true") return
        val vespa = System.getProperty("negBenchVespa", "http://localhost:8080")
        val repeat = System.getProperty("negBenchRepeat", "3").toInt()
        VespaEventStore.open(vespa, relay = RelayUrlNormalizer.normalize("ws://bench.local"), autoDeploy = false, readOnly = true).use { store ->
            runBlocking {
                repeat(repeat) { run ->
                    // Each run's storage is dropped before the next baseline, so runs do not credit one another.
                    val base = liveHeap()
                    val t0 = System.nanoTime()
                    val a0 = allocated()
                    val walked = walkAndSeal(store)
                    // Suspending unwinds the frame that resumed us, which still holds the walk's list.
                    delay(1)
                    val sealedOnly = liveHeap()
                    println(
                        "SNAPBENCH run=$run ids=${walked.ids} walk_ms=${walked.walkMs} seal_ms=${walked.sealMs} total_ms=${(System.nanoTime() - t0) / 1_000_000} " +
                            "walk_alloc_mb=${walked.walkAlloc shr 20} seal_alloc_mb=${(allocated() - a0 - walked.walkAlloc) shr 20} " +
                            "list_mb=${(walked.withList - base) shr 20} list_and_sealed_mb=${(walked.both - base) shr 20} sealed_mb=${(sealedOnly - base) shr 20} " +
                            "list_bytes_per_id=${(walked.withList - base) / walked.ids} sealed_bytes_per_id=${(sealedOnly - base) / walked.ids} size=${walked.sealed.size()}",
                    )
                }
            }
        }
    }

    private class Walked(
        val ids: Int,
        val walkMs: Long,
        val sealMs: Long,
        val walkAlloc: Long,
        val withList: Long,
        val both: Long,
        val sealed: IStorage,
    )

    /** Its own frame, so the id list is unreachable once it returns. */
    private suspend fun walkAndSeal(store: VespaEventStore): Walked {
        val a0 = allocated()
        val t0 = System.nanoTime()
        val list = store.snapshotIdsForNegentropy(listOf(Filter()), 2_000_000)
        val t1 = System.nanoTime()
        val walkAlloc = allocated() - a0
        val withList = liveHeap()
        val sealed = NegentropyServerSession.sealVector(list)
        val t2 = System.nanoTime()
        return Walked(list.size, (t1 - t0) / 1_000_000, (t2 - t1) / 1_000_000, walkAlloc, withList, liveHeap(), sealed)
    }

    private fun liveHeap(): Long {
        repeat(3) { System.gc() }
        val m = ManagementFactory.getMemoryMXBean().heapMemoryUsage
        return m.used
    }

    private fun allocated(): Long = (ManagementFactory.getThreadMXBean() as ThreadMXBean).totalThreadAllocatedBytes

    private class TimedIndex(
        private val inner: NegentropyLocalIndex,
    ) : NegentropyLocalIndex {
        val counts = AtomicInteger()
        val countNs = AtomicLong()
        val snaps = AtomicInteger()
        val snapNs = AtomicLong()
        val snapIds = AtomicLong()

        override suspend fun count(window: Filter): Int? {
            val t = System.nanoTime()
            return inner.count(window).also {
                counts.incrementAndGet()
                countNs.addAndGet(System.nanoTime() - t)
            }
        }

        override suspend fun entriesFor(window: Filter): List<IdAndTime> {
            val t = System.nanoTime()
            return inner.entriesFor(window).also {
                snaps.incrementAndGet()
                snapIds.addAndGet(it.size.toLong())
                snapNs.addAndGet(System.nanoTime() - t)
            }
        }
    }

    /** Heap in use after each young collection is the honest live set; sampled every 20 ms here. */
    private class HeapSampler {
        @Volatile private var running = true
        private val max = AtomicLong()
        private lateinit var t: Thread

        fun start() {
            t =
                thread(isDaemon = true, name = "heap-sampler") {
                    val mem = ManagementFactory.getMemoryMXBean()
                    while (running) {
                        val used = mem.heapMemoryUsage.used
                        max.accumulateAndGet(used) { a, b -> maxOf(a, b) }
                        Thread.sleep(20)
                    }
                }
        }

        fun reset() = max.set(0)

        fun peak() = max.get()

        fun stop() {
            running = false
        }
    }
}
