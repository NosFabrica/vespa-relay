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
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats
import com.nosfabrica.vespa.relay.identity.RelayIdentity
import com.nosfabrica.vespa.relay.pulse.StoreMetricsLog
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.DEFAULT_STREAM_IN_FLIGHT
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.io.File
import java.util.zip.GZIPInputStream
import kotlin.test.Test

/**
 * A real corpus (one JSON event per line, `.gz` read gzipped) through the sync plane's
 * [IngestPipeline], wired as `SyncMain` wires it, into a live Vespa: the mirror's half of
 * the ingest benchmark, against the same slice the relay's publish path is measured on.
 * Asserts nothing; prints one `CORPUS-BENCH` line. Skipped unless `BENCH_VESPA_URL` and
 * `BENCH_CORPUS` are set. `BENCH_TAKE`, `BENCH_SKIP`, `BENCH_CONCURRENCY`, `BENCH_BATCH`,
 * `BENCH_PROBES=0`, `BENCH_PASSES` (a second pass is all duplicates). `BENCH_RELAY_URL` with
 * `BENCH_NSEC` (the relay's own `RELAY_NSEC`) is `SYNC_INGEST=relay`, `BENCH_RELAY_INFLIGHT` its window: the store opened read-only
 * for the probes, every batch forwarded through [RelayWriter] to the relay there.
 */
class CorpusIngestBench {
    private val env = System.getenv()

    @Test
    fun bench() {
        val url = env["BENCH_VESPA_URL"] ?: return println("CORPUS-BENCH skipped — set BENCH_VESPA_URL")
        val corpus = env["BENCH_CORPUS"] ?: return println("CORPUS-BENCH skipped — set BENCH_CORPUS")
        val tuning = IngestTuning(env["BENCH_CONCURRENCY"]?.toInt() ?: 2, env["BENCH_BATCH"]?.toInt() ?: 1000)
        val probes = env["BENCH_PROBES"] != "0"
        val events = read(File(corpus), env["BENCH_SKIP"]?.toLong() ?: 0, env["BENCH_TAKE"]?.toLong() ?: Long.MAX_VALUE)
        val bytes = events.sumOf { it.second.toLong() }
        println("CORPUS-BENCH read ${events.size} events, ${bytes / 1_000_000} MB, from $corpus")
        val forwardTo = env["BENCH_RELAY_URL"]?.let(RelayUrlNormalizer::normalize)
        val signer =
            forwardTo?.let {
                RelayIdentity.fromEnv { key -> if (key == "RELAY_NSEC") env["BENCH_NSEC"] else null }
                    ?: error("BENCH_RELAY_URL needs BENCH_NSEC, the relay's own key")
            }
        // Forwarding, the relay has deployed and owns the writes; this process only reads.
        val relayUrl = forwardTo ?: RelayUrlNormalizer.normalize("wss://bench.example")
        VespaEventStore.open(url, relay = relayUrl, autoDeploy = forwardTo == null, readOnly = forwardTo != null).use { store ->
            repeat(env["BENCH_PASSES"]?.toInt() ?: 1) { pass ->
                val scope = CoroutineScope(Job())
                val inFlight = env["BENCH_RELAY_INFLIGHT"]?.toInt() ?: DEFAULT_STREAM_IN_FLIGHT
                val writer = forwardTo?.let { RelayWriter(it, it, signer!!, scope, maxInFlight = inFlight) } ?: StoreWriter.local(store)
                run(store, events.map { it.first }, bytes, tuning, probes, "pass ${pass + 1}", writer)
                writer.close()
                scope.cancel()
                println("CORPUS-BENCH   ${StoreMetricsLog.line("bench", store.metrics())}")
            }
        }
    }

    private fun run(
        store: VespaEventStore,
        events: List<Event>,
        bytes: Long,
        tuning: IngestTuning,
        probes: Boolean,
        label: String,
        writer: StoreWriter,
    ) = runBlocking {
        val scope = CoroutineScope(Job())
        val pipeline =
            IngestPipeline(
                store,
                tuning,
                audit = null,
                servingPressure = null,
                scope = scope,
                // The probes SyncMain and RelayMain wire, not a copy that could drift from them.
                knownIds = if (probes) store.knownIdsProbe() else null,
                newestVersions = if (probes) store.newestVersionsProbe() else null,
                writer = writer,
            )
        IngestStats.statusLine()
        pipeline.start()
        val t0 = System.nanoTime()
        events.forEach { pipeline.submit(it, skipVerify = false) }
        while (pipeline.accepted.get() + pipeline.rejected.get() < events.size) delay(5)
        val dt = System.nanoTime() - t0
        println(
            "CORPUS-BENCH $label w=${tuning.concurrency}x${tuning.batch} probes=${if (probes) "on" else "off"} n=${events.size} " +
                "${"%.1f".format(dt / 1e9)}s ${"%.0f".format(events.size * 1e9 / dt)} ev/s ${"%.2f".format(bytes * 1e3 / dt)} MB/s " +
                "accepted=${pipeline.accepted.get()} rejected=${pipeline.rejected.get()}${pipeline.rejectionBreakdown()} " +
                "writes ${writer.description}" + ((writer as? RelayWriter)?.let { ", ${it.unsignedBatches.get()} batch(es) unsigned" } ?: ""),
        )
        println("CORPUS-BENCH   ${IngestStats.statusLine()}")
        scope.cancel()
        pipeline.close()
    }

    /** Each line's event and its JSON length, [skip] lines in, at most [take] of them. */
    private fun read(
        file: File,
        skip: Long,
        take: Long,
    ): List<Pair<Event, Int>> {
        val input = if (file.name.endsWith(".gz")) GZIPInputStream(file.inputStream(), 1 shl 16) else file.inputStream()
        return input.bufferedReader().useLines { lines ->
            lines
                .filter { it.isNotBlank() }
                .drop(skip.toInt())
                .take(take.coerceAtMost(Int.MAX_VALUE.toLong()).toInt())
                .map { Event.fromJson(it) to it.length }
                .toList()
        }
    }
}
