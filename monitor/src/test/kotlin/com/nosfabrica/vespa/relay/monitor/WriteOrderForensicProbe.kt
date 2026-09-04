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
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

/**
 * Was the verdict corpus cut, or skipped? Rebuilds the fitness pass's old
 * `ConcurrentHashMap` from a `url<TAB>measured-at` TSV, walks it in iteration
 * order and prints how the fresh/stale split falls across the walk; then replays
 * the same corpus through the real pass against a store that stops answering.
 * Dials nothing, asserts nothing. Selected by `-DwriteOrderTsv=<file>`.
 */
class WriteOrderForensicProbe {
    @Test
    fun whereTheWriteLoopStopped() {
        val path = System.getProperty("writeOrderTsv")
        if (path == null) {
            println("[skip] WriteOrderForensicProbe — set -DwriteOrderTsv=<file> (url<TAB>measured-at)")
            return
        }
        val parsed =
            File(path).readLines().mapNotNull { line ->
                val (u, at) = line.split('\t').takeIf { it.size == 2 } ?: return@mapNotNull null
                val url = RelayUrlNormalizer.normalizeOrNull(u.trim()) ?: return@mapNotNull null
                url to (at.trim().toLongOrNull() ?: return@mapNotNull null)
            }
        if (parsed.isEmpty()) {
            println("[skip] WriteOrderForensicProbe — $path parsed to no usable rows (want `url<TAB>epoch-seconds`)")
            return
        }
        // Bucket order does not depend on insertion order, which is why it was stable across passes.
        val outcomes = ConcurrentHashMap<NormalizedRelayUrl, Long>()
        for ((url, at) in parsed) outcomes[url] = at

        val newest = parsed.maxOf { it.second }
        // The cohorts are hours apart, so an hour below the newest stamp cuts cleanly.
        val freshFloor = newest - 3_600
        val walk = outcomes.entries.map { it.key to (it.value >= freshFloor) }

        val fresh = walk.count { it.second }
        var crossings = 0
        for (i in 1 until walk.size) if (walk[i].second != walk[i - 1].second) crossings++
        // A cut batch predicts one crossing; a per-url cause interleaves the classes.
        val shuffledExpectation = 2.0 * fresh * (walk.size - fresh) / walk.size

        println("=".repeat(96))
        println("WHERE THE WRITE LOOP STOPPED — ${walk.size} url(s) in ConcurrentHashMap iteration order")
        println("=".repeat(96))
        println("  fresh (written in the last sweep)   $fresh (%.1f%%)".format(100.0 * fresh / walk.size))
        println("  stale (not written)                 ${walk.size - fresh}")
        println("  crossings between the two classes   $crossings")
        println("  …a cut batch predicts               1")
        println("  …a shuffled/per-url cause predicts  ~${shuffledExpectation.toInt()}")
        println()
        var bestRun = 0
        var bestAt = 0
        var bestClass = false
        var run = 1
        for (i in 1 until walk.size) {
            if (walk[i].second == walk[i - 1].second) {
                run++
            } else {
                if (run > bestRun) {
                    bestRun = run
                    bestAt = i - run
                    bestClass = walk[i - 1].second
                }
                run = 1
            }
        }
        if (run > bestRun) {
            bestRun = run
            bestAt = walk.size - run
            bestClass = walk.last().second
        }
        println("  longest unbroken run                $bestRun ${if (bestClass) "FRESH" else "STALE"} at position $bestAt")
        println()
        // If the cut point is stable, each older cohort is the slice just beyond the one before it.
        println("  cohorts by position (median verdict age per bucket):")
        val nowSec = System.currentTimeMillis() / 1000
        for (b in 0 until 40) {
            val from = b * walk.size / 40
            val to = (b + 1) * walk.size / 40
            val ages = walk.subList(from, to).map { (nowSec - outcomes.getValue(it.first)) / 3600.0 }.sorted()
            val med = ages[ages.size / 2]
            println("    %5d-%5d  median %7.1fh   p10 %6.1fh  p90 %6.1fh".format(from, to, med, ages[ages.size / 10], ages[ages.size * 9 / 10]))
        }
        println()
        println("  the walk, in 40 buckets (# = share written last sweep):")
        val buckets = 40
        for (b in 0 until buckets) {
            val from = b * walk.size / buckets
            val to = (b + 1) * walk.size / buckets
            val slice = walk.subList(from, to)
            val share = slice.count { it.second }.toDouble() / slice.size
            println("    %5d-%5d  %-20s %4.0f%%".format(from, to, "#".repeat((share * 20).toInt()), share * 100))
        }
    }

    /** The same corpus through the real pass, cut at production's share, reporting coverage over passes. */
    @Test
    fun coverageOverPassesOnTheRealCorpus() {
        val path =
            System.getProperty("writeOrderTsv") ?: run {
                println("[skip] WriteOrderForensicProbe — set -DwriteOrderTsv=<file>")
                return
            }
        val urls =
            File(path)
                .readLines()
                .mapNotNull { line ->
                    RelayUrlNormalizer.normalizeOrNull(line.substringBefore('\t').trim())
                }.distinct()
                // Scaled because the in-memory store's read-before-write is linear; the property is positional and scale-free.
                .let { all -> all.take(System.getProperty("writeOrderReplayUrls")?.toIntOrNull() ?: 4_000) }
        if (urls.isEmpty()) {
            println("[skip] WriteOrderForensicProbe — $path named no usable urls")
            return
        }
        val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
        val events = NostrSignerSync()
        val signer = NostrSignerInternal(KeyPair())
        val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
        val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
        // The store takes writes and past a point in a batch stops answering them, at production's observed share.
        val cutAt = urls.size * 12191 / 20072
        val writesThisPass = AtomicInteger()
        val store =
            object : IEventStore by inner {
                override suspend fun insert(event: Event) {
                    if (writesThisPass.incrementAndGet() > cutAt) {
                        CompletableDeferred<Unit>().await()
                        error("unreachable")
                    }
                    inner.insert(event)
                }
            }
        val pass =
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe =
                    AliasProbe(
                        fetch = { _, _, _, _ -> AliasProbe.Page(corpus) },
                        target = 40,
                        page = 40,
                        fallbackPage = 40,
                        idleMs = { 20L },
                    ),
                client = EmptyNostrClient(),
                foldedAway = { emptyMap() },
                inconsistent = { emptySet() },
                progress = Processors().of("fitness"),
                publishDeadlineMs = 50L,
                reconcile = { _, _ -> },
            )

        println("=".repeat(96))
        println("COVERAGE OVER PASSES — ${urls.size} real url(s), store stops answering after $cutAt write(s)/pass")
        println("=".repeat(96))
        println("  production, same cut, hash order:   58.8% after every pass, forever (the staircase above)")
        runBlocking {
            for (p in 1..3) {
                writesThisPass.set(0)
                pass.measure(AliasMonitor.ALL_STREAMS, urls, canDial = { true }, onEvent = {}, sockets = Sockets.NONE)
                var covered = 0
                for (chunk in urls.chunked(500)) {
                    val found =
                        store.query<Event>(
                            com.vitorpamplona.quartz.nip01Core.relay.filters.Filter(
                                kinds = listOf(com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent.KIND),
                                authors = listOf(signer.pubKey),
                                tags = mapOf("d" to chunk.map { it.url }),
                            ),
                        )
                    covered +=
                        found
                            .map { e -> e.tags.firstOrNull { it.firstOrNull() == "d" }?.getOrNull(1) }
                            .filterNotNull()
                            .toSet()
                            .size
                }
                println("  after pass %d, url-order + resume:    %5.1f%%  (%d of %d)".format(p, 100.0 * covered / urls.size, covered, urls.size))
            }
        }
    }
}
