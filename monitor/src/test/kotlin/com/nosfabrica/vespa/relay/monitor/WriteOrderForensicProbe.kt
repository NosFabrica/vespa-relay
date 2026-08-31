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
 * WAS THE CORPUS CUT, OR WAS IT SKIPPED? — the question #172's data poses and
 * that no per-url theory can answer.
 *
 * Pulled off `search-staging.brainstorm.world`, 20,075 of this monitor's
 * kind-30166 records carry a fitness grade, and their measured-at stamps fall
 * into cohorts rather than a spread: 11,812 written in the last sweep, then
 * 623, 3,212, 4,150 and 278 progressively older. The share is the same in EVERY
 * grade — `prime` 55.8% fresh, `dead` 57.7%, `alias` 61.8%, `silent` 57.7% — and
 * the median `rtt-read` is flat across the age bands (664ms fresh, 937ms in the
 * 48-96h band). Nothing about the relay predicts whether its verdict was
 * re-taken. A pass wrote about three fifths of what it earned and stopped.
 *
 * Which leaves WHERE it stopped, and that is a question about our own iteration
 * order. [FitnessPass]'s write loop used to walk `outcomes`, a
 * [ConcurrentHashMap], so it went in bucket order — arbitrary, and identical on
 * every pass over the same key set. If the drop is a cut batch, the urls
 * written last sweep are a CONTIGUOUS RUN in that order and the stale ones are
 * the run after it. If the drop is anything per-url, the two sets are shuffled
 * through each other.
 *
 * So this rebuilds the exact map the pass built — the same
 * [NormalizedRelayUrl] keys, the same [ConcurrentHashMap] — walks it in
 * iteration order, and reports how the fresh/stale split falls across that
 * walk. A cut batch is unmistakable: a long unbroken prefix of fresh, then a
 * long unbroken tail of stale, and a handful of crossings rather than
 * thousands.
 *
 * ```
 * ./gradlew :monitor:test --tests '*WriteOrderForensicProbe*' \
 *   -DwriteOrderTsv=/path/to/urls_at.tsv --rerun -i
 * ```
 *
 * The TSV is `url<TAB>measured-at-epoch-seconds`, one per line — what a
 * `#d`-scoped read of the monitor's own records yields. Dials nothing.
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
        // THE PASS'S OWN MAP, rebuilt. Keys inserted in no particular order for
        // the same reason the pass inserts in none: the dials complete
        // concurrently. Bucket order does not depend on insertion order, which
        // is exactly why it was stable across passes and why the same tail was
        // dropped every time.
        val outcomes = ConcurrentHashMap<NormalizedRelayUrl, Long>()
        for ((url, at) in parsed) outcomes[url] = at

        val newest = parsed.maxOf { it.second }
        // The last sweep's cohort: everything stamped within an hour of the
        // newest stamp. The cohorts are hours apart, so the cut is unambiguous.
        val freshFloor = newest - 3_600
        val walk = outcomes.entries.map { it.key to (it.value >= freshFloor) }

        val fresh = walk.count { it.second }
        var crossings = 0
        for (i in 1 until walk.size) if (walk[i].second != walk[i - 1].second) crossings++
        // What a cut batch predicts: ONE crossing. What a per-url cause
        // predicts: the two classes interleaved, so crossings scale with the
        // smaller class — here, thousands.
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
        // The longest run of each, and where it starts — a cut batch puts the
        // fresh run at the head and the stale run immediately after it.
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
        // …AND WHERE THE OLDER COHORTS FALL IN THE SAME WALK. If the cut point
        // is stable, each successively older cohort is the slice just beyond
        // the one before it: pass N wrote up to here, pass N-1 up to there.
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

    /**
     * THE SAME CORPUS THROUGH THE REAL PASS, cut the same way — the before/after
     * the forensics above ask for.
     *
     * The production numbers ARE the "before": the write loop reached position
     * 12,191 of 20,072 and stopped, every pass, from position zero, so the
     * urls beyond it were never re-graded at all. This replays that on the real
     * url set, against a store that stops answering at the same point, and
     * reports coverage over four passes — where "covered" is a verdict actually
     * in the store.
     *
     * Nothing is dialled: the probe answers from memory, because the question
     * is what the WRITE loop reaches and the dial phase is not part of it.
     */
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
                // A SCALED slice of the real corpus, and scaled only because
                // the in-memory store's read-before-write is linear, so the
                // replay is quadratic where production's indexed store is not.
                // The property under test is positional and scale-free: does
                // pass two begin where pass one stopped. The cut below is held
                // at production's own three-fifths.
                .let { all -> all.take(System.getProperty("writeOrderReplayUrls")?.toIntOrNull() ?: 4_000) }
        val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
        val events = NostrSignerSync()
        val signer = NostrSignerInternal(KeyPair())
        val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
        val inner = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
        // The store as production behaved: it takes writes, and past a point in
        // a batch it stops answering them. CUT_AT is the observed 12,191 of
        // 20,072 — the same three-fifths.
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
