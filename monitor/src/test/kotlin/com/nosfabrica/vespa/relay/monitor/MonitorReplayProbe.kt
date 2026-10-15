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
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.PeerClient
import com.nosfabrica.vespa.relay.peers.RelaySockets
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test

/**
 * Runs the monitor's fold and fitness passes over every url in a file, as one sweep would, against
 * an in-memory store, and writes each url's verdicts as TSV. The consistency pass is not run.
 * `-DreplayProbe=true -DreplayUrls=<file, one url a line> -DreplayOut=<tsv>`.
 */
class MonitorReplayProbe {
    @Test
    fun replayASweep() {
        if (System.getProperty("replayProbe") != "true") {
            println("[skip] MonitorReplayProbe — set -DreplayProbe=true to dial the public internet")
            return
        }
        val urls =
            File(System.getProperty("replayUrls")).readLines().mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }.distinct()
        val out = File(System.getProperty("replayOut"))
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val signer = NostrSignerInternal(KeyPair())
        val peers = PeerClient(scope, signer, connectionTimeoutSec = CONNECT_SEC)
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
        val record = RelayVerdictRecord(store, signer)
        val reach = ReachabilityProbe(null, threads = CONCURRENCY)
        val sockets = RelaySockets(peers.client, emptySet())
        val processors = Processors()

        fun probe(target: Int) = AliasProbe.over(peers.client, target) { CONNECT_SEC * 1000L }
        val folding = AliasFolding(RelayAliases(), record, probe(RelayAliases.DEFAULT_PROBE_TARGET), CONCURRENCY, progress = processors.of("aliasFold"))
        val fitness =
            FitnessPass(
                record = record,
                probe = probe(FitnessPass.FITNESS_TARGET),
                client = peers.client,
                progress = processors.of("fitness"),
                concurrency = CONCURRENCY,
                tlsCheck = reach::tls,
                world = { ServerIndex(urls.toSet()) },
            )
        // Where the sweep is, every minute, for a run that takes an hour.
        val ticker =
            scope.launch {
                while (true) {
                    delay(60_000)
                    processors.snapshot().forEach { p ->
                        p.measuring?.let { m -> System.err.println("replay: ${p.name} ${p.phase} ${m.attempted}/${m.toProbe} ${m.unit}") }
                    }
                }
            }
        try {
            peers.connect()
            System.err.println("replay: ${urls.size} url(s)")
            runBlocking {
                folding.measure(AliasMonitor.ALL_STREAMS, urls, canDial = { reach.reach(it) == Reach.REACHABLE }, sockets = sockets)
                fitness.measure(AliasMonitor.ALL_STREAMS, urls, reach::reach, onEvent = {}, sockets = sockets)
                fitness.flushPending()
            }
            val rows =
                runBlocking { store.query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND))) }.map { e ->
                    val d = e.tags.first { it[0] == "d" }[1]
                    val grade = e.tags.firstOrNull { it.size > 3 && it[0] == "l" && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }
                    val sameAs = e.tags.firstOrNull { it.size > 1 && it[0] == RelayVerdictRecord.SAME_AS_TAG }
                    listOf(d, grade?.get(1).orEmpty(), grade?.get(3).orEmpty(), sameAs?.get(1).orEmpty(), sameAs?.getOrNull(2).orEmpty())
                        .joinToString("\t") { it.replace('\t', ' ').replace('\n', ' ') }
                }
            out.writeText(rows.joinToString("\n", postfix = "\n"))
            System.err.println("replay: wrote ${rows.size} record(s) to $out")
        } finally {
            ticker.cancel()
            runCatching { peers.close() }
            scope.cancel()
        }
    }

    companion object {
        /** The deployment's `connectionTimeout`, which is also the probes' idle window. */
        private const val CONNECT_SEC = 20L

        private const val CONCURRENCY = AliasFolding.DEFAULT_DIAL_CONCURRENCY
    }
}
