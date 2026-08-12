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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.router.config.DeleteMissing
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.router.config.SyncStream
import com.nosfabrica.vespa.relay.router.progress.PagingProgress
import com.nosfabrica.vespa.relay.router.refused.RefusedIds
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import okhttp3.OkHttpClient
import java.io.File
import java.nio.file.Files
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The `deleteMissing` band, against real NIP-85 providers over the real network.
 *
 * ## What it is for
 *
 * A relay that reconciles CLEANLY and has nothing to send is the most common
 * outcome of a mature mirror, and it used to leave NOTHING behind on this path:
 * [DeleteMissingSync] reconciles through `negentropyReconcileIds` — a different
 * quartz call from the ordinary streams', because it needs `haveIds` to decide
 * deletions — and that branch never recorded a band. Only its paging fallbacks
 * did, and quartz refuses to record an empty PAGE. So the `assertions` stream
 * charted exactly the relays that happened to hand over an event, which read as
 * a mirror covering a handful of providers when it was level with all of them.
 *
 * Nothing hermetic can falsify that. The claim is about what a real provider's
 * relay does on the SECOND pass over a corpus we already hold, and a fixture
 * that scripts the reconcile pins our reading of it rather than the relay's
 * behaviour. So this dials them.
 *
 * ## What it does
 *
 * Discovers live providers the way the router does — kind 10040 pointers off
 * the example config's own indexers, `30382:*` tags giving (provider, relay) —
 * then runs the REAL [DeleteMissingSync] against each, twice, over an in-memory
 * store:
 *
 *  - **pass 1** fills the store from the provider's relay, and
 *  - **pass 2** is the case under test: same ask, nothing left to download.
 *
 * The band after pass 2 is the whole subject. `min`/`max` are printed for both
 * passes because the shape matters as much as the presence: pass 2 must widen
 * the band's ceiling to the moment it reconciled while LEAVING the floor where
 * pass 1's oldest event put it.
 *
 * ## Safety
 *
 * Read-only against other people's servers. Nothing is published, and the
 * example config's `assertions` stream is `deleteMissing = "dryRun"`, so the
 * delete side only logs — against a throwaway in-memory store either way.
 *
 * OFF by default and asserts only what a reachable provider proves: it dials
 * the public internet, so it is neither hermetic nor reproducible, and a
 * provider being down is not a code regression.
 *
 * ```
 * ./gradlew :sync:test --tests '*DeleteMissingBandProbe*' -DdeleteMissingBandProbe=true --rerun -i
 * ```
 */
class DeleteMissingBandProbe {
    /** Where NIP-85 pointers are read from: the example's own indexer urls. */
    private val indexers =
        listOf(
            "wss://purplepag.es",
            "wss://user.kindpag.es",
            "wss://indexer.coracle.social",
        )

    /** A provider's key and the relay its own scores are served from. */
    private data class Provider(
        val pubkey: String,
        val relay: NormalizedRelayUrl,
    )

    private val config: RouterConfig =
        RouterConfigLoader.parse(
            requireNotNull(
                listOf(File("../router.conf.example"), File("router.conf.example")).firstOrNull { it.isFile },
            ) { "missing router.conf.example" }.readText(),
        )

    /**
     * Found by SHAPE, not by name: this probe is about the deleteMissing code
     * path, and the stream that exercises it has been renamed before.
     */
    private val stream: SyncStream = config.streams.first { it.deleteMissing != DeleteMissing.OFF }

    @Test
    fun reportWhetherACleanEmptyReconcileRecordsABand() {
        if (System.getProperty("deleteMissingBandProbe") != "true") {
            println("[skip] DeleteMissingBandProbe — set -DdeleteMissingBandProbe=true to dial the public internet")
            return
        }
        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)

        try {
            runBlocking {
                val providers = discover(client)
                println("=".repeat(78))
                println("deleteMissing bands: ${providers.size} live NIP-85 provider(s), stream '${stream.name}'")
                println("=".repeat(78))
                assertTrue(providers.isNotEmpty(), "no NIP-85 provider was reachable — the run proves nothing")

                var proven = 0
                for (provider in providers) {
                    proven += runProvider(client, provider)
                }
                // One provider carrying the case is enough. Requiring all of
                // them would fail on somebody else's downtime, which is not a
                // regression in this repo.
                assertTrue(
                    proven > 0,
                    "no provider reached a clean second pass — nothing was proved either way, re-run when one is up",
                )
            }
        } finally {
            scope.cancel()
        }
    }

    /**
     * Two passes over one provider. Returns 1 when the second pass was the
     * empty reconcile this exists to test, 0 when it never got there.
     */
    private suspend fun runProvider(
        client: NostrClient,
        provider: Provider,
    ): Int {
        val dir = Files.createTempDirectory("dmband").toFile().also { it.deleteOnExit() }
        val store: IEventStore = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
        val bands = SyncBands(File(dir, "bands.json"))
        val refused = RefusedIds(dir, 90L * 24 * 60 * 60, 1_000_000)
        val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        val ingest = IngestPipeline(store, config, null, null, scope).also { it.start() }
        val sync = DeleteMissingSync(client, store, bands, ingest, PagingProgress(), refused)

        // One author per leg, which is what the example configures and what
        // makes a reconcile small enough to be honest about: the whole ask is
        // one service's own scores.
        val ask = stream.filter.copy(authors = listOf(provider.pubkey))
        val ownedAsk = ask.copy(kinds = ask.kinds.orEmpty().filter { it in stream.ownedKinds })

        return try {
            val first = pass(sync, provider, ask, ownedAsk, bands, store, label = "pass 1 (fill)")
            // No band after pass 1 is where the BUG shows: it is also what a
            // relay that refused negentropy leaves behind, and the two are not
            // told apart from out here. Both are reported the same way and
            // neither is asserted on — the run fails on `proven == 0` instead,
            // which is what the whole-probe verdict is for. Live discovery
            // turns up genuine refusers: a kind 10040 can point anywhere.
            if (first.band == null) {
                println("      pass 1 left no band — the reconcile recorded nothing, or never completed; skipped")
                return 0
            }
            val second = pass(sync, provider, ask, ownedAsk, bands, store, label = "pass 2 (in sync)")
            if (second.downloaded != 0) {
                println("      pass 2 still had ${second.downloaded} event(s) to fetch — not the empty case")
                return 0
            }
            val band = assertNotNull(second.band, "${provider.relay.url} reconciled clean and empty but recorded no band")
            // Presence is half of it. A band that RESET on the empty pass would
            // chart this relay as freshly discovered every cycle, and one whose
            // ceiling stood still would never show the mirror keeping up.
            assertTrue(
                band.maxCreatedAt >= first.band.maxCreatedAt,
                "the empty reconcile did not carry the ceiling forward: ${first.band.maxCreatedAt} -> ${band.maxCreatedAt}",
            )
            assertEquals(
                first.band.minCreatedAt,
                band.minCreatedAt,
                "the empty reconcile moved the floor — it saw no events and may not claim to have re-walked history",
            )
            1
        } finally {
            ingest.closeIntake()
            scope.cancel()
            ingest.close()
            bands.close()
        }
    }

    private class Pass(
        val downloaded: Int,
        val band: SyncCoverage.Band?,
    )

    private suspend fun pass(
        sync: DeleteMissingSync,
        provider: Provider,
        ask: Filter,
        ownedAsk: Filter,
        bands: SyncBands,
        store: IEventStore,
        label: String,
    ): Pass {
        val downloaded =
            withTimeoutOrNull(PER_PROVIDER_MS) {
                sync.reconcileAndDelete(stream, provider.relay, ask, sharedAuthors = emptySet())
            } ?: -1
        // Ingest is asynchronous, and pass 2's whole question is whether the
        // store holds what pass 1 downloaded. Settle before asking again.
        settle(store, ownedAsk)
        val band = bands.band(stream.name, provider.relay, ownedAsk)
        val shown =
            band?.let {
                "min=${it.minCreatedAt} max=${it.maxCreatedAt} complete=${it.complete} spans=${it.spans.keys}"
            }
        println(
            "  ${provider.relay.url.padEnd(36)} ${label.padEnd(18)} " +
                "downloaded=$downloaded held=${store.count(ownedAsk)} band=${shown ?: "NONE"}",
        )
        return Pass(downloaded, band)
    }

    /** Wait for the ingest queue to stop moving, or give up and say so. */
    private suspend fun settle(
        store: IEventStore,
        ask: Filter,
    ) {
        var last = -1
        repeat(30) {
            val now = runCatching { store.count(ask) }.getOrDefault(-1)
            if (now == last && now >= 0) return
            last = now
            delay(500)
        }
        println("      store never settled at $last record(s) — read the numbers below with that in mind")
    }

    /**
     * Live NIP-85 pointers: kind 10040 names, per score kind, the service that
     * signs it and the relay to read it from. Exactly the select the example's
     * `assertions` stream runs, done here by hand because the stream's own
     * discovery reads a store this probe deliberately starts empty.
     */
    private suspend fun discover(client: NostrClient): List<Provider> {
        val found = LinkedHashMap<String, Provider>()
        for (raw in indexers) {
            if (found.size >= MAX_PROVIDERS) break
            val url = RelayUrlNormalizer.normalizeOrNull(raw) ?: continue
            withTimeoutOrNull(DISCOVERY_MS) {
                client.fetchAllPages(url, listOf(Filter(kinds = listOf(10040), limit = 200)), idleTimeoutMs = 15_000L) { event ->
                    for (tag in event.tags) {
                        // ["30382:rank", "<provider pubkey>", "<relay>"]
                        if (tag.size < 3 || !tag[0].startsWith("30382:")) continue
                        val relay = RelayUrlNormalizer.normalizeOrNull(tag[2]) ?: continue
                        if (tag[1].length != 64) continue
                        // A pointer can name anything, and real ones name
                        // somebody's laptop. The router dials those and finds
                        // out; a probe budgeted for three providers spends a
                        // third of itself proving a loopback url is not up.
                        if (LOOPBACK.any { relay.url.contains(it) }) continue
                        found.putIfAbsent(tag[1] + relay.url, Provider(tag[1], relay))
                    }
                }
            }
            println("discovery: ${found.size} provider(s) after $raw")
        }
        return found.values.take(MAX_PROVIDERS)
    }

    private companion object {
        /**
         * A hard ceiling per call, which `reconcileAndDelete` does not have —
         * one provider that keeps answering must not hang the probe.
         */
        const val PER_PROVIDER_MS = 180_000L
        const val DISCOVERY_MS = 60_000L

        /** Enough to survive one being down, few enough to stay a probe. */
        const val MAX_PROVIDERS = 3

        /** Hosts a pointer can name that this probe has no business dialling. */
        val LOOPBACK = listOf("localhost", "127.0.0.1", "0.0.0.0", "[::1]")
    }
}
