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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.nosfabrica.vespa.relay.router.IngestPipeline
import com.nosfabrica.vespa.relay.router.config.RouterConfig
import com.nosfabrica.vespa.relay.router.config.RouterConfigLoader
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * **THE FUNNEL'S FIRST BRANCH HAS TO COUNT WHAT IT DROPS.**
 *
 * `sourced = excluded + heldOutDead + candidates` is the identity the coverage
 * card's top three rows rest on, and for as long as it existed the `excluded`
 * term was structurally 0 — not because nothing was excluded, but because
 * [StreamWorld.derive] asked [RelayDiscovery.discover] to apply the exclude
 * list and the self check FIRST and then re-tested the survivors with the same
 * two predicates. Everything that would have failed had already been dropped a
 * frame below, so `kept` was true for every url that reached the callback.
 *
 * Two numbers were wrong from one cause, which is why both are asserted here:
 * `excluded` read 0 on a config excluding thousands of urls, and `sourced` —
 * labelled "every url the streams named" — was silently a POST-exclusion
 * count, so the arithmetic closed perfectly while describing a corpus smaller
 * than the one that existed. A partition that divides cleanly because one side
 * was amputated before the cut is the failure this file exists to catch.
 *
 * The exclusion itself was never broken, and one test below pins that the fix
 * did not trade a right answer for a right number.
 */
class StreamWorldDerivationTest {
    /**
     * A ROUTABLE url on purpose. The usual test spelling for "us" is
     * `ws://localhost:7777`, and it cannot exercise the self check at all —
     * `RelayDiscovery.normalize` drops loopback before anything downstream sees
     * it, so a relay list naming us would come back excluded for the wrong
     * reason and the assertion would pass on a coincidence.
     */
    private val self = RelayUrlNormalizer.normalize("wss://ours.example")

    /**
     * The production shape this was found on: a host minting one url per npub,
     * excluded by a regex no literal list could keep up with.
     */
    private val perNpub = (1..40).map { "wss://filter.nostr.wine/npub1p4kg8${"%02d".format(it)}" }

    private fun event(
        kind: Int,
        vararg tags: Array<String>,
    ): Event = NostrSignerSync().sign(1_700_000_000L, kind, arrayOf(*tags), "")

    /**
     * The monitor block from `router.conf.example`, cut to the one source this
     * needs. Compiled through the REAL loader rather than a hand-built
     * [com.nosfabrica.vespa.relay.router.config.RelayExcludes]: the entry's
     * classification as a regex — it carries a `*`, a dot alone would not do it
     * — is the loader's decision, and a test that bypasses it cannot fail when
     * that classification changes.
     */
    private fun monitorConfig(exclude: List<String>) =
        RouterConfigLoader
            .parse(
                """
                # The loader requires one; this world runs on the monitor block
                # alone, which is the deployment the block was added for.
                streams { none { dir = "down", filter = { "kinds": [1] }, urls = [] } }
                monitor {
                    sources = [
                        {
                            select = [ { kind = 10002, tag = "r", marker = "write" } ]
                            filter = { "kinds": [10002] }
                        }
                    ]
                    exclude = [ ${exclude.joinToString(", ") { "\"$it\"" }} ]
                }
                """.trimIndent(),
            ).monitor

    /**
     * Nothing here dials, so the probe, the ingest queue and the socket
     * refcount are never reached — [StreamWorld.candidates] reads the store and
     * nothing else. Passed real rather than mocked anyway, because a
     * constructor that quietly starts doing work is exactly the change this
     * test should notice.
     */
    private fun world(
        store: NostrSemanticsStore,
        exclude: List<String>,
        monitorAuthors: List<String> = emptyList(),
    ): StreamWorld {
        val scope = CoroutineScope(Job())
        return StreamWorld(
            store = store,
            streams = emptyList(),
            probe = ReachabilityProbe(null),
            ingest =
                IngestPipeline(
                    store,
                    RouterConfig(connectionTimeoutSec = 5, streams = emptyList(), ingestConcurrency = 1, ingestBatch = 8),
                    null,
                    null,
                    scope,
                ),
            monitorAuthors = monitorAuthors,
            tor = null,
            sockets = AliasFolding.Sockets.NONE,
            monitorConfig = monitorConfig(exclude),
        )
    }

    /** One 10002 naming every per-npub url, one naming a relay nothing excludes. */
    private suspend fun storeWithPerNpubUrls(): NostrSemanticsStore {
        val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
        store.insert(event(10002, *perNpub.map { arrayOf("r", it, "write") }.toTypedArray()))
        store.insert(event(10002, arrayOf("r", "wss://ordinary.example", "write")))
        return store
    }

    @Test
    fun `the regex exclude is counted, not just applied`() =
        runBlocking {
            val store = storeWithPerNpubUrls()
            val world = world(store, listOf("wss://filter.nostr.wine/npub.*"))
            world.candidates()

            val d = world.lastDerivation
            assertEquals(perNpub.size, d.excluded, "every per-npub url the exclude dropped has to appear in the count")
            assertEquals(perNpub.size + 1, d.sourced, "`sourced` is what the store NAMED, above the exclusion, not below it")
            assertEquals(0, d.heldOutDead, "no dead verdicts were published, so nothing was held out")
        }

    @Test
    fun `the excluded urls are still kept out of the candidate set`() =
        runBlocking {
            val store = storeWithPerNpubUrls()
            val candidates = world(store, listOf("wss://filter.nostr.wine/npub.*")).candidates()

            assertEquals(1, candidates.size, "one relay survives the exclude")
            assertTrue(
                candidates.none { it.url.contains("filter.nostr.wine") },
                "counting a drop must not stop it being a drop",
            )
        }

    @Test
    fun `the funnel's first branch divides exactly`() =
        runBlocking {
            val store = storeWithPerNpubUrls()
            val world = world(store, listOf("wss://filter.nostr.wine/npub.*"))
            val candidates = world.candidates()

            val d = world.lastDerivation
            assertEquals(
                d.sourced,
                d.excluded + d.heldOutDead + candidates.size,
                "the identity the coverage card's top three rows are drawn from",
            )
        }

    @Test
    fun `our own url is counted as excluded, which is what the row says`() =
        runBlocking {
            // The row reads "excluded by config, or our own url" — the self
            // check used to reach discovery as `skip`, one frame below the
            // count, so the second half of that label could never be true
            // either.
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            store.insert(event(10002, arrayOf("r", self.url, "write"), arrayOf("r", "wss://ordinary.example", "write")))

            val world = world(store, emptyList())
            val candidates = world.candidates()

            assertEquals(1, world.lastDerivation.excluded, "our own url is a drop an operator can act on")
            assertEquals(2, world.lastDerivation.sourced)
            assertTrue(candidates.none { it == self }, "and still never dialled")
        }

    @Test
    fun `urls only our records know about are counted beside what the streams named`() =
        runBlocking {
            // THE CORPUS IS NOT ONE DERIVATION'S YIELD. A url leaves the relay
            // lists for reasons of its own — the author who listed it revised
            // their 10002 — and every measurement of it stays in the store,
            // still read by the fold. The coverage card's caption says "every
            // relay url this router knows of" and its tree was rooted at
            // `sourced`, so those urls left the corpus without a word.
            val monitor = NostrSignerInternal(KeyPair())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            store.insert(event(10002, arrayOf("r", "wss://ordinary.example", "write")))
            val record = RelayVerdictRecord(store, monitor)
            for (url in listOf("wss://forgotten.example", "wss://gone.example")) {
                record.publishFitness(RelayUrlNormalizer.normalize(url), "dead", "nothing answered", pageable = null, nip77 = null)
            }
            // …and one the relay list DOES still name, which must not be counted
            // twice: the mouth is `sourced + recordedOnly` and a url on both
            // sides of that would inflate the whole tree's denominator.
            record.publishFitness(
                RelayUrlNormalizer.normalize("wss://ordinary.example"),
                "syncable",
                "answered at a settled anchor",
                pageable = null,
                nip77 = null,
            )

            val world = world(store, emptyList(), monitorAuthors = listOf(monitor.pubKey))
            world.candidates()

            val d = world.lastDerivation
            assertEquals(1, d.sourced, "one url is what the relay lists name")
            assertEquals(2, d.recordedOnly, "the two nothing names any more are still urls this router knows of")
        }

    @Test
    fun `a router with no monitor identity claims to know nothing beyond its lists`() =
        runBlocking {
            // Same asymmetry `undialable` documents: with no signer and no named
            // monitors this router holds no records, so the honest count of what
            // it knows beyond today's relay lists is none — not "everything in
            // the store", which would let a mirrored stranger's 30166s decide
            // the size of our own corpus.
            val stranger = NostrSignerInternal(KeyPair())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            store.insert(event(10002, arrayOf("r", "wss://ordinary.example", "write")))
            RelayVerdictRecord(store, stranger)
                .publishFitness(RelayUrlNormalizer.normalize("wss://theirs.example"), "syncable", "not ours", pageable = null, nip77 = null)

            val world = world(store, emptyList())
            world.candidates()

            assertEquals(0, world.lastDerivation.recordedOnly)
        }

    @Test
    fun `an empty exclude list leaves the count at zero`() =
        runBlocking {
            // The reading that USED to be unconditional. Pinned so a regression
            // to it is a failure rather than a plausible-looking 0.
            val store = storeWithPerNpubUrls()
            val world = world(store, emptyList())
            val candidates = world.candidates()

            assertEquals(0, world.lastDerivation.excluded)
            assertEquals(perNpub.size + 1, world.lastDerivation.sourced)
            assertEquals(perNpub.size + 1, candidates.size, "nothing excluded means everything is a candidate")
        }
}
