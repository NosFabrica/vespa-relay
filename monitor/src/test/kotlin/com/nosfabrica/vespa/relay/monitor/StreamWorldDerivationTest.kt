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
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The funnel's first branch: `sourced = excluded + heldOutDead + candidates`, counted above the exclusion. */
class StreamWorldDerivationTest {
    /** Routable on purpose: `RelayDiscovery.normalize` drops loopback before the self check sees it. */
    private val self = RelayUrlNormalizer.normalize("wss://ours.example")

    /** A host minting one url per npub, excluded by regex. */
    private val perNpub = (1..40).map { "wss://filter.nostr.wine/npub1p4kg8${"%02d".format(it)}" }

    private fun event(
        kind: Int,
        vararg tags: Array<String>,
    ): Event = NostrSignerSync().sign(1_700_000_000L, kind, arrayOf(*tags), "")

    /** Compiled through the real loader, whose decision it is whether an exclude entry is a regex. */
    private fun derivations(exclude: List<String>) =
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
            ).monitorDerivations()

    /** One [RelayDiscoveryConfig] holding two sources, the shape the position is counted for. */
    private fun twoSourceDerivations() =
        RouterConfigLoader
            .parse(
                """
                streams { none { dir = "down", filter = { "kinds": [1] }, urls = [] } }
                monitor {
                    sources = [
                        {
                            select = [ { kind = 10002, tag = "r", marker = "write" } ]
                            filter = { "kinds": [10002] }
                        },
                        {
                            select = [ { kind = 10009, tag = "group", relay = 2 } ]
                            filter = { "kinds": [10009] }
                        }
                    ]
                }
                """.trimIndent(),
            ).monitorDerivations()

    /**
     * Nothing here dials, so the probe and sockets are never reached; they are passed real so a
     * constructor that starts doing work is noticed. Nothing here submits, so the sink throws.
     */
    private fun world(
        store: NostrSemanticsStore,
        exclude: List<String>,
        monitorAuthors: List<String> = emptyList(),
        self: String? = null,
        progress: Processors.Handle? = null,
    ): StreamWorld =
        StreamWorld(
            store = store,
            derivations = derivations(exclude),
            probe = ReachabilityProbe(null),
            monitorAuthors = monitorAuthors,
            self = self,
            tor = null,
            sockets = Sockets.NONE,
            onProbeEvent = { error("no dial runs here, so nothing can have seen an event") },
            progress = progress,
        )

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
    fun `the walk says where it has got to, and the row ends with the yield`() =
        runBlocking {
            val store = storeWithPerNpubUrls()
            val processors = Processors()
            val row = processors.of("aliasSource")
            val world = world(store, listOf("wss://filter.nostr.wine/npub.*"), progress = row)
            // A boot publishes no counts rather than claiming it named no urls.
            assertEquals(false, world.derived, "nothing has been derived before the first walk")

            val candidates = world.candidates()
            assertEquals(true, world.derived, "and the numbers are a measurement once one has")

            val after = processors.snapshot().single()
            assertEquals("source", after.measuring?.unit, "the walk declared what it was counting")
            // Per source, not per config: a position over configs could not move.
            assertEquals(1, after.measuring?.toProbe, "one unit per configured source, across every derivation")
            assertEquals(1, after.measuring?.attempted, "and it is behind the walk once that source's read ends")
            assertEquals(candidates.size, world.lastDerivation.candidates)
            assertEquals(1, world.lastDerivation.candidates, "one relay survives the exclude, and the row says so")
        }

    @Test
    fun `the position counts sources, not the configs they are grouped into`() =
        runBlocking {
            val store = storeWithPerNpubUrls()
            val processors = Processors()
            val world =
                StreamWorld(
                    store = store,
                    derivations = twoSourceDerivations(),
                    probe = ReachabilityProbe(null),
                    monitorAuthors = emptyList(),
                    self = null,
                    tor = null,
                    sockets = Sockets.NONE,
                    onProbeEvent = { error("no dial runs here, so nothing can have seen an event") },
                    progress = processors.of("aliasSource"),
                )

            world.candidates()

            val after = processors.snapshot().single()
            assertEquals(2, after.measuring?.toProbe, "two sources in one block are two units of work")
            assertEquals(2, after.measuring?.attempted, "and both are behind the walk when it ends")
        }

    @Test
    fun `the fast lane still holds out a url we call dead, asking only about what it found`() =
        runBlocking {
            // The lane asks about its own handful of urls; the answer over that subset must be identical.
            val monitor = NostrSignerInternal(KeyPair())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            store.insert(
                event(
                    10002,
                    arrayOf("r", "wss://corpse.example", "write"),
                    arrayOf("r", "wss://answering.example", "write"),
                ),
            )
            RelayVerdictRecord(store, monitor)
                .publishFitness(
                    RelayUrlNormalizer.normalize("wss://corpse.example"),
                    "dead",
                    "nothing answered",
                    pageable = null,
                    nip77 = null,
                )

            val world = world(store, emptyList(), monitorAuthors = listOf(monitor.pubKey), self = monitor.pubKey)
            val fresh = world.candidatesSince(0)

            assertEquals(
                listOf("wss://answering.example/"),
                fresh.map { it.url },
                "the dead url is held out of the lane exactly as it is out of a sweep",
            )
        }

    @Test
    fun `our own url is counted as excluded, which is what the row says`() =
        runBlocking {
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
            val monitor = NostrSignerInternal(KeyPair())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            store.insert(event(10002, arrayOf("r", "wss://ordinary.example", "write")))
            val record = RelayVerdictRecord(store, monitor)
            for (url in listOf("wss://forgotten.example", "wss://gone.example")) {
                record.publishFitness(RelayUrlNormalizer.normalize(url), "dead", "nothing answered", pageable = null, nip77 = null)
            }
            // Named by a relay list too, and not counted twice.
            record.publishFitness(
                RelayUrlNormalizer.normalize("wss://ordinary.example"),
                "prime",
                "answered at a settled anchor",
                pageable = null,
                nip77 = null,
            )

            val world = world(store, emptyList(), monitorAuthors = listOf(monitor.pubKey), self = monitor.pubKey)
            world.candidates()

            val d = world.lastDerivation
            assertEquals(1, d.sourced, "one url is what the relay lists name")
            assertEquals(2, d.recordedOnly, "the two nothing names any more are still urls this router knows of")
        }

    @Test
    fun `a url our own records know about is MEASURED again, not merely counted`() =
        runBlocking {
            val monitor = NostrSignerInternal(KeyPair())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            store.insert(event(10002, arrayOf("r", "wss://named.example", "write")))
            val record = RelayVerdictRecord(store, monitor)
            val forgotten = RelayUrlNormalizer.normalize("wss://forgotten.example")
            // `prime`, not `dead`: a dead verdict is held out for its own reason.
            record.publishFitness(forgotten, "prime", "answered at a settled anchor", pageable = null, nip77 = null)

            val world = world(store, emptyList(), monitorAuthors = listOf(monitor.pubKey), self = monitor.pubKey)
            val candidates = world.candidates()

            assertTrue(forgotten in candidates, "a url only our own records name must still be re-measured")
            assertEquals(2, world.lastDerivation.candidates, "both the named url and the recorded one are the corpus")
            assertEquals(1, world.lastDerivation.sourced, "…and `sourced` still means what a relay list named this round")
            assertEquals(1, world.lastDerivation.recordedOnly, "…with the other side of the union named as its own number")
        }

    @Test
    fun `a derivation that collapses says what the round before it named`() =
        runBlocking {
            // A short read and a shrunk network are the same picture; only the round before separates them.
            val monitor = NostrSignerInternal(KeyPair())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            for (i in 0 until 140) {
                store.insert(event(10002, arrayOf("r", "wss://relay$i.example", "write")))
            }
            val world = world(store, emptyList(), monitorAuthors = listOf(monitor.pubKey), self = monitor.pubKey)

            world.candidates()
            assertEquals(140, world.lastDerivation.sourced)
            assertNull(world.lastDerivation.sourcedLastRound, "the first round has nothing to be compared against")

            world.candidates()
            assertEquals(140, world.lastDerivation.sourcedLastRound, "the second round carries what the first one named")
        }

    @Test
    fun `a router with no monitor identity claims to know nothing beyond its lists`() =
        runBlocking {
            // Counting everything in the store would let a mirrored stranger's records size our corpus.
            val stranger = NostrSignerInternal(KeyPair())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = self)
            store.insert(event(10002, arrayOf("r", "wss://ordinary.example", "write")))
            RelayVerdictRecord(store, stranger)
                .publishFitness(RelayUrlNormalizer.normalize("wss://theirs.example"), "prime", "not ours", pageable = null, nip77 = null)

            val world = world(store, emptyList())
            world.candidates()

            assertEquals(0, world.lastDerivation.recordedOnly)
        }

    @Test
    fun `an empty exclude list leaves the count at zero`() =
        runBlocking {
            val store = storeWithPerNpubUrls()
            val world = world(store, emptyList())
            val candidates = world.candidates()

            assertEquals(0, world.lastDerivation.excluded)
            assertEquals(perNpub.size + 1, world.lastDerivation.sourced)
            assertEquals(perNpub.size + 1, candidates.size, "nothing excluded means everything is a candidate")
        }
}
