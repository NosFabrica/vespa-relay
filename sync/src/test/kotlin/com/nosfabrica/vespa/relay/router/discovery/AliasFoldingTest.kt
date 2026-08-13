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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The fold is two halves that run at different times, and the whole point of
 * the split is WHICH half touches the network.
 *
 * [AliasFolding.apply] runs in front of a fan-out on every cycle, so it must
 * never dial; [AliasFolding.measure] runs on [AliasMonitor]'s clock, where a
 * multi-minute probe pass costs nobody a download. A regression that quietly
 * moved a probe back onto the read path would not fail any other test in this
 * repo — it would just make cycles slow again, which is the thing that was
 * wrong in the first place.
 */
class AliasFoldingTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val canonical = RelayUrlNormalizer.normalize("wss://nos.lol")
    private val alias = RelayUrlNormalizer.normalize("wss://nos.lol/cipher-zulu")
    private val elsewhere = RelayUrlNormalizer.normalize("wss://nostr.mom")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /**
     * A relay set that answers per url, counting every dial — which is what
     * these tests assert on far more than they assert on the folding itself.
     */
    private class Upstreams(
        private val corpusFor: (NormalizedRelayUrl) -> List<Event>,
    ) {
        val dials = AtomicInteger()

        /** Which relays were contacted at all — one url may cost several asks. */
        val contacted: MutableSet<NormalizedRelayUrl> =
            java.util.concurrent.ConcurrentHashMap
                .newKeySet()

        suspend fun fetch(
            at: NormalizedRelayUrl,
            want: Int,
            until: Long?,
            @Suppress("UNUSED_PARAMETER") kinds: List<Int>?,
        ): List<Event> {
            dials.incrementAndGet()
            contacted += at
            return corpusFor(at).filter { until == null || it.createdAt <= until }.take(want)
        }
    }

    /**
     * A store that refuses every read, for the one case the fold has to survive
     * without acting on: a query that FAILED is not a store saying "no verdict".
     */
    private class Unreadable(
        private val inner: IEventStore,
    ) : IEventStore by inner {
        override suspend fun <T : Event> query(filter: Filter): List<T> = throw IllegalStateException("the store cannot answer")
    }

    private fun folding(
        store: NostrSemanticsStore,
        upstreams: Upstreams,
        aliases: RelayAliases = RelayAliases(),
        undecidableCooldownMs: Long = AliasFolding.DEFAULT_UNDECIDABLE_COOLDOWN_MS,
    ) = AliasFolding(
        aliases = aliases,
        record = RelayAliasRecord(store, signer),
        probe = AliasProbe(fetch = upstreams::fetch, target = 40, page = 40, fallbackPage = 40),
        undecidableCooldownMs = undecidableCooldownMs,
    )

    /** Every url serves the same 40 events, so any two of them fold. */
    private fun upstreams(): Upstreams {
        val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
        return Upstreams { corpus }
    }

    /**
     * Every url serves its OWN 40 events, so nothing folds however many times
     * it is asked — a host where the paths are real relays.
     */
    private fun distinctUpstreams(): Upstreams {
        val byUrl = HashMap<String, List<Event>>()
        return Upstreams { at ->
            byUrl.getOrPut(at.url) {
                (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "${at.url}#$it") }
            }
        }
    }

    @Test
    fun `apply never dials, however much there is to learn`() =
        runBlocking {
            val up = upstreams()
            val cleaned = folding(newStore(), up).apply(listOf(canonical, alias))

            assertEquals(0, up.dials.get(), "apply() opened ${up.dials.get()} socket(s); it runs on the cycle's critical path")
            // Nothing measured yet, so nothing may be folded away: the only safe
            // reading of "no verdict" is "dial it as it stands".
            assertEquals(listOf(canonical, alias), cleaned.dial)
            assertTrue(cleaned.aliases.isEmpty())
        }

    @Test
    fun `a url is dialled unfolded exactly once, then folds from the store`() =
        runBlocking {
            // The cost of moving the probe off the cycle, stated as a test: the
            // first cycle to see a url cannot fold it, because nothing has
            // measured it yet. The measurement happens between the two.
            val store = newStore()
            val up = upstreams()
            val fold = folding(store, up)

            assertEquals(2, fold.apply(listOf(canonical, alias)).dial.size)
            assertEquals(1, fold.measure("t", listOf(canonical, alias), canDial = { true }))
            assertEquals(listOf(canonical), fold.apply(listOf(canonical, alias)).dial)
        }

    @Test
    fun `a second process reads the verdict without re-probing`() =
        runBlocking {
            // The two halves talk through the STORE and nothing else, which is
            // what makes the split survive a restart. Fresh RelayAliases here
            // stands in for a router that has just booted.
            val store = newStore()
            val prober = upstreams()
            folding(store, prober).measure("t", listOf(canonical, alias), canDial = { true })

            val reader = upstreams()
            val cleaned = folding(store, reader).apply(listOf(canonical, alias))

            assertEquals(listOf(canonical), cleaned.dial)
            assertEquals(mapOf(alias to canonical), cleaned.aliases)
            assertEquals(0, reader.dials.get(), "the reader re-probed what the store already knew")
        }

    /**
     * A read that FAILED is not a store saying "no verdict", and the difference
     * is a whole fan-out.
     *
     * `adopt` forgets every verdict it holds before adopting what the store
     * hands back, so the store stays authoritative and the 30-day TTL means
     * something. That is only safe while a failure arrives AS a failure —
     * `load` used to swallow a failed chunk into an empty result, which turned
     * one unlucky query into up to 500 urls silently unfolded for that cycle:
     * dialled as their own relays, re-probed for verdicts already published,
     * with nothing anywhere saying so.
     */
    @Test
    fun `a store that cannot be read leaves the verdicts already held alone`() =
        runBlocking {
            val store = newStore()
            val aliases = RelayAliases()
            val fold = folding(store, upstreams(), aliases)
            assertEquals(1, fold.measure("t", listOf(canonical, alias), canDial = { true }))
            assertEquals(listOf(canonical), fold.apply(listOf(canonical, alias)).dial)

            // The same verdicts in memory, in front of a store that has stopped
            // answering. The fold must go on folding.
            val blind =
                AliasFolding(
                    aliases = aliases,
                    record = RelayAliasRecord(Unreadable(store), signer),
                    probe = AliasProbe(fetch = upstreams()::fetch, target = 40, page = 40, fallbackPage = 40),
                )

            assertEquals(listOf(canonical), blind.apply(listOf(canonical, alias)).dial, "a failed read unfolded the fan-out")
        }

    @Test
    fun `measure honours the caller's transport guard`() =
        runBlocking {
            val up = upstreams()
            // The leader is refused, so its group can never be compared — and
            // dialling the members anyway would be pure waste.
            val learned = folding(newStore(), up).measure("t", listOf(canonical, alias), canDial = { false })

            assertEquals(0, learned)
            assertEquals(0, up.dials.get())
        }

    /**
     * A fingerprint is a websocket and quartz closes none of its own, so the
     * pass has to hand every connection back to the stream that lent it.
     *
     * Unreleased, a pass leaves one socket per url it measured — up to
     * `probesPerCycle` of them — against a router whose dispatcher budget is
     * 1024 for the whole process and 20 per HOST. The fold probes widest group
     * first, so the 55-url host is exactly where it bites: the fan-out queues
     * behind connections nothing will ever close.
     *
     * Balance, not order: the claims and the releases are per url and the
     * whole point is that none is left outstanding when the pass returns.
     */
    @Test
    fun `every url the pass dials is handed back to the stream's refcount`() =
        runBlocking {
            val held = ConcurrentHashMap<NormalizedRelayUrl, Int>()
            val claims = AtomicInteger()
            val sockets =
                object : AliasFolding.Sockets {
                    override fun claim(url: NormalizedRelayUrl) {
                        claims.incrementAndGet()
                        held.merge(url, 1, Int::plus)
                    }

                    override fun release(url: NormalizedRelayUrl) {
                        held.compute(url) { _, n -> ((n ?: 1) - 1).takeIf { it > 0 } }
                    }
                }
            val up = upstreams()
            folding(newStore(), up).measure("t", listOf(canonical, alias), canDial = { true }, sockets = sockets)

            assertEquals(2, claims.get(), "the leader and its member are each one dial, each one claim")
            assertTrue(held.isEmpty(), "the pass returned holding ${held.keys}, so those sockets can never be closed")
        }

    /**
     * The guard runs BEFORE the claim: a url the stream refuses to dial must not
     * be counted as holding a connection it never opened, or the refcount never
     * reaches zero and the fan-out's own release stops closing anything.
     */
    @Test
    fun `a url the transport guard refuses is never claimed`() =
        runBlocking {
            val held = ConcurrentHashMap<NormalizedRelayUrl, Int>()
            val sockets =
                object : AliasFolding.Sockets {
                    override fun claim(url: NormalizedRelayUrl) {
                        held.merge(url, 1, Int::plus)
                    }

                    override fun release(url: NormalizedRelayUrl) {
                        held.compute(url) { _, n -> ((n ?: 1) - 1).takeIf { it > 0 } }
                    }
                }
            folding(newStore(), upstreams()).measure("t", listOf(canonical, alias), canDial = { false }, sockets = sockets)

            assertTrue(held.isEmpty(), "a refused url left a claim behind")
        }

    /**
     * A relay that cannot repeat itself must produce NO verdict — not a fold,
     * and above all not a signed "these are different relays".
     *
     * Measured on `fiatjaf.com`, which serves an arbitrary ten events per REQ
     * whatever limit is asked: one url walked twice from a single anchor,
     * seconds apart, shared NONE of its ten ids, and over a paged walk it
     * self-scored 0.694-0.720 while its two sibling paths scored 0.592 and 0.775
     * against EACH OTHER. The cross-url number sits inside the band the url
     * scores against itself, so which side of the 0.5 fold threshold a pass
     * lands on is chance — and the losing side publishes two urls of one relay
     * as separate relays for thirty days, during which `measured()` answers true
     * and nothing re-probes them. The duplicate stays in the fan-out for the
     * whole TTL on evidence that contradicts itself.
     *
     * The corpus here rotates per dial the way that relay's does, so the second
     * leader walk cannot match the first.
     */
    @Test
    fun `a relay that cannot reproduce its own window publishes nothing`() =
        runBlocking {
            val store = newStore()
            val a = RelayUrlNormalizer.normalize("wss://shuffling.example")
            val b = RelayUrlNormalizer.normalize("wss://shuffling.example/ember")
            // Every dial hands back a fresh, disjoint 40 events — the extreme of
            // what that relay does, so no walk can ever agree with another.
            val served = AtomicInteger()
            val shuffling =
                Upstreams {
                    val n = served.getAndIncrement()
                    (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "dial$n-e$it") }
                }
            val fold = folding(store, shuffling, RelayAliases())

            assertEquals(0, fold.measure("t", listOf(a, b), canDial = { true }), "nothing may be folded on this evidence")

            // The negative claim is the one that used to be published here: two
            // urls of one host, cleared as separate relays, for 30 days.
            val held = RelayAliasRecord(store, signer).load(listOf(a, b))
            assertTrue(held.aliases.isEmpty(), "a fold was published against a yardstick that cannot repeat itself")
            assertTrue(held.distinct.isEmpty(), "published ${held.distinct.size} url(s) as their own relay on unreproducible evidence")
            // And nothing is held in memory either, so the next pass re-measures
            // rather than resuming from half a verdict.
            assertEquals(listOf(a, b), fold.apply(listOf(a, b)).dial)
        }

    @Test
    fun `a second process does not re-probe urls a previous pass cleared`() =
        runBlocking {
            // The reason the cleared verdict is persisted at all. Two paths on
            // one host that are NOT duplicates: the first pass fingerprints
            // them and folds nothing, and without a stored verdict every later
            // boot pays for that same discovery again — 59 fingerprints against
            // a store already holding 128 folds, measured.
            val store = newStore()
            val a = RelayUrlNormalizer.normalize("wss://nostr.ac")
            val b = RelayUrlNormalizer.normalize("wss://nostr.ac/v1")
            val first = distinctUpstreams()
            assertEquals(0, folding(store, first).measure("t", listOf(a, b), canDial = { true }))
            assertTrue(first.dials.get() > 0, "the first pass never dialled, so this proves nothing")

            val second = distinctUpstreams()
            assertEquals(0, folding(store, second).measure("t", listOf(a, b), canDial = { true }))

            assertEquals(0, second.dials.get(), "a cleared url was fingerprinted again")
        }

    @Test
    fun `a fully folded host stops being probed at all`() =
        runBlocking {
            // The leader everything folded ONTO has a verdict — it is the class
            // representative — but it is nobody's alias and nobody's singleton.
            // A group left open on that basis costs one fingerprint per pass in
            // perpetuity, learning nothing, because there is no longer anything
            // in the group to compare it against.
            val store = newStore()
            val first = upstreams()
            assertEquals(1, folding(store, first).measure("t", listOf(canonical, alias), canDial = { true }))

            val second = upstreams()
            folding(store, second).measure("t", listOf(canonical, alias), canDial = { true })

            assertEquals(0, second.dials.get(), "a settled host was re-probed with nothing left to learn")
        }

    @Test
    fun `a new url on a settled host still gets measured`() =
        runBlocking {
            // The other side of that: persisting "cleared" must not freeze the
            // host. A url with no verdict leaves the group unresolved however
            // many of its neighbours are settled.
            val store = newStore()
            val a = RelayUrlNormalizer.normalize("wss://nostr.ac")
            val b = RelayUrlNormalizer.normalize("wss://nostr.ac/v1")
            val c = RelayUrlNormalizer.normalize("wss://nostr.ac/v2")
            folding(store, distinctUpstreams()).measure("t", listOf(a, b), canDial = { true })

            val next = distinctUpstreams()
            folding(store, next).measure("t", listOf(a, b, c), canDial = { true })

            assertTrue(next.dials.get() > 0, "a newly discovered url was never fingerprinted")
        }

    @Test
    fun `a group's verdict is written while the pass is still running`() =
        runBlocking {
            // A pass is background work that yields to the fan-out, so on a cold
            // store — nothing folded, mirror at its widest — it can run for a
            // quarter of an hour. Measured: 13 minutes with zero verdicts in the
            // store, because they used to be held until the whole pass ended.
            // Anything that ends the process in that window — a restart, an OOM,
            // a redeploy — threw away every fingerprint the pass had taken, and
            // a cold store is exactly when that work is most expensive to redo.
            //
            // A pass cannot be killed from inside a test, so this asserts the
            // property that makes the kill survivable: the first group's verdict
            // is readable from the store while a second group is still probing.
            val store = newStore()
            val fast = RelayUrlNormalizer.normalize("wss://nos.lol")
            val fastAlias = RelayUrlNormalizer.normalize("wss://nos.lol/cipher-zulu")
            val slow = RelayUrlNormalizer.normalize("wss://slow.example")
            val slowAlias = RelayUrlNormalizer.normalize("wss://slow.example/alpha")

            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val release = CompletableDeferred<Unit>()
            val up =
                Upstreams { at ->
                    if (at.url.contains("slow.example")) runBlocking { release.await() }
                    corpus
                }

            val pass = launch { folding(store, up).measure("t", listOf(fast, fastAlias, slow, slowAlias), canDial = { true }) }
            val reader = RelayAliasRecord(store, signer)
            try {
                withTimeout(10_000) {
                    while (reader.load(listOf(fast, fastAlias)).aliases.isEmpty()) delay(20)
                }
                // Readable, and the pass has not finished.
                assertTrue(pass.isActive, "the pass ended before the assertion, so this proves nothing")
                assertEquals(mapOf(fastAlias to fast), reader.load(listOf(fast, fastAlias)).aliases)
            } finally {
                // Unconditionally, or a FAILING run leaves the pass parked on
                // this forever and `runBlocking` waits for it — the assertion
                // error never surfaces and the suite hangs instead of failing.
                // Which is exactly what happened when this was checked against
                // the unfixed code.
                release.complete(Unit)
                pass.join()
            }
        }

    @Test
    fun `a leader too thin to be a yardstick does not drag its group onto the wire`() =
        runBlocking {
            // A leader that hands over three ids is under minSample: nothing can
            // fold onto it, and since the thin-window guard nothing can be
            // cleared against it either. Dialling its members decides nothing —
            // and did it again every pass, forever.
            val store = newStore()
            val a = RelayUrlNormalizer.normalize("wss://quiet.example")
            val b = RelayUrlNormalizer.normalize("wss://quiet.example/alpha")
            val c = RelayUrlNormalizer.normalize("wss://quiet.example/beacon")
            val thin: List<Event> = (0 until 3).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "t$it") }
            val up = Upstreams { thin }

            folding(store, up).measure("t", listOf(a, b, c), canDial = { true })

            // The leader itself still costs a probe every pass — that is the
            // price of noticing it has recovered — but nothing behind it does.
            assertEquals(setOf(a), up.contacted, "members were dialled behind a leader that could decide nothing")
        }

    @Test
    fun `a group that cannot be decided returns its probe budget`() =
        runBlocking {
            // The budget is reserved per group up front, so a group that bails
            // must hand back what it did not spend. Otherwise probesPerCycle is
            // consumed by intentions and a later group goes unprobed for it.
            val store = newStore()
            val dead = RelayUrlNormalizer.normalize("wss://dead.example")
            val deadAlias = RelayUrlNormalizer.normalize("wss://dead.example/alpha")
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { at -> if (at.url.contains("dead.example")) emptyList() else corpus }

            // Budget of 3: the dead host's group reserves 2 and must return
            // them, leaving enough for nos.lol's group of 2 to be probed.
            val fold =
                AliasFolding(
                    aliases = RelayAliases(),
                    record = RelayAliasRecord(store, signer),
                    probe = AliasProbe(fetch = up::fetch, target = 40, page = 40, fallbackPage = 40),
                    probesPerCycle = 3,
                )
            val learned = fold.measure("t", listOf(dead, deadAlias, canonical, alias), canDial = { true })

            assertEquals(1, learned, "the live group was starved by a reservation the dead group never returned")
        }

    @Test
    fun `hosts are never compared across domains`() =
        runBlocking {
            // Two different relays that happen to serve identical events. The
            // fold groups by hostname first, so this can never collapse however
            // well the fingerprints match.
            val store = newStore()
            val fold = folding(store, upstreams())

            assertEquals(0, fold.measure("t", listOf(canonical, elsewhere), canDial = { true }))
            assertEquals(listOf(canonical, elsewhere), fold.apply(listOf(canonical, elsewhere)).dial)
        }

    /**
     * A host that answers every fingerprint with nothing. It can never be
     * decided — no yardstick, so [RelayAliases.learn] has nothing to compare —
     * and, critically, a pass writes NOTHING down about it.
     */
    private fun silentUpstreams(): Upstreams = Upstreams { emptyList() }

    @Test
    fun `a host that cannot be decided is not re-dialled by the very next pass`() =
        runBlocking {
            // The starvation this fixes: nothing is published for an undecidable
            // group, so `unresolved` hands it back every pass — widest first,
            // and a host wearing many minted paths is exactly that shape. It was
            // re-dialled at the front of every pass forever, learning nothing
            // each time and spending budget a foldable host never got.
            val store = newStore()
            val up = silentUpstreams()
            val fold = folding(store, up)
            val urls = listOf(canonical, alias)

            assertEquals(0, fold.measure("t", urls, canDial = { true }))
            val firstPass = up.dials.get()
            assertTrue(firstPass > 0, "the first pass has to actually dial it — that is how we learn it is silent")

            assertEquals(0, fold.measure("t", urls, canDial = { true }))
            assertEquals(firstPass, up.dials.get(), "the second pass re-dialled a host the first one already proved silent")
        }

    @Test
    fun `the cooldown lapses, so a host that recovers comes back`() =
        runBlocking {
            // The other half: this is a note about OUR pass, not a verdict about
            // their server, so it must expire on its own. A relay that was
            // mid-restart has to be measurable again without waiting out a
            // signed record's TTL.
            val store = newStore()
            val up = silentUpstreams()
            val fold = folding(store, up, undecidableCooldownMs = 0L)
            val urls = listOf(canonical, alias)

            assertEquals(0, fold.measure("t", urls, canDial = { true }))
            val firstPass = up.dials.get()
            assertEquals(0, fold.measure("t", urls, canDial = { true }))

            assertTrue(up.dials.get() > firstPass, "a lapsed cooldown still held the host back")
        }

    @Test
    fun `a silent host does not stop a foldable one being measured`() =
        runBlocking {
            // The symptom as reported: `relay.lightning.pub` folds four urls in
            // two seconds at containment 1.000, and was not folding in
            // production. Both hosts are in one candidate set here, the silent
            // one first — which is where the widest-first ordering puts it.
            val store = newStore()
            val quietHost = RelayUrlNormalizer.normalize("wss://silent.example")
            val quietAlias = RelayUrlNormalizer.normalize("wss://silent.example/umbra")
            // One corpus shared by every url that answers at all, so the
            // foldable host folds; the silent one answers nothing, ever.
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { at -> if (RelayAliases.hostOf(at.url) == "silent.example") emptyList() else corpus }
            val fold = folding(store, up)
            val urls = listOf(quietHost, quietAlias, canonical, alias)

            assertEquals(1, fold.measure("t", urls, canDial = { true }), "the foldable host was not measured")
            // …and on the pass after, the silent host costs nothing at all while
            // the fold that was already earned still stands.
            assertEquals(listOf(quietHost, quietAlias, canonical), fold.apply(urls).dial)
        }

    @Test
    fun `a single url is returned untouched by both halves`() =
        runBlocking {
            val up = upstreams()
            val fold = folding(newStore(), up)

            assertEquals(listOf(canonical), fold.apply(listOf(canonical)).dial)
            assertEquals(0, fold.measure("t", listOf(canonical), canDial = { true }))
            assertEquals(0, up.dials.get())
        }
}
