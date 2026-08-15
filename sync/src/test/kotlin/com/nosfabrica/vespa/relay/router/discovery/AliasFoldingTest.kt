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
        /**
         * Which urls ANSWER at all. False is our transport giving up — a null
         * page, silence — and is a different fact from a relay that answers and
         * serves nothing. The fold now draws opposite conclusions from the two,
         * so no fixture may blur them: see [AliasFolding.foldUnreadableGroups].
         */
        private val answers: (NormalizedRelayUrl) -> Boolean = { true },
        /**
         * Which filters this relay will answer at all — everything, unless a
         * test says otherwise.
         *
         * A NIP-29 relay refuses every unscoped query whatever kinds are named
         * (khatru: `CLOSED blocked: invalid query, must have 'h', 'e' or 'a'
         * tag`), and a REFUSAL reaches the walk as an empty page rather than as
         * silence — which is the distinction the ladder's third rung is gated
         * on.
         */
        private val serves: (List<Int>?) -> Boolean = { true },
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
            kinds: List<Int>?,
        ): List<Event>? {
            dials.incrementAndGet()
            contacted += at
            if (!answers(at)) return null
            if (!serves(kinds)) return emptyList()
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
        foldUnreadableGroups: Boolean = AliasFolding.DEFAULT_FOLD_UNREADABLE_GROUPS,
    ) = AliasFolding(
        aliases = aliases,
        record = RelayAliasRecord(store, signer),
        probe = AliasProbe(fetch = upstreams::fetch, target = 40, page = 40, fallbackPage = 40),
        undecidableCooldownMs = undecidableCooldownMs,
        foldUnreadableGroups = foldUnreadableGroups,
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

    /**
     * A NIP-29 relay: it refuses every general query and serves its short list
     * of groups — the same list on every path it wears, which is what the live
     * hosts do (containment 1.000 on a minted path, 6 of 6 measured).
     */
    private fun groupsUpstreams(groups: Int = 7): Upstreams {
        val corpus: List<Event> = (0 until groups).map { events.sign(1_700_000_000L - it, 39_000, emptyArray(), "g$it") }
        return Upstreams(serves = { it == RelayAliases.GROUP_METADATA_KINDS }) { corpus }
    }

    @Test
    fun `a NIP-29 host folds on the one window a general filter cannot reach`() =
        runBlocking {
            // `groups.satsdisco.com` and its eleven minted paths, the shape the
            // ladder's third rung exists for. Both general filters are refused,
            // so the group had NO YARDSTICK, wrote nothing down, and came back
            // widest-first on every pass forever.
            //
            // Seven groups on purpose: that is `groups.hzrd149.com`, and it is
            // under DEFAULT_MIN_SAMPLE. The rung alone recovers nothing here —
            // the floor has to follow the filter too.
            val store = newStore()
            val fold = folding(store, groupsUpstreams(groups = 7))
            val group = listOf(canonical, alias)

            assertEquals(1, fold.measure("t", group, canDial = { true }), "the group list is a perfectly good fingerprint")
            // Read back through the store, which is the only claim that matters:
            // the next cycle's apply() must dial one url, not two.
            assertEquals(listOf(canonical), fold.apply(group).dial)
        }

    @Test
    fun `a NIP-29 host whose paths serve different groups is left alone`() =
        runBlocking {
            // The other side of the lowered floor, and the reason it is allowed
            // to fold but never to clear. These paths share nothing, so no fold
            // is made — and the tempting next step, signing "each of these is a
            // relay in its own right" for thirty days on the strength of seven
            // ids, is refused. Nothing is published and both urls stay dialled.
            val store = newStore()
            val byUrl = HashMap<String, List<Event>>()
            val up =
                Upstreams(serves = { it == RelayAliases.GROUP_METADATA_KINDS }) { at ->
                    byUrl.getOrPut(at.url) {
                        (0 until 7).map { events.sign(1_700_000_000L - it, 39_000, emptyArray(), "${at.url}#g$it") }
                    }
                }
            val fold = folding(store, up)
            val group = listOf(canonical, alias)

            assertEquals(0, fold.measure("t", group, canDial = { true }))
            assertEquals(group, fold.apply(group).dial, "an undecided host must stay in the fan-out")
            assertEquals(group, fold.apply(group).unmeasured, "and must carry no verdict at all")
        }

    /** A host where every url is reachable and none of them will serve anything. */
    private fun unreadableUpstreams(): Upstreams = Upstreams(serves = { false }) { emptyList() }

    @Test
    fun `a host that answers everywhere and serves nowhere folds onto its survivor`() =
        runBlocking {
            // THE INVERTED DEFAULT. Every url answers — an EOSE or a CLOSED, not
            // silence — and none serves a window through any filter, so nothing
            // distinguishes them and they collapse on the shared host name.
            // Measured live on support.flotilla.social, budabit.nostr1.com and
            // relay.andotherstuff.org, all auth-gated.
            val store = newStore()
            val fold = folding(store, unreadableUpstreams())
            val group = listOf(canonical, alias)

            assertEquals(1, fold.measure("t", group, canDial = { true }))
            assertEquals(listOf(canonical), fold.apply(group).dial, "the survivor is the preferred url")
        }

    @Test
    fun `a url that never spoke keeps its whole group out of the fold`() =
        runBlocking {
            // Our own transport failing is not evidence about their server. One
            // unreachable url makes the group "we do not know", not "all alike",
            // and folding it would publish our outage as a claim about them.
            val store = newStore()
            val up =
                Upstreams(
                    answers = { RelayAliases.pathOf(it.url).isNotEmpty() },
                    serves = { false },
                ) { emptyList() }
            val fold = folding(store, up)
            val group = listOf(canonical, alias)

            assertEquals(0, fold.measure("t", group, canDial = { true }))
            assertEquals(group, fold.apply(group).dial, "a group holding a silent url must stay in the fan-out")
        }

    @Test
    fun `one url that serves anything keeps the empty ones separate`() =
        runBlocking {
            // The haven shape, and the boundary of the whole policy. `/chat` and
            // `/private` answer with nothing while the bare url serves a window —
            // measured on haven.calva.dev, where NIP-11 names them as genuinely
            // different relays. Because SOMETHING on the host answered, the rule
            // does not fire and the empty urls get no verdict at all, which is
            // what keeps them being dialled.
            val store = newStore()
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { at -> if (RelayAliases.pathOf(at.url).isEmpty()) corpus else emptyList() }
            val fold = folding(store, up)
            val group = listOf(canonical, alias)

            fold.measure("t", group, canDial = { true })

            assertEquals(group, fold.apply(group).dial, "an empty path was folded away while the host was readable")
        }

    @Test
    fun `the inverted default can be switched off`() =
        runBlocking {
            val store = newStore()
            val fold = folding(store, unreadableUpstreams(), foldUnreadableGroups = false)
            val group = listOf(canonical, alias)

            assertEquals(0, fold.measure("t", group, canDial = { true }))
            assertEquals(group, fold.apply(group).dial)
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
     * Unreleased, a pass leaves one socket per url it measured — and a pass
     * measures its whole candidate set — against a router whose dispatcher
     * budget is 1024 for the whole process and 20 per HOST. The fold probes widest group
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
    fun `a leader too thin to measure with still decides its own scheme twin`() =
        runBlocking {
            // The other half of that rule. A three-event window can measure
            // nothing, so the group is correctly left alone — but `ws://x` and
            // `wss://x` are not decided by a window at all, and both of them
            // answering is the entire verdict. Left as it was, a small relay
            // reachable on both schemes was permanently undecidable: abandoned
            // here at the front of every pass, for a fold that costs one dial.
            val store = newStore()
            val secure = RelayUrlNormalizer.normalize("wss://quiet.example")
            val plain = RelayUrlNormalizer.normalize("ws://quiet.example")
            val other = RelayUrlNormalizer.normalize("wss://quiet.example/alpha")
            val thin: List<Event> = (0 until 3).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "t$it") }
            val up = Upstreams { thin }

            assertEquals(1, folding(store, up).measure("t", listOf(secure, plain, other), canDial = { true }))

            // The twin, and nothing else: the third url still has nothing it
            // could be measured against.
            assertEquals(setOf(secure, plain), up.contacted)
            // Signed, so the next boot and every other router reading this
            // monitor's records fold it without paying for the dials again.
            val reader = folding(store, upstreams()).apply(listOf(secure, plain, other))
            assertEquals(mapOf(plain to secure), reader.aliases)
            assertEquals(listOf(secure, other), reader.dial)
        }

    @Test
    fun `the fold published for a scheme twin quotes the pairing, not a containment`() =
        runBlocking {
            // These pairs are folded precisely where the windows could not
            // decide, so a "9 shared" beside the `same-as` would offer as the
            // reason a number the verdict was never based on — in a signed,
            // month-long statement about somebody else's server.
            val store = newStore()
            val secure = RelayUrlNormalizer.normalize("wss://quiet.example")
            val plain = RelayUrlNormalizer.normalize("ws://quiet.example")
            val thin: List<Event> = (0 until 3).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "t$it") }

            folding(store, Upstreams { thin }).measure("t", listOf(secure, plain), canDial = { true })

            val record =
                store
                    .query<Event>(Filter(kinds = listOf(30166), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(plain.url))))
                    .single()
            val sameAs = record.tags.single { it[0] == RelayAliasRecord.SAME_AS_TAG }
            assertEquals(secure.url, sameAs[1])
            assertTrue(sameAs[2].startsWith("same endpoint as ${secure.url} over TLS, both answered"), "the evidence reads: ${sameAs[2]}")
        }

    @Test
    fun `a ws url serving what its wss twin does not is left in the fan-out`() =
        runBlocking {
            // "Both work, keep the secure one" is not "prefer wss whatever it
            // serves". A plain url holding events its secure twin never returned
            // is a url that cannot be folded away without losing them, and the
            // fold's whole failure mode is silently ceasing to mirror a relay
            // nobody will notice is missing.
            val store = newStore()
            val secure = RelayUrlNormalizer.normalize("wss://lopsided.example")
            val plain = RelayUrlNormalizer.normalize("ws://lopsided.example")
            val full: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { at -> if (at.url.startsWith("wss://")) emptyList() else full }

            assertEquals(0, folding(store, up).measure("t", listOf(secure, plain), canDial = { true }))
            assertEquals(listOf(secure, plain), folding(store, upstreams()).apply(listOf(secure, plain)).dial)
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
     * A host that never answers: our transport gives up and no page comes back,
     * so nothing can be decided about it and a pass writes NOTHING down.
     *
     * **Not the same fixture as [unreadableUpstreams], and the tests below turn
     * on the difference.** This one returns NULL — "we do not know" — while the
     * other ANSWERS and serves nothing, which since
     * [AliasFolding.DEFAULT_FOLD_UNREADABLE_GROUPS] is a fold. It used to be one
     * fixture returning an empty list for both jobs, and the policy inversion is
     * what made that ambiguity untenable: the cooldown and the bounded yardstick
     * walk belong to silence alone now.
     */
    private fun silentUpstreams(): Upstreams = Upstreams(answers = { false }) { emptyList() }

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
            val up =
                Upstreams(answers = { RelayAliases.hostOf(it.url) != "silent.example" }) { corpus }
            val fold = folding(store, up)
            val urls = listOf(quietHost, quietAlias, canonical, alias)

            assertEquals(1, fold.measure("t", urls, canDial = { true }), "the foldable host was not measured")
            // …and on the pass after, the silent host costs nothing at all while
            // the fold that was already earned still stands.
            assertEquals(listOf(quietHost, quietAlias, canonical), fold.apply(urls).dial)
        }

    @Test
    fun `a host whose preferred survivor will not answer still folds onto one that will`() =
        runBlocking {
            // AS REPORTED, and the shape both reports had in common: the group's
            // preferred leader is the pathless url, and that is the one url on
            // the host that says nothing. Every other url serves the identical
            // window — measured on `asia.azzamo.net`, twelve urls all at
            // containment 1.000 — so the host is trivially foldable and was
            // abandoned whole, every pass, because one member of it was mute.
            val store = newStore()
            val bare = RelayUrlNormalizer.normalize("wss://paths.example")
            val first = RelayUrlNormalizer.normalize("wss://paths.example/kilo-yonder")
            val second = RelayUrlNormalizer.normalize("wss://paths.example/xray-alpha-jade")
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { at -> if (RelayAliases.pathOf(at.url).isEmpty()) emptyList() else corpus }
            val fold = folding(store, up)
            val urls = listOf(bare, first, second)

            assertEquals(1, fold.measure("t", urls, canDial = { true }), "the group was abandoned on its silent leader")

            // The survivor is the best url that could actually be MEASURED, not
            // the best url in the abstract: the bare one stays in the fan-out
            // because nothing was ever proved about it, which is the correct
            // reading of silence.
            assertEquals(listOf(bare, first), fold.apply(urls).dial)
            assertEquals(mapOf(second to first), fold.apply(urls).aliases)
        }

    @Test
    fun `a url the transport declined mid-search is still measured as a member`() =
        runBlocking {
            // What the yardstick search SKIPS is not what it exhausts, and
            // conflating the two costs a url its measurement.
            //
            // The distinction is invisible while the transport guard answers the
            // same way twice — a url it refuses as a yardstick it also refuses
            // as a member, so dropping it changes nothing. It bites when the
            // guard RECOVERS: `canDial` is `tor.socksAnswers() && tcpReachable`,
            // both of which are live checks, so a circuit that comes back
            // between the search and the member walk is ordinary. Marked
            // exhausted on the refusal, that url is skipped for the rest of the
            // pass on OUR outage rather than on its own behaviour.
            val store = newStore()
            val flaky = RelayUrlNormalizer.normalize("wss://mixed.example")
            val first = RelayUrlNormalizer.normalize("wss://mixed.example/alpha")
            val second = RelayUrlNormalizer.normalize("wss://mixed.example/beacon")
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { corpus }
            val fold = folding(store, up)
            // Refused once — the yardstick attempt, which is the pathless url
            // PREFERENCE puts first — and reachable from then on.
            val refusals = AtomicInteger()
            val canDial: suspend (NormalizedRelayUrl) -> Boolean = { url ->
                url != flaky || refusals.getAndIncrement() > 0
            }

            val learned = fold.measure("t", listOf(flaky, first, second), canDial = canDial)

            assertEquals(2, learned, "the url refused during the search was never re-asked once the transport recovered")
            assertTrue(flaky in up.contacted, "it was dropped from the member walk rather than dialled")
            assertEquals(listOf(first), fold.apply(listOf(flaky, first, second)).dial)
        }

    @Test
    fun `the yardstick search is bounded and never re-asks a url it has already tried`() =
        runBlocking {
            // The one place a dead url delays another dial rather than merely
            // holding a permit, so the walk stops at YARDSTICK_ATTEMPTS — and a
            // url that failed BOTH filters as a yardstick is not asked a third
            // time as a member.
            val store = newStore()
            val host = "wss://mute.example"
            val urls = (listOf(host) + (0 until 5).map { "$host/p$it" }).map { RelayUrlNormalizer.normalize(it) }
            val up = silentUpstreams()

            assertEquals(0, folding(store, up).measure("t", urls, canDial = { true }))

            // Three urls, and `leaderPrint` asks each of them twice — the bare
            // filter, then the kinds fallback — with the empty-page retry inside
            // each. What must not happen is a fourth url being dialled, or one
            // of the three coming round again as a member.
            assertEquals(
                AliasFolding.YARDSTICK_ATTEMPTS,
                up.contacted.size,
                "the search went past its cap, or re-asked a url it had already given up on",
            )
        }

    @Test
    fun `paths that duplicate each other fold even when the group's leader is a different endpoint`() =
        runBlocking {
            // MEASURED LIVE, and the reason this exists: `haven.calva.dev` wears
            // `/inbox` plus six minted paths. `/inbox` is a genuinely different
            // endpoint AND the shortest url, so it leads the group; all six
            // disagree with it correctly and were each signed as a relay in their
            // own right for thirty days. Probed against each other they are ONE
            // relay at containment 1.000 — seven dials for two endpoints.
            val store = newStore()
            val inbox = RelayUrlNormalizer.normalize("wss://haven.example/inbox")
            val paths =
                listOf("dynamo", "vertex", "victor-tango")
                    .map { RelayUrlNormalizer.normalize("wss://haven.example/$it") }
            val pool: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "pool$it") }
            val mail: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 4, emptyArray(), "mail$it") }
            val up = Upstreams { at -> if (RelayAliases.pathOf(at.url) == "inbox") mail else pool }
            val fold = folding(store, up)
            val urls = listOf(inbox) + paths

            // `/inbox` leads: shortest url on the host.
            assertEquals(inbox, RelayAliases().toProbe(urls.sortedWith(compareBy { it.url.length })).first())
            assertEquals(2, fold.measure("t", urls, canDial = { true }), "the minted paths were never compared to each other")

            // Two endpoints, two dials — not four. The inbox keeps its own place;
            // it really is a different relay and nothing here says otherwise.
            val cleaned = fold.apply(urls)
            assertEquals(listOf(inbox, paths.first()), cleaned.dial)
            assertEquals(paths.drop(1).associateWith { paths.first() }, cleaned.aliases)
        }

    @Test
    fun `a host of genuinely distinct endpoints keeps every one of them`() =
        runBlocking {
            // The other side of the same change, and the one a clustering bug
            // would break silently: `lang.relays.land` partitions by language and
            // `nostr.ac` serves 20 paths of different content. Comparing members
            // to each other must not collapse them — every url here serves its
            // own events, so every url has to survive.
            val store = newStore()
            val urls =
                listOf("de", "fr", "ja", "la").map { RelayUrlNormalizer.normalize("wss://lang.example/$it") }
            val up =
                Upstreams { at ->
                    val tag = RelayAliases.pathOf(at.url)
                    (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "$tag-$it") }
                }
            val fold = folding(store, up)

            assertEquals(0, fold.measure("t", urls, canDial = { true }), "distinct endpoints were folded together")
            assertEquals(urls, fold.apply(urls).dial)
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
