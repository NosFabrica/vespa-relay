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
 * [AliasFolding.apply] runs in front of every fan-out and must never dial;
 * [AliasFolding.measure] runs on the monitor's clock and may. Most tests count dials.
 */
class AliasFoldingTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val canonical = RelayUrlNormalizer.normalize("wss://nos.lol")
    private val alias = RelayUrlNormalizer.normalize("wss://nos.lol/cipher-zulu")
    private val elsewhere = RelayUrlNormalizer.normalize("wss://nostr.mom")
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /** A relay set that answers per url and counts every dial. */
    private class Upstreams(
        /** False is our transport giving up (a null page), a different fact from a relay that answers and serves nothing. */
        private val answers: (NormalizedRelayUrl) -> Boolean = { true },
        /** Which filters get a window; a refusal reaches the walk as an empty page, not silence. */
        private val serves: (List<Int>?) -> Boolean = { true },
        /** Which urls turn our credentials down: an answer, and a different one. */
        private val refuses: (NormalizedRelayUrl) -> Boolean = { false },
        private val corpusFor: (NormalizedRelayUrl) -> List<Event>,
    ) {
        val dials = AtomicInteger()

        /** Which urls were contacted at all; one url may cost several asks. */
        val contacted: MutableSet<NormalizedRelayUrl> =
            java.util.concurrent.ConcurrentHashMap
                .newKeySet()

        suspend fun fetch(
            at: NormalizedRelayUrl,
            want: Int,
            until: Long?,
            kinds: List<Int>?,
        ): AliasProbe.Page {
            dials.incrementAndGet()
            contacted += at
            if (!answers(at)) return AliasProbe.Page(null)
            if (refuses(at)) return AliasProbe.Page(emptyList(), authRefused = true)
            if (!serves(kinds)) return AliasProbe.Page(emptyList())
            return AliasProbe.Page(corpusFor(at).filter { until == null || it.createdAt <= until }.take(want))
        }
    }

    /** A store that refuses every read: a query that failed is not a store saying "no verdict". */
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
        record = RelayVerdictRecord(store, signer),
        probe = AliasProbe(fetch = upstreams::fetch, target = 40, page = 40, fallbackPage = 40),
        undecidableCooldownMs = undecidableCooldownMs,
        foldUnreadableGroups = foldUnreadableGroups,
    )

    /** Every url serves the same 40 events, so any two of them fold. */
    private fun upstreams(): Upstreams {
        val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
        return Upstreams { corpus }
    }

    /** Every url serves its own 40 events, so nothing folds: a host whose paths are real relays. */
    private fun distinctUpstreams(): Upstreams {
        val byUrl = HashMap<String, List<Event>>()
        return Upstreams { at ->
            byUrl.getOrPut(at.url) {
                (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "${at.url}#$it") }
            }
        }
    }

    /** A NIP-29 relay: refuses every general query and serves the same short group list on every path. */
    private fun groupsUpstreams(groups: Int = 7): Upstreams {
        val corpus: List<Event> = (0 until groups).map { events.sign(1_700_000_000L - it, 39_000, emptyArray(), "g$it") }
        return Upstreams(serves = { it == RelayAliases.GROUP_METADATA_KINDS }) { corpus }
    }

    @Test
    fun `a NIP-29 host folds on the one window a general filter cannot reach`() =
        runBlocking {
            // Seven groups on purpose: under DEFAULT_MIN_SAMPLE, so the rung alone
            // recovers nothing and the floor has to follow the filter too.
            val store = newStore()
            val fold = folding(store, groupsUpstreams(groups = 7))
            val group = listOf(canonical, alias)

            assertEquals(1, fold.measure("t", group, canDial = { true }), "the group list is a perfectly good fingerprint")
            // Read back through the store: the next cycle's apply() must dial one url, not two.
            assertEquals(listOf(canonical), fold.applyVerdicts(group).dial)
        }

    @Test
    fun `a NIP-29 host whose paths serve different groups is left alone`() =
        runBlocking {
            // The lowered floor may fold but never clear: seven ids cannot carry a
            // signed "relay in its own right", so nothing is published and both stay dialled.
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
            assertEquals(group, fold.applyVerdicts(group).dial, "an undecided host must stay in the fan-out")
            assertEquals(group, fold.applyVerdicts(group).unmeasured, "and must carry no verdict at all")
        }

    /** A host where every url is reachable and none of them will serve anything. */
    private fun unreadableUpstreams(): Upstreams = Upstreams(serves = { false }) { emptyList() }

    @Test
    fun `a host that answers everywhere and serves nowhere folds onto its survivor`() =
        runBlocking {
            // Every url answers (an EOSE or a CLOSED, not silence) and none serves a
            // window through any filter, so they collapse on the shared host name.
            val store = newStore()
            val fold = folding(store, unreadableUpstreams())
            val group = listOf(canonical, alias)

            assertEquals(1, fold.measure("t", group, canDial = { true }))
            assertEquals(listOf(canonical), fold.applyVerdicts(group).dial, "the survivor is the preferred url")
        }

    @Test
    fun `a hundred unreadable urls collapse to one, and the dials are counted`() =
        runBlocking {
            // Each url is asked the whole ladder, since an empty EOSE is not a refusal
            // and cannot end it; 3 asks per url pins that, and catches a fourth rung
            // or the sweep re-asking a url the yardstick walk already tried.
            val store = newStore()
            val up = unreadableUpstreams()
            val fold = folding(store, up)
            val host = "wss://nwc.example"
            val urls = (listOf(host) + (0 until 99).map { "$host/p$it" }).map { RelayUrlNormalizer.normalize(it) }

            assertEquals(99, fold.measure("t", urls, canDial = { true }), "the whole host must collapse")
            assertEquals(listOf(urls.first()), fold.applyVerdicts(urls).dial, "100 urls, one dial")
            assertEquals(100, up.contacted.size, "every url has to answer before the group may fold")
            assertEquals(
                300,
                up.dials.get(),
                "the ladder is 3 rungs and no url may be asked twice — got ${up.dials.get()} asks for 100 urls",
            )
        }

    @Test
    fun `a window found past the third attempt is adopted, not thrown away`() =
        runBlocking {
            // The sweep dials the urls the yardstick walk never reached, and a window
            // one of those serves is a measurement, which beats the shared-name default.
            val store = newStore()
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            // The first three in preference order answer with nothing; the last two serve one window.
            val quiet = setOf("", "/a", "/b")
            val up =
                Upstreams(serves = { true }) { at ->
                    if (RelayAliases.pathOf(at.url).let { p -> quiet.contains(if (p.isEmpty()) "" else "/$p") }) emptyList() else corpus
                }
            val fold = folding(store, up)
            val host = "wss://late.example"
            val urls = listOf(host, "$host/a", "$host/b", "$host/c", "$host/d").map { RelayUrlNormalizer.normalize(it) }

            val learned = fold.measure("t", urls, canDial = { true })

            // A real fold, not the shared-name default: /d folds onto /c on matching windows.
            assertEquals(1, learned, "the window the sweep turned up was discarded")
            val dial = fold.applyVerdicts(urls).dial
            assertTrue(RelayUrlNormalizer.normalize("$host/d") !in dial, "the measured duplicate is still being dialled")
            assertTrue(RelayUrlNormalizer.normalize("$host/c") in dial, "the survivor must be the url that answered")
        }

    @Test
    fun `the sweep adopts a window it can measure with, not merely the first one`() =
        runBlocking {
            // /c hands over five ids and wins on preference order; /d and /e serve an
            // identical 40 and are the only pair that can fold.
            val store = newStore()
            val thin: List<Event> = (0 until 5).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "thin$it") }
            val full: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "full$it") }
            val up =
                Upstreams { at ->
                    when (RelayAliases.pathOf(at.url)) {
                        "", "a", "b" -> emptyList()
                        "c" -> thin
                        else -> full
                    }
                }
            val fold = folding(store, up)
            val host = "wss://mixed.example"
            val urls = listOf(host, "$host/a", "$host/b", "$host/c", "$host/d", "$host/e").map { RelayUrlNormalizer.normalize(it) }

            assertEquals(1, fold.measure("t", urls, canDial = { true }), "a thin window beat a usable one")
            assertTrue(
                RelayUrlNormalizer.normalize("$host/e") !in fold.applyVerdicts(urls).dial,
                "the two urls serving an identical window were left unfolded",
            )
        }

    @Test
    fun `a group where only a thin window came back is not folded on its name`() =
        runBlocking {
            // A thin-only host leaves `found` null; that must not drop it into the
            // shared-name default, since anything served at all disqualifies it.
            val store = newStore()
            val thin: List<Event> = (0 until 5).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "thin$it") }
            val up = Upstreams { at -> if (RelayAliases.pathOf(at.url) == "c") thin else emptyList() }
            val fold = folding(store, up)
            val host = "wss://thinonly.example"
            val urls = listOf(host, "$host/a", "$host/b", "$host/c").map { RelayUrlNormalizer.normalize(it) }

            assertEquals(0, fold.measure("t", urls, canDial = { true }))
            assertEquals(urls, fold.applyVerdicts(urls).dial, "a host that served a window was folded on its name")
        }

    @Test
    fun `a url that never spoke keeps its whole group out of the fold`() =
        runBlocking {
            // One unreachable url makes the group "we do not know", not "all alike".
            val store = newStore()
            val up =
                Upstreams(
                    answers = { RelayAliases.pathOf(it.url).isNotEmpty() },
                    serves = { false },
                ) { emptyList() }
            val fold = folding(store, up)
            val group = listOf(canonical, alias)

            assertEquals(0, fold.measure("t", group, canDial = { true }))
            assertEquals(group, fold.applyVerdicts(group).dial, "a group holding a silent url must stay in the fan-out")
        }

    @Test
    fun `one url that serves anything keeps the empty ones separate`() =
        runBlocking {
            // Something on the host served, so the rule does not fire and the empty
            // urls get no verdict at all, which is what keeps them dialled.
            val store = newStore()
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { at -> if (RelayAliases.pathOf(at.url).isEmpty()) corpus else emptyList() }
            val fold = folding(store, up)
            val group = listOf(canonical, alias)

            fold.measure("t", group, canDial = { true })

            assertEquals(group, fold.applyVerdicts(group).dial, "an empty path was folded away while the host was readable")
        }

    @Test
    fun `urls that answered DIFFERENTLY are still folded together`() =
        runBlocking {
            // Documents a limit rather than asserting it is right: the rule counts a
            // credential refusal and an empty EOSE as the same answer. If it is ever
            // tightened to demand the same kind of answer, this test inverts.
            val store = newStore()
            val up =
                Upstreams(
                    serves = { false },
                    refuses = { RelayAliases.pathOf(it.url).isNotEmpty() },
                ) { emptyList() }
            val fold = folding(store, up)
            val group = listOf(canonical, alias)

            assertEquals(1, fold.measure("t", group, canDial = { true }))
            assertEquals(listOf(canonical), fold.applyVerdicts(group).dial, "current behaviour: a refusal and an EOSE fold together")
        }

    @Test
    fun `the inverted default can be switched off`() =
        runBlocking {
            val store = newStore()
            val fold = folding(store, unreadableUpstreams(), foldUnreadableGroups = false)
            val group = listOf(canonical, alias)

            assertEquals(0, fold.measure("t", group, canDial = { true }))
            assertEquals(group, fold.applyVerdicts(group).dial)
        }

    @Test
    fun `apply never dials, however much there is to learn`() =
        runBlocking {
            val up = upstreams()
            val cleaned = folding(newStore(), up).applyVerdicts(listOf(canonical, alias))

            assertEquals(0, up.dials.get(), "apply() opened ${up.dials.get()} socket(s); it runs on the cycle's critical path")
            // The only safe reading of "no verdict" is "dial it as it stands".
            assertEquals(listOf(canonical, alias), cleaned.dial)
            assertTrue(cleaned.aliases.isEmpty())
        }

    @Test
    fun `a url is dialled unfolded exactly once, then folds from the store`() =
        runBlocking {
            // The first cycle to see a url cannot fold it; the measurement happens between the two.
            val store = newStore()
            val up = upstreams()
            val fold = folding(store, up)

            assertEquals(2, fold.applyVerdicts(listOf(canonical, alias)).dial.size)
            assertEquals(1, fold.measure("t", listOf(canonical, alias), canDial = { true }))
            assertEquals(listOf(canonical), fold.applyVerdicts(listOf(canonical, alias)).dial)
        }

    @Test
    fun `a second process reads the verdict without re-probing`() =
        runBlocking {
            // The two halves talk through the store alone; a fresh RelayAliases is a router that just booted.
            val store = newStore()
            val prober = upstreams()
            folding(store, prober).measure("t", listOf(canonical, alias), canDial = { true })

            val reader = upstreams()
            val cleaned = folding(store, reader).applyVerdicts(listOf(canonical, alias))

            assertEquals(listOf(canonical), cleaned.dial)
            assertEquals(mapOf(alias to canonical), cleaned.aliases)
            assertEquals(0, reader.dials.get(), "the reader re-probed what the store already knew")
        }

    @Test
    fun `a url that arrives alone on a measured host folds against the stored world`() =
        runBlocking {
            // Grouped from the candidates alone the newcomer is a group of one; the
            // host's history is in the store and has to be read.
            val store = newStore()
            folding(store, upstreams()).measure("t", listOf(canonical, alias), canDial = { true })

            // A fresh RelayAliases: the host's history exists only in the store.
            val newcomer = RelayUrlNormalizer.normalize("wss://nos.lol/delta-new")
            val up = upstreams()
            val fold = folding(store, up)

            assertEquals(1, fold.measure("t", listOf(newcomer), canDial = { true }), "the newcomer had a canonical to measure against")
            // A fold is applied only where its survivor is in the set, see `collapse`.
            assertEquals(
                listOf(canonical),
                fold.applyVerdicts(listOf(canonical, newcomer)).dial,
                "one new url, measured against the host's history, and the fan-out dials the survivor",
            )
            assertEquals(mapOf(newcomer to canonical), fold.applyVerdicts(listOf(canonical, newcomer)).aliases)
            // The urls pulled in for context are the yardstick: the leader is dialled, the settled alias is not.
            assertTrue(canonical in up.contacted, "the survivor is the yardstick and has to answer for itself")
            assertTrue(alias !in up.contacted, "a url that already carries a verdict must not cost a dial")
        }

    @Test
    fun `a fold whose survivor is not in the set is not applied to it`() =
        runBlocking {
            // Every consumer applies the map by dropping the alias and none puts the
            // canonical back, so a fold whose survivor is absent would empty the fan-out.
            val store = newStore()
            folding(store, upstreams()).measure("t", listOf(canonical, alias), canDial = { true })

            val fold = folding(store, upstreams())
            // The survivor is present, so the fold applies.
            assertEquals(listOf(canonical), fold.applyVerdicts(listOf(canonical, alias)).dial)

            // The alias alone: the canonical held out as dead, or gone from the relay list.
            val stranded = fold.applyVerdicts(listOf(alias))
            assertEquals(listOf(alias), stranded.dial, "the only live address we have must still be dialled")
            assertTrue(stranded.aliases.isEmpty(), "a drop-only consumer would take the relay out of the fan-out")
            assertTrue(stranded.unmeasured.isEmpty(), "…and it is still a MEASURED url: nothing here needs re-probing")
        }

    @Test
    fun `a group whose survivor is held out folds onto the best url still present`() =
        runBlocking {
            // The bare host went dead after its paths folded onto it. Unfolding is
            // right for a group of one and wrong for a group of four.
            val store = newStore()
            val host = "wss://typedcypher.example"
            val root = RelayUrlNormalizer.normalize(host)
            // Not in preference order, so the assertion below is about the order.
            val paths = listOf("$host/tango-jade", "$host/onyx", "$host/ember-november").map { RelayUrlNormalizer.normalize(it) }
            val fold = folding(store, upstreams())

            fold.measure("t", listOf(root) + paths, canDial = { true })
            assertEquals(
                paths.associateWith { root },
                fold.applyVerdicts(listOf(root) + paths).aliases,
                "the fixture needs all three paths folded onto the bare host",
            )

            // The next sweep, with the survivor held out as dead.
            val stranded = fold.applyVerdicts(paths)
            val survivor = RelayUrlNormalizer.normalize("$host/onyx")
            assertEquals(listOf(survivor), stranded.dial, "one server is still one dial, and the shortest path wins it")
            assertEquals(
                paths.filter { it != survivor }.associateWith { survivor },
                stranded.standIns,
                "the others stand in on it rather than each becoming a relay of their own",
            )
            assertTrue(stranded.unmeasured.isEmpty(), "every one of them is measured — re-election is not a reason to re-probe")

            // `FitnessPass` signs an `l=alias` record for every entry in `aliases`, and
            // these paths were never measured against each other. Routing may infer;
            // publishing may not.
            assertTrue(stranded.aliases.isEmpty(), "an inferred pairing must never reach the map a verdict is signed from")

            // The stand-in is routing only: the records still name the bare host, so it
            // is picked back up with no re-probe and nothing to retract.
            val recovered = fold.applyVerdicts(listOf(root) + paths)
            assertEquals(
                paths.associateWith { root },
                recovered.aliases,
                "the stored fold is untouched and the survivor is picked straight back up",
            )
            assertTrue(recovered.standIns.isEmpty(), "a survivor that is present needs no stand-in")
        }

    @Test
    fun `a host that cannot repeat itself leaves the store's other verdicts alone`() =
        runBlocking {
            // A guard that forgets the whole group also drops the verdicts adopted
            // from the store moments earlier, still perfectly good.
            val store = newStore()
            val host = "wss://shuffles.example"
            val settled = listOf("$host/a", "$host/b").map { RelayUrlNormalizer.normalize(it) }
            // One pass on a host that behaves, so the store holds a real verdict.
            folding(store, upstreams()).measure("t", settled, canDial = { true })
            assertEquals(1, folding(store, upstreams()).applyVerdicts(settled).aliases.size, "the fixture needs a stored fold")

            // The same host now serves a different window on every ask, which
            // `reproducible` refuses to publish a negative claim against.
            val shuffling = AtomicInteger()
            val up =
                Upstreams { _ ->
                    val run = shuffling.getAndIncrement()
                    (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "run$run-e$it") }
                }
            val processors = Processors()
            val fold =
                AliasFolding(
                    aliases = RelayAliases(),
                    record = RelayVerdictRecord(store, signer),
                    probe = AliasProbe(fetch = up::fetch, target = 40, page = 40, fallbackPage = 40),
                    progress = processors.of("aliasFold"),
                )
            val newcomer = RelayUrlNormalizer.normalize("$host/c")
            fold.measure("t", settled + newcomer, canDial = { true })

            // The group was abandoned; what the store says about it survives.
            val after = fold.applyVerdicts(settled + newcomer)
            assertEquals(1, after.aliases.size, "an adopted verdict is the store's, not this pass's to drop")
            val work =
                processors
                    .snapshot()
                    .single()
                    .work
                    .single()
            assertTrue(
                work.unmeasured <= (work.newUrls ?: 0),
                "a pass cannot leave more urls undecided than arrived so — got ${work.unmeasured} of ${work.newUrls}",
            )
        }

    @Test
    fun `the pass counts the urls that arrived undecided, not the whole candidate set`() =
        runBlocking {
            // [Processors.Work.newUrls] is what the card draws its position against;
            // counted against `candidates`, a settled pass read as fully checked.
            val store = newStore()
            val processors = Processors()
            val handle = processors.of("aliasFold")
            val group = listOf(canonical, alias)
            val fold =
                AliasFolding(
                    aliases = RelayAliases(),
                    record = RelayVerdictRecord(store, signer),
                    probe = AliasProbe(fetch = upstreams()::fetch, target = 40, page = 40, fallbackPage = 40),
                    progress = handle,
                )

            fold.measure("t", group, canDial = { true })
            assertEquals(
                2,
                processors
                    .snapshot()
                    .single()
                    .work
                    .single()
                    .newUrls,
                "both urls arrived with no verdict",
            )

            fold.measure("t", group, canDial = { true })
            assertEquals(
                0,
                processors
                    .snapshot()
                    .single()
                    .work
                    .single()
                    .newUrls,
                "a settled host arrives with nothing new",
            )
        }

    /** `adopt` forgets every verdict before re-adopting from the store, so a failed read must arrive as a failure. */
    @Test
    fun `a store that cannot be read leaves the verdicts already held alone`() =
        runBlocking {
            val store = newStore()
            val aliases = RelayAliases()
            val fold = folding(store, upstreams(), aliases)
            assertEquals(1, fold.measure("t", listOf(canonical, alias), canDial = { true }))
            assertEquals(listOf(canonical), fold.applyVerdicts(listOf(canonical, alias)).dial)

            // The same verdicts in memory, in front of a store that has stopped answering.
            val blind =
                AliasFolding(
                    aliases = aliases,
                    record = RelayVerdictRecord(Unreadable(store), signer),
                    probe = AliasProbe(fetch = upstreams()::fetch, target = 40, page = 40, fallbackPage = 40),
                )

            assertEquals(listOf(canonical), blind.applyVerdicts(listOf(canonical, alias)).dial, "a failed read unfolded the fan-out")
        }

    @Test
    fun `measure honours the caller's transport guard`() =
        runBlocking {
            val up = upstreams()
            // The leader is refused, so nothing in its group can be compared.
            val learned = folding(newStore(), up).measure("t", listOf(canonical, alias), canDial = { false })

            assertEquals(0, learned)
            assertEquals(0, up.dials.get())
        }

    /**
     * A fingerprint is a websocket quartz never closes, so every claim the pass
     * makes must be released before it returns. Balance, not order.
     */
    @Test
    fun `every url the pass dials is handed back to the stream's refcount`() =
        runBlocking {
            val held = ConcurrentHashMap<NormalizedRelayUrl, Int>()
            val claims = AtomicInteger()
            val sockets =
                object : Sockets {
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

    /** The guard runs before the claim, or a refused url holds a refcount it can never release. */
    @Test
    fun `a url the transport guard refuses is never claimed`() =
        runBlocking {
            val held = ConcurrentHashMap<NormalizedRelayUrl, Int>()
            val sockets =
                object : Sockets {
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
     * A relay whose window changes on every ask lands on either side of the fold
     * bar by chance, so neither a fold nor a signed "distinct" may be published.
     * The corpus rotates per dial, so the second leader walk cannot match the first.
     */
    @Test
    fun `a relay that cannot reproduce its own window publishes nothing`() =
        runBlocking {
            val store = newStore()
            val a = RelayUrlNormalizer.normalize("wss://shuffling.example")
            val b = RelayUrlNormalizer.normalize("wss://shuffling.example/ember")
            // A fresh, disjoint 40 events on every dial, so no walk can agree with another.
            val served = AtomicInteger()
            val shuffling =
                Upstreams {
                    val n = served.getAndIncrement()
                    (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "dial$n-e$it") }
                }
            val fold = folding(store, shuffling, RelayAliases())

            assertEquals(0, fold.measure("t", listOf(a, b), canDial = { true }), "nothing may be folded on this evidence")

            // The negative claim: two urls of one host cleared as separate relays for 30 days.
            val held = RelayVerdictRecord(store, signer).load(listOf(a, b))
            assertTrue(held.aliases.isEmpty(), "a fold was published against a yardstick that cannot repeat itself")
            assertTrue(held.distinct.isEmpty(), "published ${held.distinct.size} url(s) as their own relay on unreproducible evidence")
            // Nothing is held in memory either, so the next pass re-measures rather than resuming from half a verdict.
            assertEquals(listOf(a, b), fold.applyVerdicts(listOf(a, b)).dial)
        }

    @Test
    fun `a second process does not re-probe urls a previous pass cleared`() =
        runBlocking {
            // Two paths that are not duplicates; without a stored cleared verdict every boot re-fingerprints them.
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
            // The leader everything folded onto has a verdict but is nobody's alias and
            // nobody's singleton; `unresolved` must not hand it back on that basis.
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
            // A url with no verdict leaves the group unresolved however many neighbours are settled.
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
            // A pass cannot be killed from inside a test; what makes a kill survivable
            // is that the first group's verdict is readable while the second still probes.
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
            val reader = RelayVerdictRecord(store, signer)
            try {
                withTimeout(10_000) {
                    while (reader.load(listOf(fast, fastAlias)).aliases.isEmpty()) delay(20)
                }
                // Readable, and the pass has not finished.
                assertTrue(pass.isActive, "the pass ended before the assertion, so this proves nothing")
                assertEquals(mapOf(fastAlias to fast), reader.load(listOf(fast, fastAlias)).aliases)
            } finally {
                // Unconditionally: a failed assertion would otherwise park the pass
                // forever and `runBlocking` would hang the suite instead of failing it.
                release.complete(Unit)
                pass.join()
            }
        }

    @Test
    fun `a leader too thin to be a yardstick does not drag its group onto the wire`() =
        runBlocking {
            // Three ids is under minSample: nothing can fold onto the leader or be
            // cleared against it, so dialling its members decides nothing.
            val store = newStore()
            val a = RelayUrlNormalizer.normalize("wss://quiet.example")
            val b = RelayUrlNormalizer.normalize("wss://quiet.example/alpha")
            val c = RelayUrlNormalizer.normalize("wss://quiet.example/beacon")
            val thin: List<Event> = (0 until 3).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "t$it") }
            val up = Upstreams { thin }

            folding(store, up).measure("t", listOf(a, b, c), canDial = { true })

            // The leader still costs one probe per pass, the price of noticing it has recovered.
            assertEquals(setOf(a), up.contacted, "members were dialled behind a leader that could decide nothing")
        }

    @Test
    fun `a leader too thin to measure with still decides its own scheme twin`() =
        runBlocking {
            // A three-event window measures nothing, but `ws://x` and `wss://x` are
            // decided by both answering, not by a window.
            val store = newStore()
            val secure = RelayUrlNormalizer.normalize("wss://quiet.example")
            val plain = RelayUrlNormalizer.normalize("ws://quiet.example")
            val other = RelayUrlNormalizer.normalize("wss://quiet.example/alpha")
            val thin: List<Event> = (0 until 3).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "t$it") }
            val up = Upstreams { thin }

            assertEquals(1, folding(store, up).measure("t", listOf(secure, plain, other), canDial = { true }))

            // The twin and nothing else: the third url still has nothing to be measured against.
            assertEquals(setOf(secure, plain), up.contacted)
            // Signed, so every router reading this monitor's records folds it without the dials.
            val reader = folding(store, upstreams()).applyVerdicts(listOf(secure, plain, other))
            assertEquals(mapOf(plain to secure), reader.aliases)
            assertEquals(listOf(secure, other), reader.dial)
        }

    @Test
    fun `the fold published for a scheme twin quotes the pairing, not a containment`() =
        runBlocking {
            // The pairing was decided where the windows could not, so a shared count
            // would quote a number the verdict never rested on.
            val store = newStore()
            val secure = RelayUrlNormalizer.normalize("wss://quiet.example")
            val plain = RelayUrlNormalizer.normalize("ws://quiet.example")
            val thin: List<Event> = (0 until 3).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "t$it") }

            folding(store, Upstreams { thin }).measure("t", listOf(secure, plain), canDial = { true })

            val record =
                store
                    .query<Event>(Filter(kinds = listOf(30166), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(plain.url))))
                    .single()
            val sameAs = record.tags.single { it[0] == RelayVerdictRecord.SAME_AS_TAG }
            assertEquals(secure.url, sameAs[1])
            assertTrue(sameAs[2].startsWith("same endpoint as ${secure.url} over TLS, both answered"), "the evidence reads: ${sameAs[2]}")
        }

    @Test
    fun `a ws url serving what its wss twin does not is left in the fan-out`() =
        runBlocking {
            // A plain url holding events its secure twin never returned cannot be folded away without losing them.
            val store = newStore()
            val secure = RelayUrlNormalizer.normalize("wss://lopsided.example")
            val plain = RelayUrlNormalizer.normalize("ws://lopsided.example")
            val full: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { at -> if (at.url.startsWith("wss://")) emptyList() else full }

            assertEquals(0, folding(store, up).measure("t", listOf(secure, plain), canDial = { true }))
            assertEquals(listOf(secure, plain), folding(store, upstreams()).applyVerdicts(listOf(secure, plain)).dial)
        }

    @Test
    fun `hosts are never compared across domains`() =
        runBlocking {
            // Two relays serving identical events; the fold groups by hostname first.
            val store = newStore()
            val fold = folding(store, upstreams())

            assertEquals(0, fold.measure("t", listOf(canonical, elsewhere), canDial = { true }))
            assertEquals(listOf(canonical, elsewhere), fold.applyVerdicts(listOf(canonical, elsewhere)).dial)
        }

    /**
     * A host our transport gives up on: a null page, so a pass writes nothing down.
     * Not [unreadableUpstreams], which answers and serves nothing, and so folds.
     */
    private fun silentUpstreams(): Upstreams = Upstreams(answers = { false }) { emptyList() }

    @Test
    fun `a host that cannot be decided is not re-dialled by the very next pass`() =
        runBlocking {
            // Nothing is published for an undecidable group, so without the cooldown
            // `unresolved` hands it back at the front of every pass.
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
            // The cooldown is a note about our pass, not a verdict about their server, so it expires on its own.
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
            // Both hosts in one candidate set, the silent one first, where widest-first ordering puts it.
            val store = newStore()
            val quietHost = RelayUrlNormalizer.normalize("wss://silent.example")
            val quietAlias = RelayUrlNormalizer.normalize("wss://silent.example/umbra")
            // One corpus shared by every url that answers; the silent host answers nothing, ever.
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up =
                Upstreams(answers = { RelayAliases.hostOf(it.url) != "silent.example" }) { corpus }
            val fold = folding(store, up)
            val urls = listOf(quietHost, quietAlias, canonical, alias)

            assertEquals(1, fold.measure("t", urls, canDial = { true }), "the foldable host was not measured")
            // On the pass after, the silent host costs nothing while the earned fold still stands.
            assertEquals(listOf(quietHost, quietAlias, canonical), fold.applyVerdicts(urls).dial)
        }

    @Test
    fun `a host whose preferred survivor will not answer still folds onto one that will`() =
        runBlocking {
            // The preferred leader is the pathless url, the one url on the host that
            // says nothing; every other url serves the identical window.
            val store = newStore()
            val bare = RelayUrlNormalizer.normalize("wss://paths.example")
            val first = RelayUrlNormalizer.normalize("wss://paths.example/kilo-yonder")
            val second = RelayUrlNormalizer.normalize("wss://paths.example/xray-alpha-jade")
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { at -> if (RelayAliases.pathOf(at.url).isEmpty()) emptyList() else corpus }
            val fold = folding(store, up)
            val urls = listOf(bare, first, second)

            assertEquals(1, fold.measure("t", urls, canDial = { true }), "the group was abandoned on its silent leader")

            // The survivor is the best url that could be measured; the bare one stays
            // in the fan-out because nothing was proved about it.
            assertEquals(listOf(bare, first), fold.applyVerdicts(urls).dial)
            assertEquals(mapOf(second to first), fold.applyVerdicts(urls).aliases)
        }

    @Test
    fun `a url the transport declined mid-search is still measured as a member`() =
        runBlocking {
            // `canDial` is a live check that can recover between the yardstick search
            // and the member walk; a refusal during the search is not "exhausted".
            val store = newStore()
            val flaky = RelayUrlNormalizer.normalize("wss://mixed.example")
            val first = RelayUrlNormalizer.normalize("wss://mixed.example/alpha")
            val second = RelayUrlNormalizer.normalize("wss://mixed.example/beacon")
            val corpus: List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "e$it") }
            val up = Upstreams { corpus }
            val fold = folding(store, up)
            // Refused once, on the yardstick attempt preference puts first, and reachable from then on.
            val refusals = AtomicInteger()
            val canDial: suspend (NormalizedRelayUrl) -> Boolean = { url ->
                url != flaky || refusals.getAndIncrement() > 0
            }

            val learned = fold.measure("t", listOf(flaky, first, second), canDial = canDial)

            assertEquals(2, learned, "the url refused during the search was never re-asked once the transport recovered")
            assertTrue(flaky in up.contacted, "it was dropped from the member walk rather than dialled")
            assertEquals(listOf(first), fold.applyVerdicts(listOf(flaky, first, second)).dial)
        }

    @Test
    fun `the yardstick search is bounded and never re-asks a url it has already tried`() =
        runBlocking {
            // A dead url here delays another dial rather than holding a permit, so the
            // walk stops at YARDSTICK_ATTEMPTS and a failed yardstick is not re-asked as a member.
            val store = newStore()
            val host = "wss://mute.example"
            val urls = (listOf(host) + (0 until 5).map { "$host/p$it" }).map { RelayUrlNormalizer.normalize(it) }
            val up = silentUpstreams()

            assertEquals(0, folding(store, up).measure("t", urls, canDial = { true }))

            // `leaderPrint` asks each of three urls twice, bare filter then kinds
            // fallback; no fourth url, and none of the three again as a member.
            assertEquals(
                AliasFolding.YARDSTICK_ATTEMPTS,
                up.contacted.size,
                "the search went past its cap, or re-asked a url it had already given up on",
            )
        }

    @Test
    fun `paths that duplicate each other fold even when the group's leader is a different endpoint`() =
        runBlocking {
            // `/inbox` is a different endpoint and the shortest url, so it leads; the
            // minted paths disagree with it and agree with each other.
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

            // Two endpoints, two dials; the inbox keeps its own place.
            val cleaned = fold.applyVerdicts(urls)
            assertEquals(listOf(inbox, paths.first()), cleaned.dial)
            assertEquals(paths.drop(1).associateWith { paths.first() }, cleaned.aliases)
        }

    @Test
    fun `a host of genuinely distinct endpoints keeps every one of them`() =
        runBlocking {
            // Comparing members to each other must not collapse a host whose paths each serve their own events.
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
            assertEquals(urls, fold.applyVerdicts(urls).dial)
        }

    @Test
    fun `a single url is returned untouched by both halves`() =
        runBlocking {
            val up = upstreams()
            val fold = folding(newStore(), up)

            assertEquals(listOf(canonical), fold.applyVerdicts(listOf(canonical)).dial)
            assertEquals(0, fold.measure("t", listOf(canonical), canDial = { true }))
            assertEquals(0, up.dials.get())
        }
}
