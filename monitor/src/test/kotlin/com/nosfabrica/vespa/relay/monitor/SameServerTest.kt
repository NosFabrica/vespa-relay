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
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * One server reaches the roster once: the fold decides which of a host's urls are one relay,
 * and fitness grades a url `prime` only when the fold has said it is its own. Each fixture is a
 * host as it answered on staging.
 */
class SameServerTest {
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()

    private fun url(raw: String): NormalizedRelayUrl = RelayUrlNormalizer.normalize(raw)

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = url("ws://localhost:7777"))

    /** Servers that answer per url from a corpus, counting the asks each url gets. */
    private class Servers(
        private val corpusFor: (NormalizedRelayUrl) -> List<Event>,
    ) {
        val asked = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()

        suspend fun fetch(
            at: NormalizedRelayUrl,
            want: Int,
            until: Long?,
            kinds: List<Int>?,
        ): AliasProbe.Page {
            asked.computeIfAbsent(at) { AtomicInteger() }.incrementAndGet()
            return AliasProbe.Page(corpusFor(at).filter { until == null || it.createdAt <= until }.take(want))
        }

        fun askedOf(url: NormalizedRelayUrl): Int = asked[url]?.get() ?: 0
    }

    /** Forty notes of one relay; two corpora with different names share no id. */
    private fun corpus(name: String): List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "$name#$it") }

    private fun probe(servers: Servers) = AliasProbe(fetch = servers::fetch, target = 40, page = 40, fallbackPage = 40)

    private fun folding(
        store: NostrSemanticsStore,
        servers: Servers,
    ) = AliasFolding(aliases = RelayAliases(), record = RelayVerdictRecord(store, signer), probe = probe(servers))

    private fun fitness(
        store: NostrSemanticsStore,
        servers: Servers,
        world: () -> Collection<NormalizedRelayUrl> = { emptyList() },
        progress: Processors.Handle = Processors().of("fitness"),
    ) = FitnessPass(
        record = RelayVerdictRecord(store, signer),
        probe = probe(servers),
        client = EmptyNostrClient(),
        progress = progress,
        reconcile = { _, _ -> },
        world = { ServerIndex(world().toSet()) },
    )

    private suspend fun FitnessPass.grade(
        label: String,
        candidates: List<NormalizedRelayUrl>,
    ) = measure(label, candidates, reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)

    /** Our fitness label on [url]'s record, value and evidence, or null. */
    private suspend fun gradeOf(
        store: NostrSemanticsStore,
        url: NormalizedRelayUrl,
    ): Array<String>? =
        store
            .query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey), tags = mapOf("d" to listOf(url.url))))
            .maxByOrNull { it.createdAt }
            ?.tags
            ?.firstOrNull { it.size > 2 && it[0] == RelayVerdictRecord.LABEL_TAG && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }

    private suspend fun primeAmong(
        store: NostrSemanticsStore,
        urls: List<NormalizedRelayUrl>,
    ): Set<NormalizedRelayUrl> = urls.filterTo(HashSet()) { gradeOf(store, it)?.get(1) == Verdict.PRIME.value }

    /** A record written as an older build signed it: tags given whole, epochs included. */
    private suspend fun signedEarlier(
        store: NostrSemanticsStore,
        subject: NormalizedRelayUrl,
        vararg tags: Array<String>,
    ) {
        val template = RelayDiscoveryEvent.build(subject, "", nowSeconds()) { for (tag in tags) add(tag) }
        store.insert(signer.sign(template))
    }

    private val coracle = url("wss://indexer.coracle.social/")
    private val coracleIndia = url("wss://indexer.coracle.social/india")
    private val coracleAlphaCharlie = url("wss://indexer.coracle.social/alpha-charlie")

    @Test
    fun `fitness grades a known alias alias without dialling it when its canonical is not in the batch`() =
        runBlocking {
            val store = newStore()
            val servers = Servers { corpus("coracle") }
            RelayVerdictRecord(store, signer).publish(coracleAlphaCharlie, coracleIndia, 500, 500)

            fitness(store, servers).grade(AliasMonitor.FAST_LANE, listOf(coracleAlphaCharlie))

            val grade = gradeOf(store, coracleAlphaCharlie)
            assertEquals(Verdict.ALIAS.value, grade?.get(1))
            assertEquals("folds onto ${coracleIndia.url}", grade?.get(RelayVerdictRecord.LABEL_EVIDENCE_INDEX))
            assertEquals(0, servers.askedOf(coracleAlphaCharlie))
        }

    @Test
    fun `an alias whose fold is under older rules is not signed prime before the fold re-decides it`() =
        runBlocking {
            val store = newStore()
            val servers = Servers { corpus("coracle") }
            val stamp = nowSeconds().toString()
            signedEarlier(
                store,
                coracleAlphaCharlie,
                arrayOf(RelayVerdictRecord.SAME_AS_TAG, coracle.url, "500 newest events, 500 shared", stamp, "2"),
                arrayOf(RelayVerdictRecord.LABEL_TAG, Verdict.ALIAS.value, RelayVerdictRecord.FITNESS_NAMESPACE, "folds onto ${coracle.url}", stamp, "2"),
            )
            val both = listOf(coracle, coracleAlphaCharlie)

            // The fast lane first, holding only the alias, then the sweep, both before any fold pass.
            fitness(store, servers, world = { both }).grade(AliasMonitor.FAST_LANE, listOf(coracleAlphaCharlie))
            assertNotEquals(Verdict.PRIME.value, gradeOf(store, coracleAlphaCharlie)?.get(1))
            fitness(store, servers).grade(AliasMonitor.ALL_STREAMS, both)

            assertEquals(setOf(coracle), primeAmong(store, both), "a url waiting on the fold that answers is never graded off that answer")
        }

    @Test
    fun `a url waiting on the fold has its standing prime taken back`() =
        runBlocking {
            val store = newStore()
            val servers = Servers { corpus("coracle") }
            val record = RelayVerdictRecord(store, signer)
            record.publishFitness(coracleAlphaCharlie, Verdict.PRIME.value, "answered 20 events at a settled anchor", pageable = null, nip77 = null)

            fitness(store, servers).grade(AliasMonitor.ALL_STREAMS, listOf(coracle, coracleAlphaCharlie))

            assertNull(gradeOf(store, coracleAlphaCharlie), "withdrawn, not replaced: the fold has not said what it is")
            assertEquals(setOf(coracle), primeAmong(store, listOf(coracle, coracleAlphaCharlie)))
        }

    @Test
    fun `a held path its server will not upgrade is graded dead, and a held path that answers keeps waiting`() =
        runBlocking {
            // relay.ngit.dev on production: the root a relay, every other listed path a 404 or a web page.
            val root = url("wss://relay.ngit.dev/")
            val paths = mapOf("missing" to 404, "page" to 200, "gone" to 410, "busy" to 503, "walled" to 403, "throttled" to 429)
            val refused = paths.mapKeys { url("wss://relay.ngit.dev/${it.key}") }
            val answers = url("wss://relay.ngit.dev/answers")
            val notes = corpus("ngit")
            val store = newStore()
            val all = listOf(root, answers) + refused.keys
            val asked = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()
            // The sweep before graded the root, which is what makes its paths worth an ask.
            RelayVerdictRecord(store, signer).publishFitness(root, Verdict.PRIME.value, "answered 40 events at a settled anchor", pageable = null, nip77 = null)
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe =
                    AliasProbe(
                        fetch = { at, want, until, _ ->
                            asked.computeIfAbsent(at) { AtomicInteger() }.incrementAndGet()
                            refused[at]?.let { status ->
                                AliasProbe.Page(
                                    events = null,
                                    reason = "cannot:Server Misconfigured. Response: $status X. Exception: Expected HTTP 101 response but was '$status X'",
                                )
                            } ?: AliasProbe.Page(notes.filter { until == null || it.createdAt <= until }.take(want))
                        },
                        target = 40,
                        page = 40,
                        fallbackPage = 40,
                    ),
                client = EmptyNostrClient(),
                progress = Processors().of("fitness"),
                reconcile = { _, _ -> },
            ).grade(AliasMonitor.ALL_STREAMS, all)

            val grades = all.associateWith { gradeOf(store, it)?.get(1) }
            assertEquals(Verdict.PRIME.value, grades[root])
            for (path in listOf("missing", "page", "gone")) {
                assertEquals(Verdict.DEAD.value, grades[url("wss://relay.ngit.dev/$path")], "/$path refused the upgrade as no relay")
            }
            // A server error, a wall or a rate limit is not "no relay here", and an answer is the fold's to judge.
            for (path in listOf("busy", "walled", "throttled", "answers")) {
                assertNull(grades[url("wss://relay.ngit.dev/$path")], "/$path must keep waiting on the fold")
            }
            assertTrue(refused.keys.all { asked[it]?.get() == 1 }, "a held url is asked once, never walked down the ladder: $asked")
        }

    @Test
    fun `a server the pre-probe proves gone is graded dead at every path on that socket in one sweep`() =
        runBlocking {
            // haven.almostmachines.dev: 113 paths, graded one a sweep as each took its turn as stand-in.
            val root = url("wss://gone.example/")
            val paths = (1..5).map { url("wss://gone.example/p$it") }
            val otherPort = url("wss://gone.example:7777/")
            val plain = url("ws://gone.example/plain")
            val answering = url("wss://answers.example/")
            val notes = corpus("answers")
            val store = newStore()
            val asked = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe =
                    AliasProbe(
                        fetch = { at, want, until, _ ->
                            asked.computeIfAbsent(at) { AtomicInteger() }.incrementAndGet()
                            AliasProbe.Page(notes.filter { until == null || it.createdAt <= until }.take(want))
                        },
                        target = 40,
                        page = 40,
                        fallbackPage = 40,
                    ),
                client = EmptyNostrClient(),
                progress = Processors().of("fitness"),
                reconcile = { _, _ -> },
            ).measure(
                AliasMonitor.ALL_STREAMS,
                listOf(root) + paths + listOf(otherPort, plain, answering),
                // Port 443 of the host refuses the connect; port 7777 and anything else answer.
                reach = { if (FitnessPass.socketOf(it) == "gone.example:443") Reach.PROVED_UNREACHABLE else Reach.REACHABLE },
                onEvent = {},
                sockets = Sockets.NONE,
            )

            assertEquals(Verdict.DEAD.value, gradeOf(store, root)?.get(1))
            for (path in paths) {
                val grade = gradeOf(store, path)
                assertEquals(Verdict.DEAD.value, grade?.get(1), "${path.url} shares the socket the pre-probe proved gone")
                assertEquals("no TCP answer at the pre-probe", grade?.get(RelayVerdictRecord.LABEL_EVIDENCE_INDEX))
                assertNull(asked[path], "the proof is the stand-in's; the path costs no dial")
            }
            // Another port is another socket, and plain ws another port: the proof says nothing of either.
            assertEquals(Verdict.PRIME.value, gradeOf(store, otherPort)?.get(1))
            assertNull(gradeOf(store, plain))
        }

    @Test
    fun `a held path inherits no proof the dark-network guard withheld from its stand-in`() =
        runBlocking {
            // Every host gone at once is our resolver or egress, not sixty servers.
            val roots = (0 until 60).map { url("wss://gone$it.example/") }
            val paths = (0 until 60).map { url("wss://gone$it.example/p") }
            val store = newStore()
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe = probe(Servers { emptyList() }),
                client = EmptyNostrClient(),
                progress = Processors().of("fitness"),
                reconcile = { _, _ -> },
            ).measure(AliasMonitor.ALL_STREAMS, roots + paths, reach = { Reach.PROVED_UNREACHABLE }, onEvent = {}, sockets = Sockets.NONE)

            assertTrue((roots + paths).all { gradeOf(store, it) == null }, "a withheld proof was signed onto a path")
        }

    @Test
    fun `the held paths of a server that answered HTTP are asked even when nothing on it is prime`() =
        runBlocking {
            // A server whose root refuses us with a 403 wall is up, and its 404 paths are not relays.
            val root = url("wss://walled.example/")
            val missing = listOf(url("wss://walled.example/gone-a"), url("wss://walled.example/gone-b"))
            val store = newStore()
            RelayVerdictRecord(store, signer).publishFitness(root, Verdict.RESTRICTED.value, "the websocket upgrade was refused with HTTP 403", pageable = null, nip77 = null)
            val asked = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe =
                    AliasProbe(fetch = { at, _, _, _ ->
                        asked.computeIfAbsent(at) { AtomicInteger() }.incrementAndGet()
                        val status = if (at == root) 403 else 404
                        AliasProbe.Page(events = null, reason = "cannot:Server Misconfigured. Exception: Expected HTTP 101 response but was '$status X'")
                    }),
                client = EmptyNostrClient(),
                progress = Processors().of("fitness"),
                reconcile = { _, _ -> },
            ).grade(AliasMonitor.ALL_STREAMS, listOf(root) + missing)

            // The walled root cannot stand in, so one path does and the other waits behind it.
            val (held, standIn) = missing.partition { asked[it]?.get() == 1 }
            assertEquals(1, held.size, "one path asked once, as a held url is: $asked")
            assertEquals(1, standIn.size)
            for (url in missing) assertEquals(Verdict.DEAD.value, gradeOf(store, url)?.get(1), "${url.url} refused the upgrade with a 404")
        }

    @Test
    fun `the paths of a server down at its stand-in are not asked`() =
        runBlocking {
            // nostr.ac on production: 87 held paths answering 503, as its stand-in did.
            val root = url("wss://down.example/")
            val missing = url("wss://down.example/missing")
            val asked = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()
            val store = newStore()
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe =
                    AliasProbe(fetch = { at, _, _, _ ->
                        asked.computeIfAbsent(at) { AtomicInteger() }.incrementAndGet()
                        val status = if (at == root) 503 else 404
                        AliasProbe.Page(events = null, reason = "cannot:Server Misconfigured. Exception: Expected HTTP 101 response but was '$status X'")
                    }),
                client = EmptyNostrClient(),
                progress = Processors().of("fitness"),
                reconcile = { _, _ -> },
            ).grade(AliasMonitor.ALL_STREAMS, listOf(root, missing))

            assertNull(asked[missing], "a held path is asked only beside a `prime` stand-in")
            assertNull(gradeOf(store, missing))
        }

    private val nostrNet = url("wss://relay.nostr.net/")

    /** The paths relay.nostr.net carried on staging, every one serving the root's events. */
    private val nostrNetPaths =
        (
            "beacon bravo-marble dynamo dynamo-cipher-mike echo-beacon ember flint-echo flint-zulu-flint foxtrot " +
                "foxtrot-tango-onyx hotel-yonder india karma-delta-oscar karma-papa-xray marble-yonder-golf mike-kilo-jade " +
                "november-raven-karma"
        ).split(" ").map { url("wss://relay.nostr.net/$it") }

    @Test
    fun `a server that ignores the path is graded once before the fold and folds to one url after it`() =
        runBlocking {
            val store = newStore()
            val servers = Servers { corpus("relay.nostr.net") }
            val all = listOf(nostrNet) + nostrNetPaths

            fitness(store, servers).grade(AliasMonitor.ALL_STREAMS, all)
            assertEquals(setOf(nostrNet), primeAmong(store, all), "undecided, the server stands in once")

            folding(store, servers).measure("t", all, canDial = { true })
            val held = RelayVerdictRecord(store, signer).load(all)
            assertEquals(nostrNetPaths.toSet(), held.aliases.keys)
            assertEquals(setOf(nostrNet), held.aliases.values.toSet())

            fitness(store, servers).grade(AliasMonitor.ALL_STREAMS, all)
            assertEquals(setOf(nostrNet), primeAmong(store, all))
        }

    private val haven = url("wss://nostr-relay.derekross.me/")
    private val havenInbox = url("wss://nostr-relay.derekross.me/inbox")
    private val havenChat = url("wss://nostr-relay.derekross.me/chat")
    private val havenMade =
        listOf("alpha-quartz-beacon", "anchor-zenith", "chat/alpha-victor").map { url("wss://nostr-relay.derekross.me/$it") }

    /** haven: the outbox at the root, the inbox and the chat apart, any other path answering as the root. */
    private fun havenServer(): Servers {
        val outbox = corpus("outbox")
        val inbox = corpus("inbox")
        val chat = corpus("chat")
        return Servers { at ->
            when (at) {
                havenInbox -> inbox
                havenChat -> chat
                else -> outbox
            }
        }
    }

    @Test
    fun `haven's own paths stay apart and the made-up ones fold onto the outbox`() =
        runBlocking {
            val store = newStore()
            val servers = havenServer()
            val all = listOf(haven, havenInbox, havenChat) + havenMade

            folding(store, servers).measure("t", all, canDial = { true })
            val held = RelayVerdictRecord(store, signer).load(all)
            assertEquals(havenMade.associateWith { haven }, held.aliases)

            fitness(store, servers).grade(AliasMonitor.ALL_STREAMS, all)
            assertEquals(setOf(haven, havenInbox, havenChat), primeAmong(store, all))
        }

    @Test
    fun `a path-routing host keeps every path`() =
        runBlocking {
            val store = newStore()
            val paths = listOf("", "inbox", "favorites", "personal").map { url("wss://pyramid.fiatjaf.com/$it") }
            val servers = Servers { at -> corpus(at.url) }

            folding(store, servers).measure("t", paths, canDial = { true })
            val held = RelayVerdictRecord(store, signer).load(paths)
            assertEquals(emptyMap(), held.aliases)
            assertEquals(paths.toSet(), held.distinct)

            fitness(store, servers).grade(AliasMonitor.ALL_STREAMS, paths)
            assertEquals(paths.toSet(), primeAmong(store, paths))
        }

    private val languages = listOf("zh", "it/", "fr", "ar").map { url("wss://lang.relays.land/$it") }

    @Test
    fun `language relays sharing a name do not fold onto each other`() =
        runBlocking {
            val store = newStore()
            val servers = Servers { at -> corpus(RelayAliases.pathOf(at.url)) }

            folding(store, servers).measure("t", languages, canDial = { true })

            val held = RelayVerdictRecord(store, signer).load(languages)
            assertEquals(emptyMap(), held.aliases)
            assertEquals(languages.toSet(), held.distinct)
        }

    @Test
    fun `a quiet language relay does not keep its busy neighbours from being measured`() =
        runBlocking {
            // Measured on lang.relays.land: `/ar`, `/ca` and `/cs` sort first and are quiet, `/zh` and `/es`
            // served 500 events each, disjoint.
            val store = newStore()
            val quietPaths = listOf("ar", "ca", "cs").map { url("wss://lang.relays.land/$it") }
            val busy = listOf("de", "es", "fr", "zh").map { url("wss://lang.relays.land/$it") }
            val esSlash = url("wss://lang.relays.land/es/")
            val all = quietPaths + busy + esSlash
            val servers =
                Servers { at ->
                    if (at in quietPaths) (0 until 7).map { events.sign(1_700_000_000L - it, 1, emptyArray(), "${at.url}#$it") } else corpus(RelayAliases.pathOf(at.url))
                }

            folding(store, servers).measure("t", all, canDial = { true })

            val held = RelayVerdictRecord(store, signer).load(all)
            assertEquals(busy.toSet(), held.distinct)
            assertEquals(mapOf(esSlash to url("wss://lang.relays.land/es")), held.aliases)

            // Too thin for any fold, the quiet ones are graded once between them, never behind `/es`,
            // which is its own relay that a twin happens to fold onto.
            fitness(store, servers).grade(AliasMonitor.ALL_STREAMS, all)
            val prime = primeAmong(store, all)
            assertEquals(busy.toSet(), prime - quietPaths.toSet())
            assertEquals(1, (prime intersect quietPaths.toSet()).size)
        }

    @Test
    fun `a trailing-slash twin waits on the url the fold measured and reaches the roster once`() =
        runBlocking {
            val store = newStore()
            val servers = Servers { at -> corpus(RelayAliases.pathOf(at.url)) }
            folding(store, servers).measure("t", languages, canDial = { true })
            val arSlash = url("wss://lang.relays.land/ar/")
            val ar = url("wss://lang.relays.land/ar")
            assertNotEquals(ar, arSlash, "the normalizer keeps them apart, which is why this rule exists")

            fitness(store, servers, world = { languages + arSlash }).grade(AliasMonitor.FAST_LANE, listOf(arSlash))
            assertNull(gradeOf(store, arSlash))
            assertEquals(0, servers.askedOf(arSlash), "a waiting url is asked only beside a `prime` stand-in")

            val roster = folding(store, servers).applyVerdicts(languages + arSlash)
            assertEquals(mapOf(arSlash to ar), roster.waiting)
        }

    @Test
    fun `the roster folds a server onto the url fitness grades, not one the monitor holds out`() =
        runBlocking {
            val store = newStore()
            val servers = Servers { corpus("relay.nostr.net") }
            // The root is named by the lists but held out of the monitor's set, as a dead url is.
            val graded = nostrNetPaths.take(3)

            fitness(store, servers).grade(AliasMonitor.ALL_STREAMS, graded)
            val standIn = primeAmong(store, graded).single()

            val roster = folding(store, servers).applyVerdicts(listOf(nostrNet) + graded, world = ServerIndex(graded.toSet()))
            assertEquals((listOf(nostrNet) + graded - standIn).associateWith { standIn }, roster.waiting)
        }

    @Test
    fun `a stand-in waiting on its own twin hands every url of its server straight to that twin`() {
        val plain = url("ws://chain.example/")
        val secure = url("wss://chain.example/")
        val path = url("wss://chain.example/a")
        val aliases = RelayAliases().also { it.markDistinct(plain) }

        val held = aliases.heldForFold(listOf(secure, path), listOf(plain))

        assertEquals(mapOf(secure to plain, path to plain), held)
        assertTrue(held.values.none { it in held }, "one hop, so a roster re-keying once lands on a url nothing re-keys")
    }

    @Test
    fun `a url measured distinct never stands in for another path`() {
        // The inbox is proven its own relay, which says nothing about the outbox beside it.
        val inbox = url("wss://haven.example/inbox")
        val outbox = url("wss://haven.example/outbox")
        val aliases = RelayAliases().also { it.markDistinct(inbox) }

        assertEquals(emptyMap(), aliases.heldForFold(listOf(outbox), listOf(inbox)))
    }

    @Test
    fun `a stand-in graded as refusing hands the server to the next path`() {
        val root = url("wss://refusing.example/")
        val inbox = url("wss://refusing.example/inbox")
        val outbox = url("wss://refusing.example/outbox")
        val held =
            RelayVerdictRecord.Verdicts(
                fitness = mapOf(root to RelayVerdictRecord.StandingGrade(Verdict.RESTRICTED.value, "answers only shaped queries")),
            )

        assertEquals(mapOf(root to inbox, outbox to inbox), AliasFolding.heldAmong(held, listOf(root, inbox, outbox)))
    }

    @Test
    fun `the roster folds onto a canonical the monitor knows even when the stream does not name it`() =
        runBlocking {
            val store = newStore()
            val canonical = url("wss://folded.example/")
            val alias = url("wss://folded.example/a")
            val undecided = url("wss://folded.example/b")
            RelayVerdictRecord(store, signer).publish(alias, canonical, 500, 500)

            val roster = folding(store, Servers { corpus("folded") }).applyVerdicts(listOf(alias, undecided), ServerIndex(setOf(canonical, alias, undecided)))

            assertEquals(mapOf(alias to canonical), roster.aliases)
            assertEquals(mapOf(undecided to canonical), roster.waiting)
            assertEquals(emptyMap(), roster.standIns, "no member elected in place of a canonical the monitor still grades")
        }

    @Test
    fun `a query straight after the host is the same server`() {
        assertEquals(RelayAliases.authorityOf("wss://prl.plus/"), RelayAliases.authorityOf("wss://prl.plus?reject=3"))
    }

    @Test
    fun `slashes trailing a query name the same endpoint`() {
        val decided = url("wss://prl.plus/?reject=3,6,7///")
        val longer = url("wss://prl.plus/?reject=3,6,7//////////")
        val aliases = RelayAliases().also { it.markDistinct(decided) }

        assertEquals(mapOf(longer to decided), aliases.heldForFold(listOf(longer), listOf(decided)))
    }

    @Test
    fun `a fold never settles onto the plain url when its secure twin answers`() =
        runBlocking {
            val store = newStore()
            val servers = Servers { corpus("damus") }
            val plain = url("ws://relay.damus.io/")
            val secure = url("wss://relay.damus.io/")
            val earlier = url("wss://relay.damus.io/yankee-india")
            val fresh = url("wss://relay.damus.io/glyph-victor-uniform")
            RelayVerdictRecord(store, signer).publish(earlier, plain, 500, 500)
            val all = listOf(plain, secure, earlier, fresh)

            folding(store, servers).measure("t", all, canDial = { true })

            val folds = folding(store, servers).applyVerdicts(all).aliases
            assertEquals(setOf(plain, earlier, fresh), folds.keys)
            assertEquals(setOf(secure), folds.values.toSet())
        }

    @Test
    fun `the sweep publishes how many servers are graded prime more than once`() =
        runBlocking {
            val store = newStore()
            val servers = havenServer()
            val processors = Processors()
            val all = listOf(haven, havenInbox, havenChat) + havenMade
            folding(store, servers).measure("t", all, canDial = { true })

            fitness(store, servers, progress = processors.of("fitness")).grade(AliasMonitor.ALL_STREAMS, all)

            val census = processors.snapshot().single().sharedHosts!!
            assertEquals(1, census.hosts)
            assertEquals(3, census.urls)
            assertEquals(0, census.unproven, "the outbox, the inbox and the chat are all measured as their own relays")
            assertEquals(
                mapOf(haven.url to Processors.FOLD_CANONICAL, havenInbox.url to Processors.FOLD_DISTINCT, havenChat.url to Processors.FOLD_DISTINCT),
                census.top
                    .single()
                    .relays
                    .associate { it.relay to it.fold },
            )
        }

    @Test
    fun `a server prime under paths the fold never decided is counted as unproven`() {
        val census =
            PrimeCensus.of(
                listOf(url("wss://rrr.pupupu.monster/"), url("wss://rrr.pupupu.monster/mike"), url("wss://relay.minibits.cash/lima")),
                RelayVerdictRecord.Verdicts(staleFolds = setOf(url("wss://rrr.pupupu.monster/mike"))),
            )

        assertEquals(1, census.hosts)
        assertEquals(1, census.unproven)
        assertEquals(
            listOf(Processors.FOLD_NONE, Processors.FOLD_STALE),
            census.top
                .single()
                .relays
                .map { it.fold },
        )
    }

    @Test
    fun `the fast lane does not replace the sweep's census`() =
        runBlocking {
            val store = newStore()
            val processors = Processors()

            fitness(store, Servers { corpus("coracle") }, progress = processors.of("fitness")).grade(AliasMonitor.FAST_LANE, listOf(coracle, coracleIndia))

            assertNull(processors.snapshot().single().sharedHosts)
            assertTrue(
                processors
                    .snapshot()
                    .single()
                    .counts
                    .any { it.name == FitnessPass.WAITING_ON_FOLD && it.value == 1L },
            )
        }
}
