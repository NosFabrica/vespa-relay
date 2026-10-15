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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.EmptyNostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Many relays answer only some shapes of ask, so one shape cannot grade them. Each fixture answers
 * the shapes the way a relay measured on 2026-10-15 did: events, an empty EOSE, a CLOSED, or silence.
 */
class ShapeLadderTest {
    private val signer = NostrSignerInternal(KeyPair())
    private val events = NostrSignerSync()
    private val url = RelayUrlNormalizer.normalize("wss://shaped.example/")

    /** How one shape is answered. */
    private sealed interface Answer {
        object Serves : Answer

        object Empty : Answer

        class Refuses(
            val words: String,
        ) : Answer

        object Silent : Answer
    }

    /** Forty events of [kind]; a kinds ask is served only events of that kind. */
    private fun corpus(kind: Int): List<Event> = (0 until 40).map { events.sign(1_700_000_000L - it, kind, emptyArray(), "k$kind#$it") }

    private fun fetchFor(answers: (List<Int>?) -> Answer): suspend (com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page =
        { _, want, until, kinds ->
            when (val a = answers(kinds)) {
                Answer.Serves -> AliasProbe.Page(corpus(kinds?.first() ?: 1).filter { until == null || it.createdAt <= until }.take(want))
                Answer.Empty -> AliasProbe.Page(emptyList())
                is Answer.Refuses -> AliasProbe.Page(emptyList(), refusal = a.words)
                Answer.Silent -> AliasProbe.Page(null, reason = "cannot: timed out")
            }
        }

    /** The fitness grade and its evidence after one pass over [url]. */
    private fun grade(answers: (List<Int>?) -> Answer): Pair<String?, String?> =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
            FitnessPass(
                record = RelayVerdictRecord(store, signer),
                probe = AliasProbe(fetch = fetchFor(answers), target = 20, page = 20, fallbackPage = 10),
                client = EmptyNostrClient(),
                progress = Processors().of("fitness"),
                reconcile = { _, _ -> },
            ).measure("t", listOf(url), reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)
            val label =
                store
                    .query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(signer.pubKey)))
                    .flatMap { it.tags.toList() }
                    .firstOrNull { it.size > 3 && it[0] == "l" && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }
            label?.get(1) to label?.get(RelayVerdictRecord.LABEL_EVIDENCE_INDEX)
        }

    @Test
    fun `a relay that refuses the bare filter and serves notes is graded on the notes`() {
        // haven, the wot relays, cyphernomad: CLOSED to a bare filter, every kinds filter served.
        val (grade, evidence) = grade { if (it == null) Answer.Refuses("blocked: filter must name kinds") else Answer.Serves }

        assertEquals(Verdict.PRIME.value, grade)
        assertTrue(evidence!!.contains("through kinds 1"), evidence)
    }

    @Test
    fun `an indexer is graded on the profiles it serves`() {
        // purplepag.es: refuses a bare filter, nothing for notes, serves kinds 0, 3 and 10002.
        val (grade, evidence) =
            grade {
                when (it) {
                    null -> Answer.Refuses("blocked: this relay only serves profiles and relay lists")
                    AliasProbe.FALLBACK_KINDS -> Answer.Empty
                    else -> Answer.Serves
                }
            }

        assertEquals(Verdict.PRIME.value, grade)
        assertTrue(evidence!!.contains("through kinds 0"), evidence)
    }

    @Test
    fun `a group relay is graded on its group metadata`() {
        // groups.hzrd149.com: every other shape refused for lacking an h, e or a tag.
        val (grade, evidence) =
            grade { if (it == RelayAliases.GROUP_METADATA_KINDS) Answer.Serves else Answer.Refuses("blocked: invalid query, must have 'h', 'e' or 'a' tag") }

        assertEquals(Verdict.PRIME.value, grade)
        assertTrue(evidence!!.contains("through kinds 39000"), evidence)
    }

    @Test
    fun `a group relay's complete list of groups is not judged as a walk`() {
        // groups.hzrd149.com serves its metadata whatever the `until`, which is a list, not a cursor ignored.
        val metadata = corpus(39000)
        val (grade, _) =
            runBlocking {
                val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
                val fetch: suspend (com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page =
                    { _, want, _, kinds ->
                        if (kinds == RelayAliases.GROUP_METADATA_KINDS) AliasProbe.Page(metadata.take(want)) else AliasProbe.Page(emptyList(), refusal = "blocked: must have 'h' tag")
                    }
                FitnessPass(
                    record = RelayVerdictRecord(store, signer),
                    probe = AliasProbe(fetch = fetch, target = 20, page = 20, fallbackPage = 10),
                    client = EmptyNostrClient(),
                    progress = Processors().of("fitness"),
                    reconcile = { _, _ -> },
                ).measure("t", listOf(url), reach = { Reach.REACHABLE }, onEvent = {}, sockets = Sockets.NONE)
                store
                    .query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND)))
                    .flatMap { it.tags.toList() }
                    .firstOrNull { it.size > 3 && it[0] == "l" && it[2] == RelayVerdictRecord.FITNESS_NAMESPACE }
                    ?.let { it[1] to it[3] } ?: (null to null)
            }

        assertEquals(Verdict.PRIME.value, grade)
    }

    @Test
    fun `a members-only relay that refuses every shape after our AUTH is auth-refused`() {
        // The flotilla and coracle spaces: AUTH accepted, every REQ still auth-required.
        val (grade, _) = grade { Answer.Refuses("auth-required: authentication is required for access") }

        assertEquals(Verdict.AUTH_REFUSED.value, grade)
    }

    @Test
    fun `a relay that refuses every shape for other reasons is restricted`() {
        val (grade, evidence) = grade { Answer.Refuses("blocked: only NWC requests are served here") }

        assertEquals(Verdict.RESTRICTED.value, grade)
        assertTrue(evidence!!.contains("only NWC requests"), evidence)
    }

    @Test
    fun `a relay that answers every shape empty says so`() {
        // strfry.appx.uk, relay.minds.com: an EOSE and nothing, to every shape.
        val (grade, evidence) = grade { Answer.Empty }

        assertEquals(Verdict.PRIME.value, grade)
        assertTrue(evidence!!.startsWith("answered every shape it was asked empty"), evidence)
    }

    @Test
    fun `a refusal that is about the page size is not a refusal of the shape`() =
        runBlocking {
            // The humbler page exists for `blocked: limit too high`; a relay that takes it still serves.
            val fetch: suspend (com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl, Int, Long?, List<Int>?) -> AliasProbe.Page =
                { _, want, until, kinds ->
                    if (want > 10) {
                        AliasProbe.Page(emptyList(), refusal = "blocked: limit too high")
                    } else {
                        AliasProbe.Page(corpus(kinds?.first() ?: 1).filter { until == null || it.createdAt <= until }.take(want))
                    }
                }

            val window = AliasProbe(fetch = fetch, target = 20, page = 20, fallbackPage = 10).window(url, 1_800_000_000L, null) {}

            assertEquals(null, window.refusal)
            assertTrue(window.ids!!.isNotEmpty())
        }

    @Test
    fun `the fold measures an indexer through the profiles it serves`() =
        runBlocking {
            val fetch = fetchFor { if (it == AliasProbe.PROFILE_KINDS) Answer.Serves else Answer.Refuses("blocked: profiles only") }

            val lead = AliasProbe(fetch = fetch, target = 20, page = 20, fallbackPage = 10).leaderPrint(url, 1_800_000_000L) {}.leader

            assertEquals(AliasProbe.PROFILE_KINDS, lead?.kinds)
            assertEquals(20, lead?.ids?.size)
        }
}
