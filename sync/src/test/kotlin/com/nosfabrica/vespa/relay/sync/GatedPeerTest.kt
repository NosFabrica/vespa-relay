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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.DeleteMissing
import com.nosfabrica.vespa.relay.config.SyncDirection
import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.config.SyncUpstream
import com.nosfabrica.vespa.relay.ingest.IngestPipeline
import com.nosfabrica.vespa.relay.ingest.IngestTuning
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.peers.GatedResponder
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyLocalIndex
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.DEFAULT_AUTH_GRACE_MS
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/**
 * The router against a scripted gated peer, through quartz's own client and NIP-42 responder: it
 * reads anonymously, learns the waiver from the refusal, signs in only where a refusal asks for
 * AUTH without naming it, and never deletes or pushes on the empty set a signed-in, un-waived
 * read of a gated peer answers.
 */
class GatedPeerTest {
    private val url = RelayUrlNormalizer.normalize("wss://gated.example")
    private val provider = "a1".repeat(32)
    private val ask = Filter(kinds = listOf(30382), authors = listOf(provider))
    private val cards =
        (1..20).map { n ->
            val subject = n.toString(16).padStart(64, '0')
            Event("%064x".format(n + 1000), provider, nowSeconds() - 3_600 + n, 30382, arrayOf(arrayOf("d", subject), arrayOf("rank", "50")), "", "b2".repeat(64))
        }

    private val stream =
        SyncStream(
            name = "scores",
            dir = SyncDirection.DOWN,
            filter = Filter(kinds = listOf(30382)),
            urls = listOf(url),
            trusted = true,
            negentropySyncThePastSeconds = 86_400,
            deleteMissing = DeleteMissing.ON,
            ownedKinds = setOf(30382),
        )

    private class Router(
        val client: NostrClient,
        val waivers: LensWaivers,
    )

    private fun <T> withRouter(
        peer: (CoroutineScope) -> GatedPeer,
        block: suspend (Router, GatedPeer, CoroutineScope) -> T,
    ): T =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                val p = peer(scope)
                val client = NostrClient(p, scope)
                val waivers = LensWaivers(log = {})
                GatedResponder(client, scope, NostrSignerInternal(KeyPair()), waivers)
                client.connect()
                withTimeout(30_000) { block(Router(client, waivers), p, scope) }
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `an anonymous router learns the waiver from the refusal and never signs in`() =
        withRouter({ GatedPeer(it, cards) }) { router, peer, _ ->
            val reads = WaivedReads(ClientRelayReads(router.client), router.waivers)
            reads.page(url, ask, 5_000) {}
            assertTrue(router.waivers.waives(url), "the refusal named the waiver")
            val got = mutableListOf<Event>()
            reads.page(url, ask, 5_000) { got += it }
            assertEquals(cards.size, got.size, "the whole set, with the waiver")
            assertEquals(0, peer.auths.get(), "a gated peer is read anonymously: its challenge goes unanswered")
        }

    /**
     * A page whose read sets off the NIP-42 sign-in it needs (a CLOSED `auth-required:`, an EOSE
     * `auth` hint). quartz's walk waits one scheduling hop for that sign-in to start
     * ([DEFAULT_AUTH_GRACE_MS], 1 s) and otherwise ends AUTH_REQUIRED, its subscription closed, and
     * a responder that picks the refusal up after that signs nothing in: what a starved test JVM met
     * (pre-push runs every module's tests on four cores). Such a walk, and only one that waited out
     * the whole grace, is read once more, as a visit's next pass reads it: its refusal signs in. A
     * walk that did not wait, or a read that never signs in, still fails. The events are the last walk's.
     */
    private suspend fun pageThroughSignIn(
        router: Router,
        peer: GatedPeer,
    ): Pair<PagedFetchResult, List<Event>> {
        val reads = WaivedReads(ClientRelayReads(router.client), router.waivers)
        val got = mutableListOf<Event>()
        val started = TimeSource.Monotonic.markNow()
        val walked = reads.page(url, ask, 5_000) { got += it }
        if (walked.end != PagedFetchResult.End.AUTH_REQUIRED || started.elapsedNow().inWholeMilliseconds < DEFAULT_AUTH_GRACE_MS) return walked to got
        assertEquals(0, peer.auths.get(), "an AUTH that landed should have let the first walk read through")
        got.clear()
        return reads.page(url, ask, 5_000) { got += it } to got
    }

    @Test
    fun `a relay that asks for AUTH without naming a waiver is signed in to`() =
        withRouter({ GatedPeer(it, cards, membersOnly = true, gated = false) }) { router, peer, _ ->
            val (_, got) = pageThroughSignIn(router, peer)
            assertEquals(1, peer.auths.get(), "signed in once, after the refusal")
            assertEquals(cards.size, got.size, "and read in full")
            assertTrue(!router.waivers.waives(url), "no waiver was asked for")
        }

    /** NIP-67: an EOSE carrying `auth` says the relay holds more for a signed-in reader, and names no waiver. */
    @Test
    fun `an EOSE that hints at AUTH signs in and the page is read in full`() =
        withRouter({ GatedPeer(it, cards, gated = false, authHint = true) }) { router, peer, _ ->
            val (walked, got) = pageThroughSignIn(router, peer)
            assertEquals(1, peer.auths.get(), "signed in on the hint")
            assertEquals(cards.size, got.size, "and served what it held back")
            assertTrue(VisitPool.readToTheEnd(walked), "the walk is not an AUTH_REQUIRED abort: ${walked.end}")
            assertTrue(!router.waivers.waives(url), "no waiver was named")
        }

    @Test
    fun `a NEG-ERR asking for AUTH signs in too`() =
        withRouter({ GatedPeer(it, cards, membersOnly = true, gated = false) }) { router, peer, _ ->
            runCatching { ClientRetractionReads(router.client, router.waivers).reconcileIds(url, ask, emptyList()) }
            while (peer.auths.get() == 0) delay(20)
            val diff = ClientRetractionReads(router.client, router.waivers).reconcileIds(url, ask, emptyList())
            assertEquals(cards.size, diff.needIds.size)
        }

    /** One enforcing retraction audit of [store] against the peer, after a first that may be refused. */
    private suspend fun audit(
        router: Router,
        store: NostrSemanticsStore,
        scope: CoroutineScope,
    ): RetractionAudit {
        val audit =
            RetractionAudit(
                ClientRetractionReads(router.client, router.waivers),
                store,
                SyncBands(null),
                IngestPipeline(store, IngestTuning(concurrency = 1, batch = 16), null, null, scope, null, null).also { it.start() },
                RefusedIds.disabled(),
            )
        repeat(2) { audit.reconcileAndDelete(stream, url, ask, emptySet(), verdictWaitMs = 1_000) }
        return audit
    }

    private suspend fun holding(events: List<Event>) = NostrSemanticsStore(InMemoryEventIndex()).also { it.batchInsert(events) }

    @Test
    fun `a signed-in un-waived empty answer deletes nothing and pushes nothing`() =
        withRouter({ GatedPeer(it, cards, membersOnly = true) }) { router, peer, scope ->
            val store = holding(cards)
            val audit = audit(router, store, scope)
            assertTrue(peer.auths.get() >= 1, "the premise: signed in for the members-only refusal")
            assertTrue(router.waivers.mayBeLensed(url), "and no waiver was ever named")
            assertEquals(0L, audit.deleted.get(), "the peer answered our key through its lens: nothing")
            assertTrue(audit.withheld.get() >= 1)
            assertEquals(cards.size, store.query<Event>(ask).size, "every card is still here")

            val push = UpstreamPush(router.client, store, 60, Semaphore(1), scope, router.waivers)
            push.pass(listOf(SyncUpstream(stream.name, url, ask, trusted = true)))
            assertEquals(0L, push.pushed.get(), "nor is it pushed the corpus")
            assertEquals(1L, push.withheld.get())
            assertTrue(peer.published.isEmpty())
        }

    @Test
    fun `a small retraction a waived reconcile shows is still deleted`() =
        withRouter({ GatedPeer(it, cards.drop(1)) }) { router, peer, scope ->
            val store = holding(cards)
            val audit = audit(router, store, scope)
            assertEquals(0, peer.auths.get())
            assertEquals(1L, audit.deleted.get(), "the one card the provider retracted")
            assertEquals(cards.size - 1, store.query<Event>(ask).size)
        }

    @Test
    fun `an empty upstream that reads us anonymously is pushed everything`() =
        withRouter({ GatedPeer(it, emptyList(), gated = false) }) { router, peer, scope ->
            val store = holding(cards)
            val push = UpstreamPush(router.client, store, 60, Semaphore(1), scope, router.waivers)
            push.pass(listOf(SyncUpstream(stream.name, url, ask, trusted = true)))
            assertEquals(cards.size.toLong(), push.pushed.get(), "a new relay is seeded")
            assertEquals(0L, push.withheld.get())
        }

    /** [n] cards, enough by-id batches for quartz's DownloadPace to give up on a relay that answers none. */
    private fun manyCards(n: Int) = (1..n).map { k -> Event("%064x".format(k + 10_000), provider, nowSeconds() - 7_200 + k % 3_600, 30382, arrayOf(arrayOf("d", k.toString(16).padStart(64, '0'))), "", "b2".repeat(64)) }

    private suspend fun waivedReconcile(
        router: Router,
        search: String,
        onEvent: (Event) -> Unit = {},
    ) = ClientWindowSync(router.client, FilterWidths(), idleTimeoutMs = 300, waivers = router.waivers)
        .reconcile(url, ask.copy(search = search), NegentropyLocalIndex.of(emptyList<IdAndTime>()), 100_000, null, {}) { onEvent(it) }

    /** Signed in, waived and searching: the reconcile fetches its own ids, and must fail as quartz's pipeline does when they never come. */
    @Test
    fun `a waived reconcile whose ids never come claims nothing`() =
        withRouter({ GatedPeer(it, manyCards(3_500), membersOnly = true, answersIds = false) }) { router, peer, _ ->
            router.waivers.learn(url, "auth-required: this relay answers through a web of trust. Sign in, or ask with `include:spam`.")
            WaivedReads(ClientRelayReads(router.client), router.waivers).page(url, ask, 5_000) {}
            assertTrue(peer.auths.get() >= 1, "the premise: signed in")
            val failed = runCatching { waivedReconcile(router, "bitcoin") }.exceptionOrNull()
            assertTrue(failed is NegentropySyncException, "the window must not be claimed: got $failed")
            assertEquals(NegentropySyncException.Failure.DOWNLOADS_FAILED, failed.failure)
        }

    @Test
    fun `a waived reconcile fetches its ids side by side`() =
        withRouter({ GatedPeer(it, manyCards(1_500), membersOnly = true, idsDelayMs = 100) }) { router, peer, _ ->
            router.waivers.learn(url, "auth-required: this relay answers through a web of trust. Sign in, or ask with `include:spam`.")
            WaivedReads(ClientRelayReads(router.client), router.waivers).page(url, ask, 5_000) {}
            var got = 0
            val result = waivedReconcile(router, "bitcoin") { got++ }
            assertEquals(1_500, got)
            assertEquals(1_500, result.downloaded)
            assertTrue(peer.mostIdsAtOnce.get() > 1, "three batches, fetched one at a time")
        }

    /** Anonymously quartz's own fetch re-asks a refused bare-id REQ with the search, so nothing of ours is needed. */
    @Test
    fun `an anonymous waived reconcile of a searching stream is read in full by quartz's own fetch`() =
        withRouter({ GatedPeer(it, cards) }) { router, peer, _ ->
            router.waivers.learn(url, "auth-required: this relay answers through a web of trust. Sign in, or ask with `include:spam`.")
            var got = 0
            val result = waivedReconcile(router, "bitcoin") { got++ }
            assertEquals(cards.size, got)
            assertEquals(cards.size, result.downloaded)
            assertEquals(0, peer.auths.get())
        }

    /**
     * A relay that wants NIP-42 to take events, and reads to anyone: the push's own EVENT is what signs
     * us in, and that says nothing about how the relay reads us, so its next reconcile is believed.
     */
    @Test
    fun `an upstream that signed us in to write is still pushed its backfill`() =
        withRouter({ GatedPeer(it, emptyList(), gated = false, membersWrite = true) }) { router, peer, scope ->
            val store = holding(cards)
            val push = UpstreamPush(router.client, store, 60, Semaphore(1), scope, router.waivers)
            val target = SyncUpstream(stream.name, url, ask, trusted = true)
            push.pass(listOf(target))
            assertTrue(peer.auths.get() >= 1, "the premise: the EVENT's refusal signed us in")
            assertTrue(!router.waivers.mayBeLensed(url), "a write refusal says nothing of how the relay reads us")
            push.pass(listOf(target))
            assertEquals(0L, push.withheld.get(), "the backfill is not held back")
            assertEquals(cards.size, peer.published.distinct().size, "every card reached the upstream")
        }

    /**
     * A stream whose search has words: quartz fetches a reconcile's ids with its search only when that
     * search has none, and a signed-in, un-waived ids REQ to a gated peer is answered empty.
     */
    @Test
    fun `a waived reconcile of a searching stream fetches its ids with the waiver`() =
        withRouter({ GatedPeer(it, cards, membersOnly = true) }) { router, peer, _ ->
            router.waivers.learn(url, "auth-required: this relay answers through a web of trust. Sign in, or ask with `include:spam`.")
            WaivedReads(ClientRelayReads(router.client), router.waivers).page(url, ask, 5_000) {}
            assertTrue(peer.auths.get() >= 1, "the premise: signed in, so an un-waived read is answered empty, not refused")
            val got = mutableListOf<Event>()
            val result =
                ClientWindowSync(router.client, FilterWidths(), idleTimeoutMs = 5_000, waivers = router.waivers)
                    .reconcile(url, ask.copy(search = "bitcoin"), NegentropyLocalIndex.of(emptyList<IdAndTime>()), 1_000, null, {}) { got += it }
            assertEquals(cards.size, result.needCount)
            assertEquals(cards.size, got.size, "every id the reconcile named was fetched")
            assertEquals(cards.size, result.downloaded)
            assertEquals(0, result.skipped)
        }
}
