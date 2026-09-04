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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The fingerprint walk: it pages `until` backwards so each relay's own cap is
 * the page size, driven here against a [Fake] that caps every REQ.
 */
class AliasProbeTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private val signer = NostrSignerSync()

    /**
     * A relay with [total] events one second apart, never returning more than
     * [cap] per REQ and honouring `until` inclusively, which makes the boundary re-read real.
     */
    private inner class Fake(
        val total: Int,
        val cap: Int = Int.MAX_VALUE,
        val refuseOver: Int? = null,
        // Refuses a bare filter with `CLOSED blocked: can't handle empty filters`.
        val demandsKinds: Boolean = false,
        // Turns our credentials down on every ask; it answers, and no filter changes that.
        val refusesCredentials: Boolean = false,
        // Serves a partial window with the refusal flagged on the same page.
        val servesThenRefuses: Boolean = false,
        // The same relay [newerBy] events later, a firehose seen on a later dial.
        val newerBy: Int = 0,
    ) {
        val asks = mutableListOf<Pair<Int, Long?>>()
        private val events: List<Event> =
            (-newerBy until total).map { signer.sign(BASE - it, 1, emptyArray(), "e$it") }

        val kindsAsked = mutableListOf<List<Int>?>()

        suspend fun fetch(
            @Suppress("UNUSED_PARAMETER") at: NormalizedRelayUrl,
            want: Int,
            until: Long?,
            kinds: List<Int>?,
        ): AliasProbe.Page {
            asks += want to until
            kindsAsked += kinds
            if (refusesCredentials) return AliasProbe.Page(emptyList(), authRefused = true)
            if (servesThenRefuses) {
                return AliasProbe.Page(
                    events.filter { until == null || it.createdAt <= until }.take(minOf(want, cap)),
                    authRefused = true,
                )
            }
            if (demandsKinds && kinds == null) return AliasProbe.Page(emptyList())
            // A relay that enforces its cap answers an over-large ask with nothing at all.
            if (refuseOver != null && want > refuseOver) return AliasProbe.Page(emptyList())
            return AliasProbe.Page(
                events
                    .filter { until == null || it.createdAt <= until }
                    .take(minOf(want, cap)),
            )
        }

        /** The ids this relay holds above [ts], which an anchor is there to exclude. */
        fun idsNewerThan(ts: Long): Set<String> = events.filter { it.createdAt > ts }.mapTo(HashSet()) { it.id }
    }

    private fun probe(
        fake: Fake,
        target: Int,
        page: Int = 500,
    ) = AliasProbe(fetch = fake::fetch, target = target, page = page, fallbackPage = 100)

    // `: Unit` is load-bearing: `assertNotNull` returns its value, and JUnit 5
    // silently skips a non-void @Test.
    @Test
    fun `the read latency is the FIRST page, not the walk it took to reach the target`(): Unit =
        runBlocking {
            // `rtt-read` is a round trip; timing the walk billed a relay's cap as its latency.
            val capped = Fake(total = 5_000, cap = 10)
            val walked = probe(capped, target = 40).window(url, null, null) {}
            assertTrue(capped.asks.size > 1, "the fixture has to actually page, or it tests nothing")
            assertNotNull(walked.firstPageMs)

            val single = Fake(total = 5_000, cap = 500)
            val once = probe(single, target = 40).window(url, null, null) {}
            assertEquals(1, single.asks.size)
            assertNotNull(once.firstPageMs)
        }

    @Test
    fun `a url that never spoke reports no read latency rather than an instant one`() =
        runBlocking {
            // A zero would rank a dead host as the fastest in the store.
            val silent = AliasProbe(fetch = { _, _, _, _ -> AliasProbe.Page(events = null, reason = "cannot:timeout") })
            val window = silent.window(url, null, null) {}

            assertNull(window.ids, "the fixture is a url that never answered")
            assertNull(window.firstPageMs)
        }

    @Test
    fun `a relay capping every REQ still yields the full depth`() =
        runBlocking {
            val fake = Fake(total = 5_000, cap = 500)

            val print = probe(fake, target = 1_000).fingerprint(url) {}

            assertEquals(1_000, print?.size)
            assertTrue(fake.asks.size > 1, "a capped relay must be paged, not accepted at its cap")
        }

    @Test
    fun `at the shipped defaults a full-page relay costs one round trip`() =
        runBlocking {
            val fake = Fake(total = 50_000, cap = RelayAliases.DEFAULT_PROBE_PAGE)
            val probe = AliasProbe(fetch = fake::fetch)

            val print = probe.fingerprint(url, BASE) {}

            assertEquals(RelayAliases.DEFAULT_PROBE_TARGET, print?.size)
            assertEquals(1, fake.asks.size, "a relay that serves a full page must not be paged twice")
        }

    @Test
    fun `a relay capping below the page still reaches the same depth`() =
        runBlocking {
            // A low cap becomes the page size, not the depth.
            val fake = Fake(total = 50_000, cap = 100, refuseOver = 100)
            val probe = AliasProbe(fetch = fake::fetch)

            val print = probe.fingerprint(url, BASE, AliasProbe.FALLBACK_KINDS) {}

            assertEquals(RelayAliases.DEFAULT_PROBE_TARGET, print?.size)
            assertTrue(fake.asks.size > 1, "a capped relay is paged to depth, not accepted at its cap")
        }

    @Test
    fun `the walk stops at the target rather than draining the relay`() =
        runBlocking {
            val fake = Fake(total = 100_000, cap = 500)

            assertEquals(1_000, probe(fake, target = 1_000).fingerprint(url) {}?.size)
            // Two full pages plus the boundary re-read.
            assertTrue(fake.asks.size <= 4, "walked ${fake.asks.size} pages for 1,000 ids")
        }

    @Test
    fun `a relay holding less than the target returns what it has`() =
        runBlocking {
            val fake = Fake(total = 137, cap = 500)

            // The target is a ceiling on effort, not a requirement.
            assertEquals(137, probe(fake, target = 1_000).fingerprint(url) {}?.size)
        }

    @Test
    fun `an ask over the relay's cap is retried at the smaller page`() =
        runBlocking {
            // Target over the page size, so the first ask is a full page rather than a trimmed one.
            val fake = Fake(total = 1_000, cap = 100, refuseOver = 100)

            val print = probe(fake, target = 1_000).fingerprint(url) {}

            assertEquals(1_000, print?.size)
            assertEquals(500, fake.asks.first().first, "the first ask is the normal page")
            assertEquals(100, fake.asks[1].first, "and the second drops to the fallback")
            assertTrue(fake.asks.drop(1).all { it.first <= 100 }, "the smaller page sticks for the rest of the walk")
        }

    @Test
    fun `the cursor walks backwards, never repeating the same window`() =
        runBlocking {
            val fake = Fake(total = 2_000, cap = 500)

            probe(fake, target = 1_000).fingerprint(url) {}

            val cursors = fake.asks.mapNotNull { it.second }
            assertEquals(cursors.sortedDescending(), cursors, "until must move monotonically older")
            assertEquals(cursors.distinct(), cursors, "a repeated cursor is a walk that cannot end")
        }

    @Test
    fun `a url that cannot be asked at all stays null, never empty`() =
        runBlocking {
            val probe = AliasProbe(fetch = { _, _, _, _ -> AliasProbe.Page(null) }, target = 1_000)

            // Null is what stops [RelayAliases] folding it; empty would claim the relay holds nothing.
            assertNull(probe.fingerprint(url) {})
        }

    @Test
    fun `a walk cut short mid-way keeps what it already proved`() =
        runBlocking {
            var calls = 0
            val fake = Fake(total = 5_000, cap = 500)
            val probe =
                AliasProbe(
                    fetch = { u, want, until, kinds -> if (calls++ == 0) fake.fetch(u, want, until, kinds) else AliasProbe.Page(null) },
                    target = 1_000,
                )

            assertEquals(500, probe.fingerprint(url) {}?.size)
        }

    @Test
    fun `a relay that really is empty answers empty, not null`() =
        runBlocking {
            assertEquals(emptySet(), probe(Fake(total = 0), target = 1_000).fingerprint(url) {})
        }

    @Test
    fun `a relay stuck on one timestamp cannot page forever`() =
        runBlocking {
            var asks = 0
            // Every event shares a created_at, so an inclusive `until` re-reads the same window.
            val same: List<Event> = (0 until 10).map { signer.sign(BASE, 1, emptyArray(), "same$it") }
            val probe =
                AliasProbe(fetch = { _, _, _, _ ->
                    asks++
                    AliasProbe.Page(same)
                }, target = 1_000)

            assertEquals(10, probe.fingerprint(url) {}?.size)
            assertTrue(asks < AliasProbe.DEFAULT_MAX_PAGES, "gave up after $asks pages")
        }

    @Test
    fun `a shared anchor makes two walks of a busy relay comparable`() =
        runBlocking {
            // The second walk of one relay happens after 400 new events have landed.
            val first = Fake(total = 5_000, cap = 500)
            val later = Fake(total = 5_000, cap = 500, newerBy = 400)

            val anchor = BASE - 10
            val a = probe(first, target = 1_000).fingerprint(url, anchor) {}!!
            val b = probe(later, target = 1_000).fingerprint(url, anchor) {}!!

            assertEquals(a, b)
        }

    @Test
    fun `an anchor a minute back settles a relay still taking writes`() =
        runBlocking {
            // Same anchor, but the first walk runs before the relay has indexed its last
            // few seconds; modelled as the second dial holding events the first did not.
            val now = BASE
            val settling = Fake(total = 5_000, cap = 500)
            val settled = Fake(total = 5_000, cap = 500, newerBy = 30)

            val anchor = AliasProbe.settledAnchor(now)
            val a = probe(settling, target = 1_000).fingerprint(url, anchor) {}!!
            val b = probe(settled, target = 1_000).fingerprint(url, anchor) {}!!

            assertEquals(a, b)
            assertEquals(60L, now - anchor, "the anchor is a minute behind the clock")
        }

    @Test
    fun `nothing newer than the anchor reaches the fingerprint`() =
        runBlocking {
            val fake = Fake(total = 1_000, cap = 500, newerBy = 200)
            val anchor = BASE // the 200 `newerBy` events are all above this

            val print = probe(fake, target = 1_000).fingerprint(url, anchor) {}!!
            val above = fake.idsNewerThan(anchor)

            assertTrue(above.isNotEmpty(), "the fake must actually hold events above the anchor")
            assertTrue(above.none { it in print }, "an event above the anchor is not part of the window")
        }

    @Test
    fun `without an anchor the same busy relay drifts apart`() =
        runBlocking {
            val first = Fake(total = 5_000, cap = 500)
            val later = Fake(total = 5_000, cap = 500, newerBy = 400)

            val a = probe(first, target = 1_000).fingerprint(url) {}!!
            val b = probe(later, target = 1_000).fingerprint(url) {}!!

            // 400 of each window is the other's, so containment lands at 0.6.
            val shared = a.count { it in b }
            assertTrue(shared < a.size, "unanchored walks of a moving relay must NOT be identical")
            assertEquals(600, shared, "400 new events shift the window by exactly that many")
        }

    @Test
    fun `a relay refusing bare filters is asked with kinds instead`() =
        runBlocking {
            val fake = Fake(total = 2_000, cap = 500, demandsKinds = true)

            val lead = probe(fake, target = 1_000).leaderPrint(url, BASE) {}.leader

            assertEquals(1_000, lead?.ids?.size)
            assertEquals(AliasProbe.FALLBACK_KINDS, lead?.kinds, "the group must be told which filter worked")
            assertEquals(null, fake.kindsAsked.first(), "a bare filter is still tried first")
        }

    @Test
    fun `a leader that answers bare reports no kinds, so the group stays bare`() =
        runBlocking {
            val fake = Fake(total = 2_000, cap = 500)

            val lead = probe(fake, target = 1_000).leaderPrint(url, BASE) {}.leader

            assertEquals(1_000, lead?.ids?.size)
            assertNull(lead?.kinds)
            assertTrue(fake.kindsAsked.all { it == null }, "nothing narrows a filter the relay never objected to")
        }

    /**
     * khatru in NIP-29 groups mode: refuses any query not scoped to a group and
     * serves its group list to everyone. A refusal is an answer, so it reaches
     * the walk as an empty page and not as silence.
     */
    private inner class Groups(
        groups: Int,
    ) {
        val kindsAsked = mutableListOf<List<Int>?>()
        private val events: List<Event> =
            (0 until groups).map { signer.sign(BASE - it, 39_000, emptyArray(), "g$it") }

        suspend fun fetch(
            @Suppress("UNUSED_PARAMETER") at: NormalizedRelayUrl,
            want: Int,
            until: Long?,
            kinds: List<Int>?,
        ): AliasProbe.Page {
            kindsAsked += kinds
            if (kinds != RelayAliases.GROUP_METADATA_KINDS) return AliasProbe.Page(emptyList())
            return AliasProbe.Page(events.filter { until == null || it.createdAt <= until }.take(want))
        }
    }

    @Test
    fun `a NIP-29 relay that refuses every general filter is still fingerprinted`() =
        runBlocking {
            val fake = Groups(groups = 55)

            val lead = AliasProbe(fetch = fake::fetch).leaderPrint(url, BASE) {}.leader

            assertEquals(55, lead?.ids?.size)
            assertEquals(RelayAliases.GROUP_METADATA_KINDS, lead?.kinds, "the group must be told which filter worked")
            assertEquals(
                listOf(null, AliasProbe.FALLBACK_KINDS, RelayAliases.GROUP_METADATA_KINDS),
                fake.kindsAsked.distinct(),
                "the general filters must be tried first, and group metadata only after both are refused",
            )
        }

    @Test
    fun `a refused credential stops the ladder at the first ask`() =
        runBlocking {
            // A refused credential is not a complaint about the filter, so the ladder has nothing left to find.
            val fake = Fake(total = 5_000, refusesCredentials = true)

            val attempt = AliasProbe(fetch = fake::fetch).leaderPrint(url, BASE) {}

            assertNull(attempt.leader, "a relay that refuses us has no window to give")
            assertTrue(attempt.spoke, "a refusal is the relay ANSWERING, and the fold turns on that")
            assertEquals(1, fake.asks.size, "the ladder asked again after being refused: ${fake.kindsAsked}")
        }

    @Test
    fun `a refused credential also ends the WALK, not just the ladder`() =
        runBlocking {
            // [AliasFolding] dials members through `fingerprint`, never `leaderPrint`, so the stop belongs in the walk.
            val fake = Fake(total = 5_000, refusesCredentials = true)

            val print = AliasProbe(fetch = fake::fetch).fingerprint(url, BASE, AliasProbe.FALLBACK_KINDS) {}

            // Empty rather than null: the relay answered.
            assertEquals(emptySet(), print)
            assertEquals(1, fake.asks.size, "the walk retried at the smaller page after being refused")
        }

    @Test
    fun `a page carrying both a window and a refusal keeps the window`() =
        runBlocking {
            // A partial window is still a fingerprint; the refusal ends the walk only when nothing came with it.
            val fake = Fake(total = 5_000, cap = 60, servesThenRefuses = true)

            val attempt = AliasProbe(fetch = fake::fetch, target = 1_000).leaderPrint(url, BASE) {}

            assertEquals(60, attempt.leader?.ids?.size, "a window arriving beside a refusal was thrown away")
            assertNull(attempt.leader?.kinds, "the bare filter answered, so the group must stay bare")
            assertEquals(1, fake.asks.size, "the refusal must still stop the walk after taking the window")
        }

    @Test
    fun `a url that never spoke is not asked for group metadata`() =
        runBlocking {
            // Null is our transport giving up. The attempts are sequential and
            // AliasFolding.YARDSTICK_ATTEMPTS multiplies them, so a third ask is dear on a Tor window.
            val asked = mutableListOf<List<Int>?>()
            val probe =
                AliasProbe(fetch = { _, _, _, kinds ->
                    asked += kinds
                    AliasProbe.Page(null)
                }, target = 1_000)

            assertNull(probe.leaderPrint(url, BASE) {}.leader)
            assertEquals(listOf(null, AliasProbe.FALLBACK_KINDS), asked, "silence bought a third dial")
        }

    @Test
    fun `a relay that refuses everything and holds nothing still yields no filter`() =
        runBlocking {
            val fake = Groups(groups = 0)

            assertNull(AliasProbe(fetch = fake::fetch).leaderPrint(url, BASE) {}.leader)
            assertTrue(
                RelayAliases.GROUP_METADATA_KINDS in fake.kindsAsked,
                "a refusal is an answer, so the third rung is owed its ask",
            )
        }

    @Test
    fun `a leader that answers nothing either way yields no group filter at all`() =
        runBlocking {
            val probe = AliasProbe(fetch = { _, _, _, _ -> AliasProbe.Page(null) }, target = 1_000)

            assertNull(probe.leaderPrint(url, BASE) {}.leader)
        }

    @Test
    fun `everything downloaded reaches ingest before it is counted`() =
        runBlocking {
            val seen = mutableListOf<String>()
            val fake = Fake(total = 700, cap = 500)

            val print = probe(fake, target = 1_000).fingerprint(url) { seen += it.id }

            // The probe is a sync that also identifies.
            assertEquals(700, print?.size)
            assertEquals(700, seen.toSet().size)
        }

    private companion object {
        const val BASE = 1_700_000_000L
    }
}
