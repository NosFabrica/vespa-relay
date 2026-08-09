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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The fingerprint walk. A single REQ measures a relay's LIMIT rather than the
 * relay — measured across 60 live hosts, `max_limit` is 500 on half of those
 * advertising one and 100, 1024, 2100, 10000 or 0 on the rest — so the probe
 * pages `until` backwards and lets each relay's cap be the page size.
 *
 * Every test drives a fake relay holding [Fake.total] events on descending
 * timestamps, capping each REQ at [Fake.cap], so the walk is exercised against
 * exactly the behaviour the live ones showed.
 */
class AliasProbeTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private val signer = NostrSignerSync()

    /**
     * A relay with [total] events, one per second descending from a fixed
     * moment, that never returns more than [cap] per REQ and honours `until`
     * inclusively — which is what makes the boundary re-read real.
     */
    private inner class Fake(
        val total: Int,
        val cap: Int = Int.MAX_VALUE,
        val refuseOver: Int? = null,
        // Refuses a bare filter the way 46 of 229 sweep hosts did:
        // `CLOSED blocked: can't handle empty filters`.
        val demandsKinds: Boolean = false,
        // The same relay, [newerBy] events later — what a firehose looks like
        // by the time the second url of a group gets its turn at the gate.
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
        ): List<Event> {
            asks += want to until
            kindsAsked += kinds
            if (demandsKinds && kinds == null) return emptyList()
            // A relay that ENFORCES its cap answers nothing at all, which is
            // what purplepag.es does to an over-large ask.
            if (refuseOver != null && want > refuseOver) return emptyList()
            return events
                .filter { until == null || it.createdAt <= until }
                .take(minOf(want, cap))
        }

        /** The ids this relay holds above [ts] — what an anchor is there to exclude. */
        fun idsNewerThan(ts: Long): Set<String> = events.filter { it.createdAt > ts }.mapTo(HashSet()) { it.id }
    }

    private fun probe(
        fake: Fake,
        target: Int,
        page: Int = 500,
    ) = AliasProbe(fetch = fake::fetch, target = target, page = page, fallbackPage = 100)

    @Test
    fun `a relay capping every REQ still yields the full depth`() =
        runBlocking {
            // The live case: nos.lol answered a 1,000 ask with exactly 500.
            val fake = Fake(total = 5_000, cap = 500)

            val print = probe(fake, target = 1_000).fingerprint(url) {}

            assertEquals(1_000, print?.size)
            assertTrue(fake.asks.size > 1, "a capped relay must be paged, not accepted at its cap")
        }

    @Test
    fun `at the shipped defaults a full-page relay costs one round trip`() =
        runBlocking {
            // The whole reason the target matches the page size. Measured, 500
            // decides the same folds as 1,000 at 1.4s and 562KB instead of
            // 3.4s and 1,464KB — and that saving is exactly this: one REQ.
            val fake = Fake(total = 50_000, cap = RelayAliases.DEFAULT_PROBE_PAGE)
            val probe = AliasProbe(fetch = fake::fetch)

            val print = probe.fingerprint(url, BASE) {}

            assertEquals(RelayAliases.DEFAULT_PROBE_TARGET, print?.size)
            assertEquals(1, fake.asks.size, "a relay that serves a full page must not be paged twice")
        }

    @Test
    fun `a relay capping below the page still reaches the same depth`() =
        runBlocking {
            // The other half: a low cap becomes the page size, not the depth,
            // so this relay is measured at exactly the depth every other one is.
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
            // Two full pages plus the boundary re-read, not 200 pages.
            assertTrue(fake.asks.size <= 4, "walked ${fake.asks.size} pages for 1,000 ids")
        }

    @Test
    fun `a relay holding less than the target returns what it has`() =
        runBlocking {
            val fake = Fake(total = 137, cap = 500)

            // A short walk is a fine answer — the target is a ceiling on
            // effort, not a requirement.
            assertEquals(137, probe(fake, target = 1_000).fingerprint(url) {}?.size)
        }

    @Test
    fun `an ask over the relay's cap is retried at the smaller page`() =
        runBlocking {
            // Refuses anything over 100 outright, the shape of
            // `CLOSED blocked: limit too high`. Target over the page size, so
            // the first ask really is a full page rather than a trimmed one.
            val fake = Fake(total = 1_000, cap = 100, refuseOver = 100)

            val print = probe(fake, target = 1_000).fingerprint(url) {}

            // The refusal cost one round trip, not the fold: the walk drops to
            // the smaller page and still reaches the full depth.
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
            val probe = AliasProbe(fetch = { _, _, _, _ -> null }, target = 1_000)

            // Null is what stops [RelayAliases] folding it. An empty set would
            // be an assertion that the relay holds nothing.
            assertNull(probe.fingerprint(url) {})
        }

    @Test
    fun `a walk cut short mid-way keeps what it already proved`() =
        runBlocking {
            var calls = 0
            val fake = Fake(total = 5_000, cap = 500)
            val probe =
                AliasProbe(
                    fetch = { u, want, until, kinds -> if (calls++ == 0) fake.fetch(u, want, until, kinds) else null },
                    target = 1_000,
                )

            // 500 real ids beat none: the transport gave up, the measurement
            // did not.
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
            // Every event shares a created_at, so an inclusive `until` re-reads
            // the same window however far the cursor is stepped.
            val same: List<Event> = (0 until 10).map { signer.sign(BASE, 1, emptyArray(), "same$it") }
            val probe =
                AliasProbe(fetch = { _, _, _, _ ->
                    asks++
                    same
                }, target = 1_000)

            assertEquals(10, probe.fingerprint(url) {}?.size)
            assertTrue(asks < AliasProbe.DEFAULT_MAX_PAGES, "gave up after $asks pages")
        }

    @Test
    fun `a shared anchor makes two walks of a busy relay comparable`() =
        runBlocking {
            // One relay, dialled through two urls — but the second walk happens
            // after 400 new events have landed. That is the ordinary case
            // behind a 16-permit gate on a firehose: measured live, nos.lol
            // and nos.lol/cipher-zulu scored 0.41 unanchored.
            val first = Fake(total = 5_000, cap = 500)
            val later = Fake(total = 5_000, cap = 500, newerBy = 400)

            val anchor = BASE - 10
            val a = probe(first, target = 1_000).fingerprint(url, anchor) {}!!
            val b = probe(later, target = 1_000).fingerprint(url, anchor) {}!!

            // Anchored, both walks read the same slice of the timeline, so the
            // relay being busy in between changes nothing.
            assertEquals(a, b)
        }

    @Test
    fun `an anchor a minute back settles a relay still taking writes`() =
        runBlocking {
            // The case a shared anchor alone does NOT fix: both walks anchor at
            // the same instant, but the first runs before the relay has
            // finished indexing the last few seconds and the second runs after.
            // Modelled as the second dial simply holding events the first did
            // not — same timestamps, later visibility.
            val now = BASE
            val settling = Fake(total = 5_000, cap = 500)
            val settled = Fake(total = 5_000, cap = 500, newerBy = 30)

            val anchor = AliasProbe.settledAnchor(now)
            val a = probe(settling, target = 1_000).fingerprint(url, anchor) {}!!
            val b = probe(settled, target = 1_000).fingerprint(url, anchor) {}!!

            // Everything the late arrivals could have disturbed is above the
            // anchor, so neither walk looks at it and the two agree exactly.
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

            // The regression the anchor exists to remove: 400 of each window is
            // the other's, so containment lands at 0.6 and a busier relay or a
            // slower gate pushes it under the fold's bar entirely.
            val shared = a.count { it in b }
            assertTrue(shared < a.size, "unanchored walks of a moving relay must NOT be identical")
            assertEquals(600, shared, "400 new events shift the window by exactly that many")
        }

    @Test
    fun `a relay refusing bare filters is asked with kinds instead`() =
        runBlocking {
            // 46 of 229 hosts in the full-corpus sweep answered a bare filter
            // with `CLOSED blocked: can't handle empty filters`, taking 892
            // urls out of the fold. Every one retried answered a kinds filter.
            val fake = Fake(total = 2_000, cap = 500, demandsKinds = true)

            val lead = probe(fake, target = 1_000).leaderPrint(url, BASE) {}

            assertEquals(1_000, lead?.ids?.size)
            assertEquals(AliasProbe.FALLBACK_KINDS, lead?.kinds, "the group must be told which filter worked")
            assertEquals(null, fake.kindsAsked.first(), "a bare filter is still tried first")
        }

    @Test
    fun `a leader that answers bare reports no kinds, so the group stays bare`() =
        runBlocking {
            val fake = Fake(total = 2_000, cap = 500)

            val lead = probe(fake, target = 1_000).leaderPrint(url, BASE) {}

            assertEquals(1_000, lead?.ids?.size)
            assertNull(lead?.kinds)
            assertTrue(fake.kindsAsked.all { it == null }, "nothing narrows a filter the relay never objected to")
        }

    @Test
    fun `a leader that answers nothing either way yields no group filter at all`() =
        runBlocking {
            // Neither shape works: the group cannot fold and must not be probed.
            val probe = AliasProbe(fetch = { _, _, _, _ -> null }, target = 1_000)

            assertNull(probe.leaderPrint(url, BASE) {})
        }

    @Test
    fun `everything downloaded reaches ingest before it is counted`() =
        runBlocking {
            val seen = mutableListOf<String>()
            val fake = Fake(total = 700, cap = 500)

            val print = probe(fake, target = 1_000).fingerprint(url) { seen += it.id }

            // The probe is a sync that also identifies: nothing it pulled is
            // thrown away to pay for the verdict.
            assertEquals(700, print?.size)
            assertEquals(700, seen.toSet().size)
        }

    private companion object {
        const val BASE = 1_700_000_000L
    }
}
