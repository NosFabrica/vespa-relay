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

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** The band arithmetic, and the cases where a stale band must not be believed. */
class SyncBandsTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")
    private val other = RelayUrlNormalizer.normalize("wss://other.example")
    private val profiles = Filter(kinds = listOf(0))
    private val mirror = "profiles"

    private fun now(): Long = System.currentTimeMillis() / 1000

    /** A walk the relay ended by EOSEing an empty page: a claim of absence. */
    private val drainedWalk = PagedFetchResult(10, PagedFetchResult.End.DRAINED)

    /** The same walk ended on silence: no claim. */
    private val idleWalk = PagedFetchResult(10, PagedFetchResult.End.IDLE)

    private fun tempFile(): File {
        val f = File.createTempFile("sync-bands", ".json")
        f.delete()
        return f
    }

    // ---- the band arithmetic ----------------------------------------------

    @Test
    fun `with nothing recorded the whole filter is fetched`() {
        val c = SyncBands(null)
        assertEquals(listOf(profiles), c.legs(mirror, relay, profiles))
    }

    @Test
    fun `a recorded band is fetched around, not through`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, observedMin = 1_700_001_000L, observedMax = 1_700_002_000L, paged = true)

        val legs = c.legs(mirror, relay, profiles)
        assertEquals(2, legs.size, "one leg older than the band, one newer")
        assertEquals(1_700_001_000L, legs[0].until, "older leg stops AT the band's floor")
        assertNull(legs[0].since, "and reaches as far back as the filter allows")
        assertEquals(1_700_002_000L, legs[1].since, "newer leg starts AT its ceiling")
        assertNull(legs[1].until)
    }

    @Test
    fun `an event sharing the band's boundary second is still reachable`() {
        // A page boundary can fall inside a run of same-second events, so the edge second is re-read.
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)

        val legs = c.legs(mirror, relay, profiles)

        fun reachable(t: Long) = legs.any { (it.since ?: Long.MIN_VALUE) <= t && t <= (it.until ?: Long.MAX_VALUE) }

        assertTrue(reachable(1_700_001_000L), "the band's own floor second must be re-read")
        assertTrue(reachable(1_700_002_000L), "and its ceiling second")
        assertTrue(reachable(1_700_000_999L), "below the band")
        assertTrue(reachable(1_700_002_001L), "above it")
        assertTrue(!reachable(1_700_001_500L), "the covered interior is not re-read")
    }

    @Test
    fun `successive runs widen the band rather than replacing it`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        c.record(mirror, relay, profiles, 1_700_000_500L, 1_700_002_500L, paged = true)

        val band = c.band(mirror, relay, profiles)!!
        assertEquals(1_700_000_500L, band.minCreatedAt)
        assertEquals(1_700_002_500L, band.maxCreatedAt)
    }

    @Test
    fun `a capped relay walks further back on each run`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_009_000L, 1_700_010_000L, paged = true)
        assertEquals(1_700_009_000L, c.legs(mirror, relay, profiles)[0].until)

        c.record(mirror, relay, profiles, 1_700_008_000L, 1_700_008_999L, paged = true)
        assertEquals(1_700_008_000L, c.legs(mirror, relay, profiles)[0].until)
    }

    // ---- when a cursor must not be used ------------------------------------

    @Test
    fun `a negentropy sync that reported no outcome records nothing`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = false)
        assertNull(c.band(mirror, relay, profiles))
        assertEquals(listOf(profiles), c.legs(mirror, relay, profiles))
    }

    // ---- coverage: what a finished reconcile earns -------------------------

    @Test
    fun `a finished reconcile is in sync through the instant it started`() {
        val c = SyncBands(null)
        val startedAt = now() - 60
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = false, reconciledThrough = startedAt)

        val band = c.band(mirror, relay, profiles)!!
        assertTrue(band.complete)
        assertEquals(startedAt, band.maxCreatedAt)
    }

    @Test
    fun `verifiedAt advances with every reconcile and survives a reboot, quartz's fullAt does neither`() {
        val f = tempFile()
        val c = SyncBands(f)
        assertNull(c.verifiedAt(mirror, relay, profiles), "no reconcile yet, no claim")
        val first = now() - 600
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = first)
        assertEquals(first, c.verifiedAt(mirror, relay, profiles))
        val second = now() - 60
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = second)
        assertEquals(second, c.verifiedAt(mirror, relay, profiles), "the SECOND reconcile advances the clock")
        // A paged record is a walk, not a verification, so the clock holds.
        c.record(mirror, relay, profiles, now() - 30, now(), paged = true)
        assertEquals(second, c.verifiedAt(mirror, relay, profiles))
        c.flush()
        val rebooted = SyncBands(f)
        assertEquals(second, rebooted.verifiedAt(mirror, relay, profiles), "the stamp rides the band file")
        assertNull(rebooted.verifiedAt(mirror, other, profiles), "per relay, like the band it belongs to")
        f.delete()
    }

    @Test
    fun `an ask that never had a verified pass is due on its first visit`() {
        // fullAt = 0 is quartz's never.
        assertTrue(SyncBands.auditDue(fullAt = 0L, now = 1_000_000, negentropySyncThePastSeconds = 604_800))
        val week = 604_800L
        val nowSec = 2_000_000L
        assertFalse(SyncBands.auditDue(fullAt = nowSec - week + 1, now = nowSec, negentropySyncThePastSeconds = week))
        assertTrue(SyncBands.auditDue(fullAt = nowSec - week, now = nowSec, negentropySyncThePastSeconds = week))
        assertTrue(SyncBands.auditDue(fullAt = nowSec - 2 * week, now = nowSec, negentropySyncThePastSeconds = week))
    }

    @Test
    fun `an audit that cannot complete is spaced, not retried on the revisit floor`() {
        // A quarter of the knob, floored at 15 minutes and capped at 6 hours.
        assertEquals(900L, SyncBands.attemptSpacingSeconds(3600L))
        assertEquals(21_600L, SyncBands.attemptSpacingSeconds(86_400L))
        assertEquals(21_600L, SyncBands.attemptSpacingSeconds(604_800L), "a weekly audit still retries within a shift")
    }

    @Test
    fun `claimAudit admits once, spaces the retry, and stands down when verified`() {
        val c = SyncBands(null)
        val negentropySyncThePastSeconds = 86_400L
        val t0 = now()
        assertTrue(c.claimAudit(mirror, relay, profiles, negentropySyncThePastSeconds, now = t0), "never verified: due, and the claim is taken")
        assertFalse(c.claimAudit(mirror, relay, profiles, negentropySyncThePastSeconds, now = t0 + 60), "inside the attempt spacing")
        assertTrue(
            c.claimAudit(mirror, relay, profiles, negentropySyncThePastSeconds, now = t0 + SyncBands.attemptSpacingSeconds(negentropySyncThePastSeconds)),
            "spacing lapsed and still unverified: retry",
        )
        val verifiedAt = t0 + 30_000
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = verifiedAt)
        assertFalse(c.claimAudit(mirror, relay, profiles, negentropySyncThePastSeconds, now = verifiedAt + negentropySyncThePastSeconds - 1), "verified: not due")
        assertTrue(c.claimAudit(mirror, relay, profiles, negentropySyncThePastSeconds, now = verifiedAt + negentropySyncThePastSeconds), "aged past the knob")
    }

    @Test
    fun `a reconcile that downloaded nothing still records coverage`() {
        val c = SyncBands(null)
        val startedAt = now() - 60
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = startedAt)

        val leg = c.legs(mirror, relay, profiles).single()
        assertEquals(startedAt, leg.since)
        assertNull(leg.until)
    }

    @Test
    fun `an empty reconcile carries the ceiling forward and leaves the floor alone`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = false, reconciledThrough = now() - 600)
        val first = c.band(mirror, relay, profiles)!!

        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now())
        val second = c.band(mirror, relay, profiles)!!

        assertEquals(1_700_001_000L, second.minCreatedAt, "the floor is pass 1's oldest event, not the empty pass's clock")
        assertTrue(second.maxCreatedAt > first.maxCreatedAt, "the ceiling advances to the moment we reconciled")
        assertTrue(second.complete)
    }

    @Test
    fun `a first contact that finds nothing on either side is still a row`() {
        // min == max at the clock: both edges readable, or `SyncCoverageReport` drops the row.
        val c = SyncBands(null)
        val startedAt = now()
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = startedAt)

        val band = c.band(mirror, relay, profiles)!!
        assertEquals(startedAt, band.minCreatedAt)
        assertEquals(startedAt, band.maxCreatedAt)
    }

    @Test
    fun `a complete band drops its older leg, a paged one keeps it`() {
        val reconciled = SyncBands(null)
        reconciled.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = 1_700_002_000L)
        val only = reconciled.legs(mirror, relay, profiles).single()
        assertEquals(1_700_002_000L, only.since)

        val walked = SyncBands(null)
        walked.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        assertEquals(2, walked.legs(mirror, relay, profiles).size, "a paged walk says nothing about what it never asked for")
    }

    // ---- the periodic full re-walk -----------------------------------------

    @Test
    fun `a band stops narrowing once it is older than the resync period`() {
        val c = SyncBands(null, refetchThePastSeconds = 60)
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now() - 3600)
        // Recorded 'now' whatever the created_at claim, so age it by rewriting.
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now())
        assertEquals(1, c.legs(mirror, relay, profiles).size, "fresh band still narrows")

        val stale = SyncBands(null, refetchThePastSeconds = 0)
        stale.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now())
        assertSame(profiles, stale.legs(mirror, relay, profiles).single(), "a band past its period re-walks everything")
    }

    @Test
    fun `no stream says so, nothing re-fetches — and the removed env names are refused`() {
        val quiet = SyncBands.fromEnv(emptyMap(), emptyList())
        quiet.use { assertEquals(SyncBands.NEVER, it.refetchThePastSecondsFor(mirror)) }

        // An old spelling is an error, not a no-op; ignored, it would drop a running schedule on upgrade.
        for (name in listOf("SYNC_REFETCH_THE_PAST_SECONDS", "SYNC_FULL_RESYNC_SECONDS", "ROUTER_FULL_RESYNC_SECONDS")) {
            assertFailsWith<IllegalArgumentException>("$name must be refused, not ignored") {
                SyncBands.fromEnv(mapOf(name to "604800"), emptyList())
            }
        }
    }

    @Test
    fun `a stream's own re-walk period beats the router's default`() {
        // A period of 0 stands in for an aged band; a hermetic test cannot wait a week.
        val c = SyncBands(null, refetchThePastSeconds = 0, perStream = mapOf("patient" to 86_400))
        c.record("patient", relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        c.record("eager", relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)

        assertEquals(2, c.legs("patient", relay, profiles).size, "a band inside its own period still narrows")
        assertSame(profiles, c.legs("eager", relay, profiles).single(), "the default still expires the other stream")
    }

    @Test
    fun `the re-walk replaces the old claim instead of widening it`() {
        val c = SyncBands(null, refetchThePastSeconds = 0)
        c.record(mirror, relay, profiles, 1_700_000_000L, 1_700_001_000L, paged = true)
        c.record(mirror, relay, profiles, 1_700_005_000L, 1_700_006_000L, paged = true)

        val band = c.band(mirror, relay, profiles)!!
        assertEquals(1_700_005_000L, band.minCreatedAt, "the second pass walked everything; its span is the whole picture")
    }

    // ---- the shared snapshot window ----------------------------------------

    @Test
    fun `covering window collapses to the oldest ceiling once everyone is caught up`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = 1_700_009_000L)
        c.record(mirror, other, profiles, null, null, paged = false, reconciledThrough = 1_700_003_000L)

        assertEquals(1_700_003_000L, c.coveringWindow(mirror, listOf(relay, other), profiles).since)
    }

    @Test
    fun `one relay that has never synced puts the window back to the whole filter`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = 1_700_009_000L)

        // Identity, since Filter has no equals.
        assertSame(profiles, c.coveringWindow(mirror, listOf(relay, other), profiles))
        assertSame(profiles, c.coveringWindow(mirror, emptyList(), profiles))
    }

    @Test
    fun `one shared window serves a whole stream of relays`() {
        val c = SyncBands(null)
        val third = RelayUrlNormalizer.normalize("wss://third.example")
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = 1_700_009_000L)
        c.record(mirror, other, profiles, null, null, paged = false, reconciledThrough = 1_700_003_000L)
        c.record(mirror, third, profiles, null, null, paged = false, reconciledThrough = 1_700_007_000L)

        assertEquals(1_700_003_000L, c.coveringWindow(mirror, listOf(relay, other, third), profiles).since)
    }

    @Test
    fun `a relay that needs nothing does not widen the shared window`() {
        // `legs()` is empty for a covered relay, and empty must not read as "needs everything".
        val capped = Filter(kinds = listOf(0), until = 1_700_005_000L)
        val c = SyncBands(null)
        c.record(mirror, relay, capped, null, null, paged = false, reconciledThrough = 1_700_009_000L)
        assertTrue(c.legs(mirror, relay, capped).isEmpty(), "the premise: this relay wants nothing")
        c.record(mirror, other, capped, null, null, paged = false, reconciledThrough = 1_700_003_000L)

        assertEquals(1_700_003_000L, c.coveringWindow(mirror, listOf(relay, other), capped).since)
    }

    @Test
    fun `a group where nobody needs anything is asked about before a snapshot is built`() {
        // `anyOutstanding` is how the caller avoids building an id set nobody needs.
        val capped = Filter(kinds = listOf(0), until = 1_700_005_000L)
        val c = SyncBands(null)
        c.record(mirror, relay, capped, null, null, paged = false, reconciledThrough = 1_700_009_000L)
        c.record(mirror, other, capped, null, null, paged = false, reconciledThrough = 1_700_009_000L)

        assertTrue(!c.anyOutstanding(mirror, listOf(relay, other), capped), "nobody wants anything")
        assertTrue(c.anyOutstanding(mirror, listOf(relay, other), profiles), "…but a filter with no band still does")
    }

    @Test
    fun `a relay with an older gap also widens the shared window`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = 1_700_009_000L)
        c.record(mirror, other, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)

        assertSame(profiles, c.coveringWindow(mirror, listOf(relay, other), profiles))
    }

    @Test
    fun `an empty fetch records nothing`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, null, null, paged = true)
        assertNull(c.band(mirror, relay, profiles))
    }

    @Test
    fun `one misdated event does not cost a relay its whole band`() {
        // Plausibility is screened per event, not on the aggregate.
        val c = SyncBands(null)
        val far = System.currentTimeMillis() / 1000 + 400L * 86_400
        val observed = listOf(1_700_001_000L, far, 1_700_002_000L, 0L)

        val plausible = observed.filter { SyncCoverage.isPlausible(it) }
        c.record(mirror, relay, profiles, plausible.min(), plausible.max(), paged = true)

        val band = c.band(mirror, relay, profiles)!!
        assertEquals(1_700_001_000L, band.minCreatedAt)
        assertEquals(1_700_002_000L, band.maxCreatedAt)
    }

    @Test
    fun `changing the filter starts over`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)

        val wider = Filter(kinds = listOf(0, 10002))
        assertEquals(listOf(wider), c.legs(mirror, relay, wider), "a new filter has no band")
        assertNull(c.band(mirror, relay, wider))
        assertEquals(1_700_001_000L, c.band(mirror, relay, profiles)!!.minCreatedAt)
    }

    @Test
    fun `each relay keeps its own band`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        assertEquals(listOf(profiles), c.legs(mirror, other, profiles))
    }

    // ---- the filter's own bounds still win ---------------------------------

    @Test
    fun `a bounded filter never widens past its own since and until`() {
        val bounded = Filter(kinds = listOf(0), since = 1_700_001_000L, until = 1_700_005_000L)
        val c = SyncBands(null)
        c.record(mirror, relay, bounded, 1_700_002_000L, 1_700_003_000L, paged = true)

        val legs = c.legs(mirror, relay, bounded)
        assertEquals(2, legs.size)
        assertEquals(1_700_001_000L, legs[0].since, "the older leg keeps the configured floor")
        assertEquals(1_700_002_000L, legs[0].until)
        assertEquals(1_700_003_000L, legs[1].since)
        assertEquals(1_700_005_000L, legs[1].until, "the newer leg keeps the configured ceiling")
    }

    @Test
    fun `a fully covered bounded filter re-reads only its two edge seconds`() {
        val bounded = Filter(kinds = listOf(0), since = 1_700_001_000L, until = 1_700_005_000L)
        val c = SyncBands(null)
        c.record(mirror, relay, bounded, 1_700_001_000L, 1_700_005_000L, paged = true)

        val legs = c.legs(mirror, relay, bounded)
        assertEquals(2, legs.size)
        assertEquals(1_700_001_000L to 1_700_001_000L, legs[0].since to legs[0].until, "the floor second only")
        assertEquals(1_700_005_000L to 1_700_005_000L, legs[1].since to legs[1].until, "the ceiling second only")
    }

    @Test
    fun `bands survive a restart`() {
        val f = tempFile()
        SyncBands(f).apply {
            record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
            flush()
        }

        val reopened = SyncBands(f)
        assertEquals(1_700_001_000L, reopened.band(mirror, relay, profiles)!!.minCreatedAt)
        assertEquals(1_700_002_000L, reopened.band(mirror, relay, profiles)!!.maxCreatedAt)
        assertEquals(1_700_001_000L, reopened.legs(mirror, relay, profiles)[0].until)
        f.delete()
    }

    @Test
    fun `the file nests the stream, the filter and the relay`() {
        val f = tempFile()
        SyncBands(f).apply {
            record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
            flush()
        }
        val written = Json.parseToJsonElement(f.readText()).jsonObject
        val byFilter = assertNotNull(written[mirror]).jsonObject
        val byRelay = assertNotNull(byFilter[profiles.toJson()]).jsonObject
        val band = assertNotNull(byRelay[relay.url]).jsonObject
        assertEquals(1_700_001_000L, band["min"]!!.jsonPrimitive.long)
        assertEquals(1_700_002_000L, band["max"]!!.jsonPrimitive.long)
        f.delete()
    }

    @Test
    fun `two streams asking one relay the same filter keep their own bands`() {
        val f = tempFile()
        SyncBands(f).apply {
            record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
            flush()
        }
        val reopened = SyncBands(f)
        assertEquals(1_700_001_000L, reopened.band(mirror, relay, profiles)?.minCreatedAt)
        assertNull(reopened.band("archive", relay, profiles), "another stream has walked nothing")
        assertEquals(listOf(profiles), reopened.legs("archive", relay, profiles), "so it still owes the whole filter")
        f.delete()
    }

    // ---- urls the alias fold took out of the fan-out ------------------------

    @Test
    fun `a folded url's bands leave the file, and the survivor's stay`() {
        val f = tempFile()
        val c = SyncBands(f)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        c.record(mirror, other, profiles, 1_700_003_000L, 1_700_004_000L, paged = true)
        c.flush()
        assertNotNull(bandIn(f, mirror, other), "walked in its own right, before the fold")

        assertEquals(1, c.dropFolded(mirror, listOf(other)))
        c.flush()

        assertNull(bandIn(f, mirror, other), "the folded url is gone")
        assertEquals(1_700_001_000L, bandIn(f, mirror, relay)!!["min"]!!.jsonPrimitive.long, "the url it folded onto is untouched")
        assertNull(SyncBands(f).band(mirror, other, profiles))
        f.delete()
    }

    @Test
    fun `a fold is one stream's decision, not every stream's`() {
        val f = tempFile()
        val c = SyncBands(f)
        c.record(mirror, other, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        c.record("archive", other, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        c.dropFolded(mirror, listOf(other))
        c.flush()

        assertNull(bandIn(f, mirror, other))
        assertNotNull(bandIn(f, "archive", other), "the stream that never folded it keeps its own")
        f.delete()
    }

    @Test
    fun `a url a static subscription holds keeps its bands, folded or not`() {
        // One stream may carry both `urls` and `relaySource`; `keep` is the urls StaticBackfill still dials.
        val f = tempFile()
        val c = SyncBands(f)
        c.record(mirror, other, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)

        assertEquals(0, c.dropFolded(mirror, listOf(other), keep = setOf(other)))
        c.flush()

        assertEquals(1_700_001_000L, bandIn(f, mirror, other)!!["min"]!!.jsonPrimitive.long, "the pinned url keeps its progress")
        f.delete()
    }

    @Test
    fun `a second pass over the same verdicts rewrites nothing`() {
        val f = tempFile()
        val c = SyncBands(f)
        c.record(mirror, other, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        assertEquals(1, c.dropFolded(mirror, listOf(other)))
        c.flush()

        val stamp = f.lastModified()
        assertEquals(0, c.dropFolded(mirror, listOf(other)), "nothing new was learned")
        c.flush()
        assertEquals(stamp, f.lastModified(), "so the flush is a no-op")
        f.delete()
    }

    @Test
    fun `a restart re-applies the same verdicts without touching the file`() {
        val f = tempFile()
        SyncBands(f).apply {
            record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
            record(mirror, other, profiles, 1_700_003_000L, 1_700_004_000L, paged = true)
            dropFolded(mirror, listOf(other))
            flush()
        }

        val rebooted = SyncBands(f)
        val stamp = f.lastModified()
        assertEquals(0, rebooted.dropFolded(mirror, listOf(other)), "nothing was taken out of a file that never had it")
        rebooted.flush()
        assertEquals(stamp, f.lastModified(), "so the boot writes nothing")
        f.delete()
    }

    @Test
    fun `a url whose verdict expires is written again, from where it left off`() {
        // A fold carries a TTL; a url whose verdict expired is back in the fan-out.
        val f = tempFile()
        val c = SyncBands(f)
        c.record(mirror, other, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        c.dropFolded(mirror, listOf(other))
        c.flush()
        assertNull(bandIn(f, mirror, other))

        // The next cycle's verdicts no longer name it.
        assertEquals(0, c.dropFolded(mirror, emptyList()), "nothing NEW was folded")
        c.flush()
        assertEquals(1_700_001_000L, bandIn(f, mirror, other)!!["min"]!!.jsonPrimitive.long, "and it resumes, rather than starting over")
        f.delete()
    }

    /** One relay's band as the file holds it, or null when the file names neither. */
    private fun bandIn(
        f: File,
        stream: String,
        url: NormalizedRelayUrl,
    ): JsonObject? =
        Json
            .parseToJsonElement(f.readText())
            .jsonObject[stream]
            ?.jsonObject
            ?.get(profiles.toJson())
            ?.jsonObject
            ?.get(url.url)
            ?.jsonObject

    // ---- the flat keys a file written before the format nested carries -------

    /** One band under the flat key the pre-stream version wrote, built by hand rather than by the code under test. */
    private fun writeFlat(
        f: File,
        vararg keys: String,
    ) = f.writeText(
        Json.encodeToString(
            JsonObject.serializer(),
            buildJsonObject {
                keys.forEach { key ->
                    put(
                        key,
                        buildJsonObject {
                            put("min", 1_700_001_000L)
                            put("max", 1_700_002_000L)
                            put("complete", false)
                            put("fullAt", 0L)
                        },
                    )
                }
            },
        ),
    )

    /** The key the pre-stream version built: the relay url, a space, the filter's json. */
    private fun flatKey(url: NormalizedRelayUrl) = "${url.url} ${profiles.toJson()}"

    @Test
    fun `a flat key is pruned on load, not held for a claim that cannot come`() {
        val f = tempFile()
        writeFlat(f, flatKey(relay))

        val reopened = SyncBands(f)
        assertNull(reopened.band(mirror, relay, profiles), "the flat band is not adopted by the stream that asks")
        assertEquals(listOf(profiles), reopened.legs(mirror, relay, profiles), "so the pair owes its whole filter again")
        assertEquals(0, reopened.size(), "and it is not being held anywhere out of sight")
        f.delete()
    }

    @Test
    fun `the prune reaches the file on its own, with nothing else to make it dirty`() {
        val f = tempFile()
        writeFlat(f, flatKey(relay), flatKey(other))

        SyncBands(f).flush()

        assertEquals(JsonObject(emptyMap()), Json.parseToJsonElement(f.readText()).jsonObject, "both keys gone, nothing invented in their place")
        f.delete()
    }

    @Test
    fun `pruning takes the flat keys and leaves the nested ones`() {
        val f = tempFile()
        SyncBands(f).apply {
            record(mirror, relay, profiles, 1_700_003_000L, 1_700_004_000L, paged = true)
            flush()
        }
        // A flat key beside a nested one: the file of a deployment that upgraded mid-flight.
        val key = flatKey(other)
        val nested = Json.parseToJsonElement(f.readText()).jsonObject
        f.writeText(
            Json.encodeToString(
                JsonObject.serializer(),
                buildJsonObject {
                    nested.forEach { (k, v) -> put(k, v) }
                    put(
                        key,
                        buildJsonObject {
                            put("min", 1_700_001_000L)
                            put("max", 1_700_002_000L)
                            put("complete", false)
                            put("fullAt", 0L)
                        },
                    )
                },
            ),
        )

        val reopened = SyncBands(f)
        assertEquals(1_700_003_000L, reopened.band(mirror, relay, profiles)?.minCreatedAt, "the nested band resumes")
        reopened.flush()

        val written = Json.parseToJsonElement(f.readText()).jsonObject
        assertNull(written[key], "the flat key is gone")
        assertNotNull(written[mirror], "the stream it sat beside is not")
        f.delete()
    }

    @Test
    fun `a file with no flat keys is not rewritten at boot`() {
        val f = tempFile()
        SyncBands(f).apply {
            record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
            flush()
        }

        val stamp = f.lastModified()
        SyncBands(f).flush()
        assertEquals(stamp, f.lastModified(), "the boot writes nothing")
        f.delete()
    }

    @Test
    fun `a corrupt file starts fresh instead of refusing to start`() {
        val f = tempFile()
        f.writeText("{ this is not json")
        val c = SyncBands(f)
        assertEquals(0, c.size())
        assertEquals(listOf(profiles), c.legs(mirror, relay, profiles), "no band, so fetch everything")
        f.delete()
    }

    @Test
    fun `with no file configured it still works, just not across restarts`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        assertEquals(1_700_001_000L, c.band(mirror, relay, profiles)!!.minCreatedAt)
    }

    @Test
    fun `a periodic flush persists progress a hard kill would otherwise lose`() {
        val f = tempFile()
        val c = SyncBands(f).startPeriodicFlush(intervalSec = 1)
        c.record(mirror, relay, profiles, 1_700_000_000, 1_785_000_000, paged = true)

        val deadline = System.currentTimeMillis() + 15_000
        while (!f.isFile && System.currentTimeMillis() < deadline) Thread.sleep(100)
        assertTrue(f.isFile, "the periodic flush should have written it with no milestone reached")

        c.close()
        assertEquals(1_700_000_000L, SyncBands(f).band(mirror, relay, profiles)?.minCreatedAt)
        f.delete()
    }

    @Test
    fun `recording does not write, flushing does`() {
        val f = tempFile()
        val c = SyncBands(f)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        assertTrue(!f.exists(), "record() must not touch the file")

        c.flush()
        assertTrue(f.isFile, "flush() writes it")

        val stamp = f.lastModified()
        c.flush()
        assertEquals(stamp, f.lastModified(), "a clean flush is a no-op")
        f.delete()
    }

    @Test
    fun `the same filter instance is fingerprinted once`() {
        // An author-scoped filter's json runs to tens of KB, so the fingerprint is cached per instance.
        val big = Filter(kinds = listOf(30382), authors = (1..500).map { "%064x".format(it) })
        val c = SyncBands(null)
        c.record(mirror, relay, big, 1_700_001_000L, 1_700_002_000L, paged = true)

        repeat(50) { c.legs(mirror, relay, big) }
        assertEquals(1_700_001_000L, c.band(mirror, relay, big)!!.minCreatedAt)

        val copy = Filter(kinds = listOf(30382), authors = (1..500).map { "%064x".format(it) })
        assertEquals(1_700_001_000L, c.band(mirror, relay, copy)?.minCreatedAt, "identity caching must not change the key")
    }

    @Test
    fun `fromEnv is off unless a path is given`() {
        assertEquals(0, SyncBands.fromEnv(emptyMap()).size())
        assertEquals(0, SyncBands.fromEnv(mapOf("SYNC_STATE_FILE" to "  ")).size())
    }

    // ---- per-kind spans, as the router has to supply them ------------------

    @Test
    fun `a multi-kind paged walk earns a band only when it reports per kind`() {
        // Quartz refuses to let one interval speak for several kinds.
        val mixed = Filter(kinds = listOf(0, 10002, 30382))

        val unreported = SyncBands(null)
        unreported.record(mirror, relay, mixed, 1_690_000_000L, 1_700_000_000L, paged = true)
        assertNull(unreported.band(mirror, relay, mixed), "no per-kind evidence, no band")

        val reported = SyncBands(null)
        reported.record(
            mirror,
            relay,
            mixed,
            1_690_000_000L,
            1_700_000_000L,
            paged = true,
            observedByKind =
                mapOf(
                    0 to SyncCoverage.Span(1_600_000_000L, 1_700_000_000L),
                    30382 to SyncCoverage.Span(1_690_000_000L, 1_700_000_000L),
                ),
        )
        val band = assertNotNull(reported.band(mirror, relay, mixed), "reported per kind, so it earns one")
        assertEquals(setOf(0, 30382), band.spans.keys)
        assertTrue(reported.legs(mirror, relay, mixed).size > 2, "kinds with different evidence want different windows")
    }

    @Test
    fun `per-kind spans survive a restart through the state file`() {
        val mixed = Filter(kinds = listOf(0, 30382))
        val f = tempFile()
        SyncBands(f).apply {
            record(
                mirror,
                relay,
                mixed,
                null,
                null,
                paged = true,
                observedByKind =
                    mapOf(
                        0 to SyncCoverage.Span(1_600_000_000L, 1_700_000_000L),
                        30382 to SyncCoverage.Span(1_690_000_000L, 1_700_000_000L),
                    ),
            )
            flush()
        }

        val reopened = SyncBands(f)
        assertEquals(
            mapOf(
                0 to SyncCoverage.Span(1_600_000_000L, 1_700_000_000L),
                30382 to SyncCoverage.Span(1_690_000_000L, 1_700_000_000L),
            ),
            reopened.band(mirror, relay, mixed)!!.spans,
            "each kind keeps its own evidence across a restart",
        )
        assertTrue(reopened.legs(mirror, relay, mixed).size > 2, "and the restored band still narrows per kind")
        f.delete()
    }

    // ---- the floor a paged leg must carry onto the wire ---------------------

    @Test
    fun `an unbounded leg is floored before it is walked, and a bounded one is left alone`() {
        // An unfloored walk cannot terminate against a relay holding a `created_at = 0` event.
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_690_000_000L, 1_700_000_000L, paged = true)
        val older = c.legs(mirror, relay, profiles).first { it.since == null }

        assertEquals(SyncCoverage.PLAUSIBLE_FLOOR, older.flooredForPaging().since)
        assertEquals(older.until, older.flooredForPaging().until, "only the floor is added")
        assertEquals(older.kinds, older.flooredForPaging().kinds, "and nothing else about the ask changes")

        val deep = profiles.copy(since = 1_000L)
        assertEquals(1_000L, deep.flooredForPaging().since)
        val shallow = profiles.copy(since = 1_700_000_000L)
        assertEquals(1_700_000_000L, shallow.flooredForPaging().since)
    }

    @Test
    fun `flooring a leg does not cost it the drain it would otherwise have earned`() {
        // drainSettlesThePast compares floors, and the floored leg's floor is the deepest a band may claim.
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_690_000_000L, 1_700_000_000L, paged = true)
        val older = c.legs(mirror, relay, profiles).first { it.since == null }
        assertTrue(drainSettlesThePast(drainedWalk, older.flooredForPaging(), profiles))
    }

    // ---- draining: the leg a paged walk is finally allowed to close ---------

    @Test
    fun `only a leg reaching the filter's floor may settle the past`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_690_000_000L, 1_700_000_000L, paged = true)
        val legs = c.legs(mirror, relay, profiles)
        assertEquals(2, legs.size)

        val older = legs.first { it.since == profiles.since }
        val newer = legs.first { it.since != profiles.since }
        assertTrue(drainSettlesThePast(drainedWalk, older, profiles), "the older leg reaches as deep as the filter allows")
        assertTrue(!drainSettlesThePast(drainedWalk, newer, profiles), "the newer leg says nothing about history")
        assertTrue(!drainSettlesThePast(idleWalk, older, profiles), "and an idle walk claims nothing")
        assertTrue(!drainSettlesThePast(null, older, profiles), "nor does a leg that was never paged")
    }

    @Test
    fun `the sweep fallback's leg reaches bottom even though its floor is spelled out`() {
        // The sweep fallback pages `outstanding()`, which spells the floor out as PLAUSIBLE_FLOOR rather than null.
        val sweptLeg = profiles.copy(since = SyncCoverage.PLAUSIBLE_FLOOR, until = 1_690_000_000L)
        assertTrue(
            drainSettlesThePast(drainedWalk, sweptLeg, profiles),
            "a leg spelling out the plausible floor has reached the bottom of an unbounded filter",
        )
    }

    @Test
    fun `a bounded filter's floor settles the guard but not yet the leg`() {
        val bounded = Filter(kinds = listOf(0), since = 1_600_000_000L)
        val c = SyncBands(null)
        c.record(mirror, relay, bounded, 1_690_000_000L, 1_700_000_000L, paged = true)
        val older = c.legs(mirror, relay, bounded).first { it.since == bounded.since }
        assertTrue(drainSettlesThePast(drainedWalk, older, bounded))

        // Known limit: `SyncCoverage.windows` re-opens a bounded older leg even when complete.
        val drained = SyncBands(null)
        drained.record(mirror, relay, bounded, 1_690_000_000L, 1_700_000_000L, paged = true, drained = true)
        assertEquals(2, drained.legs(mirror, relay, bounded).size, "bounded: the older leg comes back anyway")

        val unbounded = SyncBands(null)
        unbounded.record(mirror, relay, profiles, 1_690_000_000L, 1_700_000_000L, paged = true, drained = true)
        assertEquals(1, unbounded.legs(mirror, relay, profiles).size, "unbounded: it really does close")
    }

    @Test
    fun `a drained walk closes the older leg and the file remembers`() {
        val f = tempFile()
        SyncBands(f).apply {
            record(mirror, relay, profiles, 1_690_000_000L, 1_700_000_000L, paged = true, drained = true)
            assertEquals(1, legs(mirror, relay, profiles).size, "drained: the past is settled")
            flush()
        }

        val reopened = SyncBands(f)
        assertTrue(
            reopened
                .band(mirror, relay, profiles)!!
                .spans
                .getValue(0)
                .complete,
            "per-kind completeness round-trips",
        )
        assertEquals(1, reopened.legs(mirror, relay, profiles).size, "…and still settles the past after a restart")
        f.delete()
    }

    @Test
    fun `completeness is per kind in the file, not one flag for the band`() {
        val mixed = Filter(kinds = listOf(0, 30382))
        val f = tempFile()
        SyncBands(f).apply {
            record(mirror, relay, mixed, null, null, paged = true, observedByKind = mapOf(0 to SyncCoverage.Span(1_690_000_000L, 1_700_000_000L)), drained = true)
            record(mirror, relay, mixed, null, null, paged = true, observedByKind = mapOf(30382 to SyncCoverage.Span(1_695_000_000L, 1_700_000_000L)))
            flush()
        }

        val spans = SyncBands(f).band(mirror, relay, mixed)!!.spans
        assertTrue(spans.getValue(0).complete, "kind 0 drained")
        assertTrue(!spans.getValue(30382).complete, "kind 30382 did not")
        f.delete()
    }

    @Test
    fun `a span written before completeness was per kind inherits the band's`() {
        // Derived from a real write, then aged, so the fixture cannot drift from the nesting `save()` produces.
        val mixed = Filter(kinds = listOf(0, 30382))
        val f = tempFile()
        SyncBands(f).apply {
            record(
                mirror,
                relay,
                mixed,
                null,
                null,
                paged = true,
                observedByKind =
                    mapOf(
                        0 to SyncCoverage.Span(1_690_000_000L, 1_700_000_000L),
                        30382 to SyncCoverage.Span(1_695_000_000L, 1_700_000_000L),
                    ),
            )
            flush()
        }
        f.writeText(ageToBandLevelComplete(Json.parseToJsonElement(f.readText()).jsonObject).toString())

        val spans = SyncBands(f).band(mirror, relay, mixed)!!.spans
        assertTrue(spans.getValue(0).complete, "the band's flag applied to every kind")
        assertTrue(spans.getValue(30382).complete)
        f.delete()
    }

    /** A current file rewritten as the pre-per-kind writer left it: `complete` on the band, none on any span. */
    private fun ageToBandLevelComplete(root: JsonObject): JsonObject =
        buildJsonObject {
            root.forEach { (stream, byFilter) ->
                put(
                    stream,
                    buildJsonObject {
                        byFilter.jsonObject.forEach { (filter, byRelay) ->
                            put(
                                filter,
                                buildJsonObject {
                                    byRelay.jsonObject.forEach { (relayUrl, band) ->
                                        put(
                                            relayUrl,
                                            buildJsonObject {
                                                band.jsonObject.forEach { (k, v) ->
                                                    if (k == "spans") {
                                                        put(
                                                            "spans",
                                                            buildJsonObject {
                                                                v.jsonObject.forEach { (kind, span) ->
                                                                    put(kind, JsonObject(span.jsonObject.filterKeys { it != "complete" }))
                                                                }
                                                            },
                                                        )
                                                    } else if (k != "complete") {
                                                        put(k, v)
                                                    }
                                                }
                                                put("complete", true)
                                            },
                                        )
                                    }
                                },
                            )
                        }
                    },
                )
            }
        }
}
