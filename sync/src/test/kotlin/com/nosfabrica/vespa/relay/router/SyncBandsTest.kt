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
package com.nosfabrica.vespa.relay.router

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
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
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * A paged relay has no memory of what it already sent, so without a cursor every
 * restart re-downloads its whole corpus. These pin the band arithmetic and, more
 * importantly, the cases where a cursor must NOT be used — a stale band silently
 * skips events, which is a worse failure than re-reading them.
 */
class SyncBandsTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")
    private val other = RelayUrlNormalizer.normalize("wss://other.example")
    private val profiles = Filter(kinds = listOf(0))
    private val mirror = "profiles"

    private fun now(): Long = System.currentTimeMillis() / 1000

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
        // A paged relay cuts pages by count, so a boundary can fall inside a run
        // of events sharing one created_at. Excluding the edge would strand the
        // rest of that second in no leg at all, while the band called it covered.
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)

        val legs = c.legs(mirror, relay, profiles)

        fun reachable(t: Long) = legs.any { (it.since ?: Long.MIN_VALUE) <= t && t <= (it.until ?: Long.MAX_VALUE) }

        assertTrue(reachable(1_700_001_000L), "the band's own floor second must be re-read")
        assertTrue(reachable(1_700_002_000L), "and its ceiling second")
        assertTrue(reachable(1_700_000_999L), "below the band")
        assertTrue(reachable(1_700_002_001L), "above it")
        // Only the interior is skipped, which is the entire point.
        assertTrue(!reachable(1_700_001_500L), "the covered interior is not re-read")
    }

    @Test
    fun `successive runs widen the band rather than replacing it`() {
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        // A later run reaches further back and picks up newer events.
        c.record(mirror, relay, profiles, 1_700_000_500L, 1_700_002_500L, paged = true)

        val band = c.band(mirror, relay, profiles)!!
        assertEquals(1_700_000_500L, band.minCreatedAt)
        assertEquals(1_700_002_500L, band.maxCreatedAt)
    }

    @Test
    fun `a capped relay walks further back on each run`() {
        // The case that makes this worth having: a relay that only ever answers
        // with its newest N events. Each run starts below the last one's floor.
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_009_000L, 1_700_010_000L, paged = true)
        assertEquals(1_700_009_000L, c.legs(mirror, relay, profiles)[0].until)

        c.record(mirror, relay, profiles, 1_700_008_000L, 1_700_008_999L, paged = true)
        assertEquals(1_700_008_000L, c.legs(mirror, relay, profiles)[0].until)
    }

    // ---- when a cursor must not be used ------------------------------------

    @Test
    fun `a negentropy sync that reported no outcome records nothing`() {
        // Only a sync that says how far it reconciled earns a band; a bare
        // paged=false call carries no claim to record.
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = false)
        assertNull(c.band(mirror, relay, profiles))
        assertEquals(listOf(profiles), c.legs(mirror, relay, profiles))
    }

    // ---- coverage: what a finished reconcile earns -------------------------

    @Test
    fun `a finished reconcile is in sync through the instant it started`() {
        // Not through the newest event it happened to see: "the relay had nothing
        // newer" and "we never asked" must not record the same thing.
        val c = SyncBands(null)
        val startedAt = now() - 60
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = false, reconciledThrough = startedAt)

        val band = c.band(mirror, relay, profiles)!!
        assertTrue(band.complete)
        assertEquals(startedAt, band.maxCreatedAt)
    }

    @Test
    fun `a reconcile that downloaded nothing still records coverage`() {
        // The empty case is the WHOLE point: nothing came back because we already
        // have it, and that is exactly when the next run should ask for a sliver.
        val c = SyncBands(null)
        val startedAt = now() - 60
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = startedAt)

        val leg = c.legs(mirror, relay, profiles).single()
        assertEquals(startedAt, leg.since)
        assertNull(leg.until)
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
        val c = SyncBands(null, fullResyncSeconds = 60)
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now() - 3600)
        // Recorded 'now' whatever the created_at claim, so age it by rewriting.
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now())
        assertEquals(1, c.legs(mirror, relay, profiles).size, "fresh band still narrows")

        val stale = SyncBands(null, fullResyncSeconds = 0)
        stale.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = now())
        assertSame(profiles, stale.legs(mirror, relay, profiles).single(), "a band past its period re-walks everything")
    }

    @Test
    fun `the re-walk replaces the old claim instead of widening it`() {
        // Widening would carry the stale band's floor forward forever and the
        // periodic pass would never actually reset anything.
        val c = SyncBands(null, fullResyncSeconds = 0)
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
        // It genuinely needs everything — narrowing the shared snapshot would
        // reconcile it against ids we never looked up.
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = 1_700_009_000L)

        // The filter itself, unnarrowed — identity, since Filter has no equals.
        assertSame(profiles, c.coveringWindow(mirror, listOf(relay, other), profiles))
        assertSame(profiles, c.coveringWindow(mirror, emptyList(), profiles))
    }

    @Test
    fun `one shared window serves a whole stream of relays`() {
        // Every url in a stream shares that stream's filter, so the static
        // backfill takes ONE snapshot for all of them instead of walking the
        // identical range once per relay — 7,683 visit pages against a single
        // selection, on a real store, for byte-identical answers.
        val c = SyncBands(null)
        val third = RelayUrlNormalizer.normalize("wss://third.example")
        c.record(mirror, relay, profiles, null, null, paged = false, reconciledThrough = 1_700_009_000L)
        c.record(mirror, other, profiles, null, null, paged = false, reconciledThrough = 1_700_003_000L)
        c.record(mirror, third, profiles, null, null, paged = false, reconciledThrough = 1_700_007_000L)

        // The hungriest of them sets the floor; the other two re-read a little.
        assertEquals(1_700_003_000L, c.coveringWindow(mirror, listOf(relay, other, third), profiles).since)
    }

    @Test
    fun `a relay that needs nothing does not widen the shared window`() {
        // The router depends on this, so it is pinned here even though the
        // arithmetic is quartz's: `legs()` returns EMPTY for a relay whose band
        // already covers its filter, and a window loop that read that through
        // singleOrNull() — null for "no legs" exactly as for "two legs" — let
        // the most caught-up relay force the widest snapshot there is. The
        // other relay's ceiling is the only real constraint here.
        val capped = Filter(kinds = listOf(0), until = 1_700_005_000L)
        val c = SyncBands(null)
        c.record(mirror, relay, capped, null, null, paged = false, reconciledThrough = 1_700_009_000L)
        assertTrue(c.legs(mirror, relay, capped).isEmpty(), "the premise: this relay wants nothing")
        c.record(mirror, other, capped, null, null, paged = false, reconciledThrough = 1_700_003_000L)

        assertEquals(1_700_003_000L, c.coveringWindow(mirror, listOf(relay, other), capped).since)
    }

    @Test
    fun `a group where nobody needs anything is asked about before a snapshot is built`() {
        // coveringWindow returns the unnarrowed filter when every relay is
        // covered — safe, and upstream's deliberate choice, because "any window
        // would do" once nothing will be reconciled. The saving is the caller's
        // to take, and [anyOutstanding] is how it asks: building the id set is
        // the most expensive thing this router does, and doing it for a fleet
        // that will then skip every relay is the whole cost for none of the
        // benefit.
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
        // No events says nothing about what the relay holds, only that this
        // window was empty — recording it would fabricate coverage.
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, null, null, paged = true)
        assertNull(c.band(mirror, relay, profiles))
    }

    @Test
    fun `one misdated event does not cost a relay its whole band`() {
        // purplepag.es downloaded 700,767 events and recorded NOTHING, because a
        // single future-dated stamp among them failed a check applied to the
        // aggregate. Screening per event keeps the honest 700,766.
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

        // Widening the kinds means the old band skipped events it never fetched.
        val wider = Filter(kinds = listOf(0, 10002))
        assertEquals(listOf(wider), c.legs(mirror, relay, wider), "a new filter has no band")
        assertNull(c.band(mirror, relay, wider))
        // ...and the original is untouched, so reverting resumes where it was.
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
        // Inclusive edges mean "covered" can never quite mean "ask for nothing":
        // the two boundary seconds are always re-read, because that is the only
        // way to catch a run of same-second events a page boundary cut in half.
        // Two seconds per cycle is the price of not stranding them.
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

        // A fresh instance, as a restart would build.
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
        // Three levels, no concatenation anywhere: the stream, then the filter
        // as the router serialises it, then the relay's own url.
        val byFilter = assertNotNull(written[mirror]).jsonObject
        val byRelay = assertNotNull(byFilter[profiles.toJson()]).jsonObject
        val band = assertNotNull(byRelay[relay.url]).jsonObject
        assertEquals(1_700_001_000L, band["min"]!!.jsonPrimitive.long)
        assertEquals(1_700_002_000L, band["max"]!!.jsonPrimitive.long)
        f.delete()
    }

    @Test
    fun `two streams asking one relay the same filter keep their own bands`() {
        // The identity the nesting makes explicit. They start at different
        // moments and stop at different depths, so neither may resume from the
        // other's claim — and a shared band let exactly that happen.
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

    // ---- MIGRATION SHIM: a file written before the format nested ------------

    /**
     * One band under the flat key the previous version wrote. Through the json
     * builder, not string interpolation: the key CONTAINS json, and a fixture
     * that forgets to escape it tests the corrupt-file path by accident.
     */
    private fun writeFlat(
        f: File,
        key: String,
        min: Long,
        max: Long,
    ) = f.writeText(
        Json.encodeToString(
            JsonObject.serializer(),
            buildJsonObject {
                put(
                    key,
                    buildJsonObject {
                        put("min", min)
                        put("max", max)
                        put("complete", false)
                        put("fullAt", 0L)
                    },
                )
            },
        ),
    )

    @Test
    fun `a pre-stream state file still loads, and is claimed by the stream that asks`() {
        // The band arithmetic moved out to quartz's SyncCoverage; the FILE did
        // not move with it, and a running deployment's SYNC_STATE_FILE has to
        // survive that. Written out by hand rather than by this code, because a
        // round trip through one implementation proves only that it agrees with
        // itself — this is the shape the previous version actually wrote.
        val f = tempFile()
        // The key the previous version built: the relay url, a space, and the
        // filter's canonical json. Assembled here rather than read back from
        // the code under test, so a change to either half fails this. Encoded
        // through the json builder only because the filter half contains quotes
        // — the SHAPE is the hand-written part.
        val key = "${relay.url} ${profiles.toJson()}"
        f.writeText(
            Json.encodeToString(
                JsonObject.serializer(),
                buildJsonObject {
                    put(
                        key,
                        buildJsonObject {
                            put("min", 1_700_001_000L)
                            put("max", 1_700_002_000L)
                            put("complete", false)
                            put("fullAt", now())
                        },
                    )
                },
            ),
        )

        val reopened = SyncBands(f)
        val band = reopened.band(mirror, relay, profiles)
        assertTrue(band != null, "the key the old code wrote must still resolve")
        assertEquals(1_700_001_000L, band!!.minCreatedAt)
        assertEquals(1_700_002_000L, band.maxCreatedAt)
        // …and it must still NARROW, not merely parse: a band that loads but
        // does not key correctly is a full re-walk nobody notices.
        assertEquals(2, reopened.legs(mirror, relay, profiles).size)
        assertEquals(1_700_001_000L, reopened.legs(mirror, relay, profiles)[0].until)

        // Asking is what claims it: the stream that asks is the stream that
        // wrote it, and from here on it is written under that name.
        reopened.flush()
        val written = Json.parseToJsonElement(f.readText()).jsonObject
        assertNull(written[key], "the flat key must not survive the claim")
        assertEquals(
            1_700_001_000L,
            written[mirror]!!
                .jsonObject[profiles.toJson()]!!
                .jsonObject[relay.url]!!
                .jsonObject["min"]!!
                .jsonPrimitive.long,
        )
        f.delete()
    }

    @Test
    fun `an unclaimed pre-stream band is written back, not dropped`() {
        // The flusher runs 30 seconds after boot and a slow stream has not
        // reached its first relay by then. Dropping what nobody has asked for
        // yet would cost that relay's whole corpus on the next restart.
        val f = tempFile()
        val key = "${other.url} ${profiles.toJson()}"
        writeFlat(f, key, 1_700_001_000L, 1_700_002_000L)

        val reopened = SyncBands(f)
        // A different pair entirely, so nothing claims the one on disk.
        reopened.record(mirror, relay, profiles, 1_700_003_000L, 1_700_004_000L, paged = true)
        reopened.flush()

        val written = Json.parseToJsonElement(f.readText()).jsonObject
        assertEquals(1_700_001_000L, written[key]!!.jsonObject["min"]!!.jsonPrimitive.long, "still there, still flat")
        assertNotNull(written[mirror], "beside the stream that has claimed its own")
        // And it is still claimable after that round trip.
        assertEquals(1_700_001_000L, SyncBands(f).band("archive", other, profiles)?.minCreatedAt)
        f.delete()
    }

    @Test
    fun `a pre-stream band goes to the first stream to ask, and only that one`() {
        val f = tempFile()
        val key = "${relay.url} ${profiles.toJson()}"
        writeFlat(f, key, 1_700_001_000L, 1_700_002_000L)

        val reopened = SyncBands(f)
        assertEquals(1_700_001_000L, reopened.band(mirror, relay, profiles)?.minCreatedAt)
        assertNull(reopened.band("archive", relay, profiles), "a second stream must not inherit the same claim")
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
        // The milestone flushes are minutes to hours apart; a SIGKILL between
        // them loses every band the run earned, and the next start re-downloads
        // the corpus. That is the cost this class exists to avoid.
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
        // A dynamic cycle records once per leg per relay. Writing there would
        // serialize the whole map thousands of times per cycle.
        val f = tempFile()
        val c = SyncBands(f)
        c.record(mirror, relay, profiles, 1_700_001_000L, 1_700_002_000L, paged = true)
        assertTrue(!f.exists(), "record() must not touch the file")

        c.flush()
        assertTrue(f.isFile, "flush() writes it")

        // And a second flush with nothing new does not rewrite.
        val stamp = f.lastModified()
        c.flush()
        assertEquals(stamp, f.lastModified(), "a clean flush is a no-op")
        f.delete()
    }

    @Test
    fun `the same filter instance is fingerprinted once`() {
        // Filter.toJson() runs to tens of thousands of characters for an
        // author-scoped filter, and the fan-out keys once per relay per cycle.
        val big = Filter(kinds = listOf(30382), authors = (1..500).map { "%064x".format(it) })
        val c = SyncBands(null)
        c.record(mirror, relay, big, 1_700_001_000L, 1_700_002_000L, paged = true)

        // Same instance, many lookups: still one band, and cheap.
        repeat(50) { c.legs(mirror, relay, big) }
        assertEquals(1_700_001_000L, c.band(mirror, relay, big)!!.minCreatedAt)

        // An equal-but-distinct instance keys the same way; it just misses the cache.
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
        // The whole reason every record() call site in this module now threads
        // observedByKind. Quartz refuses to let one interval speak for several
        // kinds, so a caller that does not report per kind gets NO band — and
        // every multi-kind stream in router.conf (kinds [0, 10002, 30382] on a
        // fetch-mode stream) would quietly re-walk from scratch every cycle.
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
        // …and the kinds genuinely narrow apart, which is the point of it.
        assertTrue(reported.legs(mirror, relay, mixed).size > 2, "kinds with different evidence want different windows")
    }

    @Test
    fun `per-kind spans survive a restart through the state file`() {
        // The file is where a per-kind band can silently flatten back into the
        // single interval it replaced.
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

    // ---- draining: the leg a paged walk is finally allowed to close ---------

    @Test
    fun `only a leg reaching the filter's floor may settle the past`() {
        // The guard between fetchAllPages' onDrained and record(). legs() gives
        // the OLDER leg the filter's own `since` and the NEWER one the band's
        // ceiling — and draining the newer one means only "nothing below the
        // ceiling we already had". Recording that as history would make the
        // band skip a past it never walked, which is the worst thing a band
        // can do.
        val c = SyncBands(null)
        c.record(mirror, relay, profiles, 1_690_000_000L, 1_700_000_000L, paged = true)
        val legs = c.legs(mirror, relay, profiles)
        assertEquals(2, legs.size)

        val older = legs.first { it.since == profiles.since }
        val newer = legs.first { it.since != profiles.since }
        assertTrue(drainSettlesThePast(true, older, profiles), "the older leg reaches as deep as the filter allows")
        assertTrue(!drainSettlesThePast(true, newer, profiles), "the newer leg says nothing about history")
        assertTrue(!drainSettlesThePast(false, older, profiles), "and no drain, no claim")
    }

    @Test
    fun `a bounded filter's own floor still counts as reaching bottom`() {
        // Compared against the filter's `since` rather than checked for null, so
        // a deliberately bounded stream is not locked out of ever settling.
        val bounded = Filter(kinds = listOf(0), since = 1_600_000_000L)
        val c = SyncBands(null)
        c.record(mirror, relay, bounded, 1_690_000_000L, 1_700_000_000L, paged = true)

        val older = c.legs(mirror, relay, bounded).first { it.since == bounded.since }
        assertTrue(drainSettlesThePast(true, older, bounded), "it reaches everything this filter can ask for")
    }

    @Test
    fun `a drained walk closes the older leg and the file remembers`() {
        // The end-to-end shape: drain in, one leg out, and the claim survives a
        // restart through SYNC_STATE_FILE rather than being re-earned every boot.
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
        // Why it moved off the band: a leg that drained kinds [0] proves nothing
        // about 30382, and the file must not flatten the two back together.
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
        // The compat path. An older file carries one `complete` beside min/max
        // and spans with none of their own — which is precisely what that flag
        // used to mean for every kind at once.
        //
        // The fixture is DERIVED from a real write rather than hand-built, so it
        // cannot drift from the nesting `save()` actually produces: write a
        // current file, then age it by stripping each span's own flag and
        // asserting the band's at the level the old writer used.
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

    /**
     * A current file rewritten the way the pre-per-kind writer would have left
     * it: `complete` on the band, and none on any span.
     */
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
