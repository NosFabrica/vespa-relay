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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * What overlapping passes must never do: hand the same relay to two workers.
 *
 * A pass ends when its last url is handed out rather than when its last worker
 * returns, so a relay slower than a pass is still being synced when the next one
 * reaches it. Dialling it again would share a socket, race on one cursor band,
 * and spend two of the pool's slots on one relay.
 */
class RelayRotationTest {
    private val a = DiscoveredRelay(RelayUrlNormalizer.normalize("wss://a.example"))
    private val b = DiscoveredRelay(RelayUrlNormalizer.normalize("wss://b.example"))
    private val c = DiscoveredRelay(RelayUrlNormalizer.normalize("wss://c.example"))

    @Test
    fun `a pass hands out everything nothing is holding`() {
        val rotation = RelayRotation()
        val pass = rotation.beginPass(listOf(a, b, c))

        assertEquals(listOf(a, b, c), pass.relays)
        assertEquals(0, pass.busy)
        assertEquals(1L, rotation.pass(), "the first pass is 1, so a log line can name it")
    }

    @Test
    fun `a relay still syncing is passed over, and counted`() {
        // Counted, not merely skipped: "still going from last time" and "never
        // reached" are the same silence otherwise, and one of them is the
        // rotation working while the other is a fan-out that stopped.
        val rotation = RelayRotation()
        assertTrue(rotation.take(b.url))

        val pass = rotation.beginPass(listOf(a, b, c))

        assertEquals(listOf(a, c), pass.relays)
        assertEquals(1, pass.busy)
    }

    @Test
    fun `it comes back on the pass after it finishes`() {
        val rotation = RelayRotation()
        rotation.take(b.url)
        rotation.beginPass(listOf(a, b, c))
        rotation.release(b.url)

        assertEquals(listOf(a, b, c), rotation.beginPass(listOf(a, b, c)).relays)
    }

    @Test
    fun `taking is the claim, and only one taker wins`() {
        val rotation = RelayRotation()

        assertTrue(rotation.take(a.url))
        assertFalse(rotation.take(a.url), "two workers would share a socket and race on one band")
        assertEquals(1, rotation.busyCount())

        rotation.release(a.url)
        assertTrue(rotation.take(a.url))
    }

    @Test
    fun `a relay freed mid-pass waits for the next one`() {
        // The pass list is a snapshot on purpose. Re-testing as the walk goes
        // would let a fast relay be dialled repeatedly within one pass while the
        // list behind it queues, which is the opposite of a rotation.
        val rotation = RelayRotation()
        rotation.take(a.url)
        val pass = rotation.beginPass(listOf(a, b, c))
        rotation.release(a.url)

        assertEquals(listOf(b, c), pass.relays, "the pass already decided what it was handing out")
    }

    @Test
    fun `a held relay is NAMED, with the clock that says how long`() {
        // The whole complaint: a stream held on two relays for eleven hours
        // published the number 2 and no url, and nothing else in the system
        // recorded them — a stalled leg earns no band, so the coverage card
        // never draws it, and the logs had rotated away long before anyone
        // looked.
        val rotation = RelayRotation()
        rotation.take(a.url, nowMs = 1_000_000)
        rotation.take(b.url, nowMs = 1_030_000)

        val held = rotation.held(nowMs = 1_100_000)

        // Neither has received anything, so both quiet clocks run from the
        // claim and the longer-held leg is also the quieter one. The tie-break
        // is what orders them, and it is `heldForSec` descending.
        assertEquals(listOf(a.url.url, b.url.url), held.relays.map { it.relay })
        assertEquals(100L, held.relays[0].heldForSec)
        assertEquals(70L, held.relays[1].heldForSec)
        assertEquals(0, held.omitted)
    }

    @Test
    fun `the rows kept are the QUIET ones, not the old ones`() {
        // The selection bug, in the direction it failed. Held is not risk: the
        // healthiest thing this router does is hold one relay for an hour while
        // it streams events, and under the old ordering those long-haulers took
        // every row while the leg that had stopped receiving fell off the end
        // of the cap into `omitted` — a list truncated on the wrong key, which
        // does not look truncated.
        val rotation = RelayRotation()
        // Two legs held far longer, both still delivering right now…
        rotation.take(a.url, nowMs = 1_000_000)
        rotation.take(b.url, nowMs = 1_000_000)
        rotation.leg(a.url)!!.received(nowMs = 1_099_000)
        rotation.leg(b.url)!!.received(nowMs = 1_099_000)
        // …and one claimed recently that has never received anything.
        rotation.take(c.url, nowMs = 1_050_000)

        val kept = rotation.held(nowMs = 1_100_000, limit = 1)

        assertEquals(listOf(c.url.url), kept.relays.map { it.relay }, "the wedged leg, not the oldest")
        assertEquals(50L, kept.relays[0].quietForSec, "quiet runs from the claim when nothing ever arrived")
        assertEquals(2, kept.omitted, "and the two healthy long-haulers are counted, not hidden")
    }

    @Test
    fun `a leg still receiving sorts below a wedged one however long it has been held`() {
        val rotation = RelayRotation()
        rotation.take(a.url, nowMs = 1)
        rotation.leg(a.url)!!.received(nowMs = 1_099_500)
        rotation.take(b.url, nowMs = 1_090_000)

        val order = rotation.held(nowMs = 1_100_000).relays

        assertEquals(b.url.url, order[0].relay, "quiet 10s beats held 1,100,000ms with events still arriving")
        assertEquals(a.url.url, order[1].relay)
    }

    @Test
    fun `a leg that has delivered nothing is told from one that is downloading`() {
        // The duration alone cannot separate them, and they want opposite
        // responses: a relay with a real backlog is spending its slot well, a
        // walk that cannot terminate is spending the same slot on nothing.
        val rotation = RelayRotation()
        rotation.take(a.url, nowMs = 1_000_000)
        rotation.take(b.url, nowMs = 1_000_000)
        repeat(3) { rotation.leg(a.url)!!.received(nowMs = 1_099_000) }

        val held = rotation.held(nowMs = 1_100_000).relays.associateBy { it.relay }

        assertEquals(3L, held[a.url.url]!!.events)
        assertEquals(1L, held[a.url.url]!!.quietForSec, "it delivered a second ago")
        assertEquals(0L, held[b.url.url]!!.events)
        assertEquals(
            100L,
            held[b.url.url]!!.quietForSec,
            "never delivered, so the quiet clock runs from the claim — the true answer, not a missing one",
        )
    }

    @Test
    fun `not being on a socket is a statement, not an absence`() {
        // Held for hours with no transfer clock is a connect that is not
        // answering; held for hours WITH one is a transfer that is not
        // finishing. Reported as one thing they are the same non-finding.
        val rotation = RelayRotation()
        rotation.take(a.url, nowMs = 1_000_000)

        assertNull(rotation.held(nowMs = 1_100_000).relays[0].transferringForSec)

        runBlocking {
            rotation.transferring(a.url, nowMs = 1_090_000) {
                assertEquals(10L, rotation.held(nowMs = 1_100_000).relays[0].transferringForSec)
            }
        }
        assertNull(
            rotation.held(nowMs = 1_100_000).relays[0].transferringForSec,
            "cleared when the transfer ends — a worker still holding the claim afterwards is doing something else",
        )
    }

    @Test
    fun `the list is capped and says what it left out`() {
        // A fan-out's admission gate is far wider than its transfer pool, so the
        // whole set is neither small nor interesting. A truncation that does not
        // disclose itself reads as the whole answer.
        val rotation = RelayRotation()
        listOf(a, b, c).forEachIndexed { i, r -> rotation.take(r.url, nowMs = 1_000L + i) }

        val held = rotation.held(nowMs = 2_000, limit = 2)

        assertEquals(2, held.relays.size)
        assertEquals(1, held.omitted)
    }

    @Test
    fun `busy count is what tells a working rotation from a stopped one`() {
        val rotation = RelayRotation()
        assertEquals(0, rotation.busyCount())

        rotation.take(a.url)
        rotation.take(b.url)
        assertEquals(2, rotation.busyCount())
        assertEquals(setOf(a.url, b.url), rotation.busyUrls().toSet())

        rotation.release(a.url)
        assertEquals(1, rotation.busyCount())
    }
}
