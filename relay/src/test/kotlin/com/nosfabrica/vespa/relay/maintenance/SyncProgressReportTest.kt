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
package com.nosfabrica.vespa.relay.maintenance

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The relay's read of the router's progress file.
 *
 * Two properties, and both are about not trusting the writer: the partition is
 * RE-DERIVED here rather than forwarded, so `accountedFor` is a statement about
 * the document being served; and every member is rebuilt individually, so a
 * hand-edited or half-migrated file can cost this object and nothing else.
 */
class SyncProgressReportTest {
    private val live =
        """
        {
          "writtenAt": 1770000000,
          "streams": [
            {
              "name": "content",
              "phase": "fetching",
              "phaseForSec": 412,
              "cycle": {
                "startedAt": 1769999000,
                "outcome": "running",
                "urls": {"discovered": 16752, "foldedOntoAnother": 11429, "excluded": 0, "taken": 5323},
                "hosts": 850,
                "taken": {"delivered": 2200, "nothingNew": 900, "unreachable": 800,
                          "transferFailed": 100, "noRoute": 1000, "hostStruckOut": 200,
                          "knownDead": 100, "torUnavailable": 0, "pending": 23},
                "foldedOnto": {"relays": [{"relay": "wss://nostr.oxtr.dev/", "urls": 55,
                                           "examples": ["wss://nostr.oxtr.dev/alpha", "wss://nostr.oxtr.dev/beta", "wss://nostr.oxtr.dev/x"]}],
                               "omitted": 480},
                "balanced": true,
                "received": 481203
              }
            }
          ]
        }
        """.trimIndent()

    private fun firstCycle(doc: JsonObject) = (doc["streams"] as JsonArray)[0].jsonObject["cycle"]!!.jsonObject

    @Test
    fun `the disposition accounts for every discovered url`() {
        // The number this whole file exists to produce: 16,752 discovered against
        // 5,323 band-bearing used to leave ~11,400 with no published disposition
        // at all.
        val doc = SyncProgressReport.build(live, nowSeconds = 1_770_000_060)!!
        val cycle = firstCycle(doc)
        val urls = cycle["urls"]!!.jsonObject
        val taken = cycle["taken"]!!.jsonObject

        assertEquals(16_752L, urls["discovered"]!!.jsonPrimitive.long)
        assertEquals(
            urls["discovered"]!!.jsonPrimitive.long,
            urls["foldedOntoAnother"]!!.jsonPrimitive.long + urls["excluded"]!!.jsonPrimitive.long + urls["taken"]!!.jsonPrimitive.long,
        )
        assertEquals(5_323L, taken.values.sumOf { it.jsonPrimitive.long })
        assertTrue(cycle["accountedFor"]!!.jsonPrimitive.booleanOrNull!!)
    }

    @Test
    fun `staleness is measured against THIS rollup's clock, not the file's`() {
        // A router that stopped writing an hour ago has to say so however recent
        // its own last timestamp looked.
        val doc = SyncProgressReport.build(live, nowSeconds = 1_770_003_600)!!

        assertEquals(3_600L, doc["staleForSec"]!!.jsonPrimitive.long)
    }

    @Test
    fun `a file stamped in the future is skew, not a negative age`() {
        val doc = SyncProgressReport.build(live, nowSeconds = 1_769_999_999)!!

        assertEquals(0L, doc["staleForSec"]!!.jsonPrimitive.long, "a negative age reads as a bug in the relay")
    }

    @Test
    fun `a partition that does not hold is published as not holding`() {
        // Forwarding the writer's own `balanced` would make this side blind to a
        // file that says one thing and carries another.
        val broken = live.replace("\"delivered\": 2200", "\"delivered\": 2500")
        val cycle = firstCycle(SyncProgressReport.build(broken, nowSeconds = 1_770_000_000)!!)

        assertFalse(cycle["accountedFor"]!!.jsonPrimitive.booleanOrNull!!, "the outcomes no longer sum to `taken`")
        assertTrue(cycle["balanced"]!!.jsonPrimitive.booleanOrNull!!, "and the router still thinks they do — which localises the fault")
    }

    @Test
    fun `an outcome the file omits counts as zero rather than shrinking the sum`() {
        // The member list is fixed on this side. Taking it from whatever the
        // writer emitted would let a future router widen the total silently.
        val thin =
            """
            {"writtenAt": 1, "streams": [{"name": "s", "cycle": {
              "urls": {"discovered": 4, "foldedOntoAnother": 0, "taken": 4},
              "taken": {"delivered": 4}}}]}
            """.trimIndent()
        val taken = firstCycle(SyncProgressReport.build(thin, nowSeconds = 1)!!)["taken"]!!.jsonObject

        assertEquals(9, taken.size, "every outcome is named, present in the file or not")
        assertEquals(0L, taken["noRoute"]!!.jsonPrimitive.long)
        assertEquals(4L, taken.values.sumOf { it.jsonPrimitive.long })
    }

    @Test
    fun `the fold summary names survivors, capped again on this side, truncation disclosed`() {
        // The router already bounds its list; this bounds it a second time
        // rather than trusting that it did, because the cap is the only thing
        // between a hand-edited file and an unbounded array in a served
        // document.
        val fold = firstCycle(SyncProgressReport.build(live, nowSeconds = 1_770_000_000)!!)["foldedOnto"]!!.jsonObject
        val row = (fold["relays"] as JsonArray)[0].jsonObject

        assertEquals("wss://nostr.oxtr.dev/", row["relay"]!!.jsonPrimitive.content)
        assertEquals(55L, row["urls"]!!.jsonPrimitive.long)
        assertEquals(2, (row["examples"] as JsonArray).size, "examples are capped on this side too")
        assertEquals(480L, fold["omitted"]!!.jsonPrimitive.long, "and what was left out is carried through")
    }

    @Test
    fun `the two not-dialled-for-being-dead states are counted apart`() {
        // One is out until a signed record ages past its TTL; the other is back
        // on the next cycle. As one number they answered "will it try again"
        // both ways at once.
        val taken = firstCycle(SyncProgressReport.build(live, nowSeconds = 1_770_000_000)!!)["taken"]!!.jsonObject

        assertEquals(200L, taken["hostStruckOut"]!!.jsonPrimitive.long)
        assertEquals(100L, taken["knownDead"]!!.jsonPrimitive.long)
    }

    @Test
    fun `a stream with no name says nothing and is dropped`() {
        val doc = SyncProgressReport.build("""{"writtenAt": 1, "streams": [{"phase": "idle"}]}""", nowSeconds = 1)!!

        assertEquals(0, (doc["streams"] as JsonArray).size)
    }

    @Test
    fun `an object where a name should be costs this object, not the section`() {
        // `jsonPrimitive` is an ASSERTION and throws on an object. One of these
        // in a file this process did not write would take the whole sync section
        // — the coverage half with it.
        val doc = SyncProgressReport.build("""{"writtenAt": 1, "streams": [{"name": {}}]}""", nowSeconds = 1)!!

        assertEquals(0, (doc["streams"] as JsonArray).size)
    }

    @Test
    fun `a corrupt or absent file is absent, never an exception`() {
        assertNull(SyncProgressReport.build(null, nowSeconds = 1))
        assertNull(SyncProgressReport.build("", nowSeconds = 1))
        assertNull(SyncProgressReport.build("{not json", nowSeconds = 1))
        assertNull(SyncProgressReport.build("[]", nowSeconds = 1))
        assertNull(SyncProgressReport.build("{}", nowSeconds = 1), "neither a heartbeat nor a stream is not a router being quiet")
    }

    @Test
    fun `a heartbeat with no streams still publishes, because that is the finding`() {
        // A router that is up and running nothing is a real state, and the one
        // the heartbeat exists to distinguish from a router that is gone.
        val doc = SyncProgressReport.build("""{"writtenAt": 1770000000, "streams": []}""", nowSeconds = 1_770_000_010)!!

        assertEquals(10L, doc["staleForSec"]!!.jsonPrimitive.long)
        assertEquals(0, (doc["streams"] as JsonArray).size)
    }
}
