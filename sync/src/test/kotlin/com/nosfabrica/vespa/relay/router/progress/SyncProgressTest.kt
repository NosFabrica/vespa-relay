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
package com.nosfabrica.vespa.relay.router.progress

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * What the router publishes about itself.
 *
 * The property under test throughout is that THE COUNTS ADD UP. A production
 * document reported 16,752 relays discovered and 5,323 carrying a band with no
 * account of the ~11,400 in between, and every fix for that is worthless if the
 * replacement partition can silently stop being one.
 */
class SyncProgressTest {
    private fun cycleOf(doc: JsonObject) = doc["streams"]!!.let { (it as kotlinx.serialization.json.JsonArray)[0] }.jsonObject["cycle"]!!.jsonObject

    private fun sumOfTaken(cycle: JsonObject) = cycle["taken"]!!.jsonObject.values.sumOf { it.jsonPrimitive.long }

    @Test
    fun `the url counts partition the discovered set`() {
        val tally = CycleTally(discovered = 16_752, foldedOntoAnother = 11_429, hosts = 850)
        val doc = SyncProgress.document(listOf(streamWith(tally)), nowSeconds = 1_000)

        val urls = cycleOf(doc)["urls"]!!.jsonObject
        assertEquals(16_752, urls["discovered"]!!.jsonPrimitive.int)
        assertEquals(
            urls["discovered"]!!.jsonPrimitive.int,
            urls["foldedOntoAnother"]!!.jsonPrimitive.int + urls["taken"]!!.jsonPrimitive.int,
            "the two halves of the first identity must cover the discovered set exactly",
        )
    }

    @Test
    fun `the outcomes sum to the urls taken WHILE the cycle is still running`() {
        // The one that matters. A partition that only closes at the end is a
        // partition nobody can check, because the interesting moment is
        // mid-fan-out.
        val tally = CycleTally(discovered = 100, foldedOntoAnother = 10, hosts = 40)
        tally.delivered.set(20)
        tally.nothingNew.set(15)
        tally.unreachable.set(5)
        tally.noRoute.set(7)

        val cycle = cycleOf(SyncProgress.document(listOf(streamWith(tally)), nowSeconds = 1_000))

        assertEquals(90, cycle["urls"]!!.jsonObject["taken"]!!.jsonPrimitive.int)
        assertEquals(43, cycle["taken"]!!.jsonObject["pending"]!!.jsonPrimitive.int, "the rest are still in flight")
        assertEquals(90L, sumOfTaken(cycle))
        assertTrue(cycle["balanced"]!!.jsonPrimitive.content.toBoolean())
    }

    @Test
    fun `a finished cycle has nothing pending`() {
        val tally = CycleTally(discovered = 10, foldedOntoAnother = 0, hosts = 4)
        tally.delivered.set(6)
        tally.nothingNew.set(2)
        tally.transferFailed.set(1)
        tally.torUnavailable.set(1)

        val cycle = cycleOf(SyncProgress.document(listOf(streamWith(tally)), nowSeconds = 1_000))

        assertEquals(0, cycle["taken"]!!.jsonObject["pending"]!!.jsonPrimitive.int)
        assertEquals(10L, sumOfTaken(cycle))
    }

    @Test
    fun `a double-counted url is disclosed rather than hidden by a clamp`() {
        // `pending` clamps at zero so a reader's arithmetic can never go
        // negative — which would otherwise let a miscount pass as a disposition.
        // `balanced` is the only thing that then says the numbers are wrong.
        val tally = CycleTally(discovered = 5, foldedOntoAnother = 0, hosts = 5)
        tally.delivered.set(4)
        tally.nothingNew.set(3)

        val cycle = cycleOf(SyncProgress.document(listOf(streamWith(tally)), nowSeconds = 1_000))

        assertEquals(0, cycle["taken"]!!.jsonObject["pending"]!!.jsonPrimitive.int, "never negative")
        assertFalse(cycle["balanced"]!!.jsonPrimitive.content.toBoolean(), "and it must say so")
    }

    @Test
    fun `hosts is published beside the urls, because urls are not servers`() {
        // 3,272 urls resolved to 850 hosts in the run this comes from. Publishing
        // only the folded number would hide the very inflation it discloses.
        val tally = CycleTally(discovered = 3_272, foldedOntoAnother = 0, hosts = 850)
        val cycle = cycleOf(SyncProgress.document(listOf(streamWith(tally)), nowSeconds = 1_000))

        assertEquals(850, cycle["hosts"]!!.jsonPrimitive.int)
        assertEquals(3_272, cycle["urls"]!!.jsonObject["taken"]!!.jsonPrimitive.int)
    }

    @Test
    fun `writtenAt advances even when nothing is happening`() {
        // The heartbeat. A router with no stream to report must still stamp the
        // document, or a quiet mirror and a stopped one are the same file.
        val doc = SyncProgress.document(emptyList(), nowSeconds = 1_770_000_000)

        assertEquals(1_770_000_000L, doc["writtenAt"]!!.jsonPrimitive.long)
    }

    @Test
    fun `a stream with no cycle yet publishes its phase and no cycle`() {
        val s = StreamPhases.Stream("content", "discovering", 12, null, null, null, null)
        val stream = (SyncProgress.document(listOf(s), 1_000)["streams"] as kotlinx.serialization.json.JsonArray)[0].jsonObject

        assertEquals("discovering", stream["phase"]!!.jsonPrimitive.content)
        assertEquals(12L, stream["phaseForSec"]!!.jsonPrimitive.long)
        assertNull(stream["cycle"], "an invented empty cycle is a claim that one ran")
    }

    @Test
    fun `an unset path writes nothing and says it writes nothing`() {
        val progress = SyncProgress(null)

        assertFalse(progress.publishes)
        assertFalse(progress.write(emptyList()))
    }

    @Test
    fun `a real write lands as parseable JSON`() {
        val dir =
            kotlin.io.path
                .createTempDirectory("sync-progress")
                .toFile()
        val file = java.io.File(dir, "nested/sync-progress.json")
        val tally = CycleTally(discovered = 3, foldedOntoAnother = 1, hosts = 2)
        tally.delivered.set(2)

        assertTrue(SyncProgress(file).write(listOf(streamWith(tally)), nowSeconds = 42))

        val parsed =
            kotlinx.serialization.json.Json
                .parseToJsonElement(file.readText())
                .jsonObject
        assertEquals(42L, parsed["writtenAt"]!!.jsonPrimitive.long)
        assertEquals(0L, sumOfTaken(cycleOf(parsed)) - 2L, "the two taken urls are both delivered")
        dir.deleteRecursively()
    }

    private fun streamWith(tally: CycleTally) =
        StreamPhases.Stream(
            name = "content",
            phase = "fetching",
            phaseForSec = 412,
            tally = tally,
            cycleStartedSec = 900,
            cycleEndedSec = null,
            outcome = "running",
        )
}
