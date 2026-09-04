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
package com.nosfabrica.vespa.relay.status

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Builds the coverage card from the `before.json` and `after.json` that
 * `SyncBandsProdScaleProbe` writes, and asserts the prune removed the unnamed
 * groups and moved no named stream by a single row. Selected by
 * `-DprodScaleProbe=true` with the same `-DprodScaleDir`; run the :sync probe first.
 */
class SyncCoverageReportProdScaleProbe {
    companion object {
        /** Enough rounds for both inputs to be JIT-warm; the best of them is reported. */
        private const val ROUNDS = 5
    }

    private val dir = File(System.getProperty("prodScaleDir") ?: "build/prod-scale")

    /** One group of the card, as a reader of `/stats.json` sees it. */
    private data class Row(
        val name: String?,
        val relays: Long,
        val hosts: Long,
        val reconciled: Long,
        val paged: Long,
    ) {
        override fun toString() =
            "name=${name ?: "None"}".padEnd(28) +
                "relays=$relays".padEnd(14) +
                "reconciled=$reconciled".padEnd(18) +
                "paged=$paged"
    }

    private fun groups(o: JsonObject): List<Row> =
        o["streams"]!!.jsonArray.map {
            val g = it.jsonObject
            Row(
                name = g["name"]?.jsonPrimitive?.content,
                relays = g["relays"]?.jsonPrimitive?.longOrNull ?: 0,
                hosts = g["hosts"]?.jsonPrimitive?.longOrNull ?: 0,
                reconciled = g["reconciled"]?.jsonPrimitive?.longOrNull ?: 0,
                paged = g["paged"]?.jsonPrimitive?.longOrNull ?: 0,
            ).also { r ->
                if (r.name == null) assertTrue(g["unnamed"]?.jsonPrimitive?.booleanOrNull == true, "an unnamed group must say so")
            }
        }

    @Test
    fun `the card loses its unnamed groups and keeps every real stream`() {
        if (System.getProperty("prodScaleProbe") != "true") {
            println("[skip] SyncCoverageReportProdScaleProbe — set -DprodScaleProbe=true, after the :sync probe has written the files")
            return
        }
        val before = File(dir, "before.json")
        val after = File(dir, "after.json")
        if (!before.isFile || !after.isFile) {
            println("[skip] no corpus in ${dir.absolutePath} — run SyncBandsProdScaleProbe with the same -DprodScaleDir first")
            return
        }

        val now = 1_754_950_000L
        val beforeText = before.readText()
        val afterText = after.readText()

        // Interleaved warm rounds: a single cold call measures the JIT, not the parser.
        var cardBefore = SyncCoverageReport.build(beforeText, null, now)!!
        var cardAfter = SyncCoverageReport.build(afterText, null, now)!!
        var msBefore = Long.MAX_VALUE
        var msAfter = Long.MAX_VALUE
        repeat(ROUNDS) {
            val t0 = System.nanoTime()
            cardBefore = SyncCoverageReport.build(beforeText, null, now)!!
            msBefore = minOf(msBefore, (System.nanoTime() - t0) / 1_000_000)
            val t1 = System.nanoTime()
            cardAfter = SyncCoverageReport.build(afterText, null, now)!!
            msAfter = minOf(msAfter, (System.nanoTime() - t1) / 1_000_000)
        }

        val rowsBefore = groups(cardBefore)
        val rowsAfter = groups(cardAfter)

        println("── the card BEFORE the prune (${before.length() / 1_000_000.0} MB, ${msBefore}ms warm) ──")
        rowsBefore.forEach { println("  $it") }
        println("── the card AFTER  the prune (${after.length() / 1_000_000.0} MB, ${msAfter}ms warm) ──")
        rowsAfter.forEach { println("  $it") }

        val unnamedBefore = rowsBefore.filter { it.name == null }
        val unnamedAfter = rowsAfter.filter { it.name == null }
        println("── verdict ─────────────────────────────────────────────")
        println("  unnamed groups       ${unnamedBefore.size} -> ${unnamedAfter.size}")
        println("  rows under them      ${unnamedBefore.sumOf { it.relays }} -> ${unnamedAfter.sumOf { it.relays }}")
        println("  report build (warm)  ${msBefore}ms -> ${msAfter}ms   best of $ROUNDS")

        assertEquals(3, unnamedBefore.size, "the corpus must reproduce the three unnamed groups first")
        unnamedBefore.forEach { assertEquals(0, it.reconciled, "an unnamed group reconciles nothing — that is what made it unreadable") }

        assertEquals(emptyList(), unnamedAfter, "no unnamed group survives the prune")

        // Whole rows, so a stream that gained or lost one relay or flipped one band fails here.
        assertEquals(rowsBefore.filter { it.name != null }, rowsAfter, "every named stream is untouched, row for row")

        // `relays` is deduplicated across groups, so this also catches a prune that took a shared url.
        fun total(
            o: JsonObject,
            k: String,
        ) = o[k]!!.jsonPrimitive.long
        val lostRows = total(cardBefore, "rows") - total(cardAfter, "rows")
        println("  document rows        ${total(cardBefore, "rows")} -> ${total(cardAfter, "rows")}  (-$lostRows)")
        println("  document relays      ${total(cardBefore, "relays")} -> ${total(cardAfter, "relays")}")
        assertEquals(unnamedBefore.sumOf { it.relays }, lostRows, "the rows lost are exactly the unnamed ones")

        // The frame is the one thing the prune may move: `from` is the deepest floor in the data,
        // so removing a stale flat band can raise it once.
        val frameBefore = total(cardBefore, "from")
        val frameAfter = total(cardAfter, "from")
        println("  frame `from`         $frameBefore -> $frameAfter  (${if (frameBefore == frameAfter) "unmoved" else "+${frameAfter - frameBefore}s, a one-time shift"})")
        assertTrue(frameAfter >= frameBefore, "dropping bands can only RAISE the floor, never deepen it")
        assertTrue(frameAfter <= now, "and the frame may never invert")
    }
}
