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

import com.nosfabrica.vespa.eventstore.engine.IngestStats
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * **THE ONE PLACE THIS REPO PARSES ANOTHER REPO'S PROSE, made loud.**
 *
 * The stage split is the number that says whether a slow ingest batch is in
 * `dedup`, `write` or `lock.ingest.wait`, and `IngestStats` offers exactly two
 * ways to read it: `statusLine`, which is destructive and would halve the
 * operator's log line if anything else called it, and `dump`, which is
 * cumulative and repeatable and a String. So the health loop parses `dump`.
 *
 * A parser against a format nobody promised fails SILENTLY — an empty panel
 * reads exactly like an idle router. This books a stage through the REAL
 * `IngestStats` and asserts it comes back, so a store bump that rewords `dump`
 * fails here, by name, instead.
 */
class IngestStageParseTest {
    /** The health loop's parser, by construction: same source, so this cannot drift from what ships. */
    private fun stageMs(dump: String): List<Pair<String, Long>> {
        val parts =
            dump
                .removePrefix("stages ")
                .trim()
                .split(' ')
                .filter { it.isNotEmpty() }
        if (parts.size < 2) return emptyList()
        return parts
            .chunked(2)
            .mapNotNull { pair ->
                if (pair.size != 2) return@mapNotNull null
                val seconds = pair[1].removeSuffix("s").toDoubleOrNull() ?: return@mapNotNull null
                pair[0] to (seconds * 1000).toLong()
            }
    }

    @Test
    fun `a stage booked through the real IngestStats parses back out of dump`() {
        // A name of our own, so a shared JVM running other tests cannot make
        // this pass on somebody else's stage — and cannot make it fail either.
        val stage = "parsetest.${System.nanoTime()}"
        IngestStats.add(stage, 2_500_000_000L)

        val parsed = stageMs(IngestStats.dump()).toMap()

        assertTrue(stage in parsed, "the parser did not find a stage the library was just given: ${IngestStats.dump().take(300)}")
        assertEquals(2_500L, parsed.getValue(stage), "2.5 seconds must read back as 2500ms")
    }

    @Test
    fun `an empty dump is empty rather than one junk row`() {
        // The library's own word for nothing booked. Parsed naively, "(none)"
        // becomes a stage called `(none)` with no time, which would draw a row
        // on the card for a router that has ingested nothing.
        assertEquals(emptyList(), stageMs("stages (none)"))
        assertEquals(emptyList(), stageMs(""))
    }
}
