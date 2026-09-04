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
 * The health loop parses `IngestStats.dump()`, a format the store never
 * promised; a store bump that rewords it must fail here by name, not as an
 * empty panel that reads like an idle router.
 */
class IngestStageParseTest {
    /** The health loop's parser, copied so it cannot drift from what ships. */
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
        // A unique name, so other tests sharing the JVM can neither pass nor fail this one.
        val stage = "parsetest.${System.nanoTime()}"
        IngestStats.add(stage, 2_500_000_000L)

        val parsed = stageMs(IngestStats.dump()).toMap()

        assertTrue(stage in parsed, "the parser did not find a stage the library was just given: ${IngestStats.dump().take(300)}")
        assertEquals(2_500L, parsed.getValue(stage), "2.5 seconds must read back as 2500ms")
    }

    @Test
    fun `an empty dump is empty rather than one junk row`() {
        // "(none)" is the library's word for nothing booked.
        assertEquals(emptyList(), stageMs("stages (none)"))
        assertEquals(emptyList(), stageMs(""))
    }
}
