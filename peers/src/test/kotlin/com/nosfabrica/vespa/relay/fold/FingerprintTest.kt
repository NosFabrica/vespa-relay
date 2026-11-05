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
package com.nosfabrica.vespa.relay.fold

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

/** A fingerprint counts exactly what the id sets it was built from have in common. */
class FingerprintTest {
    private fun hexId(random: Random): String = (1..4).joinToString("") { "%016x".format(random.nextLong()) }

    @Test
    fun `shared is the intersection of the id sets, whatever the ids look like`() {
        val random = Random(500)
        repeat(300) {
            val pool =
                (0 until 80).map {
                    when (random.nextInt(6)) {
                        0 -> "odd-${random.nextInt(40)}"

                        1 -> hexId(random).uppercase()

                        // Shares its first word with a plain hex id, so only the full id tells them apart.
                        2 -> "0000000000000000" + hexId(random).substring(16)

                        else -> hexId(random)
                    }
                }
            val a = pool.shuffled(random).take(random.nextInt(60)).toSet()
            val b = pool.shuffled(random).take(random.nextInt(60)).toSet()

            val fa = Fingerprint.of(a)
            val fb = Fingerprint.of(b)

            assertEquals(a.size, fa.size)
            assertEquals(a.count { it in b }, fa.shared(fb))
            assertEquals(fa.shared(fb), fb.shared(fa))
        }
    }

    @Test
    fun `an id differing only in case or in its last word is a different id`() {
        val random = Random(7)
        val id = hexId(random)
        val lastWord = id.substring(0, 48) + (if (id[63] == '0') '1' else '0') + id.substring(49)

        assertEquals(0, Fingerprint.of(setOf(id)).shared(Fingerprint.of(setOf(id.uppercase()))))
        assertEquals(0, Fingerprint.of(setOf(id)).shared(Fingerprint.of(setOf(lastWord))))
        assertEquals(1, Fingerprint.of(listOf(id, id)).size)
        assertEquals(Fingerprint.of(setOf(id, "x")), Fingerprint.of(listOf("x", id)))
        assertNotEquals(Fingerprint.of(setOf(id)), Fingerprint.of(setOf(lastWord)))
    }

    @Test
    fun `the index counts what every window added shares with a new one`() {
        val random = Random(66)
        val pool = (0 until 200).map { if (random.nextInt(5) == 0) "odd-$it" else hexId(random) }
        val windows = (0 until 30).map { pool.shuffled(random).take(random.nextInt(40)).toSet() }
        val index = FingerprintIndex(windows.sumOf { w -> w.count { it.length == 64 && !it.startsWith("odd") } })
        val counts = IntArray(windows.size)
        val touched = IntArray(windows.size)
        for ((k, window) in windows.withIndex()) {
            val print = Fingerprint.of(window)
            val hits = index.count(print, counts, touched)

            for (w in 0 until k) assertEquals(windows[w].count { it in window }, counts[w], "window $k against $w")
            assertEquals((0 until k).count { w -> windows[w].any { it in window } }, hits)
            for (t in 0 until hits) counts[touched[t]] = 0
            index.add(print)
        }
    }
}
