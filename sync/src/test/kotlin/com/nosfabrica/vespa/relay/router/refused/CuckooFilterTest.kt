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
package com.nosfabrica.vespa.relay.router.refused

import java.io.File
import java.nio.file.Files
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The filter's contract, and the two failure modes that matter are opposites:
 *
 *  - a **false positive** suppresses an event we wanted, silently and forever.
 *    Bounded by the fingerprint width, and measured here rather than asserted
 *    from theory.
 *  - a **false negative** costs one re-download and nothing else, which is why
 *    every concurrency and overflow compromise in the implementation is allowed
 *    to land on that side and never on the other.
 *
 * Ids here are real SHA-256 hex, because the whole bucket/fingerprint scheme is
 * "slice the bits out of the id" — feeding it sequential strings would test a
 * distribution the router never sees.
 */
class CuckooFilterTest {
    private fun id(n: Int): String =
        MessageDigest
            .getInstance("SHA-256")
            .digest("event-$n".toByteArray())
            .joinToString("") { "%02x".format(it) }

    private fun tmpDir(): File = Files.createTempDirectory("cuckoo").toFile().also { it.deleteOnExit() }

    @Test
    fun `an inserted id is reported present`() {
        CuckooFilter.open(null, 1_000).use { f ->
            assertEquals(AddResult.ADDED, f.add(id(1)))
            assertTrue(f.contains(id(1)))
        }
    }

    @Test
    fun `re-adding the same id is idempotent and costs no space`() {
        CuckooFilter.open(null, 1_000).use { f ->
            f.add(id(1))
            assertEquals(AddResult.PRESENT, f.add(id(1)))
            assertEquals(1, f.count, "a repeat refusal must not grow the table")
        }
    }

    @Test
    fun `an id that was never inserted is never reported present`() {
        // SAFETY. A false positive is silent, permanent data loss: we skip an
        // event we wanted, nothing logs it, and the same id hits the same bits
        // forever. Measured over a disjoint population rather than trusted.
        CuckooFilter.open(null, 200_000).use { f ->
            repeat(100_000) { f.add(id(it)) }
            var falsePositives = 0
            repeat(100_000) { if (f.contains(id(1_000_000 + it))) falsePositives++ }
            assertEquals(
                0,
                falsePositives,
                "at a 32-bit fingerprint the expected count over 100k probes is ~0.0002; " +
                    "anything above zero here means the fingerprint or the bucketing is wrong",
            )
        }
    }

    @Test
    fun `every inserted id is still present after the table has been churned`() {
        // The relocation chain moves OTHER fingerprints on every insert. A bug
        // there loses entries, which only ever shows up as a false negative —
        // benign per event, but it would quietly turn suppression off.
        CuckooFilter.open(null, 60_000).use { f ->
            val added = (0 until 50_000).filter { f.add(id(it)) == AddResult.ADDED }
            val missing = added.count { !f.contains(id(it)) }
            assertEquals(0, missing, "$missing of ${added.size} ids were displaced out of the table")
        }
    }

    @Test
    fun `a filter at capacity fails the insert rather than continuing to answer`() {
        // SAFETY, and the entire reason this is cuckoo and not Bloom. A Bloom
        // filter past its design point keeps answering with a false-positive
        // rate in the double digits and logs nothing.
        val f = CuckooFilter.open(null, 1_000)
        var full = false
        var inserted = 0
        for (i in 0 until 20_000) {
            when (f.add(id(i))) {
                AddResult.FULL -> {
                    full = true
                    break
                }

                else -> {
                    inserted++
                }
            }
        }
        f.close()
        assertTrue(full, "the table must refuse an insert instead of degrading silently")
        assertTrue(inserted >= 1_000, "it should hold at least its stated capacity first, held $inserted")
    }

    @Test
    fun `the table round-trips through save and load unchanged`() {
        val dir = tmpDir()
        val file = File(dir, "t.cf")
        CuckooFilter.open(file, 10_000).use { f ->
            repeat(5_000) { f.add(id(it)) }
        }
        CuckooFilter.open(file, 10_000).use { f ->
            assertEquals(5_000, f.count)
            val missing = (0 until 5_000).count { !f.contains(id(it)) }
            assertEquals(0, missing, "$missing ids did not survive the reopen")
        }
    }

    @Test
    fun `a file whose geometry disagrees is rebuilt rather than reinterpreted`() {
        // Reading a table with the wrong bucket count would scatter every
        // lookup — the one way this structure could give confident wrong
        // answers rather than merely forgetting.
        val dir = tmpDir()
        val file = File(dir, "t.cf")
        CuckooFilter.open(file, 10_000).use { it.add(id(1)) }
        CuckooFilter.open(file, 1_000_000).use { f ->
            assertEquals(0, f.count, "a resized filter must start empty, not misread the old one")
            assertFalse(f.contains(id(1)))
        }
    }

    @Test
    fun `bucket count is always a power of two, because the displacement depends on it`() {
        listOf(1, 100, 1_000, 999_999).forEach { capacity ->
            val b = CuckooFilter.bucketsFor(capacity)
            assertEquals(0, b and (b - 1), "bucketsFor($capacity) = $b is not a power of two")
        }
    }

    @Test
    fun `a non-hex id is answerable rather than an exception on the ingest path`() {
        CuckooFilter.open(null, 100).use { f ->
            f.add("not-a-hex-event-id")
            assertTrue(f.contains("not-a-hex-event-id"))
            assertFalse(f.contains("some-other-junk"))
        }
    }
}
