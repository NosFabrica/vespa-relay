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

import com.vitorpamplona.quartz.nip01Core.store.FtsReindexProgress
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** [readFtsPage]: a page is retried, and a saved cursor the store refuses starts the walk over. */
class FtsReindexPageTest {
    private fun page(cursor: String?) = FtsReindexProgress(cursor = cursor, processedThisBatch = 10, done = cursor == null)

    /**
     * A cursor saved before an author-shard change (or under another shard count) is refused by
     * the store with an IllegalArgumentException, as a cursor Vespa answers 400 to always was.
     * Retried, it failed the same way on every boot and the reindex never ran again.
     */
    @Test
    fun `a saved cursor the store refuses starts the walk over`() =
        runBlocking {
            val asked = mutableListOf<String?>()
            val said = mutableListOf<String>()
            val read =
                readFtsPage("stale", fromSavedFile = true, read = { at ->
                    asked += at
                    if (at == "stale") throw IllegalArgumentException("visit continuation 'stale' was not issued by a walk over these 2 clusters")
                    page("next")
                }, log = { said += it }, pause = {})
            assertTrue(read.restarted)
            assertEquals("next", read.progress.cursor)
            assertEquals(listOf("stale", null), asked, "refused once, then read from the start, with no retry of the stale cursor")
            assertTrue(said.single().contains("starting over"), "$said")
        }

    /** A cursor the store just handed back is not a saved one: refusing it is a failure, retried and then thrown. */
    @Test
    fun `a refusal mid-walk is retried, never read as a reason to start over`() =
        runBlocking {
            var calls = 0
            assertFailsWith<IllegalArgumentException> {
                readFtsPage("mid", fromSavedFile = false, read = {
                    calls++
                    throw IllegalArgumentException("vespa visit 400")
                }, log = {}, pause = {})
            }
            assertEquals(6, calls, "the first try and five retries, all from the same cursor")
        }

    @Test
    fun `a transient failure on a saved cursor is retried from that cursor`() =
        runBlocking {
            val asked = mutableListOf<String?>()
            val read =
                readFtsPage("saved", fromSavedFile = true, read = { at ->
                    asked += at
                    if (asked.size == 1) throw IllegalStateException("vespa visit 503")
                    page(null)
                }, log = {}, pause = {})
            assertFalse(read.restarted)
            assertEquals(listOf<String?>("saved", "saved"), asked)
        }
}
