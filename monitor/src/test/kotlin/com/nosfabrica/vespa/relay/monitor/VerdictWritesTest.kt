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
package com.nosfabrica.vespa.relay.monitor

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/** A stopped batch hands the next one every write it did not land, whatever order the writes finished in. */
class VerdictWritesTest {
    private val urls = (0 until 40).map { RelayUrlNormalizer.normalize("wss://r${it.toString().padStart(2, '0')}.example") }

    @Test
    fun `the next batch resumes at the earliest write that did not land`() =
        runBlocking {
            // From the third write on the store never answers; four writes are in flight when it trips.
            val started = Collections.synchronizedList(mutableListOf<Int>())
            val tally =
                writeEach(urls, concurrency = 4, deadlineMs = 100L, wedgeLimit = 3, wedgeBudgetMs = Long.MAX_VALUE, progress = null, stage = "write") { url ->
                    val i = urls.indexOf(url)
                    started += i
                    if (i >= 2) CompletableDeferred<Unit>().await()
                    true
                }
            assertNotNull(tally.stoppedBy, "a store that stopped answering must stop the batch")
            assertEquals(urls[2], tally.resumeAt, "a write started before the one that tripped the limit was skipped")
            // Every write is published, declined, wedged or never started, and nothing is counted twice.
            assertEquals(started.size, tally.published + tally.declined + tally.wedged)
            assertEquals(2, tally.published)
        }

    @Test
    fun `a batch that ran to its end leaves no cursor`() =
        runBlocking {
            val tally = writeEach(urls, concurrency = 4, deadlineMs = 1_000L, wedgeLimit = 3, wedgeBudgetMs = Long.MAX_VALUE, progress = null, stage = "write") { true }
            assertEquals(null, tally.resumeAt)
            assertEquals(urls.size, tally.published)
        }
}
