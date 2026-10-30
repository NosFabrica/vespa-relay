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

import com.nosfabrica.vespa.eventstore.EngineReads
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.time.Duration.Companion.milliseconds

/** What the stats tiers see of the store's grouping read, and what `/stats.json` may repeat of it. */
class StatsVespaTest {
    @Test
    fun `an engine that stops answering fails the query instead of holding the tier`() {
        val vespa = StatsVespa({ _, _, _ -> awaitCancellation() }, requestTimeout = 300.milliseconds)
        runBlocking {
            withTimeout(10_000) {
                val failed = assertFailsWith<StatsQueryFailed> { vespa.group(StatsYql.TOTAL) }
                assertContains(publicReason(failed), "did not answer")
            }
        }
    }

    /** `/stats.json` is public; the store's message can quote Vespa's body, which names hosts and ports. */
    @Test
    fun `what the engine answered never reaches the published message`() {
        val secret = "content-node-7.internal:19107"
        val vespa = StatsVespa({ _, _, _ -> throw IllegalStateException("vespa 502: upstream $secret refused") })
        val failed = assertFailsWith<StatsQueryFailed> { runBlocking { vespa.group(StatsYql.TOTAL) } }
        val published = publicReason(failed)
        assertFalse(secret in published, "leaked: $published")
        assertContains(published, "engine detail in the relay log")
        assertEquals("IllegalArgumentException (detail in the relay log)", publicReason(IllegalArgumentException("/var/lib/secret")))
    }

    @Test
    fun `a shutdown is not a failed statistic`() {
        val vespa = StatsVespa({ _, _, _ -> throw CancellationException("shutting down") })
        assertFailsWith<CancellationException> { runBlocking { vespa.group(StatsYql.TOTAL) } }
    }

    @Test
    fun `a scope reaches the store as the same window and kinds, inclusive at both ends`() {
        val seen = mutableListOf<Pair<EventQuery, EngineReads.Source>>()
        val vespa =
            StatsVespa({ _, query, source ->
                seen += query to source
                JsonObject(emptyMap())
            })
        runBlocking {
            vespa.group(StatsYql.TOTAL, StatsYql.window(10, 20).copy(kinds = listOf(1, 7)))
            vespa.group(StatsYql.TOTAL, StatsYql.after(500))
            vespa.group(StatsYql.TOTAL, source = EngineReads.Source.REPUTATION)
        }
        assertEquals(
            listOf(
                EventQuery(kinds = listOf(1, 7), since = 10, until = 20) to EngineReads.Source.EVENTS,
                EventQuery(since = 501) to EngineReads.Source.EVENTS,
                EventQuery() to EngineReads.Source.REPUTATION,
            ),
            seen,
        )
    }
}
