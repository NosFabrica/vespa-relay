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

import com.nosfabrica.vespa.relay.sync.PoolFixture
import com.nosfabrica.vespa.relay.sync.SyncBands
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** What the search page learns from the sync about one author: the pairs that ask for them, and how far each has got. */
class AuthorCoverageTest {
    private val alice = "a1".repeat(32)
    private val bob = "b0".repeat(32)
    private val carol = "c3".repeat(32)
    private val url = RelayUrlNormalizer.normalize("wss://outbox.example")

    private fun now() = System.currentTimeMillis() / 1000

    private fun rows(doc: JsonObject) = doc["relays"]!!.jsonArray.map { it.jsonObject }

    @Test
    fun `an author's rows are the pairs that ask for them, as far as the walk got`(): Unit =
        runBlocking {
            val streams =
                listOf(
                    PoolFixture.stream("aliceNotes", url, Filter(kinds = listOf(1), authors = listOf(alice))),
                    PoolFixture.stream("bobNotes", url, Filter(kinds = listOf(1), authors = listOf(bob))),
                    // Bound to nobody: reads everyone's events on this relay.
                    PoolFixture.stream("everyone", url, Filter(kinds = listOf(1))),
                )
            val bands = SyncBands(null)
            val corpus = listOf(PoolFixture.event(1, now() - 600, pubKey = alice), PoolFixture.event(2, now() - 300, pubKey = bob))
            val scope = CoroutineScope(SupervisorJob())
            val pool =
                try {
                    PoolFixture(scope, streams, PoolFixture.holding(corpus), bands).pool().also { pool ->
                        pool.start()
                        // Both streams walked, not merely the first: one visit is all a tail waits for.
                        withTimeout(10_000) {
                            while (RelayStatusReport.build(bands.snapshot(), pool.primeUnits(), now())?.let { report ->
                                    report["rows"]!!.jsonArray.count { it.jsonObject["syncStatus"]!!.jsonPrimitive.content == RelayStatusReport.COMPLETE }
                                } != 3
                            ) {
                                delay(20)
                            }
                        }
                    }
                } finally {
                    scope.cancel()
                }
            val coverage = AuthorCoverage(bands, pool::primeUnitsFor)

            val forAlice = coverage.answer(alice, emptyMap())!!
            assertTrue(forAlice["followed"]!!.jsonPrimitive.boolean)
            val row = rows(forAlice).single()
            assertEquals("aliceNotes", row["stream"]!!.jsonPrimitive.content, "only the pair bound to this author, with no relay of theirs named: $forAlice")
            assertEquals(url.url, row["relay"]!!.jsonPrimitive.content)
            assertEquals(RelayStatusReport.COMPLETE, row["syncStatus"]!!.jsonPrimitive.content, "a walk that drained reads complete: $row")

            val forCarol = coverage.answer(carol, emptyMap())!!
            assertFalse(forCarol["followed"]!!.jsonPrimitive.boolean, "no stream is bound to this author, and no relay of theirs was named")
            assertTrue(rows(forCarol).isEmpty())

            // Named as Carol's write relay, the stream bound to nobody reads her events there.
            val elsewhere = "wss://unread.example/"
            val withRelays = coverage.answer(carol, mapOf("relay" to listOf("wss://outbox.example", elsewhere, "not a relay")))!!
            assertTrue(withRelays["followed"]!!.jsonPrimitive.boolean)
            assertEquals(listOf("everyone"), rows(withRelays).map { it["stream"]!!.jsonPrimitive.content }, "$withRelays")
            assertEquals(
                listOf(elsewhere),
                withRelays["unread"]!!.jsonArray.map { it.jsonPrimitive.content },
                "a relay of hers nothing reads is named, and what is not a relay is dropped",
            )
        }

    @Test
    fun `a row says how far the walk got, never why a relay refused us`() {
        val unit =
            RelayStatusReport.PrimeUnit(
                relay = "wss://refusing.example/",
                stream = "content",
                coverageKeys = listOf("content"),
                askKeys = setOf("""{"authors":["$alice"],"kinds":[1]}"""),
                visiting = false,
                live = false,
                kindCap = 3,
                watched = false,
                abortReason = "refused",
                abortSaid = "blocked: internal policy note",
                abortAtSec = now() - 60,
            )
        val coverage = AuthorCoverage(bands = { null }, units = { _, _ -> listOf(unit) })

        val row = rows(coverage.answer(alice, emptyMap())!!).single()
        assertEquals(RelayStatusReport.REFUSED, row["syncStatus"]!!.jsonPrimitive.content)
        for (member in listOf("relaySaid", "refusedFor", "refusedAgoSec", "kindCap", "unwatched", "fault", "asks")) {
            assertNull(row[member], "the public row carries $member: $row")
        }
    }

    @Test
    fun `past its rate a fresh answer is refused, and a cached author is still answered`() {
        var now = 1_000L
        val coverage =
            AuthorCoverage(
                bands = { null },
                units = { _, _ -> emptyList() },
                clock = { now },
                gate = AuthorCoverage.RateGate(perSecond = 1, burst = 1, nowMs = { 1_000_000L }),
            )
        assertEquals(alice, coverage.answer(alice, emptyMap())!!["pubkey"]!!.jsonPrimitive.content)
        assertNull(coverage.answer(bob, emptyMap()), "a second fresh answer in the same instant is over the rate")
        assertEquals(alice, coverage.answer(alice, emptyMap())!!["pubkey"]!!.jsonPrimitive.content, "a fresh answer is served from the cache")
        now += AuthorCoverage.CACHE_SECONDS
        assertNull(coverage.answer(alice, emptyMap()), "an expired answer is a fresh one, and pays the rate")
    }

    @Test
    fun `the gate refills at its rate and never banks past its burst`() {
        var now = 0L
        val gate = AuthorCoverage.RateGate(perSecond = 10, burst = 3, nowMs = { now })
        assertEquals(3, (1..10).count { gate.take() }, "a full bucket is the burst")
        now += 100
        assertEquals(1, (1..10).count { gate.take() }, "one token per tenth of a second")
        now += 60_000
        assertEquals(3, (1..10).count { gate.take() }, "a minute idle still buys only the burst")
    }
}
