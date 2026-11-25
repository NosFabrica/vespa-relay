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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.peers.AuthGate
import com.nosfabrica.vespa.relay.sync.PoolFixture.Companion.event
import com.nosfabrica.vespa.relay.sync.PoolFixture.Companion.stream
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import com.vitorpamplona.quartz.utils.TimeUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The real [VisitPool] against a relay that gates reads by trust, as vespa-relay does with
 * `REQUIRE_READ_LENS`: a relay that refused naming the waiver gets it on every later ask, a relay
 * that never named it never sees a `search`, and a gate that offers no waiver stays a loud refusal.
 */
class VisitPoolWaiverTest {
    private val url = RelayUrlNormalizer.normalize("wss://gated.example")
    private val streams = listOf(stream("content", url, Filter(kinds = listOf(1))))
    private val corpus = (1..5).map { event(it, TimeUtils.now() - 3_600L * it) }

    private val lensRefusal = "auth-required: this relay answers through a web of trust. Sign in, name an observer, or ask with `include:spam`."

    private fun waives(filter: Filter) = SearchQuery.parse(filter.search).includeSpam

    /** A relay that serves [corpus] only to a waiving ask and refuses the rest with [says]. */
    private fun gated(says: String): suspend (Filter, suspend (Event) -> Unit) -> PagedFetchResult =
        { filter, onEvent ->
            if (waives(filter)) {
                PoolFixture.holding(corpus)(filter, onEvent)
            } else {
                PagedFetchResult(0, PagedFetchResult.End.AUTH_REQUIRED, says)
            }
        }

    private suspend fun visit(
        fixture: PoolFixture,
        waivers: LensWaivers,
    ): VisitPool {
        val pool = fixture.pool(waivers = waivers)
        pool.start()
        withTimeout(10_000) {
            while (fixture.tails.isEmpty() && (fixture.counts()["abortedVisits"] ?: 0L) == 0L) delay(20)
        }
        return pool
    }

    @Test
    fun `a relay whose waiver is known gets it on every ask and on the live tail`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val fixture = PoolFixture(scope, streams, gated(lensRefusal))
                visit(fixture, LensWaivers(log = {}).also { it.learn(url, lensRefusal) })
                assertTrue(fixture.asked.isNotEmpty())
                assertTrue(fixture.asked.all(::waives), "every walk: ${fixture.asked.map { it.search }}")
                assertTrue(
                    fixture.tails.values
                        .flatten()
                        .all(::waives),
                    "and the tail: ${fixture.tails}",
                )
                assertEquals(0L, fixture.counts()["abortedVisits"] ?: 0L)
                assertEquals(1L, fixture.counts()["waivedRelays"])
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a relay that refuses naming the waiver is re-asked with it in the same visit`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val fixture = PoolFixture(scope, streams, gated(lensRefusal))
                visit(fixture, LensWaivers(log = {}))
                assertTrue(!waives(fixture.asked.first()), "the first ask is the stream's own")
                assertTrue(fixture.asked.drop(1).all(::waives), "every ask after the refusal waives: ${fixture.asked.map { it.search }}")
                assertTrue(fixture.tails.isNotEmpty(), "the visit finished")
                assertEquals(0L, fixture.counts()["abortedAuthRequired"] ?: 0L, "a refusal the waiver rescued is not an abort")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a relay that never names the waiver never sees a search`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val fixture = PoolFixture(scope, streams, PoolFixture.holding(corpus))
                visit(fixture, LensWaivers(log = {}))
                assertTrue(fixture.asked.isNotEmpty())
                assertTrue(fixture.asked.all { it.search == null }, "asked verbatim: ${fixture.asked.map { it.search }}")
                assertTrue(
                    fixture.tails.values
                        .flatten()
                        .all { it.search == null },
                )
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a wall that offers no waiver is still refused outright, loudly`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val fixture = PoolFixture(scope, streams, gated("auth-required: we only serve authenticated users"))
                visit(fixture, LensWaivers(log = {}))
                assertTrue(fixture.asked.none(::waives), "nothing taught us a waiver: ${fixture.asked.map { it.search }}")
                assertTrue((fixture.counts()["abortedAuthRequired"] ?: 0L) > 0, "counted as the wall it is")
                assertTrue(fixture.tails.isEmpty())
            } finally {
                scope.cancel()
            }
        }

    /**
     * vespa-relay's own refusal names the waiver at its end, past the 200 characters a visit keeps
     * of a relay's sentence; the gate's listener hears the whole frame, and the visit must re-ask on
     * what it learned.
     */
    @Test
    fun `a waiver the gate learned from a long refusal is re-asked with in the same visit`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val waivers = LensWaivers(log = {})
                val long =
                    "auth-required: this relay answers through a web of trust and has no house observer to lend you. " +
                        "Sign in (NIP-42 on the socket, NIP-98 over HTTP), or name whose trust ranks this read with the NIP-50 " +
                        "`observer:<64-hex pubkey>` token, or ask for the whole corpus unranked with `include:spam`."
                val relay = gated(long)
                val fixture =
                    PoolFixture(scope, streams, { filter, onEvent ->
                        relay(filter, onEvent).also { if (it.message != null) waivers.heard(url, AuthGate.Refusal(it.message!!, ofRead = true)) }
                    })
                visit(fixture, waivers)
                assertTrue(fixture.asked.drop(1).all(::waives), "every ask after the refusal waives: ${fixture.asked.map { it.search }}")
                assertTrue(fixture.tails.isNotEmpty(), "the visit finished")
                assertEquals(0L, fixture.counts()["abortedAuthRequired"] ?: 0L)
            } finally {
                scope.cancel()
            }
        }
}
