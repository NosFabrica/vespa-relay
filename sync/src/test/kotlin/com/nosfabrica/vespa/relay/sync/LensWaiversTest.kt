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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11FetchException
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11Fetcher
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11RelayInformation
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** Which relays are read with `include:spam`, and what the token does to a stream's own search. */
class LensWaiversTest {
    private val vespa = RelayUrlNormalizer.normalize("wss://search.example")
    private val strfry = RelayUrlNormalizer.normalize("wss://strfry.example")
    private val hex = "c".repeat(64)

    private val documents =
        mapOf(
            vespa to listOf("ext include:spam", "ext observer"),
            strfry to null,
        )
    private val waivers = LensWaivers({ documents[it] }, log = {})

    private fun parsed(filter: Filter) = SearchQuery.parse(filter.search)

    @Test
    fun `a relay that advertises the waiver is read with it, and one that does not is asked verbatim`() =
        runBlocking {
            val ask = Filter(kinds = listOf(1), since = 10)
            assertEquals("include:spam", waivers.onWire(vespa, ask).search)
            assertSame(ask, waivers.onWire(strfry, ask), "a relay without NIP-50 refuses any filter carrying search")
            assertEquals(1, waivers.waived)
        }

    @Test
    fun `the waiver joins whatever the stream already searches for`() {
        val cases =
            listOf(
                "bitcoin",
                "\"exact phrase\" -spam",
                "observer:$hex",
                "pizza sort:rank filter:rank:gte:50",
            )
        for (search in cases) {
            val before = SearchQuery.parse(search)
            val after = parsed(LensWaivers.waived(Filter(kinds = listOf(1), search = search)))
            assertTrue(after.includeSpam, search)
            assertEquals(before.terms, after.terms, search)
            assertEquals(before.phrases, after.phrases, search)
            assertEquals(before.notTerms, after.notTerms, search)
            assertEquals(before.extensions, after.extensions - SearchQuery.INCLUDE, search)
        }
        assertEquals("bitcoin include:spam", LensWaivers.waived(Filter(search = "bitcoin")).search, "appended as written")
    }

    @Test
    fun `an unclosed quote is rebuilt rather than swallowing the waiver`() {
        val waived = LensWaivers.waived(Filter(search = "\"bitcoin core"))
        val q = parsed(waived)
        assertTrue(q.includeSpam, waived.search)
        assertEquals(listOf("bitcoin core"), q.phrases)
    }

    @Test
    fun `a filter that already waives is left alone`() {
        val f = Filter(kinds = listOf(1), search = "include:spam observer:$hex")
        assertSame(f, LensWaivers.waived(f))
    }

    @Test
    fun `a refusal teaches the waiver only when it names it as the way in`() {
        val gated = RelayUrlNormalizer.normalize("wss://gated.example")
        assertFalse(waivers.learn(gated, null))
        assertFalse(waivers.learn(gated, "blocked: include:spam is not for you"), "not a lens refusal")
        assertFalse(waivers.learn(gated, "auth-required: we only serve authenticated users"), "a wall that offers no waiver")
        assertTrue(waivers.learn(gated, "auth-required: sign in, name an observer, or ask with `include:spam`."))
        assertFalse(waivers.learn(gated, "auth-required: sign in, name an observer, or ask with `include:spam`."), "learned once")
        assertTrue(runBlocking { waivers.waives(gated) })
    }

    @Test
    fun `a NIP-11 that cannot be read reads as no waiver`() =
        runBlocking {
            val failing =
                object : Nip11Fetcher {
                    override suspend fun fetch(relay: NormalizedRelayUrl): Nip11RelayInformation = throw Nip11FetchException("HTTP 404")
                }
            assertNull(LensWaivers.nip50Off(failing)(vespa))
            assertFalse(LensWaivers({ LensWaivers.nip50Off(failing)(it) }, log = {}).waives(vespa))
        }
}
