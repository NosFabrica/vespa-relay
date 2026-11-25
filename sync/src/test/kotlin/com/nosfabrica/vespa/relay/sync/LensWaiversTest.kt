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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** Which relays are read with `include:spam` or signed in to, and what the token does to a stream's own search. */
class LensWaiversTest {
    private val gated = RelayUrlNormalizer.normalize("wss://search.example")
    private val paid = RelayUrlNormalizer.normalize("wss://paid.example")
    private val strfry = RelayUrlNormalizer.normalize("wss://strfry.example")
    private val hex = "c".repeat(64)
    private val lensRefusal = "auth-required: this relay answers through a web of trust. Sign in, or ask with `include:spam`."

    private fun parsed(filter: Filter) = SearchQuery.parse(filter.search)

    @Test
    fun `a relay is asked verbatim until a refusal names the waiver`() {
        val waivers = LensWaivers(log = {})
        val ask = Filter(kinds = listOf(1), since = 10)
        assertSame(ask, waivers.onWire(strfry, ask), "a relay without NIP-50 refuses any filter carrying search")
        waivers.heard(gated, lensRefusal)
        assertEquals("include:spam", waivers.onWire(gated, ask).search)
        assertSame(ask, waivers.onWire(strfry, ask))
        assertEquals(1, waivers.waived)
    }

    @Test
    fun `only a refusal that names no waiver signs us in`() {
        val waivers = LensWaivers(log = {})
        assertFalse(waivers.mayAuthenticate(gated), "anonymous until asked")
        waivers.heard(gated, lensRefusal)
        assertFalse(waivers.mayAuthenticate(gated), "the waiver is the way in, not our key")
        waivers.heard(paid, "auth-required: members only")
        assertTrue(waivers.mayAuthenticate(paid))
        waivers.heard(strfry, "restricted: not on the whitelist")
        assertFalse(waivers.mayAuthenticate(strfry), "restricted asks for no AUTH")
    }

    @Test
    fun `a signed-in relay with no waiver may be answering through a lens`() {
        val waivers = LensWaivers(log = {})
        waivers.heard(paid, "auth-required: members only")
        assertTrue(waivers.mayBeLensed(paid))
        waivers.heard(paid, lensRefusal)
        assertFalse(waivers.mayBeLensed(paid), "once its reads carry the waiver")
        assertFalse(waivers.mayBeLensed(strfry), "never signed in")
    }

    @Test
    fun `the waiver joins whatever the stream already searches for`() {
        for (search in listOf("bitcoin", "\"exact phrase\" -spam", "observer:$hex", "pizza sort:rank filter:rank:gte:50")) {
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
    fun `include holds one value, so a stream's own include gives way to the waiver`() {
        val waived = parsed(LensWaivers.waived(Filter(search = "pizza include:nsfw")))
        assertEquals(SearchQuery.SPAM, waived.extensions[SearchQuery.INCLUDE], "quartz keeps the last of a key")
        assertEquals("pizza", waived.terms)
        assertTrue(waived.includeSpam)
    }

    @Test
    fun `an unclosed quote is rebuilt rather than swallowing the waiver`() {
        val q = parsed(LensWaivers.waived(Filter(search = "\"bitcoin core")))
        assertTrue(q.includeSpam)
        assertEquals(listOf("bitcoin core"), q.phrases)
    }

    @Test
    fun `a filter that already waives is left alone`() {
        val f = Filter(kinds = listOf(1), search = "include:spam observer:$hex")
        assertSame(f, LensWaivers.waived(f))
    }

    @Test
    fun `a learned waiver is forgotten only when the relay refuses the search field`() {
        val waivers = LensWaivers(log = {})
        assertTrue(waivers.learn(gated, lensRefusal))
        assertFalse(waivers.unlearn(gated, "auth-required: members only"), "not about the field")
        assertFalse(waivers.unlearn(gated, "blocked: rate limited"))
        assertTrue(waivers.unlearn(gated, "error: unrecognised filter item: search"))
        assertFalse(waivers.waives(gated))
    }

    @Test
    fun `retain keeps the roster and the declared relays`() {
        val declared = RelayUrlNormalizer.normalize("wss://declared.example")
        val waivers = LensWaivers(pinned = setOf(declared), log = {})
        listOf(gated, declared, strfry).forEach { waivers.learn(it, lensRefusal) }
        waivers.retain(setOf(gated))
        assertTrue(waivers.waives(gated))
        assertTrue(waivers.waives(declared), "pinned")
        assertFalse(waivers.waives(strfry), "off the roster")
    }

    @Test
    fun `none is a registry of its own each time`() {
        val one = LensWaivers.none()
        one.learn(gated, lensRefusal)
        assertFalse(LensWaivers.none().waives(gated), "what one learns no other default sees")
    }
}
