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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.relay.config.defaultRelayLimits
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RelayInfoTest {
    @Test
    fun `advertises the implemented nips and identity`() {
        val doc = Json.parseToJsonElement(relayInfoJson(name = "sot v2", selfPubkey = "f".repeat(64))).jsonObject
        assertEquals("sot v2", doc.getValue("name").jsonPrimitive.content)
        assertEquals("f".repeat(64), doc.getValue("self").jsonPrimitive.content)
        // The one field in this doc that points off the relay. Nothing else fails when it
        // rots, so the ownership move that retargeted it is the reason it is pinned here.
        assertEquals("https://github.com/NosFabrica/vespa-relay", doc.getValue("software").jsonPrimitive.content)
        val nips = doc.getValue("supported_nips").jsonArray.map { it.jsonPrimitive.int }
        assertEquals(listOf(1, 9, 11, 40, 42, 45, 50, 62, 77), nips)
        // The NIP-50 extensions, which on this relay are not garnish: since
        // LensRequiredPolicy, `observer:` and `include:spam` are the two ways an
        // unauthenticated client is answered at all, and this list is the only
        // place it can learn that before being refused.
        val nip50 = doc.getValue("nip50").jsonArray.map { it.jsonPrimitive.content }
        assertTrue("ext observer" in nip50 && "ext include:spam" in nip50, "the two ways past the read gate: $nip50")
        val limitation = doc.getValue("limitation").jsonObject
        // FALSE even with that gate on, and deliberately: both ways past it are
        // unsigned, so claiming auth_required would send every client that reads
        // this document looking for a key it does not need.
        assertEquals("false", limitation.getValue("auth_required").jsonPrimitive.content)
        assertTrue("restricted_writes" in limitation)
    }

    @Test
    fun `limitation block mirrors the enforced RelayLimits`() {
        val doc = Json.parseToJsonElement(relayInfoJson(limits = defaultRelayLimits())).jsonObject
        val limitation = doc.getValue("limitation").jsonObject
        assertEquals(20, limitation.getValue("max_filters").jsonPrimitive.int)
        assertEquals(50, limitation.getValue("max_subscriptions").jsonPrimitive.int)
        assertEquals(5_000, limitation.getValue("max_limit").jsonPrimitive.int)
        // A client that names no `limit` is told it still gets the full window,
        // so its own paging can trust the doc rather than probe for the cutoff.
        assertEquals(5_000, limitation.getValue("default_limit").jsonPrimitive.int)
    }

    @Test
    fun `carries the human contact and an explicit version override`() {
        val doc =
            Json
                .parseToJsonElement(
                    relayInfoJson(contact = "mailto:admin@example.com", version = "9.9-test"),
                ).jsonObject
        assertEquals("mailto:admin@example.com", doc.getValue("contact").jsonPrimitive.content)
        assertEquals("9.9-test", doc.getValue("version").jsonPrimitive.content)
    }

    /**
     * A NIP-86 rewrite is not only the served document: the relay's own kind 0
     * is derived from it, so the holder has to SAY when it changes or the
     * profile keeps publishing what the environment said at boot while the doc
     * says something else. Asserted here because the alternative failure is
     * closed — nothing looks wrong, the two just quietly disagree.
     */
    @Test
    fun `an admin rewrite tells whoever derives from the document`() {
        val seen = mutableListOf<String?>()
        val holder = MutableRelayInfo(buildRelayInfo(Nip11Info(name = "before"), defaultRelayLimits()), onChange = { seen += it.name })
        assertTrue(seen.isEmpty(), "the initial document is the constructor's argument, not a change")

        holder.set(buildRelayInfo(Nip11Info(name = "after"), defaultRelayLimits()))
        assertEquals(listOf<String?>("after"), seen)
        // …and the served json follows in the same step, so a reader of either
        // side sees one document.
        assertEquals(
            "after",
            Json
                .parseToJsonElement(holder.nip11Json())
                .jsonObject
                .getValue("name")
                .jsonPrimitive.content,
        )
    }

    @Test
    fun `advertises NIP-86 only when it is wired`() {
        val withAdmin =
            Json.parseToJsonElement(relayInfoJson(supportedNips = BASE_SUPPORTED_NIPS + 86)).jsonObject
        assertTrue(86 in withAdmin.getValue("supported_nips").jsonArray.map { it.jsonPrimitive.int })
        // Default (no admin) never claims 86.
        val plain = Json.parseToJsonElement(relayInfoJson()).jsonObject
        assertTrue(86 !in plain.getValue("supported_nips").jsonArray.map { it.jsonPrimitive.int })
    }
}
