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
        val limitation = doc.getValue("limitation").jsonObject
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
