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

import com.nosfabrica.vespa.relay.peers.Verdict
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A NIP-11 document is a stranger's JSON: every field optional, every type a
 * suggestion, and a parse that throws costs the facts beside the malformed one.
 */
class RelayDocumentTest {
    @Test
    fun `the four fields with somewhere to go are read, in NIP-66's spelling`() {
        val doc =
            RelayDocument.parse(
                """
                {
                  "name": "the name", "description": "not published anywhere",
                  "software": "git+https://github.com/hoytech/strfry.git", "version": "1.0.3",
                  "supported_nips": [1, 11, 50],
                  "limitation": { "auth_required": false, "payment_required": true, "min_pow_difficulty": 0, "restricted_writes": true }
                }
                """.trimIndent(),
            )!!
        assertEquals("git+https://github.com/hoytech/strfry.git", doc.software)
        assertEquals("1.0.3", doc.version)
        assertEquals(listOf(1, 11, 50), doc.supportedNips)
        // The key names the restriction, not the permission: `restricted_writes: true` is `writes`.
        assertEquals(setOf("!auth", "payment", "!pow", "writes"), doc.requirements.toSet())
        assertEquals(
            listOf("!writes"),
            RelayDocument.parse("""{ "limitation": { "restricted_writes": false } }""")!!.requirements,
        )
    }

    @Test
    fun `a limitation the document does not mention produces no claim at all`() {
        // `!payment` asserts the relay is free; a silent document asserted nothing.
        val doc = RelayDocument.parse("""{ "software": "haven", "limitation": { "auth_required": true } }""")!!
        assertEquals(listOf("auth"), doc.requirements)
    }

    @Test
    fun `min_pow_difficulty is a number, and zero is a claim rather than a silence`() {
        assertEquals(listOf("pow"), RelayDocument.parse("""{ "limitation": { "min_pow_difficulty": 28 } }""")!!.requirements)
        assertEquals(listOf("!pow"), RelayDocument.parse("""{ "limitation": { "min_pow_difficulty": 0 } }""")!!.requirements)
    }

    @Test
    fun `one malformed field does not cost us the ones beside it`() {
        val doc = RelayDocument.parse("""{ "software": "strfry", "supported_nips": ["one", 11], "limitation": [] }""")!!
        assertEquals("strfry", doc.software)
        assertEquals(listOf(11), doc.supportedNips, "the readable entries survive the unreadable one")
        assertEquals(emptyList(), doc.requirements)
    }

    @Test
    fun `a document that is not a document reads as nothing rather than throwing`() {
        // Most relays answer this ask with a homepage, a 404 body or a redirect.
        assertNull(RelayDocument.parse("<!doctype html><title>hello</title>"))
        assertNull(RelayDocument.parse(""))
        assertNull(RelayDocument.parse("[1,2,3]"))
        assertNull(RelayDocument.parse("""{ "name": "a relay with nothing we publish" }"""), "parsed, but says none of the four")
        assertNull(RelayDocument.parse("""{ "software": "" }"""), "an empty string is not a software name")
    }

    @Test
    fun `nothing in the document can reach a verdict`() {
        val fields = RelayDocument.Doc::class.members.map { it.name }.toSet()
        for (verdict in Verdict.entries) {
            assertTrue(fields.none { it.equals(verdict.value, ignoreCase = true) }, "the document must not carry `${verdict.value}`")
        }
        assertEquals(setOf("software", "version", "supportedNips", "requirements"), fields.filter { it !in OBJECT_MEMBERS }.toSet())
    }

    companion object {
        /** What every data class carries, which is not part of the shape under test. */
        private val OBJECT_MEMBERS =
            setOf("equals", "hashCode", "toString", "copy") +
                (1..8).map { "component$it" }
    }
}
