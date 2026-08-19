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
 * READING SOMEBODY ELSE'S JSON, which is the whole risk here.
 *
 * A NIP-11 document is written by sixteen thousand strangers running a dozen
 * implementations, so every field is optional, every type is a suggestion, and
 * a document that throws costs us the facts beside the one that was malformed.
 * The parse is pure and static precisely so this can be asserted without a
 * relay; the fetch around it is the part with nothing to decide.
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
        // NIP-66's `!` form, which is not decoration: sampled in the wild,
        // `!auth` is on 681 of 800 records and is how most say "open to read".
        // `restricted_writes: true` means writes ARE restricted, so the
        // requirement APPLIES — the negation is what an open relay gets. The
        // key names the restriction, not the permission.
        assertEquals(setOf("!auth", "payment", "!pow", "writes"), doc.requirements.toSet())
        assertEquals(
            listOf("!writes"),
            RelayDocument.parse("""{ "limitation": { "restricted_writes": false } }""")!!.requirements,
        )
    }

    @Test
    fun `a limitation the document does not mention produces no claim at all`() {
        // `!payment` ASSERTS the relay is free. A document that is silent has
        // asserted nothing, and publishing the negation would put a claim the
        // relay never made into a record signed by us.
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
        // The failure mode this is written against: a relay whose
        // `supported_nips` holds strings, or whose `limitation` is an array,
        // taking its perfectly good `software` string down with it.
        val doc = RelayDocument.parse("""{ "software": "strfry", "supported_nips": ["one", 11], "limitation": [] }""")!!
        assertEquals("strfry", doc.software)
        assertEquals(listOf(11), doc.supportedNips, "the readable entries survive the unreadable one")
        assertEquals(emptyList(), doc.requirements)
    }

    @Test
    fun `a document that is not a document reads as nothing rather than throwing`() {
        // Most relays answer this ask with their homepage, a 404 body or a
        // redirect. None of those is news, and none of them may throw: the pass
        // dials the whole corpus and a document is a nice-to-have on it.
        assertNull(RelayDocument.parse("<!doctype html><title>hello</title>"))
        assertNull(RelayDocument.parse(""))
        assertNull(RelayDocument.parse("[1,2,3]"))
        assertNull(RelayDocument.parse("""{ "name": "a relay with nothing we publish" }"""), "parsed, but says none of the four")
        assertNull(RelayDocument.parse("""{ "software": "" }"""), "an empty string is not a software name")
    }

    @Test
    fun `nothing in the document can reach a verdict`() {
        // The rule the fitness pass is built on, restated where it could be
        // broken: this type carries no grade, no reachability and no
        // pageability, so a relay cannot talk its way into a roster.
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
