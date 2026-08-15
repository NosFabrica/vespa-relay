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
package com.nosfabrica.vespa.relay.router.discovery

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The verdict vocabulary is a WIRE CONTRACT, not an implementation detail: a
 * stream's whole relay list is the filter `"#s": ["syncable"]` against records
 * this pass signed, and records live for weeks. Renaming a value strands every
 * standing record on the old word — which is [RelayAliasRecord.FITNESS_EPOCH]'s
 * job to handle, so a rename without an epoch bump is the bug these pins exist
 * to catch.
 */
class FitnessVocabularyTest {
    @Test
    fun `syncable is the only admitting value, spelled exactly as streams filter for it`() {
        assertEquals("syncable", FitnessPass.Verdict.SYNCABLE.value)
        assertEquals("s", RelayAliasRecord.STATUS_TAG, "the status tag must stay single-letter — it is the one tag streams can filter on")
    }

    @Test
    fun `every refusal is descriptive and none of them collides with the admitting value`() {
        val values = FitnessPass.Verdict.entries.map { it.value }
        assertEquals(values.toSet().size, values.size, "two verdicts sharing a value would make records unreadable")
        assertEquals(
            setOf("syncable", "dead", "silent", "alias", "inconsistent", "unpageable", "auth-refused", "restricted"),
            values.toSet(),
            "the vocabulary is a wire contract; changing it needs a FITNESS_EPOCH bump and a matching stream-side change",
        )
    }

    @Test
    fun `five of the refusals describe relays that are alive`() {
        // The argument for the name: "live" would admit most of what this
        // vocabulary exists to refuse. If this list shrinks, the name argument
        // changes and the glossary prose should too.
        val aliveButRefused =
            setOf(
                FitnessPass.Verdict.SILENT,
                FitnessPass.Verdict.ALIAS,
                FitnessPass.Verdict.INCONSISTENT,
                FitnessPass.Verdict.UNPAGEABLE,
                FitnessPass.Verdict.AUTH_REFUSED,
                FitnessPass.Verdict.RESTRICTED,
            )
        assertTrue(aliveButRefused.size >= 5)
    }
}
