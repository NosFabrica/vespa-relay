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

import com.nosfabrica.vespa.relay.peers.RelayFacts
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.peers.Verdict
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The grade vocabulary and its tag are a wire contract: streams filter on them
 * and records live for weeks, so a rename needs a [RelayVerdictRecord.FITNESS_EPOCH] bump.
 */
class FitnessVocabularyTest {
    @Test
    fun `prime is the only admitting value, spelled exactly as streams filter for it`() {
        assertEquals("prime", Verdict.PRIME.value)
        assertEquals(
            1,
            RelayVerdictRecord.LABEL_TAG.length,
            "the grade must stay on a single-letter tag — only those are indexed, and it is the one tag streams filter on",
        )
    }

    @Test
    fun `the grade does not squat a tag the rest of the network has already spent`() {
        // `s` is the relay's software to every other NIP-66 monitor; the grade is a NIP-32 label.
        assertEquals("l", RelayVerdictRecord.LABEL_TAG)
        assertEquals("L", RelayVerdictRecord.LABEL_NAMESPACE_TAG)
        assertEquals(RelayFacts.SOFTWARE_TAG, RelayVerdictRecord.LEGACY_STATUS_TAG, "`s` means software now, which is why the writer must own it")
    }

    @Test
    fun `the namespace names the judgement rather than the deployment that makes it`() {
        val namespace = RelayVerdictRecord.FITNESS_NAMESPACE
        assertEquals("relay.fitness", namespace)
        for (ours in listOf("vespa", "nosfabrica", "brainstorm", "sync")) {
            assertTrue(!namespace.contains(ours), "the namespace must not name us: `$ours` is in `$namespace`")
        }
    }

    @Test
    fun `every refusal is descriptive and none of them collides with the admitting value`() {
        val values = Verdict.entries.map { it.value }
        assertEquals(values.toSet().size, values.size, "two verdicts sharing a value would make records unreadable")
        assertEquals(
            setOf("prime", "dead", "silent", "alias", "inconsistent", "unpageable", "noncompliant", "auth-refused", "restricted"),
            values.toSet(),
            "the vocabulary is a wire contract; changing it needs a FITNESS_EPOCH bump and a matching stream-side change",
        )
    }

    @Test
    fun `six of the refusals describe relays that are alive`() {
        // The argument for the name "fitness" over "live"; if this list shrinks the glossary prose changes too.
        val aliveButRefused =
            setOf(
                Verdict.SILENT,
                Verdict.ALIAS,
                Verdict.INCONSISTENT,
                Verdict.UNPAGEABLE,
                Verdict.NONCOMPLIANT,
                Verdict.AUTH_REFUSED,
                Verdict.RESTRICTED,
            )
        assertTrue(aliveButRefused.size >= 6)
    }
}
