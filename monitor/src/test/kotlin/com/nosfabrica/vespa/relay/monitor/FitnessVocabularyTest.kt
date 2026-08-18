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
 * The grade vocabulary is a WIRE CONTRACT, not an implementation detail: a
 * stream's whole relay list is the filter `"#l": ["prime"]` against records
 * this pass signed, and records live for weeks. Renaming a value strands every
 * standing record on the old word — which is [RelayVerdictRecord.FITNESS_EPOCH]'s
 * job to handle, so a rename without an epoch bump is the bug these pins exist
 * to catch.
 *
 * The TAG is as much of the contract as the values are, and the pins below say
 * so in the direction it went wrong: the grade rode `s` until `s` turned out to
 * be the software field to every other monitor on the network.
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
        // `s` is the relay's SOFTWARE to every monitor deployed under NIP-66 —
        // sampled live off relay.nostr.watch and nos.lol, 12 monitors, and
        // every value is a repository url. The grade sat there for months, so
        // our records said `s: dead` where a reader expected strfry's git url.
        // NIP-32's label is the seam for an opinion; the namespace is what
        // keeps it from colliding the same way twice.
        assertEquals("l", RelayVerdictRecord.LABEL_TAG)
        assertEquals("L", RelayVerdictRecord.LABEL_NAMESPACE_TAG)
        assertEquals(RelayFacts.SOFTWARE_TAG, RelayVerdictRecord.LEGACY_STATUS_TAG, "`s` means software now, which is why the writer must own it")
    }

    @Test
    fun `the namespace names the judgement rather than the deployment that makes it`() {
        // A monitor's opinion is only worth publishing if somebody else can act
        // on it. `nosfabrica.*`, `vespa.*` or `brainstorm.*` would say the
        // grade is about our stack rather than about the relay — and the whole
        // point of the move is that a crawler or an archiver can read it.
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
            setOf("prime", "dead", "silent", "alias", "inconsistent", "unpageable", "auth-refused", "restricted"),
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
                Verdict.SILENT,
                Verdict.ALIAS,
                Verdict.INCONSISTENT,
                Verdict.UNPAGEABLE,
                Verdict.AUTH_REFUSED,
                Verdict.RESTRICTED,
            )
        assertTrue(aliveButRefused.size >= 5)
    }
}
