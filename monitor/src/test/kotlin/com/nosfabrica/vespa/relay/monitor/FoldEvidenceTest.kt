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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals

/** The fold's evidence is published text: each sentence must read exactly as the record has always carried it. */
class FoldEvidenceTest {
    private val canonical = RelayUrlNormalizer.normalize("wss://relay.example/")

    @Test
    fun `every fold's evidence reads as it was signed before the sentences moved`() {
        assertEquals("500 newest events, 498 shared with wss://relay.example/", FoldEvidence.contained(canonical, 500, 498))
        assertEquals(
            "same endpoint as wss://relay.example/ over TLS, both answered; 40 newest events here",
            FoldEvidence.secureTwin(canonical, 40, groupList = false),
        )
        assertEquals(
            "same endpoint as wss://relay.example/ over TLS, both answered; 7 group definitions here",
            FoldEvidence.secureTwin(canonical, 7, groupList = true),
        )
        assertEquals("same group list as wss://relay.example/: 5 of 7 group definitions shared", FoldEvidence.groupList(canonical, 7, 5))
        assertEquals(
            "the same complete store of 12 event(s) as wss://relay.example/, as a path no relay list names also serves",
            FoldEvidence.wholeStore(canonical, 12),
        )
        assertEquals(
            "nothing readable at any of 3 url(s) on this host; folded on the shared name, not on a measurement",
            FoldEvidence.unreadable(3),
        )
        assertEquals(
            "500 newest events, best 2 shared with 3 compared endpoint(s) on this host",
            FoldEvidence.distinct(500, "3 compared endpoint(s) on this host", 2),
        )
    }
}
