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
package com.nosfabrica.vespa.relay.maintenance

import com.nosfabrica.vespa.eventstore.engine.doc.ReputationDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ServiceKey
import com.nosfabrica.vespa.eventstore.trust.TrustReconciler
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** VERIFY_TRUST_ON_START rewrites reputation documents, so only the exact word `repair` may write. */
class TrustVerifyTest {
    private val subject = "dd".repeat(32)
    private val lens = ServiceKey("7d".repeat(32))
    private val other = ServiceKey("0e".repeat(32))

    @Test
    fun `unset or blank runs no audit`() {
        assertNull(trustVerifyRepair(null))
        assertNull(trustVerifyRepair(""))
        assertNull(trustVerifyRepair("  "))
    }

    @Test
    fun `exactly repair repairs`() {
        assertEquals(true, trustVerifyRepair("repair"))
        assertEquals(true, trustVerifyRepair(" repair "))
    }

    @Test
    fun `any other value is check only and never stops the boot`() {
        for (value in listOf("check", "true", "1", "yes", "REPAIR", "Repair", "fix", "false", "0")) {
            assertEquals(false, trustVerifyRepair(value), value)
        }
    }

    @Test
    fun `a card with no cell under its service reads as stale`() {
        // The 2026-11-03 report: rank 70 on the card, nothing under that service in the document.
        val drift =
            TrustReconciler.TrustDrift(
                subject,
                expected = ReputationDoc(subject, influenceScores = mapOf(lens to 70, other to 12)),
                actual = ReputationDoc(subject, influenceScores = mapOf(other to 12)),
            )
        assertEquals("stale: 1 cell(s) differ", driftKind(drift))
    }

    @Test
    fun `follower cells compare at the stored precision`() {
        val drift =
            TrustReconciler.TrustDrift(
                subject,
                expected = ReputationDoc(subject, followerCounts = mapOf(lens to 3433.0000001)),
                actual = ReputationDoc(subject, followerCounts = mapOf(lens to 3433.0, other to 5.0)),
            )
        assertEquals("stale: 1 cell(s) differ", driftKind(drift))
    }

    @Test
    fun `missing and orphan documents are named as such`() {
        val doc = ReputationDoc(subject, influenceScores = mapOf(lens to 70))
        assertEquals("missing: records score it, no document", driftKind(TrustReconciler.TrustDrift(subject, doc, null)))
        // An emptied parent is the absent one, as the reconciler's own match treats it.
        assertEquals("missing: records score it, no document", driftKind(TrustReconciler.TrustDrift(subject, doc, ReputationDoc(subject))))
        assertEquals("orphan: a document with no records behind it", driftKind(TrustReconciler.TrustDrift(subject, null, doc)))
    }

    @Test
    fun `the report says how to repair what a check found`() {
        val drift = TrustReconciler.TrustDrift(subject, ReputationDoc(subject, influenceScores = mapOf(lens to 70)), null)
        val checked = trustVerifyReport(TrustReconciler.TrustAudit(10, 9, 1, listOf(drift), repaired = false), 4)
        assertTrue("1 drifted" in checked, checked)
        assertTrue("VERIFY_TRUST_ON_START=repair" in checked, checked)
        assertTrue(subject in checked, checked)

        val repaired = trustVerifyReport(TrustReconciler.TrustAudit(10, 9, 1, listOf(drift), repaired = true), 4)
        assertTrue("all re-derived" in repaired, repaired)

        val clean = trustVerifyReport(TrustReconciler.TrustAudit(10, 9, 0, emptyList(), repaired = false), 4)
        assertTrue("every document matches its records" in clean, clean)
    }
}
