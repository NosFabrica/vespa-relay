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
package com.nosfabrica.vespa.relay.pulse

import com.nosfabrica.vespa.eventstore.TrustHealth
import com.nosfabrica.vespa.eventstore.engine.DegradedReads
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The document `/trust.html` charts. What is pinned here is the distinction the
 * page must not lose: never measured is a different fact from measured at zero,
 * and a served degradation is more dangerous than a refused one.
 */
class TrustDocumentTest {
    private fun health(
        measuredAt: Long,
        lensesResolvable: Long = 246,
        lensesTotal: Long = 360,
    ) = TrustHealth(
        servicesNamed = 1060,
        servicesProjected = 233,
        lensesTotal = lensesTotal,
        lensesResolvable = lensesResolvable,
        measuredAtMs = measuredAt,
        steps = listOf(TrustHealth.Step("trust-reconcile", "walking unprojected services", 40, 114, 120, false)),
    )

    /**
     * NEVER MEASURED IS NOT ZERO. Drawn as 0% it reads as a total outage; it
     * means no reconcile has finished yet. Four separate conclusions were wrong
     * today for want of that distinction.
     */
    @Test
    fun `an unmeasured coverage says so rather than reporting zero`() {
        val d = TrustDocument.of(health(measuredAt = 0), emptyList(), "test")
        assertFalse(d["measured"]!!.jsonPrimitive.boolean)
        assertEquals(0L, d["measuredAtMs"]!!.jsonPrimitive.long)
    }

    @Test
    fun `a measured coverage carries both ratios and its age`() {
        val d = TrustDocument.of(health(measuredAt = 1_000_000), emptyList(), "test")
        assertTrue(d["measured"]!!.jsonPrimitive.boolean)
        assertEquals(246L, d["lensesResolvable"]!!.jsonPrimitive.long)
        assertEquals(360L, d["lensesTotal"]!!.jsonPrimitive.long)
        assertEquals(233L, d["servicesProjected"]!!.jsonPrimitive.long)
        assertEquals(1060L, d["servicesNamed"]!!.jsonPrimitive.long)
    }

    /** A step without a denominator must reach the page as 0, so it can say "not known" rather than draw 0%. */
    @Test
    fun `a step with no denominator carries a zero total, not a fake one`() {
        val h =
            TrustHealth(0, 0, 0, 0, 0, listOf(TrustHealth.Step("trust-rebuild", "deriving", 91_000, 0, 300, false)))
        val step = TrustDocument.of(h, emptyList(), "test")["steps"]!!.jsonArray[0].jsonObject
        assertEquals(0L, step["total"]!!.jsonPrimitive.long)
        assertEquals(91_000L, step["done"]!!.jsonPrimitive.long)
    }

    /**
     * A SERVED degradation is the dangerous one — nothing throws and ranked
     * pages quietly get shorter — so the flag must survive into the document.
     */
    @Test
    fun `degraded reads carry whether they were refused or served`() {
        val served = DegradedReads.Reading("recency", "match-phase", "search", refused = false, count = 31, lastCoverage = 61, lastDocuments = 200_000_000)
        val row = TrustDocument.of(health(1), listOf(served), "test")["degradedReads"]!!.jsonArray[0].jsonObject
        assertFalse(row["refused"]!!.jsonPrimitive.boolean)
        assertEquals("match-phase", row["flags"]!!.jsonPrimitive.content)
        assertEquals("search", row["shape"]!!.jsonPrimitive.content)
        assertEquals(61, row["lastCoverage"]!!.jsonPrimitive.int)
    }
}
