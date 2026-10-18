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
package com.nosfabrica.vespa.relay.progress

import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** One member name, defined in each document's own words; the published-member rule still decides which names ship. */
class StatusVocabularyPlanesTest {
    /** `stage` is a probe pass's step on the monitor and an ingest stage on the mirror. */
    private val document = buildJsonObject { put("stage", "write") }

    private fun definitionOf(plane: StatusPlane) = StatusVocabulary.termsFor(document, plane)["stage"]!!.jsonPrimitive.content

    @Test
    fun `a name both planes publish reads each plane's own definition`() {
        assertTrue("ingest" in definitionOf(StatusPlane.MIRROR))
        assertFalse("probe pass" in definitionOf(StatusPlane.MIRROR), "the mirror's reader got the monitor's meaning")
        assertTrue("probe pass" in definitionOf(StatusPlane.MONITOR))
        assertFalse("ingest stage" in definitionOf(StatusPlane.MONITOR), "the monitor's reader got the mirror's meaning")
    }

    @Test
    fun `the flat vocabulary still defines every name, a per-plane pair as both`() {
        val flat = StatusVocabulary.TERMS["stage"]!!.jsonPrimitive.content
        assertTrue(definitionOf(StatusPlane.MIRROR) in flat && definitionOf(StatusPlane.MONITOR) in flat, flat)
        // A plane's lookup falls back to the flat map, so a section a name is filed under never hides it.
        for (plane in StatusPlane.entries) {
            assertEquals(StatusVocabulary.TERMS.keys, StatusVocabulary.termsFor(StatusVocabulary.TERMS, plane).keys)
        }
    }

    @Test
    fun `a document gets only the names it publishes, whichever plane it is`() {
        val doc = buildJsonObject { put("queued", JsonPrimitive(3)) }
        for (plane in StatusPlane.entries) assertEquals(setOf("queued"), StatusVocabulary.termsFor(doc, plane).keys)
    }
}
