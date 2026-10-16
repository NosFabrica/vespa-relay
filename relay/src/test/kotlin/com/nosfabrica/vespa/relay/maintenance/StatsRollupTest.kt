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

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The relay's `/stats.json`: the headers the page's live panels start from, and the mirror's kind set. */
class StatsRollupTest {
    private val dir = createTempDirectory("stats-rollup").toFile().also { it.deleteOnExit() }

    private fun manifest(json: String): File = File(dir, "sync-manifest.json").also { it.writeText(json) }

    private fun listedSections(doc: JsonObject) =
        doc["tiers"]!!
            .jsonObject[StatsRollup.TIER]!!
            .jsonObject["sections"]!!
            .jsonArray
            .map { it.jsonPrimitive.content }

    @Test
    fun `a serve-only relay still publishes the members the page connects with`() {
        val doc = StatsRollup(relayUrl = "wss://relay.example/").compute(everySeconds = 60)

        assertEquals("wss://relay.example/", doc["relay"]!!.jsonPrimitive.content)
        assertEquals(StatsRollup.SCHEMA_VERSION, doc["schema"]!!.jsonPrimitive.int)
        assertNull(doc["sync"], "\"0 relays\" would read as a broken mirror")
        assertEquals(emptyList(), listedSections(doc), "a listed section that is not in the document reads as one that failed")
    }

    @Test
    fun `a pass states its own cadence`() {
        val doc = StatsRollup(relayUrl = "wss://relay.example/").compute(everySeconds = 60)

        assertEquals(
            60,
            doc["tiers"]!!
                .jsonObject[StatsRollup.TIER]!!
                .jsonObject["everySeconds"]!!
                .jsonPrimitive.int,
        )
    }

    @Test
    fun `the mirrored kinds ride the sync section`() {
        val file = manifest("""{"writtenAt":1800000000,"streams":[{"name":"content","dir":"down","kinds":[1,0,30023]}]}""")
        val doc = StatsRollup(relayUrl = "wss://relay.example/", syncManifestFile = file).compute()

        val sync = assertNotNull(doc["sync"]).jsonObject
        assertEquals("ok", sync["status"]!!.jsonPrimitive.content)
        val kinds =
            sync["data"]!!
                .jsonObject["mirrors"]!!
                .jsonObject["kinds"]!!
                .jsonArray
                .map { it.jsonPrimitive.int }
        assertEquals(listOf(0, 1, 30023), kinds)
        assertEquals(listOf("sync"), listedSections(doc))
    }

    @Test
    fun `a router that never wrote its manifest is no sync section, not a failed one`() {
        val doc = StatsRollup(relayUrl = "wss://relay.example/", syncManifestFile = File(dir, "absent.json")).compute()

        assertNull(doc["sync"])
    }

    @Test
    fun `the document no longer carries the corpus sections the page now counts itself`() {
        val doc = StatsRollup(relayUrl = "wss://relay.example/").compute()

        val retired = setOf("corpus", "trust", "kinds", "authors", "activity", "kindActivity", "zaps", "relayDistribution")
        assertTrue(doc.keys.none { it in retired }, "a stale chart section would be drawn as current: ${doc.keys}")
    }
}
