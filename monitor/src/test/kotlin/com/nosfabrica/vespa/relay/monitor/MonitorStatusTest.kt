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

import com.nosfabrica.vespa.relay.progress.Processors
import com.nosfabrica.vespa.relay.web.CachedPage
import com.nosfabrica.vespa.relay.web.favicon
import com.nosfabrica.vespa.relay.web.installPageDefaults
import com.nosfabrica.vespa.relay.web.respondPage
import com.nosfabrica.vespa.relay.web.webModules
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The monitor plane's status document, and the shared page that draws it off
 * the `monitor` section the document carries.
 */
class MonitorStatusTest {
    private fun rows(): Processors =
        Processors().apply {
            of(MonitorEngine.FOLD_PROCESSOR).let { p ->
                p.phase(Processors.RUNNING)
                p.counts { listOf(Processors.Count("foldedAway", 8), Processors.Count("candidates", 40)) }
            }
        }

    @Test
    fun `the document carries this plane's rows, and the definitions for them alone`() {
        val doc = MonitorStatus(rows(), everySeconds = 30).document(nowSeconds = 1_000)

        assertEquals(MonitorStatus.SCHEMA_VERSION, doc["schema"]!!.jsonPrimitive.content.toInt())
        assertEquals(
            30L,
            doc["tiers"]!!
                .jsonObject[MonitorStatus.TIER]!!
                .jsonObject["everySeconds"]!!
                .jsonPrimitive.long,
        )

        val data = assertNotNull(doc["monitor"]).jsonObject["data"]!!.jsonObject
        val processors = data["progress"]!!.jsonObject["processors"] as JsonArray
        assertEquals(MonitorEngine.FOLD_PROCESSOR, processors[0].jsonObject["name"]!!.jsonPrimitive.content)

        val terms = data["terms"]!!.jsonObject
        assertTrue("foldedAway" in terms, "a member this document carries is defined in it")
        assertTrue("queued" !in terms, "and the mirror's ingest queue is not")
    }

    @Test
    fun `a deployment with no monitor publishes no section at all`() {
        // A card of zeroes would read as a monitor that is failing rather than one nobody configured.
        val doc = MonitorStatus(Processors(), everySeconds = 30).document(nowSeconds = 1_000)

        assertNull(doc["monitor"])
        assertEquals(0, (doc["tiers"]!!.jsonObject[MonitorStatus.TIER]!!.jsonObject["sections"] as JsonArray).size)
    }

    @Test
    fun `the shared page is served, and every module it imports resolves`() =
        testApplication {
            val page =
                assertNotNull(
                    MonitorStatus::class.java.getResourceAsStream("/stats.html")?.use { it.readBytes().decodeToString() },
                    "stats.html is not on the classpath from :monitor — no page could be served",
                )
            val cached = CachedPage(page)
            application {
                installPageDefaults()
                routing {
                    get("/") { call.respondPage(cached) }
                    webModules()
                    favicon()
                }
            }

            // The markup on disk carries the relay's heading; the monitor's comes from the document's `title`.
            assertTrue(client.get("/").bodyAsText().contains("mountStatsPage"))
            for (asset in IMPORTS) {
                assertEquals(HttpStatusCode.OK, client.get(asset).status, "$asset — imported by the monitor page")
            }
        }

    @Test
    fun `the document names itself, because one page is served by three services`() {
        val doc = MonitorStatus(rows(), everySeconds = 30, relayUrl = "ws://localhost:7777").document(nowSeconds = 1_000)

        assertEquals("Relay monitor", doc["title"]!!.jsonPrimitive.content)
        // Deriving the relay from `location` would dial the status site's own port.
        assertEquals("ws://localhost:7777", doc["relay"]!!.jsonPrimitive.content)
    }

    private companion object {
        /** What the page imports, listed by hand: a regex over the markup would stop matching silently. */
        val IMPORTS =
            listOf(
                "/web/shared/stats.css",
                "/web/shared/statspage.js",
                "/web/shared/page.js",
                "/web/shared/processors.js",
                "/web/shared/sync.js",
                "/web/shared/relay.js",
                "/web/shared/verdicts.js",
                "/web/monitor/cards.js",
            )
    }
}
