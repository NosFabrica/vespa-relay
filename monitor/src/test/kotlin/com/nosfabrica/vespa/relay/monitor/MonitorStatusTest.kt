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
import com.nosfabrica.vespa.relay.web.WebAssets
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
 * The monitor plane's own status document and the page over it.
 *
 * The properties here are the ones the split created: the rows are THIS plane's
 * (nothing sorts a shared array by name any more), the glossary is the subset
 * this document publishes, and the page and every module it imports resolve
 * across two jars — `/web/monitor/cards.js` from here and the shared modules
 * under `/web/shared/` from :web. A missing resource is a blank page that
 * compiles perfectly.
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

        // The glossary is the subset THIS document publishes. The mirror's
        // members are defined in the same map and shipped in its document.
        val terms = data["terms"]!!.jsonObject
        assertTrue("foldedAway" in terms, "a member this document carries is defined in it")
        assertTrue("queued" !in terms, "and the mirror's ingest queue is not")
    }

    @Test
    fun `a deployment with no monitor publishes no section at all`() {
        // No signer, no passes, no rows. A card of zeroes here would read as a
        // monitor that is failing rather than one nobody configured — the same
        // distinction the mirror's document draws for a serve-only relay.
        val doc = MonitorStatus(Processors(), everySeconds = 30).document(nowSeconds = 1_000)

        assertNull(doc["monitor"])
        assertEquals(0, (doc["tiers"]!!.jsonObject[MonitorStatus.TIER]!!.jsonObject["sections"] as JsonArray).size)
    }

    @Test
    fun `the page is served, and every module it imports resolves across both jars`() =
        testApplication {
            val page =
                assertNotNull(
                    MonitorStatus::class.java.getResourceAsStream("/monitor_stats.html")?.use { it.readBytes().decodeToString() },
                    "monitor_stats.html is not on the :monitor classpath — the page would be unservable",
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

            assertTrue(client.get("/").bodyAsText().contains("Relay monitor"))
            for (asset in IMPORTS) {
                assertEquals(HttpStatusCode.OK, client.get(asset).status, "$asset — imported by the monitor page")
            }
        }

    @Test
    fun `the cards ship here and the shared modules in web, and one route serves both`() {
        assertNotNull(WebAssets.get("monitor/cards.js"), "the monitor's cards ship in :monitor")
        assertNotNull(WebAssets.get("shared/processors.js"), "the processor card is shared, in :web")
    }

    private companion object {
        /**
         * What the page pulls in, as a list rather than parsed out of the
         * markup: a regex over `import` lines would silently stop matching the
         * day someone reformats the page, and this list failing to compile is
         * the point of it.
         */
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
