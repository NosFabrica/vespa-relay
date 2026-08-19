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
 * The monitor plane's own status document, and the one page that draws it.
 *
 * The rows are THIS plane's — nothing sorts a shared array by name any more —
 * and the glossary is the subset this document publishes. The page itself ships
 * in :web and is served by all three services; what makes it draw the MONITOR's
 * cards is the `monitor` section this document carries, which is the property
 * worth pinning here.
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

            // The markup is one file for three services, so what it says on
            // disk is the relay's heading — the monitor's comes from this
            // document's `title` on the first render. What this asserts is that
            // the page is SERVED and mounts at all.
            assertTrue(client.get("/").bodyAsText().contains("mountStatsPage"))
            for (asset in IMPORTS) {
                assertEquals(HttpStatusCode.OK, client.get(asset).status, "$asset — imported by the monitor page")
            }
        }

    @Test
    fun `the document names itself, because one page is served by three services`() {
        val doc = MonitorStatus(rows(), everySeconds = 30, relayUrl = "ws://localhost:7777").document(nowSeconds = 1_000)

        // The heading and the browser tab. A reader has two or three of these
        // open at once, and a tab reading "Relay stats" on the monitor's port
        // is worse than no title at all.
        assertEquals("Relay monitor", doc["title"]!!.jsonPrimitive.content)
        // …and the relay to dial. Deriving it from `location` would open a
        // websocket against this page's own port, which is the status site.
        assertEquals("ws://localhost:7777", doc["relay"]!!.jsonPrimitive.content)
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
