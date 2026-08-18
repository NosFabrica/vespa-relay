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
package com.nosfabrica.vespa.relay.status

import com.nosfabrica.vespa.relay.web.CachedPage
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.nosfabrica.vespa.relay.web.WebAssets
import com.nosfabrica.vespa.relay.web.favicon
import com.nosfabrica.vespa.relay.web.installPageDefaults
import com.nosfabrica.vespa.relay.web.respondPage
import com.nosfabrica.vespa.relay.web.statsDocument
import com.nosfabrica.vespa.relay.web.webModules
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The mirror's status site, driven in-process.
 *
 * The page and every module it imports are RESOURCES, spread across two jars —
 * `sync_stats.html` and `/web/sync/cards.js` in this module, and the shared
 * modules under `/web/shared/` in
 * :web — and nothing but a request proves they resolve. A missing one is a
 * blank page in production and compiles perfectly, which is exactly the class
 * of failure `RelayWebAssetsTest` exists for on the relay's side.
 */
class SyncStatusSiteTest {
    /** The page as `SyncMain` loads it: off this module's own classpath, no fallback. */
    private fun page(): String =
        assertNotNull(
            SyncStatus::class.java.getResourceAsStream("/sync_stats.html")?.use { it.readBytes().decodeToString() },
            "sync_stats.html is not on the :sync classpath — the status page would be unservable",
        )

    @Test
    fun `the page is served, and every module it imports resolves`() =
        testApplication {
            val snapshot = StatsSnapshot(null)
            val cached = CachedPage(page())
            application {
                installPageDefaults()
                routing {
                    get("/") { call.respondPage(cached) }
                    statsDocument(snapshot)
                    webModules()
                    favicon()
                }
            }

            val html = client.get("/")
            assertEquals(HttpStatusCode.OK, html.status)
            val body = html.bodyAsText()
            assertTrue(body.contains("Mirror status"), "the page is the mirror's, not the relay's")

            // EVERY `import` and `<link>` the page names, fetched. A page whose
            // engine 404s renders a heading and nothing else, and the heading
            // is what makes that look like a mirror with no state rather than a
            // broken deploy.
            for (asset in IMPORTS) {
                val res = client.get(asset)
                assertEquals(HttpStatusCode.OK, res.status, "$asset — imported by the status page")
            }
        }

    @Test
    fun `nothing computed yet is a 503, never a document of zeroes`() =
        testApplication {
            val snapshot = StatsSnapshot(null)
            application { routing { statsDocument(snapshot) } }

            // "No document yet" and "this mirror holds nothing" are different
            // facts: the first is a state a poller retries and the second is a
            // finding, and a 200 carrying zeroes cannot be told from either.
            assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/stats.json").status)

            snapshot.publish(buildJsonObject { put("schema", SyncStatus.SCHEMA_VERSION) })
            assertEquals(HttpStatusCode.OK, client.get("/stats.json").status)
        }

    @Test
    fun `the page and its modules are found across BOTH jars, which is the whole classpath argument`() {
        // `/web/shared/page.js` ships in :web and `/web/sync/cards.js` in
        // :sync. One route serves both because WebAssets resolves off the
        // CLASSPATH rather than under any module's resource root — see its
        // KDoc. Asserted directly as well as over HTTP so a failure names which
        // half is missing.
        assertNotNull(WebAssets.get("shared/page.js"), "the shared page module ships in :web")
        assertNotNull(WebAssets.get("sync/cards.js"), "the mirror's cards ship in :sync")
    }

    private companion object {
        /**
         * What the page pulls in, kept as a list rather than parsed out of the
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
                "/web/sync/cards.js",
            )
    }
}
