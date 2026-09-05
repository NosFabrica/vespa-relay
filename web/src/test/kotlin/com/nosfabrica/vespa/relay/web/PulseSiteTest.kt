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
package com.nosfabrica.vespa.relay.web

import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The pulse site driven in-process. The page and the modules it imports are
 * resources spread across two jars, and only a request proves they resolve —
 * a page whose imports 404 draws a blank card and reports no error anywhere.
 */
class PulseSiteTest {
    /** The page as the two mains load it: off the classpath, no fallback. */
    private fun page(): String =
        assertNotNull(
            PulseSiteTest::class.java.getResourceAsStream("/pulse.html")?.use { it.readBytes().decodeToString() },
            "pulse.html is not on the classpath — no pulse page could be served",
        )

    @Test
    fun `the page is served, and every module it imports resolves`() =
        testApplication {
            application {
                installPageDefaults()
                routing {
                    get("/") { call.respondPage(CachedPage(page())) }
                    pulseDocument({ buildJsonObject { put("schema", 1) } })
                    webModules()
                    favicon()
                }
            }

            val html = client.get("/")
            assertEquals(HttpStatusCode.OK, html.status)
            assertTrue(html.bodyAsText().contains("pulse.json"), "the page must read the document this site serves")

            for (asset in IMPORTS) {
                assertEquals(HttpStatusCode.OK, client.get(asset).status, "$asset — imported by the pulse page")
            }
        }

    @Test
    fun `a process with no metered store answers 503, never a document of zeroes`() =
        testApplication {
            var doc: JsonObject? = null
            application { routing { pulseDocument({ doc }) } }

            // A 200 of zeros would read as a store answering and doing nothing,
            // which is the opposite of "there is nothing here to measure".
            assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/pulse.json").status)

            doc = buildJsonObject { put("schema", 1) }
            assertEquals(HttpStatusCode.OK, client.get("/pulse.json").status)
        }

    @Test
    fun `the document is built per request and never cached`() =
        testApplication {
            var built = 0
            application {
                routing {
                    pulseDocument({
                        built++
                        buildJsonObject { put("uptimeSeconds", built) }
                    })
                }
            }

            val first = client.get("/pulse.json")
            val second = client.get("/pulse.json")

            // The page's whole method is differencing two consecutive polls, so
            // a cached or 304'd document would leave every rate at zero forever.
            assertEquals("no-store", first.headers[HttpHeaders.CacheControl])
            assertEquals(null, first.headers[HttpHeaders.ETag], "an ETag over a document whose every field moves can never match")
            assertEquals(2, built, "each request must read the counters again")
            assertTrue(second.bodyAsText().contains("\"uptimeSeconds\":2"))
        }

    private companion object {
        /** Kept as a list rather than parsed from the markup, so a reformat cannot silently empty it. */
        val IMPORTS =
            listOf(
                "/web/shared/stats.css",
                "/web/shared/page.js",
                "/web/shared/pulse.js",
            )
    }
}
