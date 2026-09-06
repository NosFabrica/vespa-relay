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
package com.nosfabrica.vespa.relay.server

import com.nosfabrica.vespa.relay.web.IconedPage
import com.nosfabrica.vespa.relay.web.StatsSnapshot
import com.nosfabrica.vespa.relay.web.WebAssets
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** The two routes the statistics page needs, driven in-process. */
class StatsPageTest {
    private val doc = buildJsonObject { put("schema", 1) }

    @Test
    fun `an uncomputed document is a 503, not a page of zeros`() =
        testApplication {
            val snapshot = StatsSnapshot()
            application { routing { corpusStats(null, snapshot) } }

            // A poller must tell "ask again later" from "this relay holds nothing".
            val empty = client.get("/stats.json")
            assertEquals(HttpStatusCode.ServiceUnavailable, empty.status)
            assertTrue(empty.bodyAsText().contains("no statistics computed yet"))

            snapshot.publish(doc)
            assertEquals(HttpStatusCode.OK, client.get("/stats.json").status)
        }

    @Test
    fun `the document is json, revalidated, and a repeat ask with its etag is a 304`() =
        testApplication {
            val snapshot = StatsSnapshot().also { it.publish(doc) }
            application { routing { corpusStats(null, snapshot) } }

            val first = client.get("/stats.json")
            assertEquals(HttpStatusCode.OK, first.status)
            assertEquals("application/json", first.headers[HttpHeaders.ContentType]?.substringBefore(';')?.trim())
            // Revalidate rather than guess a max-age: the rollup interval is a setting this route cannot see.
            assertEquals("no-cache", first.headers[HttpHeaders.CacheControl])
            val etag = assertNotNull(first.headers[HttpHeaders.ETag])

            // The page polls faster than the rollup, so most fetches are for bytes the reader holds.
            assertEquals(HttpStatusCode.NotModified, client.get("/stats.json") { header(HttpHeaders.IfNoneMatch, etag) }.status)
            assertEquals(HttpStatusCode.NotModified, client.get("/stats.json") { header(HttpHeaders.IfNoneMatch, "W/$etag") }.status)
            assertEquals(HttpStatusCode.OK, client.get("/stats.json") { header(HttpHeaders.IfNoneMatch, "\"0000000000000000\"") }.status)

            // A new rollup must break the reader's cache.
            snapshot.publish(
                buildJsonObject {
                    put("schema", 1)
                    put("corpus", buildJsonObject { put("events", 9) })
                },
            )
            assertEquals(HttpStatusCode.OK, client.get("/stats.json") { header(HttpHeaders.IfNoneMatch, etag) }.status)
        }

    @Test
    fun `the page is served and reads the document it charts`() =
        testApplication {
            val html = assertNotNull(javaClass.getResource("/stats.html")?.readText(), "the page is on the classpath")
            application { routing { corpusStats(IconedPage(html, null), StatsSnapshot().also { it.publish(doc) }) } }

            val res = client.get("/stats.html")
            assertEquals(HttpStatusCode.OK, res.status)
            val body = res.bodyAsText()
            assertTrue(body.contains("/stats.json"), "the page fetches the document rather than carrying its own numbers")

            // Without the scope line a reader compares a mirror's total against a network-wide dashboard's.
            assertTrue(body.contains("id=\"scope\""), "the page has somewhere to print the document's scope")

            // Imports are document-relative so the page survives a path-prefix mount, and matched as
            // that literal: an absolute `/web/` is answered by whatever sits at the host root.
            for (spec in Regex("""from "(\./web/[^"]+)"""").findAll(body).map { it.groupValues[1] }) {
                assertNotNull(WebAssets.get(spec.removePrefix("./web/")), "$spec is imported but not servable")
            }
        }

    /** A 301, not a 404: the url is bookmarked and this repo's history points at it. */
    @Test
    fun `the old kind_stats url redirects to the page that replaced it`() =
        testApplication {
            val html = assertNotNull(javaClass.getResource("/stats.html")?.readText())
            application { routing { corpusStats(IconedPage(html, null), null) } }

            // Not followed, so the status and target are both assertable.
            val res = createClient { followRedirects = false }.get("/kind_stats.html")
            assertEquals(HttpStatusCode.MovedPermanently, res.status)
            assertEquals("/stats.html", res.headers[HttpHeaders.Location])
        }
}
