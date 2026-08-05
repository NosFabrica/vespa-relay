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

import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.charset
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The /web module cache: what it will serve, and what it must refuse.
 *
 * This replaced Ktor's `staticResources`, so the traversal guard that came free
 * with it is now ours to hold. The rest is the property that makes the ETag
 * worth having at all — same bytes, same tag; different bytes, different tag —
 * because a validator that does not track the content is a stale page nobody
 * can flush.
 */
class RelayWebAssetsTest {
    @Test
    fun `serves a module with a utf-8 content type and a content etag`() {
        val app = assertNotNull(WebAssets.get("app.js"), "the page's entry module is on the classpath")
        // Whatever Ktor derives from the extension, unchanged from what
        // staticResources sent — the same mime table answers both.
        assertEquals("text/javascript", "${app.contentType.contentType}/${app.contentType.contentSubtype}")
        // The modules carry non-ASCII — every clipped label ends in "…" — so an
        // unstated charset would be the browser guessing at the bytes.
        assertEquals("UTF-8", app.contentType.charset()?.name())
        assertTrue(app.etag.matches(Regex("^\"[0-9a-f]{16}\"$")), "a quoted strong etag, got ${app.etag}")
        assertTrue(app.bytes.isNotEmpty())
    }

    @Test
    fun `the same module is read once and keeps one identity`() {
        val first = assertNotNull(WebAssets.get("shared/relay.js"))
        assertSame(first, WebAssets.get("shared/relay.js"), "cached, not re-read per request")
        val other = assertNotNull(WebAssets.get("shared/nip19.js"))
        assertTrue(first.etag != other.etag, "different content must not share a validator")
    }

    @Test
    fun `refuses anything that is not a plain path under web`() {
        // The traversal cases are the point: a request names a file under
        // resources/web or it names nothing. `..` never reaches the classloader.
        for (bad in listOf(
            "",
            "..",
            "../index.html",
            "shared/../../index.html",
            "/app.js",
            "shared//relay.js",
            "app.js?x=1",
            "sha re/relay.js",
        )) {
            assertNull(WebAssets.get(bad), "must refuse $bad")
        }
        assertNull(WebAssets.get("shared/nope.js"), "a well-shaped path to nothing is a 404, not an error")
    }

    @Test
    fun `every module the landing page preloads is actually servable`() {
        // tools/webtest/preload.test.mjs checks the hints against the import
        // graph on disk; this checks them against the route that has to answer
        // them. A hint whose href does not resolve here is 23 wasted requests
        // and a silent return to the three-wave waterfall.
        val html = assertNotNull(javaClass.getResource("/index.html")?.readText())
        val hrefs = Regex("""<link rel="modulepreload" href="/web/([^"]+)"""").findAll(html).map { it.groupValues[1] }.toList()
        assertTrue(hrefs.isNotEmpty(), "the landing page still carries preload hints")
        for (href in hrefs) assertNotNull(WebAssets.get(href), "/web/$href is hinted but not servable")
    }

    @Test
    fun `the route serves a nested module, and a second ask with its etag is a 304`() =
        testApplication {
            application { routing { webModules() } }

            // The tailcard has to survive the directory: shared/ and cards/ are
            // where all but one of the modules live.
            val first = client.get("/web/shared/nip19.js")
            assertEquals(HttpStatusCode.OK, first.status)
            assertTrue(first.bodyAsText().contains("export function nip19Parse"), "the real module, not an index page")
            assertEquals("max-age=60", first.headers[HttpHeaders.CacheControl])
            val etag = assertNotNull(first.headers[HttpHeaders.ETag], "a validator, or the 304 below can never happen")

            // The whole point: an unchanged module costs a header exchange.
            val again = client.get("/web/shared/nip19.js") { header(HttpHeaders.IfNoneMatch, etag) }
            assertEquals(HttpStatusCode.NotModified, again.status)
            assertEquals("", again.bodyAsText())

            // A proxy may weaken the tag on the way back. One immutable resource,
            // one hash — treating that as a mismatch would re-send the body.
            val weak = client.get("/web/shared/nip19.js") { header(HttpHeaders.IfNoneMatch, "W/$etag") }
            assertEquals(HttpStatusCode.NotModified, weak.status)

            // A stale tag is a real fetch, not a silent 304.
            val stale = client.get("/web/shared/nip19.js") { header(HttpHeaders.IfNoneMatch, "\"0000000000000000\"") }
            assertEquals(HttpStatusCode.OK, stale.status)

            assertEquals(HttpStatusCode.NotFound, client.get("/web/nope.js").status)
            assertEquals(HttpStatusCode.NotFound, client.get("/web/").status)
        }
}
