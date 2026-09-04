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

import com.nosfabrica.vespa.relay.web.WebAssets
import com.nosfabrica.vespa.relay.web.favicon
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The tab icon is a hand-kept copy of the header mark. Only `favicon.svg` is
 * compared to the mark; an edit to the mark also needs the .ico redrawn, which nothing here checks.
 */
class RelayFaviconTest {
    private val index = assertNotNull(javaClass.getResource("/index.html")?.readText())
    private val favicon = assertNotNull(javaClass.getResource("/web/favicon.svg")?.readText())

    /** Every `<circle>`'s geometry in document order: the shapes, not their paint. */
    private fun circles(svg: String) =
        Regex("""<circle cx="([\d.]+)" cy="([\d.]+)" r="([\d.]+)"""")
            .findAll(svg)
            .map { it.groupValues.drop(1) }
            .toList()

    private fun linkPath(svg: String) = assertNotNull(Regex("""<path d="([^"]+)"""").find(svg)).groupValues[1]

    /** The `<svg class="mark">` out of the landing page's header. */
    private fun mark(): String {
        val at = index.indexOf("""<svg class="mark"""")
        assertTrue(at >= 0, "index.html still draws the brand mark")
        return index.substring(at, index.indexOf("</svg>", at))
    }

    @Test
    fun `the favicon draws the page's own brand mark, shape for shape`() {
        // Geometry only: the favicon differs in paint, stroke and margin on purpose.
        // The count first, or a regex that stopped matching compares two empty lists.
        assertEquals(4, circles(mark()).size, "the hub and three nodes")
        assertEquals(circles(mark()), circles(favicon), "the hub and the three nodes must be the mark's")
        // Compared as a string, which is why index.html spells the third link `M14.9 12 17.7 12` rather than `h2.8`.
        assertEquals(linkPath(mark()), linkPath(favicon), "the three links must be the mark's")
        assertTrue(favicon.contains("""viewBox="0 0 24 24""""), "and in the mark's own 24-unit box")
    }

    @Test
    fun `the ico carries the three sizes, each a png`() {
        val ico = assertNotNull(WebAssets.get("favicon.ico")).bytes

        fun u16(at: Int) = (ico[at].toInt() and 0xff) or ((ico[at + 1].toInt() and 0xff) shl 8)

        fun u32(at: Int) = u16(at) or (u16(at + 2) shl 16)

        assertEquals(0, u16(0), "ICONDIR reserved")
        assertEquals(1, u16(2), "type 1 = icon, not cursor")
        val count = u16(4)
        assertEquals(3, count)

        val sizes = mutableListOf<Int>()
        for (i in 0 until count) {
            val e = 6 + 16 * i
            // A one-byte field in which 0 means 256.
            val w = ico[e].toInt() and 0xff
            assertEquals(w, ico[e + 1].toInt() and 0xff, "square")
            sizes += w
            assertEquals(32, u16(e + 6), "32bpp, i.e. the alpha survives")
            val off = u32(e + 12)
            val len = u32(e + 8)
            assertTrue(off + len <= ico.size, "entry $i points inside the file")
            // PNG entries rather than BMP: no hand-built AND mask for the transparency to get wrong.
            val magic = ico.copyOfRange(off, off + 8).map { it.toInt() and 0xff }
            assertEquals(listOf(0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a), magic, "entry $i is a PNG")
        }
        assertEquals(listOf(16, 32, 48), sizes)
        // A `sizes` that over-promises makes a browser pick this over the SVG and scale it.
        assertTrue(index.contains("""href="./web/favicon.ico" sizes="16x16 32x32 48x48""""), "index.html says so too")
    }

    @Test
    fun `every page's icon links resolve, and both formats keep their own content type`() {
        for (page in listOf("/index.html", "/stats.html", "/observer_stats.html")) {
            val html = assertNotNull(javaClass.getResource(page)?.readText(), "$page is on the classpath")
            val hrefs =
                // Document-relative so a page survives a path-prefix mount, and asserted
                // as the literal `./web/` so a reference back to the host root fails here.
                Regex("""<link rel="icon" href="\./web/([^"]+)"""")
                    .findAll(html)
                    .map { it.groupValues[1] }
                    .toList()
            assertEquals(listOf("favicon.svg", "favicon.ico"), hrefs, "$page hints both, SVG first")
            for (href in hrefs) assertNotNull(WebAssets.get(href), "$page hints ./web/$href, which does not resolve")
        }
        val svg = assertNotNull(WebAssets.get("favicon.svg"))
        assertEquals("image/svg+xml", "${svg.contentType.contentType}/${svg.contentType.contentSubtype}")
        val ico = assertNotNull(WebAssets.get("favicon.ico"))
        assertEquals("image", ico.contentType.contentType)
        // Not text, so no charset is invented for it.
        assertEquals(null, ico.contentType.parameter("charset"))
    }

    @Test
    fun `the well-known path serves the icon, and revalidates like any other asset`() =
        testApplication {
            application { routing { favicon() } }

            val first = client.get("/favicon.ico")
            assertEquals(HttpStatusCode.OK, first.status)
            val etag = assertNotNull(first.headers[HttpHeaders.ETag], "or the 304 below can never happen")

            val again = client.get("/favicon.ico") { header(HttpHeaders.IfNoneMatch, etag) }
            assertEquals(HttpStatusCode.NotModified, again.status)

            // On its own the route answers only the icon, with no /{nip19} catch-all beneath it.
            assertEquals(HttpStatusCode.NotFound, client.get("/favicon.png").status)
        }
}
