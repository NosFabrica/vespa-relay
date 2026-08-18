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
import com.nosfabrica.vespa.relay.web.favicon
import com.nosfabrica.vespa.relay.web.iconOverride
import com.nosfabrica.vespa.relay.web.pageWithIcon
import io.ktor.client.request.get
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * `RELAY_ICON` and the favicon are one icon now, and this is where the seam is
 * held: what an unset icon publishes, what a set one replaces, and the loop
 * that opens if the two are ever confused for each other.
 */
class RelayIconTest {
    @Test
    fun `an unset icon becomes this relay's own, at the origin and never at the relay's path`() {
        assertEquals("https://relay.example.com/favicon.ico", selfIconUrl("wss://relay.example.com"))
        assertEquals("https://relay.example.com/favicon.ico", selfIconUrl("wss://relay.example.com/"))
        // A relay answering its websocket under a path still serves the icon at
        // the root, because that is where `favicon()` mounts it.
        assertEquals("https://relay.example.com/favicon.ico", selfIconUrl("wss://relay.example.com/alpha"))
        // A port is part of the authority and has to survive.
        assertEquals("https://relay.example.com:8443/favicon.ico", selfIconUrl("wss://relay.example.com:8443"))
        // RELAY_HTTP_URL is already an http url — the caller passes whichever it has.
        assertEquals("https://relay.example.com/favicon.ico", selfIconUrl("https://relay.example.com"))
    }

    @Test
    fun `an address a stranger cannot reach publishes no icon at all`() {
        // The one that motivated the rule: the compose default. Concatenating
        // blindly would sign `http://localhost:7777/favicon.ico` into a public,
        // replaceable kind 0 on every development boot — an address that
        // resolves to the READER's machine, which is a claim we cannot support.
        assertNull(selfIconUrl("ws://localhost:7777"))
        assertNull(selfIconUrl("http://192.168.1.4:7777"))
        assertNull(selfIconUrl("ws://relay.example.com"), "plain ws clearnet is not a deployment we can name")
        assertNull(selfIconUrl(null))
        assertNull(selfIconUrl("   "))
        assertNull(selfIconUrl("relay.example.com"), "no scheme, no origin")
        // A hidden service is reachable by name and by nothing else, so http is
        // the normal spelling and it is admitted on either scheme.
        val onion = "x".repeat(56) + ".onion"
        assertEquals("http://$onion/favicon.ico", selfIconUrl("ws://$onion"))
        assertEquals("https://$onion/favicon.ico", selfIconUrl("wss://$onion"))
    }

    @Test
    fun `our own url is not an override, the redirect would point at itself`() {
        val self = "https://relay.example.com/favicon.ico"
        // The trap this function exists for: with RELAY_ICON unset the doc
        // carries our own url, and treating that as an override sends
        // /favicon.ico to the route that issued it.
        assertNull(iconOverride(self, self))
        assertNull(iconOverride(null, self))
        assertNull(iconOverride("", self))
        assertNull(iconOverride("   ", self))
        assertEquals("https://cdn.example/logo.png", iconOverride("https://cdn.example/logo.png", self))
        // No self url to compare against (an unreachable RELAY_URL): an icon in
        // the doc can only have come from the operator.
        assertEquals("https://cdn.example/logo.png", iconOverride("https://cdn.example/logo.png", null))
    }

    @Test
    fun `with no override the markup is the classpath's own bytes`() {
        val html = assertNotNull(javaClass.getResource("/index.html")?.readText())
        assertSame(html, pageWithIcon(html, null), "not even re-rendered")
        assertSame(html, pageWithIcon(html, "  "))
    }

    @Test
    fun `an override replaces every icon link, because the svg would outrank it`() {
        val html = assertNotNull(javaClass.getResource("/index.html")?.readText())
        val themed = pageWithIcon(html, "https://cdn.example/logo.png")

        val links = Regex("""<link rel="icon"[^>]*>""").findAll(themed).map { it.value }.toList()
        assertEquals(1, links.size, "one icon link, not three")
        assertEquals("""<link rel="icon" href="https://cdn.example/logo.png" />""", links.single())
        // The half that would silently do nothing: Chrome, Firefox and Edge all
        // prefer an SVG icon, so an override left beside the built-in one loses
        // everywhere except Safari.
        assertFalse(themed.contains("/web/favicon.svg"), "the built-in svg must be gone, not merely outnumbered")
        assertFalse(themed.contains("/web/favicon.ico"))
        // Nothing else about the page moved.
        assertEquals(html.lines().size - 1, themed.lines().size, "exactly one line fewer")
        assertTrue(themed.contains("<title>SearchOverTrust</title>"))
    }

    @Test
    fun `an icon from a NIP-86 rpc cannot break out of the attribute it lands in`() {
        val html = assertNotNull(javaClass.getResource("/index.html")?.readText())
        // `changerelayicon` is an authenticated admin over the NETWORK, and this
        // is what puts its argument into a page served to everyone.
        val themed = pageWithIcon(html, """x" onerror="alert(1)" a="<script>&""")
        assertFalse(themed.contains("onerror=\""), "the quote must not close the attribute")
        assertFalse(themed.contains("<script>"))
        assertTrue(themed.contains("&quot;"), "quoted, not dropped")
        assertTrue(themed.contains("&lt;script&gt;"))
        assertTrue(themed.contains("&amp;"))
    }

    @Test
    fun `a page re-themes when the icon moves, and keeps its identity when it does not`() {
        val page = IconedPage("<head>\n<link rel=\"icon\" href=\"/web/favicon.svg\" />\n</head>\n", null)
        val before = page.page
        assertTrue(before.html.contains("/web/favicon.svg"))

        page.icon(null)
        assertSame(before, page.page, "an rpc that changed something else must not re-hash 25KB of markup")

        page.icon("https://cdn.example/logo.png")
        assertTrue(page.page.html.contains("https://cdn.example/logo.png"))
        assertFalse(page.page.html.contains("/web/favicon.svg"))
        // The etag has to move with it, or a reader who already has the page
        // keeps the old icon until the cache expires — which for a `no-cache`
        // page is never.
        assertTrue(before.etag != page.page.etag, "a new drawing needs a new validator")

        page.icon(null)
        assertEquals(before.html, page.page.html, "and back")
        assertEquals(before.etag, page.page.etag)
    }

    @Test
    fun `the well-known path redirects to an operator's icon, and serves ours without one`() =
        testApplication {
            var icon: String? = null
            application { routing { favicon { icon } } }

            val ours = client.get("/favicon.ico")
            assertEquals(HttpStatusCode.OK, ours.status)

            icon = "https://cdn.example/logo.png"
            // Not the default client: it follows redirects, and the target is a
            // host that does not exist. The redirect itself is the assertion.
            val moved = createClient { followRedirects = false }.get("/favicon.ico")
            assertEquals(HttpStatusCode.Found, moved.status, "302, so a changed icon is never cached forever")
            assertEquals("https://cdn.example/logo.png", moved.headers[HttpHeaders.Location])
        }
}
