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
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The public pages under [PAGE_CONTENT_SECURITY_POLICY]. The policy runs no inline script, and a
 * page that put one back would not fail loudly: it would stop working, in a browser only, so
 * these fail the build instead.
 */
class PageSecurityTest {
    /** Every page this module ships, read from the source tree so a new one cannot be left out. */
    private val pages: Map<String, String> =
        File("src/main/resources")
            .listFiles { f -> f.name.endsWith(".html") }
            .orEmpty()
            .associate { it.name to it.readText() }

    private fun Application.mountPages() {
        installPageDefaults()
        routing {
            for ((name, html) in pages) get("/$name") { call.respondPage(CachedPage(html)) }
            get("/stats.json") { call.respondText("{}") }
            webModules()
        }
    }

    @Test
    fun `no page carries inline script, an inline handler or an inline stylesheet`() {
        assertTrue(pages.keys.containsAll(listOf("index.html", "stats.html", "trust.html", "observer_stats.html", "pulse.html")), "found ${pages.keys}")
        for ((name, html) in pages) {
            val scripts = SCRIPT_TAG.findAll(html).map { it.value }.toList()
            assertTrue(scripts.isNotEmpty(), "$name loads no script — this check would pass by finding nothing")
            for (tag in scripts) assertTrue(" src=" in tag, "$name carries an inline script: $tag")
            assertFalse(
                HANDLER.containsMatchIn(html.replace(COMMENT, "")),
                "$name carries an inline event handler: ${HANDLER.find(html.replace(COMMENT, ""))?.value}",
            )
            assertFalse("<style" in html, "$name carries an inline stylesheet")
            assertFalse(Regex("""href=["']?javascript:""", RegexOption.IGNORE_CASE).containsMatchIn(html), "$name carries a javascript: url")
        }
    }

    @Test
    fun `no module needs eval, which the policy refuses`() {
        val modules = File("src/main/resources/web").walk().filter { it.extension == "js" }.toList()
        assertTrue(modules.size > 50, "found ${modules.size} modules")
        for (f in modules) {
            val src = f.readText()
            assertFalse(Regex("""\beval\(|new Function\(""").containsMatchIn(src), "${f.name} evaluates a string")
        }
    }

    @Test
    fun `every page is served under a policy that admits only its own scripts`() =
        testApplication {
            application { mountPages() }

            for (name in pages.keys - "pulse.html") {
                val res = client.get("/$name")
                assertEquals(HttpStatusCode.OK, res.status)
                val csp = assertNotNull(res.headers["Content-Security-Policy"], "$name is served without a policy")
                val script = csp.split(';').map { it.trim() }.single { it.startsWith("script-src ") }
                assertEquals("script-src 'self'", script, "an injected script must not be able to run")
                assertFalse("unsafe-eval" in csp)
                assertTrue("default-src 'none'" in csp, "every kind of load must be named explicitly")
                assertTrue("object-src 'none'" in csp)
                assertTrue("base-uri 'none'" in csp, "nothing may rewrite what a relative url resolves against")
                assertTrue("frame-ancestors 'none'" in csp)
                // The search page signs and decrypts through NIP-07: nothing may frame it to steer a click.
                assertEquals("DENY", res.headers["X-Frame-Options"])
                assertEquals("nosniff", res.headers["X-Content-Type-Options"])
                assertEquals("no-referrer", res.headers["Referrer-Policy"])
            }
            // A document opened as a page is under the same terms.
            assertNotNull(client.get("/stats.json").headers["Content-Security-Policy"])
        }

    @Test
    fun `every file a page loads is served from its own origin`() =
        testApplication {
            application { mountPages() }

            for ((name, html) in pages) {
                val refs =
                    REFERENCE
                        .findAll(html)
                        .map { it.groupValues[1] }
                        .filter { it.startsWith("./web/") }
                        .toList()
                assertTrue(refs.isNotEmpty(), "$name references nothing under ./web/")
                for (ref in refs) {
                    assertEquals(HttpStatusCode.OK, client.get(ref.removePrefix(".")).status, "$name loads $ref, which is not served")
                }
            }
        }

    private companion object {
        val SCRIPT_TAG = Regex("""<script\b[^>]*>""", RegexOption.IGNORE_CASE)
        val HANDLER = Regex("""<[a-z][^<>]*\son[a-z]+\s*=""", RegexOption.IGNORE_CASE)
        val COMMENT = Regex("""<!--.*?-->""", RegexOption.DOT_MATCHES_ALL)
        val REFERENCE = Regex("""<(?:script|link)\b[^>]*\b(?:src|href)="([^"]+)"""")
    }
}
