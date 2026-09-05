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

import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip98HttpAuth.HTTPAuthorizationEvent
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * WHO MAY READ THE PULSE DOCUMENT, driven in-process.
 *
 * This document is the one page in the repo that is NOT public: it names the
 * observer lenses and search terms driving the relay's load and, with the
 * slow-read log on, quotes the queries people typed. Every case here is
 * written in the direction its bug would fail — and the direction that matters
 * is always the same one, a reader getting numbers they should not have.
 */
class PulseSiteTest {
    private val admin = NostrSignerSync()
    private val stranger = NostrSignerSync()

    /** The origin tokens are signed against, as an operator would configure it. */
    private val origin = "http://pulse.test"

    private fun guard(sessions: AdminSessions = AdminSessions()) = PulseGuard(Nip98AdminGate(setOf(admin.pubKey), origin), sessions)

    /** A NIP-98 token for [path], signed by [by]. Single-use: quartz's verifier remembers it. */
    private fun token(
        path: String,
        method: String,
        by: NostrSignerSync = admin,
        createdAt: Long = System.currentTimeMillis() / 1000,
    ): String =
        by
            .sign(HTTPAuthorizationEvent.build(origin + path, method, null, createdAt) {})
            .toAuthToken()

    /** The page as the two mains load it: off the classpath, no fallback. */
    private fun page(): String =
        assertNotNull(
            PulseSiteTest::class.java.getResourceAsStream("/pulse.html")?.use { it.readBytes().decodeToString() },
            "pulse.html is not on the classpath — no pulse page could be served",
        )

    private fun ApplicationTestBuilder.mount(
        g: PulseGuard,
        doc: () -> JsonObject? = { buildJsonObject { put("schema", 1) } },
    ) {
        application {
            installPulseDefaults()
            routing {
                get("/") { call.respondPage(CachedPage(page())) }
                pulseDocument(g, doc)
                pulseSession(g)
                webModules()
                favicon()
            }
        }
    }

    private suspend fun HttpResponse.json(): JsonObject = Json.parseToJsonElement(bodyAsText()).jsonObject

    // ── the refusals ────────────────────────────────────────────────────────

    @Test
    fun `the document is refused outright without credentials`() =
        testApplication {
            mount(guard())

            val res = client.get(PULSE_DOC_PATH)

            assertEquals(HttpStatusCode.Unauthorized, res.status)
            // The refusal must carry what to sign: the server checks `u` against
            // its OWN configured origin, which behind a tunnel is not the address
            // the browser dialled — and a client left to guess fails forever with
            // nothing on screen to explain it.
            val sign = res.json()["sign"]!!.jsonObject
            assertEquals("$origin$PULSE_DOC_PATH", sign["url"]!!.jsonPrimitive.content)
            assertEquals("GET", sign["method"]!!.jsonPrimitive.content)
            assertEquals(27235, sign["kind"]!!.jsonPrimitive.content.toInt())
            // And, separately, what to sign to open a SESSION — a different url and
            // a different method. A client that signed the document's url and
            // posted it here would get a 405 and no explanation, which is the
            // whole reason these are two members and not one.
            val session = res.json()["session"]!!.jsonObject
            assertEquals("$origin$PULSE_SESSION_PATH", session["url"]!!.jsonPrimitive.content)
            assertEquals("POST", session["method"]!!.jsonPrimitive.content)
            assertEquals("Nostr", res.headers[HttpHeaders.WWWAuthenticate])
        }

    @Test
    fun `the document is never built for a reader who is refused`() =
        testApplication {
            var built = 0
            mount(guard()) {
                built++
                buildJsonObject { put("schema", 1) }
            }

            client.get(PULSE_DOC_PATH)
            client.get(PULSE_DOC_PATH)

            // Not only "not served" — not even READ. An anonymous poller must not
            // be able to make this process walk its counters.
            assertEquals(0, built, "the document was built for a request that was refused")
        }

    @Test
    fun `a verified non-administrator is told so, and still gets nothing`() =
        testApplication {
            mount(guard())

            val res =
                client.get(PULSE_DOC_PATH) {
                    header(HttpHeaders.Authorization, token(PULSE_DOC_PATH, "GET", by = stranger))
                }

            // 403, not 401: they proved who they are and the answer is still no.
            // Saying which key was refused saves an operator from debugging a
            // silent refusal that was a typo in the admin list.
            assertEquals(HttpStatusCode.Forbidden, res.status)
            assertEquals(stranger.pubKey, res.json()["pubkey"]!!.jsonPrimitive.content)
            assertFalse(res.bodyAsText().contains("schema"), "a refused reader must not receive any part of the document")
        }

    @Test
    fun `a token signed for another url does not open this one`() =
        testApplication {
            mount(guard())

            // THE POINT OF THE `u` TAG. A token the administrator signed for some
            // other service — or for the logout route — is not a bearer credential
            // for this one.
            val res =
                client.get(PULSE_DOC_PATH) {
                    header(HttpHeaders.Authorization, token(PULSE_LOGOUT_PATH, "GET"))
                }

            assertEquals(HttpStatusCode.Unauthorized, res.status)
        }

    // ── the two ways in ─────────────────────────────────────────────────────

    @Test
    fun `an administrator's own token reads the document directly`() =
        testApplication {
            mount(guard())

            val res =
                client.get(PULSE_DOC_PATH) {
                    header(HttpHeaders.Authorization, token(PULSE_DOC_PATH, "GET"))
                }

            // The path a script takes: no session, one signature per request.
            assertEquals(HttpStatusCode.OK, res.status)
            assertEquals(
                1,
                res
                    .json()["schema"]!!
                    .jsonPrimitive.content
                    .toInt(),
            )
        }

    @Test
    fun `one signature opens a session, and the session reads the document`() =
        testApplication {
            mount(guard())

            val opened =
                client.post(PULSE_SESSION_PATH) {
                    header(HttpHeaders.Authorization, token(PULSE_SESSION_PATH, "POST"))
                }
            assertEquals(HttpStatusCode.OK, opened.status)
            assertEquals(admin.pubKey, opened.json()["pubkey"]!!.jsonPrimitive.content)

            val cookie = assertNotNull(opened.headers[HttpHeaders.SetCookie], "no session cookie was set")
            // THE WHOLE REASON THE SESSION EXISTS. NIP-98 tokens are single-use, so
            // a page polling every two seconds would need an extension popup every
            // two seconds. One signature, then the cookie.
            assertTrue(cookie.startsWith("$PULSE_COOKIE="))
            assertTrue(cookie.contains("HttpOnly", ignoreCase = true), "the cookie must be unreadable from the page's own scripts")
            assertTrue(cookie.contains("SameSite=Strict", ignoreCase = true), "no other site may cause this cookie to be sent")
            assertTrue(cookie.contains("Max-Age=1800"), "the session must state its own lifetime")
            assertTrue(cookie.contains("Path=/"))
            // Ktor also stamps a `${'$'}x-enc` attribute of its own. Browsers ignore
            // unknown attributes and never send them back, so it is noise rather
            // than a property; the four above are the properties.

            val read = client.get(PULSE_DOC_PATH) { header(HttpHeaders.Cookie, sessionOf(cookie)) }
            assertEquals(HttpStatusCode.OK, read.status)
        }

    @Test
    fun `a query string does not change what a token must be signed over`() =
        testApplication {
            mount(guard())

            // The expected `u` comes from the ROUTE's path, so a cache-buster or
            // any other parameter cannot make an administrator's token stop
            // working — and cannot make a token signed for some other route
            // start working either.
            val res =
                client.get("$PULSE_DOC_PATH?t=1") {
                    header(HttpHeaders.Authorization, token(PULSE_DOC_PATH, "GET"))
                }

            assertEquals(HttpStatusCode.OK, res.status)
        }

    @Test
    fun `the session cookie is Secure where the deployment is served over TLS`() =
        testApplication {
            val tls = PulseGuard(Nip98AdminGate(setOf(admin.pubKey), "https://pulse.example"))
            mount(tls)
            val signed =
                admin
                    .sign(HTTPAuthorizationEvent.build("https://pulse.example$PULSE_SESSION_PATH", "POST", null, System.currentTimeMillis() / 1000) {})
                    .toAuthToken()

            val opened = client.post(PULSE_SESSION_PATH) { header(HttpHeaders.Authorization, signed) }

            // Decided from the operator's declared origin, not the request's
            // scheme: behind a TLS-terminating proxy the request reads `http`,
            // and that is the deployment where the mark matters most.
            assertTrue(assertNotNull(opened.headers[HttpHeaders.SetCookie]).contains("Secure", ignoreCase = true))
        }

    @Test
    fun `a token cannot be spent twice`() =
        testApplication {
            mount(guard())
            val once = token(PULSE_DOC_PATH, "GET")

            assertEquals(HttpStatusCode.OK, client.get(PULSE_DOC_PATH) { header(HttpHeaders.Authorization, once) }.status)

            // Replay protection is what makes this a proof rather than a bearer
            // token: an Authorization header captured off the wire is already spent.
            assertEquals(
                HttpStatusCode.Unauthorized,
                client.get(PULSE_DOC_PATH) { header(HttpHeaders.Authorization, once) }.status,
            )
        }

    @Test
    fun `a cookie cannot open a session, only a signature can`() =
        testApplication {
            mount(guard())
            val opened = client.post(PULSE_SESSION_PATH) { header(HttpHeaders.Authorization, token(PULSE_SESSION_PATH, "POST")) }
            val cookie = sessionOf(assertNotNull(opened.headers[HttpHeaders.SetCookie]))

            val again = client.post(PULSE_SESSION_PATH) { header(HttpHeaders.Cookie, cookie) }

            // Otherwise an expiring session could renew itself forever and the
            // fixed lifetime would mean nothing.
            assertEquals(HttpStatusCode.Unauthorized, again.status)
        }

    @Test
    fun `signing out ends the session`() =
        testApplication {
            mount(guard())
            val opened = client.post(PULSE_SESSION_PATH) { header(HttpHeaders.Authorization, token(PULSE_SESSION_PATH, "POST")) }
            val cookie = sessionOf(assertNotNull(opened.headers[HttpHeaders.SetCookie]))

            assertEquals(HttpStatusCode.NoContent, client.post(PULSE_LOGOUT_PATH) { header(HttpHeaders.Cookie, cookie) }.status)

            assertEquals(HttpStatusCode.Unauthorized, client.get(PULSE_DOC_PATH) { header(HttpHeaders.Cookie, cookie) }.status)
            // And a logout with no session is still a 204: it must not report
            // whether a token was live.
            assertEquals(HttpStatusCode.NoContent, client.post(PULSE_LOGOUT_PATH).status)
        }

    // ── the shell, and what it must not leak ────────────────────────────────

    @Test
    fun `the page shell is served unauthenticated, and carries no numbers`() =
        testApplication {
            mount(guard())

            val html = client.get("/")

            // Deliberately open: a browser cannot put an Authorization header on a
            // navigation, so gating this would make the page unreachable rather
            // than more private. It holds markup and a script that asks for a
            // signature; every number is behind the guard.
            assertEquals(HttpStatusCode.OK, html.status)
            val body = html.bodyAsText()
            // The shell is now markup only — the prompt itself lives in the module,
            // which is where it has to be checked.
            assertTrue(body.contains("./web/pulse/page.js"), "the page must load its logic")
            assertTrue(
                client.get("/web/pulse/page.js").bodyAsText().contains("Administrators only"),
                "an unauthenticated visitor must be told what this is",
            )
            assertTrue(client.get("/web/pulse/page.js").bodyAsText().contains("pulse.json"), "the page must read the document this site serves")

            for (asset in IMPORTS) {
                assertEquals(HttpStatusCode.OK, client.get(asset).status, "$asset — imported by the pulse page")
            }
        }

    @Test
    fun `the page carries no inline script or style, so the policy can forbid both`() =
        testApplication {
            mount(guard())

            val body = client.get("/").bodyAsText()

            // THE REASON THIS PAGE IS SHAPED DIFFERENTLY from every other page
            // here. `script-src 'self'` cannot admit an inline script, so putting
            // one back would not fail loudly — the page would simply stop
            // working, and only in a browser. This fails in the build instead.
            assertFalse(body.contains("<script>") || body.contains("<script type=\"module\">"), "the page carries an inline script")
            assertFalse(body.contains("<style"), "the page carries an inline stylesheet")
            assertFalse(body.contains("style=\""), "the page carries an inline style attribute")
            assertTrue(body.contains("src=\"./web/pulse/page.js\""), "the page must load its logic from a file")
        }

    @Test
    fun `the policy denies by default and admits no inline code`() =
        testApplication {
            mount(guard())

            val csp = assertNotNull(client.get("/").headers["Content-Security-Policy"])

            assertTrue(csp.contains("default-src 'none'"), "every kind of load must be named explicitly")
            assertTrue(csp.contains("script-src 'self'") && !csp.contains("script-src 'self' 'unsafe-inline'"))
            assertFalse(csp.contains("unsafe-inline"), "an injected script or style must not be able to run")
            assertFalse(csp.contains("unsafe-eval"))
            assertTrue(csp.contains("connect-src 'self'"), "the page may only talk to its own origin")
            assertTrue(csp.contains("frame-ancestors 'none'"))
            assertTrue(csp.contains("base-uri 'none'"), "nothing may rewrite what a relative url resolves against")
            // The one relaxation, and only for images: a NIP-86 rpc can point this
            // deployment's icon at another origin, and a favicon loads under
            // `img-src`. An image cannot execute.
            assertTrue(csp.contains("img-src 'self' data: https:"))
        }

    @Test
    fun `this site opens itself to no other origin`() =
        testApplication {
            mount(guard())

            val res = client.get("/")

            // installPageDefaults' anyHost() CORS is right for the public stats
            // document and would be the whole vulnerability here: this site
            // answers with a cookie. SameSite=Strict is the other half; both are
            // needed, and this pins that the permissive one is not installed.
            assertNull(res.headers["Access-Control-Allow-Origin"])
            assertEquals("DENY", res.headers["X-Frame-Options"])
            assertEquals("nosniff", res.headers["X-Content-Type-Options"])
            assertTrue(res.headers["Content-Security-Policy"]!!.contains("frame-ancestors 'none'"))
        }

    @Test
    fun `the document is never cached, by anything`() =
        testApplication {
            var built = 0
            mount(guard()) {
                built++
                buildJsonObject { put("uptimeSeconds", built) }
            }
            // Different seconds, deliberately. Two NIP-98 tokens for the same url
            // and method in the same second are the same EVENT — same id — and the
            // replay check rejects the second. That is the tokens working as
            // designed, and it is exactly why a polling page uses the session
            // instead of signing per request.
            val now = System.currentTimeMillis() / 1000

            val first = client.get(PULSE_DOC_PATH) { header(HttpHeaders.Authorization, token(PULSE_DOC_PATH, "GET", createdAt = now)) }
            val second = client.get(PULSE_DOC_PATH) { header(HttpHeaders.Authorization, token(PULSE_DOC_PATH, "GET", createdAt = now - 1)) }

            // The page's whole method is differencing two consecutive polls, so a
            // cached or 304'd document would leave every rate at zero forever —
            // and this is admin-only content that must not sit in a shared cache.
            assertEquals("no-store", first.headers[HttpHeaders.CacheControl])
            assertNull(first.headers[HttpHeaders.ETag], "an ETag over a document whose every field moves can never match")
            assertEquals(2, built, "each request must read the counters again")
            assertTrue(second.bodyAsText().contains("\"uptimeSeconds\":2"))
        }

    @Test
    fun `a process with no metered store answers 503 to an administrator, never a document of zeroes`() =
        testApplication {
            mount(guard()) { null }

            val res = client.get(PULSE_DOC_PATH) { header(HttpHeaders.Authorization, token(PULSE_DOC_PATH, "GET")) }

            assertEquals(HttpStatusCode.ServiceUnavailable, res.status)
        }

    /** The `name=value` pair out of a Set-Cookie line, which is all a client sends back. */
    private fun sessionOf(setCookie: String): String = setCookie.substringBefore(';')

    private companion object {
        /** Kept as a list rather than parsed from the markup, so a reformat cannot silently empty it. */
        val IMPORTS =
            listOf(
                "/web/shared/stats.css",
                "/web/shared/page.js",
                "/web/shared/pulse.js",
                "/web/shared/pulseauth.js",
                "/web/pulse/page.js",
                "/web/pulse/pulse.css",
            )
    }
}
