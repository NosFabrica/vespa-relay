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

import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.plugins.compression.Compression
import io.ktor.server.plugins.compression.deflate
import io.ktor.server.plugins.compression.gzip
import io.ktor.server.plugins.compression.minimumSize
import io.ktor.server.response.header
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

/** The cookie one signature opens. Not `__Host-` prefixed: the page must work over plain http on a private port. */
const val PULSE_COOKIE = "pulse_session"

/** Where a signature is exchanged for a session, and what a 401 points at. */
const val PULSE_SESSION_PATH = "/pulse/session"

/** Where a session is ended. */
const val PULSE_LOGOUT_PATH = "/pulse/logout"

/** The document itself. */
const val PULSE_DOC_PATH = "/pulse.json"

/**
 * EVERYTHING THAT DECIDES WHO MAY READ THE PULSE DOCUMENT, in one object so no
 * route can be mounted without it.
 *
 * The document names the observer lenses and the search terms driving this
 * relay's load and, with the slow-read log on, quotes the queries people
 * typed. `/stats.json` is public precisely because every field in it is a fact
 * about stored events; this is the other kind of document, and it is served
 * only to a proven administrator.
 *
 * There is no unauthenticated mode and no switch that creates one. A
 * deployment with no administrators configured cannot serve this page at all —
 * the boot refuses rather than opening it, because "no admins" and "everyone
 * is an admin" are one typo apart and only one of them is safe.
 */
class PulseGuard(
    val gate: Nip98AdminGate,
    val sessions: AdminSessions = AdminSessions(),
)

/**
 * Compression and the headers an admin page wants, and DELIBERATELY NO CORS.
 *
 * `installPageDefaults` opens the public pages to any origin, which is right
 * for a document anyone may chart. Here it would be the whole vulnerability:
 * this site answers with a cookie, and a permissive origin policy is what
 * would let a page the administrator happens to be visiting read the response.
 * `SameSite=Strict` on the cookie is the other half; both are needed, because
 * either alone has been enough to lose this argument before.
 *
 * THE CSP DENIES BY DEFAULT and names every kind of load this page makes. No
 * `unsafe-inline` on either script or style, which is why this page — alone
 * among the pages in this repo — keeps its logic in `/web/pulse/page.js` and
 * its styling in `/web/pulse/pulse.css` rather than in the markup: an inline
 * `<script>` cannot satisfy `script-src 'self'`, and that is the point. Every
 * value the page renders already goes through `textContent`, so this is the
 * second lock rather than the first; on the one page here that is not public,
 * a second lock is worth the file.
 *
 * `img-src` is the one relaxation, and only for `https:`. A NIP-86 rpc can
 * point this deployment's icon at another origin, and a favicon is fetched
 * under `img-src`; refusing it would break a setting that has nothing to do
 * with this page. An image cannot execute.
 */
fun Application.installPulseDefaults() {
    install(Compression) {
        gzip { priority = 1.0 }
        deflate { priority = 0.9 }
        minimumSize(1024)
    }
    install(
        createApplicationPlugin("PulseHeaders") {
            onCall { call ->
                call.response.header("X-Frame-Options", "DENY")
                call.response.header("X-Content-Type-Options", "nosniff")
                call.response.header("Referrer-Policy", "no-referrer")
                call.response.header(
                    "Content-Security-Policy",
                    "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data: https:; " +
                        "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
                )
            }
        },
    )
}

/**
 * Who this request is, by cookie first and then by a fresh NIP-98 token.
 *
 * The cookie is checked first because it is the common case — a page polling
 * every two seconds — and because verifying a token costs a signature check
 * and a lock on the replay set.
 */
private suspend fun ApplicationCall.admin(
    guard: PulseGuard,
    path: String,
    method: String,
): Admitted {
    guard.sessions.holder(request.cookies[PULSE_COOKIE])?.let { return Admitted.Admin(it) }
    // Judged against the ROUTE'S path, not the request target. Deriving the
    // expected `u` from `request.uri` made two things disagree: this, and the
    // refusal that tells a client what to sign — which uses the route's path.
    // The route is also the canonical spelling, so a request that reaches this
    // handler under some other encoding of the same path is judged against the
    // one url a token could sensibly have been signed over. A query string
    // never enters it, which is why `/pulse.json?t=1` works.
    return guard.gate.admit(request.headers[HttpHeaders.Authorization], method, guard.gate.urlFor(path))
}

/**
 * Refuse, in the terms the caller needs to do better.
 *
 * The 401 carries the exact `u` and method a token must be signed over. That
 * is not a secret — it is an operator setting the client could be told any
 * number of ways — and publishing it removes the one failure this scheme
 * otherwise produces constantly: a token signed against the url the browser
 * dialled while the server expects the url the operator configured.
 *
 * A wrong pubkey gets 403 and its own pubkey back. Saying "you are not an
 * administrator" to somebody who just proved who they are costs nothing and
 * saves an operator from debugging a silent 401 that was really a typo in
 * `RELAY_ADMIN_PUBKEYS`.
 */
private suspend fun ApplicationCall.refuse(
    verdict: Admitted,
    guard: PulseGuard,
    signPath: String,
    signMethod: String,
) {
    response.header(HttpHeaders.CacheControl, "no-store")

    // TWO ANSWERS TO "WHAT DO I SIGN", because there are two ways in and they
    // are signed over different urls. `sign` opens THIS route directly, which
    // is the script's path: one token per request. `session` opens a session,
    // which is the browser's: one token, then a cookie. Confusing the two is
    // the mistake this member exists to prevent — a client that signed the
    // document's url and posted it to the session route gets a 405 and no
    // explanation.
    fun howToSign(
        path: String,
        method: String,
    ) = buildJsonObject {
        put("url", guard.gate.urlFor(path))
        put("method", method)
        put("kind", 27235)
    }
    val (status, body) =
        when (verdict) {
            is Admitted.NotAdmin -> {
                HttpStatusCode.Forbidden to
                    buildJsonObject {
                        put("error", "not an administrator of this relay")
                        // Echoed back on purpose: telling somebody who just proved who
                        // they are that they are not on the list saves an operator from
                        // debugging a silent refusal that was a typo in the admin set.
                        put("pubkey", verdict.pubkey)
                    }
            }

            is Admitted.BadCredentials -> {
                HttpStatusCode.Unauthorized to
                    buildJsonObject {
                        put("error", verdict.reason)
                        put("sign", howToSign(signPath, signMethod))
                        put("session", howToSign(PULSE_SESSION_PATH, "POST"))
                    }
            }

            else -> {
                HttpStatusCode.Unauthorized to
                    buildJsonObject {
                        put("error", "administrator sign-in required")
                        put("sign", howToSign(signPath, signMethod))
                        put("session", howToSign(PULSE_SESSION_PATH, "POST"))
                    }
            }
        }
    if (status == HttpStatusCode.Unauthorized) response.header(HttpHeaders.WWWAuthenticate, "Nostr")
    respondText(JSON.encodeToString(JsonObject.serializer(), body), ContentType.Application.Json, status)
}

/**
 * `GET /pulse.json`, the live document the pulse page charts — ADMINISTRATORS
 * ONLY, by cookie or by a fresh NIP-98 token.
 *
 * BUILT PER REQUEST, unlike `/stats.json`. That document costs Vespa queries,
 * so it is rolled up on a clock and served from memory; this one is a read of
 * in-process counters, so rolling it up would only make it older. The
 * [document] lambda is called on the request thread and must stay cheap —
 * `PulseDocument.of` is. It is called only AFTER the request is admitted, so
 * an anonymous poller cannot even make this process do the work.
 *
 * `no-store`, and no ETag. Every field moves, `generatedAt` above all, so a
 * validator would never match and computing one would be pure cost. It also
 * must not sit in any cache: the page's whole method is differencing two
 * consecutive polls, and this is admin-only content.
 *
 * Its own function so a test can mount it without a store.
 */
fun Route.pulseDocument(
    guard: PulseGuard,
    document: () -> JsonObject?,
    path: String = PULSE_DOC_PATH,
) {
    get(path) {
        val who = call.admin(guard, path, "GET")
        if (who !is Admitted.Admin) return@get call.refuse(who, guard, path, "GET")
        val doc = document()
        call.response.header(HttpHeaders.CacheControl, "no-store")
        if (doc == null) {
            // 503, not an empty document: this process has no metered store, and
            // a 200 of zeros would read as a store doing nothing.
            call.respondText(
                """{"error":"this process serves no metered store"}""",
                ContentType.Application.Json,
                HttpStatusCode.ServiceUnavailable,
            )
        } else {
            call.respondText(JSON.encodeToString(JsonObject.serializer(), doc), ContentType.Application.Json)
        }
    }
}

/**
 * `POST /pulse/session` — one NIP-98 signature in, one short-lived session
 * cookie out; and `POST /pulse/logout`, which ends it.
 *
 * This exists because NIP-98 tokens are single-use by design (quartz's
 * verifier remembers what it has seen), and a page that polls would otherwise
 * need a signature — an extension popup — every two seconds. The signature
 * still happens; it happens once.
 *
 * The cookie is `HttpOnly` so no script on the page can read it, `SameSite=Strict`
 * so no other site can cause it to be sent, and `Secure` when this deployment
 * is reached over TLS. Conditional rather than always, because the intended
 * deployment is a private port through an SSH tunnel and an unconditional
 * `Secure` there would set a cookie the browser never sends back.
 *
 * DECIDED FROM THE OPERATOR'S OWN `publicUrl`, not from the request. Ktor's
 * `origin.scheme` is the local socket's unless a forwarded-headers plugin is
 * installed — which none is here, deliberately, since this site must not trust
 * headers the caller controls. Behind a TLS-terminating proxy that reads
 * `http` and the session cookie would go out without `Secure`, which is the
 * one deployment where it matters most. The configured origin is a fact the
 * operator stated.
 */
fun Route.pulseSession(guard: PulseGuard) {
    post(PULSE_SESSION_PATH) {
        // The cookie is NOT accepted here: a session may only be opened by a
        // signature, or an expired one could renew itself forever.
        val who =
            guard.gate.admit(
                call.request.headers[HttpHeaders.Authorization],
                "POST",
                guard.gate.urlFor(PULSE_SESSION_PATH),
            )
        if (who !is Admitted.Admin) return@post call.refuse(who, guard, PULSE_SESSION_PATH, "POST")
        val token = guard.sessions.open(who.pubkey)
        // Default (URI) encoding: the token is base64url, so encoding it changes
        // nothing, and it is the one setting under which Ktor's own `${'$'}x-enc`
        // attribute at least says something true. That attribute is Ktor's, not
        // ours; browsers ignore unknown cookie attributes and never send them
        // back, so it is noise rather than a property.
        call.response.cookies.append(
            name = PULSE_COOKIE,
            value = token,
            maxAge = guard.sessions.ttlMillis / 1000,
            path = "/",
            secure = guard.gate.servesOverTls,
            httpOnly = true,
            extensions = mapOf("SameSite" to "Strict"),
        )
        call.response.header(HttpHeaders.CacheControl, "no-store")
        call.respondText(
            """{"pubkey":"${who.pubkey}","expiresInSeconds":${guard.sessions.ttlMillis / 1000}}""",
            ContentType.Application.Json,
        )
    }

    post(PULSE_LOGOUT_PATH) {
        guard.sessions.close(call.request.cookies[PULSE_COOKIE])
        call.response.cookies.append(
            name = PULSE_COOKIE,
            value = "",
            maxAge = 0,
            path = "/",
            secure = guard.gate.servesOverTls,
            httpOnly = true,
            extensions = mapOf("SameSite" to "Strict"),
        )
        call.response.header(HttpHeaders.CacheControl, "no-store")
        // Always 204, whether or not there was a session: a logout must not
        // report whether a token was live.
        call.respondText("", ContentType.Application.Json, HttpStatusCode.NoContent)
    }
}

/** Not pretty-printed: the document is machine-read first, and polled. */
private val JSON = Json

/**
 * The pulse page on its own port, with the document it charts and the assets
 * it needs — the document behind [PulseGuard], the page shell in front of it.
 *
 * WHY THE SHELL IS NOT GATED. It holds no numbers: it is markup and a script
 * that asks for a signature and then fetches `/pulse.json`. Gating it would
 * make the page unreachable from a browser rather than more private, because a
 * browser cannot put an `Authorization` header on a navigation — there would
 * be nothing left to sign with. Everything that carries data is behind the
 * guard, and an unauthenticated visitor gets a sign-in prompt and nothing
 * else.
 *
 * ITS OWN PORT AS WELL, not instead. The guard is the boundary that matters;
 * the port is the one that survives a mistake in it. Bind it on the private
 * side of the network anyway.
 *
 * [wait] is false by default: this is never what keeps a process alive.
 */
fun servePulseSite(
    port: Int,
    page: String,
    guard: PulseGuard,
    document: () -> JsonObject?,
    // Null keeps the page's markup byte-identical to the classpath's. See [pageWithIcon].
    icon: String? = null,
    wait: Boolean = false,
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> {
    val cached = CachedPage(pageWithIcon(page, icon))
    return embeddedServer(Netty, port = port) {
        installPulseDefaults()
        routing {
            get("/") { call.respondPage(cached) }
            get("/pulse.html") { call.respondPage(cached) }
            pulseDocument(guard, document)
            pulseSession(guard)
            webModules()
            favicon { icon }
        }
    }.start(wait = wait)
}
