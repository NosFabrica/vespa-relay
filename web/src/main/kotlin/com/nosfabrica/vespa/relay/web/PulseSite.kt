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

/** The cookie one signature opens. Not `__Host-` prefixed: the page must work over plain http. */
const val PULSE_COOKIE = "pulse_session"

/** Where a signature is exchanged for a session, and what a 401 points at. */
const val PULSE_SESSION_PATH = "/pulse/session"

const val PULSE_LOGOUT_PATH = "/pulse/logout"

const val PULSE_DOC_PATH = "/pulse.json"

/**
 * Everything that decides who may read the pulse document, in one object so no route can be
 * mounted without it. There is no unauthenticated mode: a deployment with no administrators
 * refuses to boot rather than open a document that quotes what people searched for.
 */
class PulseGuard(
    val gate: Nip98AdminGate,
    val sessions: AdminSessions = AdminSessions(),
)

/**
 * Compression, the headers an admin page wants, and deliberately no CORS: this site answers
 * with a cookie. The CSP admits no inline script or style, which is why this page keeps its
 * logic and styling in `/web/pulse/`; `img-src https:` is the one relaxation, for a NIP-86 icon.
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
 * Who this request is, by cookie first and then by a fresh NIP-98 token. The cookie is the
 * common case, and a token costs a signature check and a lock on the replay set.
 */
private suspend fun ApplicationCall.admin(
    guard: PulseGuard,
    path: String,
    method: String,
): Admitted {
    guard.sessions.holder(request.cookies[PULSE_COOKIE])?.let { return Admitted.Admin(it) }
    // Judged against the route's path, not the request target, so the expected `u` is the one the
    // refusal tells a client to sign, and a query string never enters it.
    return guard.gate.admit(request.headers[HttpHeaders.Authorization], method, guard.gate.urlFor(path))
}

/**
 * Refuse in the terms the caller needs to do better: a 401 carries the exact `u` and method a
 * token must be signed over, since the operator's origin is not what the browser dialled, and a
 * verified non-administrator gets 403 and its own pubkey back.
 */
private suspend fun ApplicationCall.refuse(
    verdict: Admitted,
    guard: PulseGuard,
    signPath: String,
    signMethod: String,
) {
    response.header(HttpHeaders.CacheControl, "no-store")

    // Two answers to "what do I sign": `sign` opens this route directly, one token per request;
    // `session` opens a session, with a token signed over a different url and method.
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
 * `GET /pulse.json`, the live document the pulse page charts, for administrators only. Built
 * per request and only after the request is admitted, so [document] must stay cheap and an
 * anonymous poller cannot make this process do the work. `no-store`, no ETag: every field moves.
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
            // 503, not an empty document: a 200 of zeros would read as a store doing nothing.
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
 * `POST /pulse/session`, one NIP-98 signature in and one short-lived session cookie out, and
 * `POST /pulse/logout`. The cookie is `HttpOnly`, `SameSite=Strict`, and `Secure` only where the
 * operator's origin is https: over an SSH tunnel an unconditional `Secure` is never sent back.
 */
fun Route.pulseSession(guard: PulseGuard) {
    post(PULSE_SESSION_PATH) {
        // A cookie cannot open a session, or an expiring one could renew itself forever.
        val who =
            guard.gate.admit(
                call.request.headers[HttpHeaders.Authorization],
                "POST",
                guard.gate.urlFor(PULSE_SESSION_PATH),
            )
        if (who !is Admitted.Admin) return@post call.refuse(who, guard, PULSE_SESSION_PATH, "POST")
        val token = guard.sessions.open(who.pubkey)
        // The token is base64url, so Ktor's default URI encoding changes nothing.
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
        // Always 204: a logout must not report whether a token was live.
        call.respondText("", ContentType.Application.Json, HttpStatusCode.NoContent)
    }
}

/** Not pretty-printed: the document is machine-read first, and polled. */
private val JSON = Json

/**
 * The pulse page on its own port: the document behind [PulseGuard], the page shell in front of
 * it. The shell holds no numbers and is not gated, because a browser cannot put an
 * `Authorization` header on a navigation. [wait] is false by default.
 */
fun servePulseSite(
    port: Int,
    page: String,
    guard: PulseGuard,
    document: () -> JsonObject?,
    // Null keeps the page's markup byte-identical to the classpath's.
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
