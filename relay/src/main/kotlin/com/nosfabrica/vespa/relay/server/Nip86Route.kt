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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip86RelayManagement.server.BanStore
import com.vitorpamplona.quartz.nip86RelayManagement.server.Nip86HttpHandler
import com.vitorpamplona.quartz.nip86RelayManagement.server.Nip86Server
import com.vitorpamplona.quartz.nip98HttpAuth.Nip98AuthVerifier
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.response.header
import io.ktor.server.response.respondText
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeout

/**
 * The NIP-86 configuration. [banStore] is shared with the server's BanListPolicy; [purge] removes
 * stored events matching a filter; [relayHttpUrl] is the `u` a NIP-98 auth event must carry.
 */
class Nip86Admin(
    val banStore: BanStore,
    val adminPubkeys: Set<String>,
    val relayHttpUrl: String,
    val purge: suspend (Filter) -> Unit,
    /** How long an admitted call may take to upload its body before it is a 408. */
    val bodyTimeoutMs: Long = DEFAULT_BODY_TIMEOUT_MS,
    /** Admin calls uploading their body at once; one more is a 503. Handling holds no slot. */
    val concurrency: Int = DEFAULT_CONCURRENCY,
    val nowSeconds: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    companion object {
        const val DEFAULT_BODY_TIMEOUT_MS = 10_000L
        const val DEFAULT_CONCURRENCY = 4
    }
}

/**
 * The NIP-86 admin RPC, answered on `POST /` beside NIP-FE's commands ([relayPosts]). The token's
 * signer is checked before the body is read; that check, parsing, full NIP-98 auth and dispatch are quartz's.
 */
fun nip86Answer(
    admin: Nip86Admin,
    info: Nip86Server.InfoHolder,
): suspend (ApplicationCall) -> Unit {
    val server =
        Nip86Server(
            admin.banStore,
            info,
            admin.purge,
            admin.adminPubkeys,
        )
    val handler = Nip86HttpHandler(server, admin.relayHttpUrl, Nip98AuthVerifier(now = admin.nowSeconds), Nip86HttpHandler.DEFAULT_MAX_BODY_BYTES)
    val rpcType = ContentType.parse(Nip86HttpHandler.CONTENT_TYPE)
    val slots = Semaphore(admin.concurrency)

    return ret@{ call ->
        val auth = call.request.headers[HttpHeaders.Authorization]
        // Every check but the payload hash and replay, before a byte of the body is read.
        handler.preauthorize(auth)?.let { return@ret call.respond(it, rpcType) }
        // The slot bounds uploads only: a purge that takes minutes must not turn every other admin call into a 503.
        if (!slots.tryAcquire()) {
            call.response.header(HttpHeaders.RetryAfter, "1")
            return@ret call.respondText("Busy: ${admin.concurrency} admin calls are already uploading", status = HttpStatusCode.ServiceUnavailable)
        }
        val body =
            try {
                call.receiveRpcBody(admin.bodyTimeoutMs) ?: return@ret
            } finally {
                slots.release()
            }
        call.respond(handler.handle(auth, body), rpcType)
    }
}

/** The rpc body within [timeoutMs], or null once the refusal (413 or 408) has been answered. */
private suspend fun ApplicationCall.receiveRpcBody(timeoutMs: Long): ByteArray? {
    // Bounded before buffering: NIP-98 binds the token to the body's sha256, so quartz can only check size after reading.
    val max = Nip86HttpHandler.DEFAULT_MAX_BODY_BYTES
    return try {
        withTimeout(timeoutMs) { receiveBounded(max.toLong()) }
            ?: null.also { respondText("Payload exceeds $max bytes", status = HttpStatusCode.PayloadTooLarge) }
    } catch (_: TimeoutCancellationException) {
        response.header(HttpHeaders.Connection, "close")
        respondText("The body did not arrive within ${timeoutMs / 1000.0}s", status = HttpStatusCode.RequestTimeout)
        null
    }
}

private suspend fun ApplicationCall.respond(
    result: Nip86HttpHandler.Response,
    rpcType: ContentType,
) = when (result) {
    is Nip86HttpHandler.Response.Ok -> respondText(result.json, rpcType, HttpStatusCode.OK)
    is Nip86HttpHandler.Response.MissingAuth -> respondUnauthorized("Missing NIP-98 Authorization")
    is Nip86HttpHandler.Response.BadAuth -> respondUnauthorized(result.reason)
    is Nip86HttpHandler.Response.NotAdmin -> respondText("Not an admin", status = HttpStatusCode.Forbidden)
    is Nip86HttpHandler.Response.BadRequest -> respondText(result.reason, status = HttpStatusCode.BadRequest)
    is Nip86HttpHandler.Response.PayloadTooLarge -> respondText("Payload exceeds ${result.cap} bytes", status = HttpStatusCode.PayloadTooLarge)
}

private suspend fun ApplicationCall.respondUnauthorized(reason: String) {
    response.header(HttpHeaders.WWWAuthenticate, Nip86HttpHandler.WWW_AUTHENTICATE)
    respondText(reason, status = HttpStatusCode.Unauthorized)
}
