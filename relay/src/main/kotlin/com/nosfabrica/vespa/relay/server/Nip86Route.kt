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
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.contentLength
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.header
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.post
import io.ktor.utils.io.readRemaining
import kotlinx.io.readByteArray

/**
 * The NIP-86 relay-management configuration. [banStore] is shared with
 * [NostrRelayServer]'s BanListPolicy, so bans take effect on ingest
 * immediately. [purge] removes already-stored events matching a filter.
 * [relayHttpUrl] is the `u` the NIP-98 auth event must be tagged with.
 */
class Nip86Admin(
    val banStore: BanStore,
    val adminPubkeys: Set<String>,
    val relayHttpUrl: String,
    val purge: suspend (Filter) -> Unit,
)

/**
 * Mount the NIP-86 admin RPC on `POST /`. Parsing, NIP-98 auth and dispatch
 * live in Quartz's [Nip86HttpHandler]; this bridges Ktor's request to it and
 * maps the typed result onto HTTP status codes. Successful name/description/
 * icon changes flow into [info], so `GET /` reflects them.
 */
fun Route.nip86Admin(
    admin: Nip86Admin,
    info: Nip86Server.InfoHolder,
) {
    val server =
        Nip86Server(
            admin.banStore,
            info,
            admin.purge,
            admin.adminPubkeys,
        )
    val handler = Nip86HttpHandler(server, admin.relayHttpUrl, Nip98AuthVerifier(), Nip86HttpHandler.DEFAULT_MAX_BODY_BYTES)
    val rpcType = ContentType.parse(Nip86HttpHandler.CONTENT_TYPE)

    post("/") {
        val auth = call.request.headers["Authorization"]
        // Bound the read BEFORE buffering. NIP-98 binds the token to the
        // sha256 of the whole body, so the handler can only check size after
        // reading — its KDoc makes bounding the read itself the adapter's
        // job. An unbounded receive here is a pre-auth OOM vector: anyone
        // who finds the port could fill the heap without ever presenting a
        // token. The declared length gets a clean early 413; the +1 read
        // catches liars and chunked uploads.
        val max = Nip86HttpHandler.DEFAULT_MAX_BODY_BYTES
        if ((call.request.contentLength() ?: 0) > max) {
            call.respondText("Payload exceeds $max bytes", status = HttpStatusCode.PayloadTooLarge)
            return@post
        }
        val body = call.receiveChannel().readRemaining(max + 1L).readByteArray()
        if (body.size > max) {
            call.respondText("Payload exceeds $max bytes", status = HttpStatusCode.PayloadTooLarge)
            return@post
        }
        when (val result = handler.handle(auth, body)) {
            is Nip86HttpHandler.Response.Ok -> {
                call.respondText(result.json, rpcType, HttpStatusCode.OK)
            }

            is Nip86HttpHandler.Response.MissingAuth -> {
                call.response.header("WWW-Authenticate", Nip86HttpHandler.WWW_AUTHENTICATE)
                call.respondText("Missing NIP-98 Authorization", status = HttpStatusCode.Unauthorized)
            }

            is Nip86HttpHandler.Response.BadAuth -> {
                call.response.header("WWW-Authenticate", Nip86HttpHandler.WWW_AUTHENTICATE)
                call.respondText(result.reason, status = HttpStatusCode.Unauthorized)
            }

            is Nip86HttpHandler.Response.NotAdmin -> {
                call.respondText("Not an admin", status = HttpStatusCode.Forbidden)
            }

            is Nip86HttpHandler.Response.BadRequest -> {
                call.respondText(result.reason, status = HttpStatusCode.BadRequest)
            }

            is Nip86HttpHandler.Response.PayloadTooLarge -> {
                call.respondText("Payload exceeds ${result.cap} bytes", status = HttpStatusCode.PayloadTooLarge)
            }
        }
    }
}
