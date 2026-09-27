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

import com.vitorpamplona.quartz.nip86RelayManagement.server.Nip86HttpHandler
import com.vitorpamplona.quartz.nipFERelayOverHttp.HttpRelayHandler
import com.vitorpamplona.quartz.nipFERelayOverHttp.HttpRelayLines
import com.vitorpamplona.quartz.nipFERelayOverHttp.HttpRelayReaderStalled
import com.vitorpamplona.quartz.nipFERelayOverHttp.HttpRelayRequest
import com.vitorpamplona.quartz.nipFERelayOverHttp.HttpRelayResponse
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.http.content.suppressCompression
import io.ktor.server.plugins.origin
import io.ktor.server.request.contentLength
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.header
import io.ktor.server.response.respond
import io.ktor.server.response.respondBytesWriter
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.post
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readRemaining
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import kotlinx.io.readByteArray
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import kotlin.time.Duration.Companion.milliseconds

/**
 * The HTTP command endpoint's settings. [origins] are the URLs a NIP-98 `u` may name, asked per
 * request because the hidden service can come up after the server did; never derived from the request.
 */
class HttpRelay(
    val gate: HttpRelayGate,
    val deadlineMs: Long,
    val origins: () -> List<String>,
    val clients: ClientAddresses = ClientAddresses(),
    /** How long an admitted request may take to upload its body; a trickle holds its gate slot no longer. */
    val bodyTimeoutMs: Long = DEFAULT_BODY_TIMEOUT_MS,
) {
    companion object {
        const val DEFAULT_DEADLINE_SECONDS = 30L
        const val DEFAULT_BODY_TIMEOUT_MS = 10_000L
    }
}

/**
 * NIP-FE's host half: quartz's [HttpRelayHandler] runs the posted frame on a session of [relay];
 * this admits it, reads the body, and writes the answer with its headers and a line-flushed gzip.
 */
fun httpRelayAnswer(
    relay: NostrRelayServer,
    settings: HttpRelay,
): suspend (ApplicationCall) -> Unit {
    val handler = HttpRelayHandler(relay, settings.origins, settings.deadlineMs.milliseconds)
    return { call -> call.answer(handler, settings) }
}

/**
 * `POST /`, the relay's URL, shared as NIP-FE shares it: NIP-86's `application/nostr+json+rpc` is
 * the admin [rpc], anything else a [commands] frame (a command needs no `Content-Type`). With the
 * commands off every POST is the rpc's, as before NIP-FE; with the rpc off, one typed for it is a 404.
 */
fun Route.relayPosts(
    commands: (suspend (ApplicationCall) -> Unit)?,
    rpc: (suspend (ApplicationCall) -> Unit)?,
) {
    if (commands == null && rpc == null) return
    post("/") {
        val answer = if (commands != null && !call.isNip86Call()) commands else rpc
        if (answer == null) call.respond(HttpStatusCode.NotFound) else answer(call)
    }
}

/** Compared as text: parsing would throw on a malformed header, and a command needs none. */
private fun ApplicationCall.isNip86Call(): Boolean {
    val type = request.headers[HttpHeaders.ContentType] ?: return false
    return type.substringBefore(';').trim().equals(Nip86HttpHandler.CONTENT_TYPE, ignoreCase = true)
}

/** Compressed bytes held back between flushes before they are handed to the socket anyway. */
private const val PENDING_LIMIT = 64 * 1024

internal val NDJSON: ContentType = ContentType.parse("application/x-ndjson")

private suspend fun ApplicationCall.answer(
    handler: HttpRelayHandler,
    settings: HttpRelay,
) {
    // The engine counts characters and a UTF-8 character is up to three bytes, so the handler says how much to read.
    val cap = handler.maxBodyBytes ?: (defaultMaxMessageLength * 3L)
    // The gate first, before the upload: a refused request neither buffers its body nor reaches the NIP-98 check.
    settings.gate.through(settings.clients.of(this), refused = { respondBusy(it, settings.gate) }) {
        val body =
            try {
                withTimeout(settings.bodyTimeoutMs) { receiveBounded(cap) }
                    ?: return@through respondNotice("invalid: the body exceeds $cap bytes", HttpStatusCode.PayloadTooLarge)
            } catch (_: TimeoutCancellationException) {
                response.header(HttpHeaders.Connection, "close")
                return@through respondNotice("invalid: the body did not arrive within ${settings.bodyTimeoutMs / 1000.0}s", HttpStatusCode.RequestTimeout)
            }
        try {
            handler.handle(HttpRelayRequest(request.headers[HttpHeaders.Authorization], body), KtorAnswer(this))
        } catch (_: HttpRelayReaderStalled) {
            // The client stopped reading; its connection is dropped with the answer unfinished.
        }
    }
}

/** The handler's answer, written through Ktor: a single frame with its status, or a 200 stream. */
private class KtorAnswer(
    private val call: ApplicationCall,
) : HttpRelayResponse {
    override suspend fun single(
        status: Int,
        frame: String,
    ) = call.respondFrames(frame, HttpStatusCode.fromValue(status))

    override suspend fun stream(lines: suspend HttpRelayLines.() -> Unit) =
        call.streaming { sink ->
            try {
                sink.lines()
                sink.finish()
            } finally {
                // A reader that stalled, or a cancelled write, still frees the native deflater.
                sink.release()
            }
        }
}

/**
 * The 200 answer. Gzipped here rather than by the Compression plugin, which holds output until its
 * buffer fills: each [LineSink.flush] is a sync flush, so a line compressed is a line delivered.
 */
private suspend fun ApplicationCall.streaming(producer: suspend (LineSink) -> Unit) {
    val gzip = acceptsGzip(request.headers[HttpHeaders.AcceptEncoding])
    suppressCompression()
    if (gzip) response.header(HttpHeaders.ContentEncoding, "gzip")
    response.header(HttpHeaders.Vary, HttpHeaders.AcceptEncoding)
    // A buffering proxy (nginx) would otherwise hold every line until the answer ended.
    response.header("X-Accel-Buffering", "no")
    response.header(HttpHeaders.CacheControl, "no-store")
    respondBytesWriter(NDJSON, HttpStatusCode.OK) { producer(LineSink(this, gzip)) }
}

/** Frames out, one per line, optionally through a gzip stream that is sync-flushed on every [flush]. */
private class LineSink(
    private val out: ByteWriteChannel,
    gzip: Boolean,
) : HttpRelayLines {
    private val pending = ByteArrayOutputStream()
    private val zip = if (gzip) GZIPOutputStream(pending, true) else null

    override suspend fun line(frame: String) {
        val bytes = (frame + "\n").encodeToByteArray()
        if (zip == null) return out.writeFully(bytes)
        zip.write(bytes)
        // Handed on as it grows, so a burst meets the socket's backpressure instead of piling up here.
        if (pending.size() >= PENDING_LIMIT) drainPending()
    }

    override suspend fun flush() {
        zip?.flush()
        drainPending()
        out.flush()
    }

    /** Ends the gzip stream (and frees its native deflater) and sends what is left. */
    suspend fun finish() {
        zip?.close()
        drainPending()
        out.flush()
    }

    /** Frees the deflater of an answer that did not [finish]; idempotent, and writes nothing to the socket. */
    fun release() {
        zip?.close()
    }

    // Taken before the write suspends: a write cancelled mid-way must not send these bytes twice.
    private suspend fun drainPending() {
        if (pending.size() == 0) return
        val bytes = pending.toByteArray()
        pending.reset()
        out.writeFully(bytes)
    }
}

private suspend fun ApplicationCall.respondFrames(
    frame: String,
    status: HttpStatusCode,
) {
    if (status == HttpStatusCode.Unauthorized) response.header(HttpHeaders.WWWAuthenticate, "Nostr")
    if (status == HttpStatusCode.TooManyRequests || status == HttpStatusCode.ServiceUnavailable) response.header(HttpHeaders.RetryAfter, "1")
    response.header(HttpHeaders.CacheControl, "no-store")
    respondText(frame + "\n", NDJSON, status)
}

/** A refusal before any command runs is one NOTICE line, as NIP-FE has it. */
private suspend fun ApplicationCall.respondNotice(
    reason: String,
    status: HttpStatusCode,
) = respondFrames(HttpRelayHandler.notice(reason), status)

private suspend fun ApplicationCall.respondBusy(
    refusal: HttpRelayGate.Refusal,
    gate: HttpRelayGate,
) = when (refusal) {
    HttpRelayGate.Refusal.CLIENT_BUSY -> {
        respondNotice("rate-limited: ${gate.perClient} HTTP commands are already running from this address", HttpStatusCode.TooManyRequests)
    }

    HttpRelayGate.Refusal.RELAY_BUSY -> {
        respondNotice("rate-limited: the relay is running its limit of ${gate.total} HTTP commands", HttpStatusCode.ServiceUnavailable)
    }
}

/** The body, or null when it is over [max]. The +1 read catches a lying Content-Length and chunked uploads. */
internal suspend fun ApplicationCall.receiveBounded(max: Long): ByteArray? {
    if ((request.contentLength() ?: 0) > max) return null
    return receiveChannel().readRemaining(max + 1).readByteArray().takeIf { it.size <= max }
}

/** Unset limits are the defaults', so the body cap and the engine's cap never disagree. */
private val defaultMaxMessageLength: Int =
    com.nosfabrica.vespa.relay.server.config
        .defaultRelayLimits()
        .maxMessageLength ?: 262_144

/**
 * Whether an `Accept-Encoding` admits gzip. A `gzip` entry decides on its own; `*` speaks only for
 * codings the header does not name (RFC 9110 12.5.3).
 */
internal fun acceptsGzip(header: String?): Boolean {
    val weights =
        header
            .orEmpty()
            .split(',')
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .associate { entry ->
                val parts = entry.split(';').map { it.trim() }
                val q =
                    parts
                        .drop(1)
                        .firstOrNull { it.startsWith("q=", ignoreCase = true) }
                        ?.substring(2)
                        ?.toDoubleOrNull() ?: 1.0
                parts[0].lowercase() to q
            }
    val q = weights["gzip"] ?: weights["*"] ?: return false
    return q > 0
}

/**
 * Where a request came from, for the gate: the nearest address in [header] that is not one of
 * [trustedProxies], read right to left while the hop that added it is trusted; the socket's peer
 * when that is not a trusted proxy. An IPv6 client is keyed by its /64, which one subscriber holds.
 */
class ClientAddresses(
    val header: String? = null,
    val trustedProxies: List<Cidr> = emptyList(),
) {
    fun of(call: ApplicationCall): String {
        val peer = call.request.origin.remoteAddress
        val name = header ?: return key(peer)
        if (!trusted(peer)) return key(peer)
        val hops =
            call.request.headers
                .getAll(name)
                .orEmpty()
                .flatMap { it.split(',') }
                .map { it.trim() }
                .filter { it.isNotEmpty() }
        return key(hops.lastOrNull { !trusted(it) } ?: hops.firstOrNull() ?: peer)
    }

    private fun trusted(address: String): Boolean {
        val bytes = Cidr.literalBytes(address) ?: return false
        return trustedProxies.any { it.contains(bytes) }
    }

    private fun key(address: String): String {
        val bytes = Cidr.literalBytes(address)?.takeIf { it.size == 16 } ?: return address
        return bytes.copyOf(8).joinToString("", postfix = "::/64") { "%02x".format(it) }
    }
}

/** An address block, `10.0.0.0/8` or `::1/128`; a bare address is its own block. Literals only, never a hostname. */
class Cidr private constructor(
    private val network: ByteArray,
    private val prefix: Int,
) {
    fun contains(address: String): Boolean = literalBytes(address)?.let(::contains) ?: false

    /** [contains] over an address already parsed, so a caller matching many blocks parses it once. */
    fun contains(bytes: ByteArray): Boolean {
        if (bytes.size != network.size) return false
        val whole = prefix / 8
        for (i in 0 until whole) if (bytes[i] != network[i]) return false
        val rest = prefix % 8
        if (rest == 0) return true
        val mask = (0xFF shl (8 - rest)) and 0xFF
        return (bytes[whole].toInt() and mask) == (network[whole].toInt() and mask)
    }

    override fun toString(): String = "${java.net.InetAddress.getByAddress(network).hostAddress}/$prefix"

    companion object {
        /** [spec] as a block, or null when it is not an address literal with an in-range prefix. */
        fun parse(spec: String): Cidr? {
            val address = spec.substringBefore('/').trim()
            val bytes = literalBytes(address) ?: return null
            val prefix =
                if ('/' in spec) spec.substringAfter('/').trim().toIntOrNull() ?: return null else bytes.size * 8
            if (prefix !in 0..bytes.size * 8) return null
            return Cidr(bytes, prefix)
        }

        /** An address literal's bytes. IPv4 is parsed here, so a malformed one is never sent to DNS as a name. */
        internal fun literalBytes(address: String): ByteArray? {
            val bare = address.removePrefix("[").removeSuffix("]")
            if (':' in bare) {
                return runCatching {
                    java.net.InetAddress
                        .getByName(bare)
                        .address
                }.getOrNull()
            }
            val octets = bare.split('.').map { it.toIntOrNull()?.takeIf { o -> o in 0..255 && it.length <= 3 } ?: return null }
            return if (octets.size == 4) ByteArray(4) { octets[it].toByte() } else null
        }
    }
}
