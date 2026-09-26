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

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.OptimizedJsonMapper
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix
import com.vitorpamplona.quartz.nip98HttpAuth.Nip98AuthVerifier
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.http.content.suppressCompression
import io.ktor.server.plugins.origin
import io.ktor.server.request.contentLength
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.header
import io.ktor.server.response.respondBytesWriter
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.post
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readRemaining
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.io.readByteArray
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.GZIPOutputStream

/**
 * The HTTP read endpoints' settings. [origins] are the prefixes a NIP-98 `u` may carry, asked per
 * request because the hidden service can come up after the server did; never derived from the request.
 */
class HttpReads(
    val gate: HttpReadGate,
    val deadlineMs: Long,
    val origins: () -> List<String>,
    // The header a fronting proxy names the client in; null keys the gate on the socket's peer.
    val clientHeader: String? = null,
    // Its own replay cache: public reads must not be able to evict the admin rpc's.
    val verifier: Nip98AuthVerifier = Nip98AuthVerifier(),
) {
    /** The `u` values a token for [path] may carry. */
    fun urlsFor(path: String): List<String> = origins().map { it.trimEnd('/') + path }

    companion object {
        const val DEFAULT_DEADLINE_SECONDS = 30L
    }
}

/**
 * `POST /req` and `POST /count`: one REQ or COUNT without a socket, answered by the same session,
 * policies and store as the websocket. The body is the filters, one object or an array of them; the
 * answer is the relay's NIP-01 frames, one per line, ending at EOSE (or COUNT, or CLOSED) with no live tail.
 */
fun Route.httpReadRoutes(
    relay: NostrRelayServer,
    reads: HttpReads,
) {
    for (ask in HttpAsk.entries) {
        post(ask.path) { call.answer(relay, reads, ask) }
    }
}

/** The two reads HTTP carries, and the frame each one's answer ends on. */
internal enum class HttpAsk(
    val verb: String,
    val path: String,
    private val answerFrame: String,
) {
    REQ("REQ", "/req", """["EOSE""""),
    COUNT("COUNT", "/count", """["COUNT""""),
    ;

    /** Whether [frame] is the last one this ask will get. NOTICE is a command that never reached a handler. */
    fun ends(frame: String): Boolean = frame.startsWith(answerFrame) || frame.startsWith(CLOSED_FRAME) || frame.startsWith(NOTICE_FRAME)
}

/** The subscription id every HTTP read runs under; each request is its own connection. */
internal const val HTTP_SUB_ID = "http"

private const val CLOSED_FRAME = """["CLOSED""""
private const val NOTICE_FRAME = """["NOTICE""""

/** The challenge every connection opens with; an HTTP read proves its key by NIP-98 instead. */
private const val AUTH_FRAME = """["AUTH""""

/** Frames queued ahead of a slow reader before the answer is cut short, as on the websocket. */
private const val MAX_BUFFERED_FRAMES = 8192

/** The body cap when the relay configures no message length. */
private const val DEFAULT_MAX_BODY_BYTES = 262_144

internal val NDJSON: ContentType = ContentType.parse("application/x-ndjson")

private suspend fun ApplicationCall.answer(
    relay: NostrRelayServer,
    reads: HttpReads,
    ask: HttpAsk,
) {
    val max = relay.limits?.maxMessageLength ?: DEFAULT_MAX_BODY_BYTES
    val body = receiveBounded(max) ?: return respondClosed("invalid: the filters exceed $max bytes", HttpStatusCode.PayloadTooLarge)
    val filters = parseFilters(body) ?: return respondClosed("invalid: the body must be a JSON filter object or an array of them")
    val reader =
        when (val proof = readerOf(reads, ask.path, body)) {
            is Proof.Anonymous -> null
            is Proof.Signed -> proof.pubkey
            is Proof.Refused -> return respondClosed(MachineReadablePrefix.AUTH_REQUIRED.format(proof.reason))
        }
    val command = JsonArray(listOf(JsonPrimitive(ask.verb), JsonPrimitive(HTTP_SUB_ID)) + filters).toString()
    reads.gate.through(clientOf(reads.clientHeader), refused = { respondBusy(it, reads.gate) }) {
        exchange(relay, reads.deadlineMs, ask, reader, command)
    }
}

/**
 * Runs [command] on its own connection and streams what it sends. The status waits for the first
 * frame, so a refusal before any event is an HTTP error; after that an error can only be a frame.
 */
private suspend fun ApplicationCall.exchange(
    relay: NostrRelayServer,
    deadlineMs: Long,
    ask: HttpAsk,
    reader: HexKey?,
    command: String,
) = coroutineScope {
    val frames = Channel<String>(MAX_BUFFERED_FRAMES)
    val ended = CompletableDeferred<Unit>()
    val overflowed = AtomicBoolean(false)
    // Called on the store's coroutines and cannot suspend, so a frame that does not fit ends the answer.
    val send: (String) -> Unit = send@{ frame ->
        if (frame.startsWith(AUTH_FRAME)) return@send
        val sent = frames.trySend(frame)
        when {
            sent.isClosed -> {}

            sent.isFailure -> {
                overflowed.set(true)
                frames.close()
                ended.complete(Unit)
            }

            ask.ends(frame) -> {
                frames.close()
                ended.complete(Unit)
            }
        }
    }
    val session =
        launch {
            relay.serveAs(reader, send) {
                it.receive(command)
                ended.await()
            }
        }
    val startedNs = System.nanoTime()

    fun remainingMs() = deadlineMs - (System.nanoTime() - startedNs) / 1_000_000
    try {
        val first = withTimeoutOrNull(deadlineMs) { frames.receiveCatching().getOrNull() }
        when {
            first == null -> {
                respondClosed("error: no answer within ${deadlineMs / 1000}s", HttpStatusCode.ServiceUnavailable)
            }

            first.startsWith(NOTICE_FRAME) -> {
                respondFrames(first, HttpStatusCode.BadRequest)
            }

            first.startsWith(CLOSED_FRAME) -> {
                respondFrames(first, statusFor(closedReason(first)))
            }

            else -> {
                streaming {
                    val drained = withTimeoutOrNull(remainingMs().coerceAtLeast(0)) { drain(first, frames) }
                    when {
                        drained == null -> write(closedFrame("error: the answer ran past ${deadlineMs / 1000}s"))
                        overflowed.get() -> write(closedFrame("error: slow reader, over $MAX_BUFFERED_FRAMES frames waiting"))
                    }
                }
            }
        }
    } finally {
        session.cancel()
    }
}

/** Writes [first] and every frame after it, flushing whenever none is waiting so a burst leaves as one write. */
private suspend fun LineSink.drain(
    first: String,
    frames: Channel<String>,
) {
    write(first)
    if (frames.isEmpty) flush()
    for (frame in frames) {
        write(frame)
        if (frames.isEmpty) flush()
    }
}

/**
 * The 200 answer. Gzipped here rather than by the Compression plugin, which holds output until its
 * buffer fills: each [LineSink.flush] is a sync flush, so a line compressed is a line delivered.
 */
private suspend fun ApplicationCall.streaming(producer: suspend LineSink.() -> Unit) {
    val gzip = acceptsGzip(request.headers[HttpHeaders.AcceptEncoding])
    suppressCompression()
    if (gzip) response.header(HttpHeaders.ContentEncoding, "gzip")
    response.header(HttpHeaders.Vary, HttpHeaders.AcceptEncoding)
    // A buffering proxy (nginx) would otherwise hold every line until the answer ended.
    response.header("X-Accel-Buffering", "no")
    response.header(HttpHeaders.CacheControl, "no-store")
    respondBytesWriter(NDJSON, HttpStatusCode.OK) {
        val sink = LineSink(this, gzip)
        try {
            sink.producer()
        } finally {
            sink.finish()
        }
    }
}

/** Frames out, one per line, optionally through a gzip stream that is sync-flushed on every [flush]. */
private class LineSink(
    private val out: ByteWriteChannel,
    gzip: Boolean,
) {
    private val pending = ByteArrayOutputStream()
    private val zip = if (gzip) GZIPOutputStream(pending, true) else null

    suspend fun write(frame: String) {
        val bytes = (frame + "\n").encodeToByteArray()
        if (zip == null) out.writeFully(bytes) else zip.write(bytes)
    }

    suspend fun flush() {
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

    private suspend fun drainPending() {
        if (pending.size() == 0) return
        out.writeFully(pending.toByteArray())
        pending.reset()
    }
}

/** Whether an `Accept-Encoding` admits gzip: named, or by `*`, and not at `q=0`. */
internal fun acceptsGzip(header: String?): Boolean =
    header.orEmpty().split(',').any { entry ->
        val parts = entry.split(';').map { it.trim() }
        val q =
            parts
                .drop(1)
                .firstOrNull { it.startsWith("q=") }
                ?.removePrefix("q=")
                ?.toDoubleOrNull() ?: 1.0
        (parts[0].equals("gzip", ignoreCase = true) || parts[0] == "*") && q > 0
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

private suspend fun ApplicationCall.respondClosed(
    reason: String,
    status: HttpStatusCode = statusFor(reason),
) = respondFrames(closedFrame(reason), status)

private suspend fun ApplicationCall.respondBusy(
    refusal: HttpReadGate.Refusal,
    gate: HttpReadGate,
) = when (refusal) {
    HttpReadGate.Refusal.CLIENT_BUSY -> {
        respondClosed("rate-limited: ${gate.perClient} HTTP reads are already running from this address")
    }

    HttpReadGate.Refusal.RELAY_BUSY -> {
        respondClosed("rate-limited: the relay is running its limit of ${gate.total} HTTP reads", HttpStatusCode.ServiceUnavailable)
    }
}

/** The HTTP status a NIP-01 machine-readable prefix stands for. */
internal fun statusFor(reason: String): HttpStatusCode =
    when (reason.substringBefore(':', "")) {
        "auth-required" -> HttpStatusCode.Unauthorized
        "restricted", "blocked" -> HttpStatusCode.Forbidden
        "rate-limited" -> HttpStatusCode.TooManyRequests
        "error" -> HttpStatusCode.InternalServerError
        else -> HttpStatusCode.BadRequest
    }

internal fun closedFrame(reason: String): String = JsonArray(listOf(JsonPrimitive("CLOSED"), JsonPrimitive(HTTP_SUB_ID), JsonPrimitive(reason))).toString()

private fun closedReason(frame: String): String = runCatching { (Json.parseToJsonElement(frame) as JsonArray)[2].let { (it as JsonPrimitive).content } }.getOrDefault("")

/** The body, or null when it is over [max]. The +1 read catches a lying Content-Length and chunked uploads. */
private suspend fun ApplicationCall.receiveBounded(max: Int): ByteArray? {
    if ((request.contentLength() ?: 0) > max) return null
    return receiveChannel().readRemaining(max + 1L).readByteArray().takeIf { it.size <= max }
}

/** One filter object or a non-empty array of them, re-serialized so the body can only ever be filters. */
internal fun parseFilters(body: ByteArray): List<JsonObject>? {
    val parsed =
        try {
            Json.parseToJsonElement(body.decodeToString())
        } catch (_: SerializationException) {
            return null
        }
    return when (parsed) {
        is JsonObject -> listOf(parsed)
        is JsonArray -> parsed.takeIf { it.isNotEmpty() && it.all { f -> f is JsonObject } }?.map { it as JsonObject }
        else -> null
    }
}

/** Who a request reads as. */
private sealed interface Proof {
    data object Anonymous : Proof

    class Signed(
        val pubkey: HexKey,
    ) : Proof

    class Refused(
        val reason: String,
    ) : Proof
}

/**
 * A NIP-98 header, checked against the address it names when that is one of ours, so a token signed
 * at the .onion verifies there too. The token must bind the body's hash: it authorizes one query.
 */
private suspend fun ApplicationCall.readerOf(
    reads: HttpReads,
    path: String,
    body: ByteArray,
): Proof {
    val header = request.headers[HttpHeaders.Authorization]?.takeIf { it.isNotBlank() } ?: return Proof.Anonymous
    val accepted = reads.urlsFor(path)
    val url = claimedUrl(header)?.takeIf { it in accepted } ?: accepted.firstOrNull() ?: return Proof.Refused("this relay names no url to sign")
    return when (val r = reads.verifier.verify(header, "POST", url, body)) {
        is Nip98AuthVerifier.Result.Verified -> Proof.Signed(r.pubkey)
        is Nip98AuthVerifier.Result.Malformed -> Proof.Refused("NIP-98 ${r.reason}")
        is Nip98AuthVerifier.Result.Missing -> Proof.Anonymous
    }
}

/** The `u` tag of a NIP-98 header, or null when it does not decode; the verifier reports why. */
private fun claimedUrl(header: String): String? =
    runCatching {
        val json = Base64.getDecoder().decode(header.removePrefix(Nip98AuthVerifier.SCHEME).trim()).decodeToString()
        OptimizedJsonMapper
            .fromJson(json)
            .tags
            .firstOrNull { it.size > 1 && it[0] == "u" }
            ?.get(1)
    }.getOrNull()

/** The address the gate counts this request against: the proxy's last hop when one is named, else the socket's peer. */
private fun ApplicationCall.clientOf(header: String?): String =
    header
        ?.let { request.headers[it] }
        ?.substringAfterLast(',')
        ?.trim()
        ?.takeIf { it.isNotEmpty() }
        ?: request.origin.remoteAddress
