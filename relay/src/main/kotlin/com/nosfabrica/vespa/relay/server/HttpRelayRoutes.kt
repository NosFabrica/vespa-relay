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
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.io.readByteArray
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.util.zip.GZIPOutputStream

/**
 * The HTTP command endpoints' settings. [origins] are the prefixes a NIP-98 `u` may carry, asked per
 * request because the hidden service can come up after the server did; never derived from the request.
 */
class HttpRelay(
    val gate: HttpRelayGate,
    val deadlineMs: Long,
    val origins: () -> List<String>,
    val clients: ClientAddresses = ClientAddresses(),
) {
    /** The `u` values a token for [path] may carry. */
    fun urlsFor(path: String): List<String> = origins().map { it.trimEnd('/') + path }

    companion object {
        const val DEFAULT_DEADLINE_SECONDS = 30L
    }
}

/**
 * `POST /req`, `/count` and `/event`: one client command per request, answered by the same session,
 * policies and store as the websocket, as the relay's own frames without their subscription id, one
 * per line, ending on the command's answer (EOSE, COUNT, OK, or a refusal). No subscription outlives
 * the request.
 */
fun Route.httpRelayRoutes(
    relay: NostrRelayServer,
    settings: HttpRelay,
) {
    for (command in HttpCommand.entries) {
        post(command.path) { call.answer(relay, settings, command) }
    }
}

/** Frames queued ahead of a slow reader before the answer is cut short, as on the websocket. */
private const val MAX_BUFFERED_FRAMES = 8192

/** How long past the deadline the last line and the stream's end may take; a reader slower than this is dropped. */
private const val TAIL_GRACE_MS = 5_000L

/** Compressed bytes held back between flushes before they are handed to the socket anyway. */
private const val PENDING_LIMIT = 64 * 1024

/** The challenge every connection opens with; an HTTP command proves its key by NIP-98 instead. */
private const val AUTH_VERB = "AUTH"

internal val NDJSON: ContentType = ContentType.parse("application/x-ndjson")

private suspend fun ApplicationCall.answer(
    relay: NostrRelayServer,
    settings: HttpRelay,
    command: HttpCommand,
) {
    val max = relay.limits?.maxMessageLength ?: defaultMaxMessageLength
    val body = receiveBounded(max) ?: return respondClosed("invalid: the body exceeds $max bytes", HttpStatusCode.PayloadTooLarge)
    val frame =
        parseBody(body)?.let(command::frame)
            ?: return respondClosed("invalid: the body is not this command's arguments; see docs/configuration.md")
    // The engine measures the whole frame, so the cap is checked on what it will see.
    if (frame.length > max) return respondClosed("invalid: the command exceeds $max characters", HttpStatusCode.PayloadTooLarge)
    // The gate first: a token verified for a request the gate then refuses would be spent for nothing.
    settings.gate.through(settings.clients.of(this), refused = { respondBusy(it, settings.gate) }) {
        when (val proof = readerOf(settings, command.path, body)) {
            is Proof.Refused -> respondClosed(MachineReadablePrefix.AUTH_REQUIRED.format(proof.reason))
            is Proof.Anonymous -> exchange(relay, settings.deadlineMs, command, null, frame)
            is Proof.Signed -> exchange(relay, settings.deadlineMs, command, proof.pubkey, frame)
        }
    }
}

/**
 * Runs [frame] on its own connection and streams what it sends. The status waits for the first
 * frame, so a refusal is an HTTP error; after that an error can only be a frame.
 */
private suspend fun ApplicationCall.exchange(
    relay: NostrRelayServer,
    deadlineMs: Long,
    command: HttpCommand,
    reader: HexKey?,
    frame: String,
) = coroutineScope {
    val frames = Channel<String>(MAX_BUFFERED_FRAMES)
    val ended = CompletableDeferred<Unit>()
    // Called on the store's coroutines and cannot suspend, so a frame that does not fit ends the answer.
    val send: (String) -> Unit = send@{ raw ->
        if (verbOf(raw) == AUTH_VERB) return@send
        val out = withoutSubId(raw)
        val sent = frames.trySend(out)
        when {
            sent.isClosed -> {}

            sent.isFailure -> {
                frames.close()
                ended.complete(Unit)
            }

            command.ends(out) -> {
                frames.close()
                ended.complete(Unit)
            }
        }
    }
    val session =
        launch {
            relay.serveAs(reader, send) {
                it.receive(frame)
                ended.await()
            }
        }
    val startedNs = System.nanoTime()

    fun remainingMs() = (deadlineMs - (System.nanoTime() - startedNs) / 1_000_000).coerceAtLeast(0)
    try {
        val first = withTimeoutOrNull(deadlineMs) { frames.receiveCatching().getOrNull() }
        val status = first?.let(::statusOf)
        when {
            first == null -> {
                respondClosed("error: no answer within ${deadlineMs / 1000}s", HttpStatusCode.ServiceUnavailable)
            }

            status != HttpStatusCode.OK -> {
                respondFrames(first, status ?: HttpStatusCode.InternalServerError)
            }

            else -> {
                streaming { sink ->
                    // The deadline is read between frames and never interrupts a write, so every line
                    // leaves whole; a reader that stops reading altogether is dropped at the hard stop.
                    withTimeout(remainingMs() + TAIL_GRACE_MS) {
                        when (sink.drain(first, frames, command, startedNs + deadlineMs * 1_000_000)) {
                            Ending.ANSWERED -> {}

                            Ending.DEADLINE -> {
                                sink.write(closedFrame("error: the answer ran past ${deadlineMs / 1000}s"))
                            }

                            Ending.CUT -> {
                                sink.write(closedFrame("error: slow reader, over $MAX_BUFFERED_FRAMES frames waiting"))
                            }
                        }
                        sink.finish()
                    }
                }
            }
        }
    } finally {
        session.cancel()
    }
}

/** How a streamed answer stopped: at its answer frame, at the deadline, or cut because the reader fell behind. */
private enum class Ending { ANSWERED, DEADLINE, CUT }

/**
 * Writes [first] and what follows up to the command's answer, flushing whenever nothing is waiting so
 * a burst leaves as one write. Stops at the answer: a live event queued behind it is not part of it.
 */
private suspend fun LineSink.drain(
    first: String,
    frames: Channel<String>,
    command: HttpCommand,
    deadlineNs: Long,
): Ending {
    var frame = first
    while (true) {
        write(frame)
        if (command.ends(frame)) return Ending.ANSWERED
        frame = frames.tryReceive().getOrNull() ?: run {
            flush()
            val leftMs = (deadlineNs - System.nanoTime()) / 1_000_000
            if (leftMs <= 0) return Ending.DEADLINE
            val next = withTimeoutOrNull(leftMs) { frames.receiveCatching() } ?: return Ending.DEADLINE
            // Closed with no answer frame in it: the send side gave up on this reader.
            next.getOrNull() ?: return Ending.CUT
        }
        if (System.nanoTime() >= deadlineNs) return Ending.DEADLINE
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
) {
    private val pending = ByteArrayOutputStream()
    private val zip = if (gzip) GZIPOutputStream(pending, true) else null

    suspend fun write(frame: String) {
        val bytes = (frame + "\n").encodeToByteArray()
        if (zip == null) return out.writeFully(bytes)
        zip.write(bytes)
        // Handed on as it grows, so a burst meets the socket's backpressure instead of piling up here.
        if (pending.size() >= PENDING_LIMIT) drainPending()
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

private suspend fun ApplicationCall.respondClosed(
    reason: String,
    status: HttpStatusCode = statusFor(reason),
) = respondFrames(closedFrame(reason), status)

private suspend fun ApplicationCall.respondBusy(
    refusal: HttpRelayGate.Refusal,
    gate: HttpRelayGate,
) = when (refusal) {
    HttpRelayGate.Refusal.CLIENT_BUSY -> {
        respondClosed("rate-limited: ${gate.perClient} HTTP commands are already running from this address")
    }

    HttpRelayGate.Refusal.RELAY_BUSY -> {
        respondClosed("rate-limited: the relay is running its limit of ${gate.total} HTTP commands", HttpStatusCode.ServiceUnavailable)
    }
}

/** The body, or null when it is over [max]. The +1 read catches a lying Content-Length and chunked uploads. */
internal suspend fun ApplicationCall.receiveBounded(max: Int): ByteArray? {
    if ((request.contentLength() ?: 0) > max) return null
    return receiveChannel().readRemaining(max + 1L).readByteArray().takeIf { it.size <= max }
}

/** Who a request acts as. */
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
 * at the .onion verifies there too. The token must bind the body's hash: it authorizes one command,
 * and may repeat it inside its window.
 * Another scheme (a proxy's Basic auth, a client's Bearer) is not addressed to us and is ignored.
 */
private suspend fun ApplicationCall.readerOf(
    settings: HttpRelay,
    path: String,
    body: ByteArray,
): Proof {
    val header = request.headers[HttpHeaders.Authorization]?.trim().orEmpty()
    if (!header.regionMatches(0, Nip98AuthVerifier.SCHEME, 0, Nip98AuthVerifier.SCHEME.length, ignoreCase = true)) return Proof.Anonymous
    val token = Nip98AuthVerifier.SCHEME + header.substring(Nip98AuthVerifier.SCHEME.length).trim()
    val accepted = settings.urlsFor(path)
    val url = claimedUrl(token)?.takeIf { it in accepted } ?: accepted.firstOrNull() ?: return Proof.Refused("this relay names no url to sign")
    // A fresh verifier per request, so a token is not single-use: a request may land on any
    // instance, which one process's memory of spent tokens cannot follow, and the body's hash already
    // limits a captured token to the one command it signs, inside its window. Quartz's NIP-FE
    // handler does the same; the verifier itself stays single-use for the admin rpc.
    return when (val r = Nip98AuthVerifier().verify(token, "POST", url, body)) {
        is Nip98AuthVerifier.Result.Verified -> Proof.Signed(r.pubkey)
        is Nip98AuthVerifier.Result.Malformed -> Proof.Refused("NIP-98 ${r.reason}")
        is Nip98AuthVerifier.Result.Missing -> Proof.Anonymous
    }
}

/** The `u` tag of a NIP-98 token, or null when it does not decode; the verifier reports why. */
private fun claimedUrl(token: String): String? =
    runCatching {
        val json = Base64.getDecoder().decode(token.removePrefix(Nip98AuthVerifier.SCHEME).trim()).decodeToString()
        OptimizedJsonMapper
            .fromJson(json)
            .tags
            .firstOrNull { it.size > 1 && it[0] == "u" }
            ?.get(1)
    }.getOrNull()

/** Unset limits are the defaults', so the body cap and the engine's cap never disagree. */
private val defaultMaxMessageLength: Int =
    com.nosfabrica.vespa.relay.server.config
        .defaultRelayLimits()
        .maxMessageLength ?: 262_144

/**
 * Where a request came from, for the gate: the socket's peer, unless that peer is one of
 * [trustedProxies], in which case the last entry of [header] across every line of it, the one that
 * proxy appended. A header from anyone else is the client's own claim and is not read.
 */
class ClientAddresses(
    val header: String? = null,
    val trustedProxies: List<Cidr> = emptyList(),
) {
    fun of(call: ApplicationCall): String {
        val peer = call.request.origin.remoteAddress
        val name = header ?: return peer
        if (trustedProxies.none { it.contains(peer) }) return peer
        return call.request.headers
            .getAll(name)
            .orEmpty()
            .joinToString(",")
            .substringAfterLast(',')
            .trim()
            .ifEmpty { peer }
    }
}

/** An address block, `10.0.0.0/8` or `::1/128`; a bare address is its own block. Literals only, never a hostname. */
class Cidr private constructor(
    private val network: ByteArray,
    private val prefix: Int,
) {
    fun contains(address: String): Boolean {
        val bytes = literalBytes(address) ?: return false
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
        private fun literalBytes(address: String): ByteArray? {
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
