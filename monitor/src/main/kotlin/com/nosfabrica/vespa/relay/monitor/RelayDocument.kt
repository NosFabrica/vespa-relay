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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.peers.RelayFacts
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * The relay's NIP-11 document, for the facts nothing can be measured against. No verdict is
 * read off it, and the measured half of [RelayFacts.requirements] overwrites it where they
 * disagree. Fetched through the transport the dial would use, so a `.onion` stays in the circuit.
 */
class RelayDocument(
    private val clientFor: (NormalizedRelayUrl) -> OkHttpClient,
    private val timeoutSeconds: Long = DEFAULT_TIMEOUT_SECONDS,
    /** Documents in flight at once: the passes' dials, each of which asks for one. */
    concurrency: Int = AliasFolding.DEFAULT_DIAL_CONCURRENCY,
) {
    /** Its own, so a fetch neither waits behind the mirror's live tails on the shared client nor holds a slot they need. */
    private val dispatcher =
        Dispatcher().apply {
            maxRequests = concurrency
            maxRequestsPerHost = minOf(concurrency, DOCUMENTS_PER_HOST)
        }

    /** What one document says, restricted to the fields NIP-66 has a tag for. */
    data class Doc(
        val software: String? = null,
        val version: String? = null,
        val supportedNips: List<Int> = emptyList(),
        /** NIP-11 `limitation` keys in NIP-66's spelling. An omitted key produces no tag, not a negation. */
        val requirements: List<String> = emptyList(),
    )

    /** What one ask learned. A null [doc] beside a real [openMs] is the ordinary case. */
    data class Reading(
        val doc: Doc? = null,
        val openMs: Long? = null,
    )

    /**
     * Asks [url] for its document and times the connect that carries it, which shares the
     * websocket's host, port and TLS and so is `rtt-open`. A pooled connection reports null.
     */
    suspend fun read(url: NormalizedRelayUrl): Reading {
        val address = httpAddressOf(url) ?: return Reading()
        val timer = ConnectTimer()
        val body =
            try {
                // Over the queue as well as the call: OkHttp's own clock starts only once a call leaves it.
                withTimeoutOrNull(timeoutSeconds * 1000) { fetch(url, address, timer) }
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                null
            }
        // The connect is kept without a body: a 404 still proves the handshake.
        return Reading(doc = body?.let { parse(it) }, openMs = timer.openMs)
    }

    /** The document's body, or null for a status that is not success. Cancelling the caller cancels the call. */
    private suspend fun fetch(
        url: NormalizedRelayUrl,
        address: String,
        timer: ConnectTimer,
    ): String? {
        val client =
            clientFor(url)
                .newBuilder()
                .dispatcher(dispatcher)
                .callTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .eventListener(timer)
                .build()
        val request =
            Request
                .Builder()
                .url(address)
                .header("Accept", NIP11_CONTENT_TYPE)
                .get()
                .build()
        val call = client.newCall(request)
        return suspendCancellableCoroutine { waiting ->
            waiting.invokeOnCancellation { call.cancel() }
            call.enqueue(
                object : Callback {
                    override fun onFailure(
                        call: Call,
                        e: IOException,
                    ) = waiting.resumeWithException(e)

                    override fun onResponse(
                        call: Call,
                        response: Response,
                    ) {
                        val body =
                            try {
                                response.use { if (it.isSuccessful) it.peekBody(MAX_DOCUMENT_BYTES).string() else null }
                            } catch (e: IOException) {
                                waiting.resumeWithException(e)
                                return
                            }
                        waiting.resume(body)
                    }
                },
            )
        }
    }

    /** Brackets the TCP and TLS handshake alone. One instance per call. */
    private class ConnectTimer : okhttp3.EventListener() {
        @Volatile private var startedAt: Long? = null

        @Volatile var openMs: Long? = null
            private set

        override fun connectStart(
            call: okhttp3.Call,
            inetSocketAddress: java.net.InetSocketAddress,
            proxy: java.net.Proxy,
        ) {
            startedAt = System.nanoTime()
        }

        override fun connectEnd(
            call: okhttp3.Call,
            inetSocketAddress: java.net.InetSocketAddress,
            proxy: java.net.Proxy,
            protocol: okhttp3.Protocol?,
        ) {
            startedAt?.let { openMs = (System.nanoTime() - it) / 1_000_000 }
        }
    }

    companion object {
        const val NIP11_CONTENT_TYPE = "application/nostr+json"

        const val DEFAULT_TIMEOUT_SECONDS = 10L

        /** Documents asked of one host at once; a host serving many paths is still one server. */
        const val DOCUMENTS_PER_HOST = 4

        /** Anything larger is a url that answered with something other than a document. */
        const val MAX_DOCUMENT_BYTES = 64L * 1024

        /** The document address: same host, port and path over the paired http scheme. Null without a host. */
        fun httpAddressOf(url: NormalizedRelayUrl): String? {
            val raw = url.url
            val address =
                when {
                    raw.startsWith("wss://", ignoreCase = true) -> "https://" + raw.substring(6)
                    raw.startsWith("ws://", ignoreCase = true) -> "http://" + raw.substring(5)
                    else -> return null
                }
            return address.takeIf { runCatching { java.net.URI(it).host }.getOrNull()?.isNotBlank() == true }
        }

        /** Reads a document that may be anything at all; each field is taken independently. */
        fun parse(body: String): Doc? {
            val root = runCatching { Json.parseToJsonElement(body).jsonObject }.getOrNull() ?: return null
            val doc =
                Doc(
                    software = root.string("software"),
                    version = root.string("version"),
                    supportedNips =
                        runCatching {
                            root["supported_nips"]?.jsonArray.orEmpty().mapNotNull { it.jsonPrimitive.intOrNull }
                        }.getOrDefault(emptyList()),
                    requirements = requirementsOf(root),
                )
            // A document that says none of the things this reads is no document to the record.
            return doc.takeIf {
                it.software != null || it.supportedNips.isNotEmpty() || it.requirements.isNotEmpty()
            }
        }

        /**
         * NIP-11's `limitation` block in NIP-66's vocabulary. `min_pow_difficulty = 0` is `!pow`,
         * not silence.
         */
        private fun requirementsOf(root: JsonObject): List<String> {
            val limitation = runCatching { root["limitation"]?.jsonObject }.getOrNull() ?: return emptyList()
            return buildList {
                limitation.boolean("auth_required")?.let { add(RelayFacts.requirement(RelayFacts.REQUIREMENT_AUTH, it)) }
                limitation.boolean("payment_required")?.let { add(RelayFacts.requirement(RelayFacts.REQUIREMENT_PAYMENT, it)) }
                limitation.boolean("restricted_writes")?.let { add(RelayFacts.requirement(RelayFacts.REQUIREMENT_WRITES, it)) }
                limitation.int("min_pow_difficulty")?.let { add(RelayFacts.requirement(RelayFacts.REQUIREMENT_POW, it > 0)) }
            }
        }

        private fun JsonObject.string(key: String): String? =
            runCatching {
                (this[key] as? JsonPrimitive)
                    ?.takeIf { it.isString }
                    ?.content
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
            }.getOrNull()

        private fun JsonObject.boolean(key: String): Boolean? = runCatching { (this[key] as? JsonPrimitive)?.booleanOrNull }.getOrNull()

        private fun JsonObject.int(key: String): Int? = runCatching { (this[key] as? JsonPrimitive)?.intOrNull }.getOrNull()
    }
}
