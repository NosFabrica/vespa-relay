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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.OkHttpClient
import okhttp3.Request
import java.util.concurrent.TimeUnit

/**
 * The relay's NIP-11 document, for the facts nothing can be measured against
 * (`software`, `supported_nips`, the limitation flags). No verdict is read off
 * it, and the measured half of [RelayFacts.requirements] overwrites it where
 * the two disagree. Fetched through the same transport the dial would use, so
 * a `.onion` document stays inside the circuit. A miss is not news: most
 * relays serve no document.
 */
class RelayDocument(
    private val clientFor: (NormalizedRelayUrl) -> OkHttpClient,
    private val timeoutSeconds: Long = DEFAULT_TIMEOUT_SECONDS,
) {
    /** What one document says, restricted to the fields NIP-66 has a tag for. */
    data class Doc(
        val software: String? = null,
        val version: String? = null,
        val supportedNips: List<Int> = emptyList(),
        /** NIP-11 `limitation` keys in NIP-66's spelling. A key the document omits produces no tag, not a negation. */
        val requirements: List<String> = emptyList(),
    )

    /** What one ask learned. A null [doc] beside a real [openMs] is the ordinary case and still worth publishing. */
    data class Reading(
        val doc: Doc? = null,
        val openMs: Long? = null,
    )

    /**
     * Asks [url] for its document and times the connection that carries the
     * ask; the document shares the websocket's host, port and TLS, so this
     * connect is `rtt-open`. A pooled connection reports null, never zero.
     */
    suspend fun read(url: NormalizedRelayUrl): Reading {
        val address = httpAddressOf(url) ?: return Reading()
        val timer = ConnectTimer()
        val body =
            withContext(Dispatchers.IO) {
                runCatching {
                    val client =
                        clientFor(url)
                            .newBuilder()
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
                    client.newCall(request).execute().use { response ->
                        // A homepage or an error page parses to nothing below; the bound keeps a non-relay's body small.
                        if (response.isSuccessful) response.peekBody(MAX_DOCUMENT_BYTES).string() else null
                    }
                }.getOrNull()
            }
        // The connect is kept without a body: a 404 still proves the handshake.
        return Reading(doc = body?.let { parse(it) }, openMs = timer.openMs)
    }

    /** Brackets the TCP and TLS handshake alone; timing the call would fold in DNS and the body. One instance per call. */
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

        /** Short, so the document never paces a sweep whose real work is the dial. */
        const val DEFAULT_TIMEOUT_SECONDS = 10L

        /** A NIP-11 document is a few kilobytes; anything larger is a url that answered with something else. */
        const val MAX_DOCUMENT_BYTES = 64L * 1024

        /** The document address: same host, port and path, over the http scheme that pairs with the websocket one. Null without a host. */
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

        /** Reads a document that may be anything at all; each field is taken independently of the others. */
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

        /** NIP-11's `limitation` block in NIP-66's vocabulary. `min_pow_difficulty = 0` is `!pow`, not silence. */
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
