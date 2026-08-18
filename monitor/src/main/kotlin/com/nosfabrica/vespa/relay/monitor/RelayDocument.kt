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
 * THE RELAY'S OWN ACCOUNT OF ITSELF — its NIP-11 document, for the handful of
 * facts nothing can be measured against.
 *
 * The fitness pass is emphatic that no VERDICT is read off one: documents
 * routinely disagree with practice, and a relay claiming `auth_required: false`
 * while refusing our key is exactly the case its `auth-refused` grade exists
 * for. That rule is unchanged. This is the other half of the same sentence —
 * NIP-66 records carry descriptive fields too, and for `software`,
 * `supported_nips` and the payment/pow/writes limits there is no instrument at
 * all. A relay's software name is a claim it makes about itself; the only
 * honest options are to publish the claim as a claim or to publish nothing, and
 * publishing nothing is what left our records the least informative on the
 * network.
 *
 * NIP-66 says the same thing from its own side: *"Information corresponding to
 * field in a relay's NIP 11 document MAY contradict actual values if monitors
 * find that a different policy is implemented than is advertised."* The
 * contradiction is expected and allowed, so the measured half of
 * [RelayFacts.requirements] overwrites this one where the two disagree.
 *
 * ## The fetch
 *
 * Plain HTTP GET with `Accept: application/nostr+json` at the relay's own
 * address — `wss://` becomes `https://`, `ws://` becomes `http://`, path and
 * port kept, since a relay served under a path serves its document there too.
 *
 * THROUGH THE SAME TRANSPORT THE DIAL WOULD USE, which is why [clientFor] is a
 * function of the url rather than one client: a `.onion` document has to be
 * fetched inside the Tor circuit, and asking the direct client for one would
 * both fail and leak the hidden service to the local resolver — the rule
 * `TorTransport` exists to hold.
 *
 * Failure is NOT news. Most relays serve no document at all, and a monitor that
 * logged every miss would print a line per url per sweep. A null is "we did not
 * learn this", which is exactly what the absent tags then say.
 */
class RelayDocument(
    private val clientFor: (NormalizedRelayUrl) -> OkHttpClient,
    private val timeoutSeconds: Long = DEFAULT_TIMEOUT_SECONDS,
) {
    /**
     * What one document says, in the shape [RelayFacts] publishes.
     *
     * Only the fields with somewhere to go. A NIP-11 document also carries a
     * name, a description, an icon, fee schedules and retention policy, none of
     * which NIP-66 has a tag for — and inventing tags for them is the mistake
     * this whole change is undoing.
     */
    data class Doc(
        val software: String? = null,
        val version: String? = null,
        val supportedNips: List<Int> = emptyList(),
        /**
         * NIP-11 `limitation` keys in NIP-66's spelling, negations included.
         *
         * A limitation the document does not mention produces NO tag, rather
         * than a negation: `!payment` asserts the relay is free, and a document
         * that is silent has not asserted it. Only `auth_required`,
         * `payment_required`, `min_pow_difficulty` and `restricted_writes` are
         * read, because those are the four keys NIP-66 names.
         */
        val requirements: List<String> = emptyList(),
    )

    /**
     * What one ask learned: the document, and how long the relay took to accept
     * a connection.
     *
     * The two are independent, and both halves matter on their own. Most relays
     * serve no document — so a null [doc] beside a real [openMs] is the ordinary
     * case, and it is still worth publishing, because "this host completes a
     * handshake in 40ms" is exactly what a `rtt-open` reader wants and it does
     * not depend on the relay describing itself.
     */
    data class Reading(
        val doc: Doc? = null,
        val openMs: Long? = null,
    )

    /**
     * Ask [url] for its document, and time the connection that carries the ask.
     *
     * **`rtt-open` IS THIS CONNECT, and that is not a stand-in for the
     * websocket's.** The document is served at the relay's own host, port and
     * TLS configuration — the websocket handshake's first two legs are the same
     * two legs — so timing `connectStart` to `connectEnd` here measures the same
     * thing an explicit open probe would, on a connection this pass was opening
     * anyway. The alternative was a second TCP connect per url per sweep to
     * learn a number we were already in a position to observe.
     *
     * A REUSED connection reports null rather than zero. OkHttp fires no connect
     * event when it takes one from the pool, and publishing the 0 that a naive
     * stopwatch would produce is how a relay on the other side of the world ends
     * up advertised as the fastest in the store.
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
                        // A relay with no document answers this with its own
                        // homepage, a 404 or a redirect to one — all of which
                        // parse to nothing below rather than needing a status
                        // check here. What must not happen is reading a
                        // multi-megabyte body from a url that is not a relay,
                        // hence `peekBody`'s bound rather than `string()`.
                        if (response.isSuccessful) response.peekBody(MAX_DOCUMENT_BYTES).string() else null
                    }
                }.getOrNull()
            }
        // The connect is kept even when the body was not: a 404 from a relay
        // that has no document still proves the handshake, and that is the
        // half of this ask that never fails to be informative.
        return Reading(doc = body?.let { parse(it) }, openMs = timer.openMs)
    }

    /**
     * The stopwatch, as OkHttp's own callback rather than a wrapper around the
     * call: `connectStart`/`connectEnd` bracket exactly the TCP handshake and
     * the TLS one, where timing the call would fold in DNS, our own dispatcher
     * queue and the relay's time to serve a body.
     *
     * One call, one listener instance, so there is nothing to key by url and no
     * state shared between the sweep's concurrent asks.
     */
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

        /**
         * Ten seconds. The document is a nice-to-have on a pass whose real work
         * is the dial, so it may not become the thing that paces a sweep of the
         * whole corpus.
         */
        const val DEFAULT_TIMEOUT_SECONDS = 10L

        /**
         * A NIP-11 document is a few kilobytes. Anything larger is a url that
         * answered with something else — a homepage, an error page, a file
         * server — and reading it in full is the failure mode a monitor dialling
         * sixteen thousand strangers cannot afford.
         */
        const val MAX_DOCUMENT_BYTES = 64L * 1024

        /**
         * The relay's document address: same host, same port, same path, over
         * the http scheme that pairs with its websocket one.
         *
         * Null for a url that has no host to ask, which is the same guard the
         * TCP pre-probe makes for the same reason.
         */
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

        /**
         * Read a document that may be anything at all.
         *
         * Every field is optional and every field is somebody else's json, so
         * each one is taken independently: a `supported_nips` array holding a
         * string must not cost us the `software` string beside it. This is the
         * half worth testing, so it is pure and static.
         */
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
            // A document that parsed but said none of the four things this
            // reads is not a document as far as the record is concerned.
            return doc.takeIf {
                it.software != null || it.supportedNips.isNotEmpty() || it.requirements.isNotEmpty()
            }
        }

        /**
         * NIP-11's `limitation` block in NIP-66's vocabulary.
         *
         * `min_pow_difficulty` is a NUMBER where the other three are booleans,
         * and the requirement is whether it is above zero — a relay stating `0`
         * is stating that it asks for no work, which is `!pow` rather than no
         * claim at all.
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
