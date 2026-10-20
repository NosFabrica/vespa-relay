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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.util.strictChoice
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/** `local` writes the store from this process; `relay` forwards every write to the serving relay. */
const val SYNC_INGEST_ENV = "SYNC_INGEST"

/** The relay's websocket for [SYNC_INGEST_ENV]`=relay`; defaults to `RELAY_URL`. */
const val SYNC_INGEST_URL_ENV = "SYNC_INGEST_URL"

/**
 * Where this process's writes go. [dial] is null for `local`, the store itself; set, the sync
 * opens its store read-only and every write (ingest's batches, the monitor's verdicts, the
 * retraction audit's deletes) goes to the relay at [dial] as a client signed in with the relay's
 * own key, so the relay is the only process that writes the index.
 */
class IngestTarget(
    val dial: NormalizedRelayUrl?,
) {
    val viaRelay: Boolean get() = dial != null
}

/**
 * [SYNC_INGEST_ENV] and [SYNC_INGEST_URL_ENV], strictly: a value that is not `local` or `relay`,
 * or, in relay mode, a url that does not parse, stops the boot. [relayUrl] is `RELAY_URL`, the
 * default dial.
 */
fun ingestTargetFromEnv(
    env: Map<String, String>,
    relayUrl: NormalizedRelayUrl,
): IngestTarget {
    val mode = env.strictChoice(SYNC_INGEST_ENV, setOf("local", "relay")) ?: "local"
    val raw = env[SYNC_INGEST_URL_ENV]?.trim()?.takeIf { it.isNotEmpty() }
    // Not refused when local: compose sets the url for both modes, so the switch alone flips them.
    if (mode == "local") return IngestTarget(null)
    val dial =
        raw?.let {
            RelayUrlNormalizer.normalizeOrNull(it) ?: error("$SYNC_INGEST_URL_ENV='$it' is not a relay websocket url (ws://relay:7777).")
        } ?: relayUrl
    return IngestTarget(dial)
}

/**
 * The relay's own pubkey as its NIP-11 document states it (`self`), read over the http(s) twin of
 * [dial]; null when the relay does not answer or states none. The relay puts a socket on its
 * mirror lane only when it signs in as this key, so a sync holding a different `RELAY_NSEC`
 * would write everything through the publishers' lane and never know.
 */
fun relaySelfPubkey(dial: NormalizedRelayUrl): String? =
    try {
        val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
        val request =
            HttpRequest
                .newBuilder(URI.create(dial.url.replaceFirst("ws", "http")))
                .header("Accept", "application/nostr+json")
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) {
            null
        } else {
            Json
                .parseToJsonElement(response.body())
                .jsonObject["self"]
                ?.jsonPrimitive
                ?.content
                ?.lowercase()
        }
    } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        null
    } catch (_: Exception) {
        null
    }
