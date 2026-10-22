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

/** Removed: the relay is the only writer. Read only to refuse a deployment still asking for `local`. */
const val SYNC_INGEST_ENV = "SYNC_INGEST"

/** The relay's websocket, where every write this process makes goes; defaults to `RELAY_URL`. */
const val SYNC_INGEST_URL_ENV = "SYNC_INGEST_URL"

/**
 * Where this process's writes go: the serving relay, dialled at [SYNC_INGEST_URL_ENV] (default
 * [relayUrl], `RELAY_URL`), as a client signed in with the relay's own key. The relay is the only
 * process that writes the index, so the sync opens its store read-only.
 *
 * `SYNC_INGEST=local`, which wrote the store from here beside the relay's own writes, is gone: two
 * writers race each other's admission checks (docs/decisions/ingest.md, "One writer"). A
 * deployment still setting it stops the boot rather than quietly changing who writes; `relay`, the
 * only mode left, is accepted and needs no setting.
 */
fun ingestDialFromEnv(
    env: Map<String, String>,
    relayUrl: NormalizedRelayUrl,
): NormalizedRelayUrl {
    when (val mode = env[SYNC_INGEST_ENV]?.trim()?.lowercase()?.takeIf { it.isNotEmpty() }) {
        null, "relay" -> {
            Unit
        }

        "local" -> {
            error(
                "$SYNC_INGEST_ENV=local was removed: the relay is the index's only writer, and this process forwards every " +
                    "write to it. Unset $SYNC_INGEST_ENV, and give the sync the relay's RELAY_NSEC.",
            )
        }

        else -> {
            error("$SYNC_INGEST_ENV='$mode' is not a mode; the relay is the only writer, so unset it.")
        }
    }
    val raw = env[SYNC_INGEST_URL_ENV]?.trim()?.takeIf { it.isNotEmpty() } ?: return relayUrl
    return RelayUrlNormalizer.normalizeOrNull(raw) ?: error("$SYNC_INGEST_URL_ENV='$raw' is not a relay websocket url (ws://relay:7777).")
}

/** What the relay's NIP-11 document says about its own key. */
sealed interface RelaySelf {
    /** No document: the relay is down or still booting. */
    data object Unreachable : RelaySelf

    /** A document with no `self`: a relay with no `RELAY_NSEC`, so no mirror lane and no delete hook. */
    data object NoKey : RelaySelf

    data class Key(
        val pubkey: String,
    ) : RelaySelf
}

/**
 * The relay's own pubkey as its NIP-11 document states it (`self`), read over the http(s) twin of
 * [dial]. The relay puts a socket on its mirror lane only when it signs in as this key, so a sync
 * holding a different `RELAY_NSEC` would write everything through the publishers' lane and never
 * know; and a relay that states no key runs no lane and no `MirrorDelete` hook at all.
 */
fun relaySelf(dial: NormalizedRelayUrl): RelaySelf =
    try {
        val request =
            HttpRequest
                .newBuilder(URI.create(dial.url.replaceFirst("ws", "http")))
                .header("Accept", "application/nostr+json")
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build()
        val response = nip11Http.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) {
            RelaySelf.Unreachable
        } else {
            Json
                .parseToJsonElement(response.body())
                .jsonObject["self"]
                ?.jsonPrimitive
                ?.content
                ?.lowercase()
                ?.let { RelaySelf.Key(it) } ?: RelaySelf.NoKey
        }
    } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        RelaySelf.Unreachable
    } catch (_: Exception) {
        RelaySelf.Unreachable
    }

/** One client for every [relaySelf]: the boot asks four times a second while its relay starts, and each client is a selector thread. */
private val nip11Http: HttpClient by lazy { HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build() }

/** What the relay's stated key means for this process, which holds the relay's `RELAY_NSEC`. */
sealed interface KeyVerdict {
    /** The relay is not answering: nothing is known yet. */
    data object Pending : KeyVerdict

    /** The relay states this process's key: its writes take the mirror lane. */
    data object Ours : KeyVerdict

    /** The relay states another key or none: this process must not run against it. */
    data class Refused(
        val reason: String,
    ) : KeyVerdict
}

/**
 * [self], the relay at [dial]'s stated key, judged against [ours]. Another key: every write would
 * still be stored, through the publishers' lane, and nothing would say so, since the relay signs
 * in any key and only its own takes the mirror lane. No key: no lane and no `MirrorDelete` hook, so
 * the audit's deletes would be answered OK and do nothing.
 */
fun keyVerdict(
    self: RelaySelf,
    dial: NormalizedRelayUrl,
    ours: String,
): KeyVerdict =
    when (self) {
        RelaySelf.Unreachable -> {
            KeyVerdict.Pending
        }

        RelaySelf.NoKey -> {
            KeyVerdict.Refused(
                "the relay at ${dial.url} states no key (NIP-11 `self`): it has no RELAY_NSEC, so it runs no mirror lane and no " +
                    "delete hook. Give it the same RELAY_NSEC as this process.",
            )
        }

        is RelaySelf.Key -> {
            if (self.pubkey == ours) {
                KeyVerdict.Ours
            } else {
                KeyVerdict.Refused(
                    "the relay at ${dial.url} is ${self.pubkey}, but this process's RELAY_NSEC is $ours. The relay writes a socket's " +
                        "events as the mirror's only when it signs in as the relay's key, so both processes need the same RELAY_NSEC.",
                )
            }
        }
    }

/**
 * Keeps reading [dial]'s key after a boot that could not, until the relay answers once: the boot
 * check gives up after a minute so a relay that is down does not hold the mirror's reads, and
 * without this a relay that then came up with another key, or none, would never be caught. Calls
 * [onVerified] or [onRefused] once and stops. A daemon thread: it must never hold the process up.
 */
fun watchRelayKey(
    dial: NormalizedRelayUrl,
    ours: String,
    pollMillis: Long,
    read: (NormalizedRelayUrl) -> RelaySelf = ::relaySelf,
    onVerified: () -> Unit,
    onRefused: (String) -> Unit,
): Thread =
    Thread({
        try {
            while (true) {
                when (val verdict = keyVerdict(read(dial), dial, ours)) {
                    KeyVerdict.Pending -> {
                        Thread.sleep(pollMillis)
                    }

                    KeyVerdict.Ours -> {
                        onVerified()
                        return@Thread
                    }

                    is KeyVerdict.Refused -> {
                        onRefused(verdict.reason)
                        return@Thread
                    }
                }
            }
        } catch (_: InterruptedException) {
            // Shutdown: nothing left to check.
        }
    }, "relay-key-watch").apply {
        isDaemon = true
        start()
    }
