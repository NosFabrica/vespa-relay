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
package com.nosfabrica.vespa.relay.config

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.isOnion
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.toHttp
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * The addresses this relay answers at besides `RELAY_URL`: the `.onion` a hidden service publishes
 * in front of the same port. Tor mints it on its first start, so [hostnameFile] is watched by
 * mtime, at most once per [LOOK_INTERVAL_MS], and a rotated address lands without a restart.
 */
class RelayAddresses(
    private val declared: Set<NormalizedRelayUrl> = emptySet(),
    private val hostnameFile: Path? = null,
    private val announce: (String) -> Unit = { System.err.println(it) },
    // Whether the clearnet endpoint may name the hidden service. AUTH is accepted for it either way.
    private val advertise: Boolean = true,
    private val nanoTime: () -> Long = System::nanoTime,
) {
    @Volatile
    private var addresses: Set<NormalizedRelayUrl> = declared

    /** The `Onion-Location` value, or null. `http://`, not `ws://`: readers parse it with an http url parser. */
    @Volatile
    private var advertised: String? = onionAmong(declared)

    /** The published file's mtime, or 0 while there is no file; -1 before the first look. */
    @Volatile
    private var seenStamp: Long = -1L

    /**
     * When the file may be looked at again. Seeded from the clock so the first look is due, and
     * compared as a difference because `nanoTime` has no origin and wraps.
     */
    @Volatile
    private var nextLook: Long = nanoTime()

    init {
        declared.forEach(::announceAddress)
    }

    /** Whatever is known right now, asked per connection so a late hidden service needs no restart. */
    fun alternates(): Set<NormalizedRelayUrl> {
        refresh()
        return addresses
    }

    /** The `Onion-Location` header value for the clearnet endpoint, or null. */
    fun onionLocation(): String? {
        if (!advertise) return null
        refresh()
        return advertised
    }

    private fun refresh() {
        val file = hostnameFile ?: return
        val now = nanoTime()
        if (now - nextLook < 0) return
        nextLook = now + LOOK_INTERVAL_MS * 1_000_000
        val stamp = file.toFile().lastModified()
        if (stamp != seenStamp) adopt(file, stamp)
    }

    /** Re-reads the published hostname. Synchronized and re-checked because [refresh] is lock-free. */
    @Synchronized
    private fun adopt(
        file: Path,
        stamp: Long,
    ) {
        if (stamp == seenStamp) return
        seenStamp = stamp
        // Gone, or never there. Keep the address held: a mount blinking does not end a hidden service.
        if (stamp == 0L) return

        val raw =
            runCatching { Files.readString(file) }
                .getOrNull()
                ?.trim()
                ?.takeIf { it.isNotEmpty() }
                ?: return

        val url = RelayUrlNormalizer.normalizeOrNull(raw)
        if (url == null) {
            announce("onion: $file holds \"$raw\", which is not a relay address — no second address in use")
            return
        }

        if (url !in addresses) {
            val next = declared + url
            // The published address wins the advertisement: it is the service this deployment runs.
            advertised = (if (url.isOnion()) url.toHttp() else null) ?: onionAmong(next)
            addresses = next
            announceAddress(url)
        }
    }

    /** The first `.onion` in [from], as an http url. */
    private fun onionAmong(from: Set<NormalizedRelayUrl>): String? = from.firstOrNull { it.isOnion() }?.toHttp()

    private fun announceAddress(url: NormalizedRelayUrl) = announce("onion: this relay also answers at ${url.url} — NIP-42 AUTH is accepted for it")
}

/** How long a look at the hostname file is good for; keeps the cost independent of traffic. */
private const val LOOK_INTERVAL_MS = 1_000L

/**
 * `RELAY_ONION_URL` (a declared second address; malformed is fatal), `RELAY_ONION_HOSTNAME_FILE`
 * (where the hidden service writes its hostname; absent is normal) and `RELAY_ONION_ADVERTISE`
 * (whether `Onion-Location` names it; default true).
 */
fun relayAddressesFromEnv(env: Map<String, String>): RelayAddresses {
    val declared =
        env["RELAY_ONION_URL"]
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let {
                setOf(
                    RelayUrlNormalizer.normalizeOrNull(it)
                        ?: throw IllegalArgumentException("RELAY_ONION_URL '$it' is not a valid relay url."),
                )
            }.orEmpty()

    val hostnameFile =
        env["RELAY_ONION_HOSTNAME_FILE"]
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { Paths.get(it) }

    return RelayAddresses(
        declared = declared,
        hostnameFile = hostnameFile,
        advertise = env["RELAY_ONION_ADVERTISE"]?.trim()?.toBooleanStrictOrNull() ?: true,
    )
}
