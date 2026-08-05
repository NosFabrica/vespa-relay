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
 * The addresses this relay answers at BESIDES `RELAY_URL` — today, the `.onion`
 * a Tor hidden service publishes in front of the same port.
 *
 * Only one decision needs the whole list: whether a NIP-42 auth event names
 * this relay (MultiAddressAuthPolicy). Everything else is already
 * address-agnostic — the web UI derives its relay url from the page's own
 * origin, the NIP-11 doc names no host, and the store is keyed by the one
 * canonical `RELAY_URL` whichever door an event came through.
 *
 * The hidden-service address is DISCOVERED rather than declared. Tor derives it
 * from a key it generates the first time the service starts, so at the moment
 * the relay boots there may be no address to configure yet. [hostnameFile] is
 * the file Tor's container publishes the hostname into (a volume shared with
 * this one), and it is consulted per connection: an address that appears an
 * hour after boot is picked up by the next client instead of waiting for a
 * relay restart nobody knew to perform.
 *
 * "On demand" is once per websocket connect, and it costs one `stat`: the file
 * is only re-read when its timestamp moves. `File.lastModified()` rather than
 * `Files.readString` in a `runCatching` for exactly that reason — a missing
 * path answers 0 instead of building a `NoSuchFileException` with a stack
 * trace, and MISSING IS THE DEFAULT: compose sets the path on every relay,
 * including the ones with no hidden service at all.
 *
 * Watching the timestamp rather than reading once is also what makes a
 * ROTATED address land. Delete the key volume and Tor mints a new .onion; a
 * relay that had cached the old one would reject every auth event from the new
 * address until someone restarted it — the same silent downgrade this class
 * exists to prevent, in its most confusing form.
 */
class RelayAddresses(
    private val declared: Set<NormalizedRelayUrl> = emptySet(),
    private val hostnameFile: Path? = null,
    private val announce: (String) -> Unit = { System.err.println(it) },
) {
    @Volatile
    private var addresses: Set<NormalizedRelayUrl> = declared

    /**
     * The `Onion-Location` value, ready to write, or null when there is no
     * hidden service to name. Held as the finished string because it is emitted
     * on EVERY http response and changes about once in a deployment's life.
     *
     * `http://…onion/`, not `ws://…onion/`: the header is HTTP's, and the
     * clients that read it parse the value with an http url parser — okhttp's
     * `toHttpUrl()` in Amethyst's case, which returns null for a ws scheme and
     * would drop the advertisement on the floor without a word.
     */
    @Volatile
    private var advertised: String? = onionAmong(declared)

    /** The published file's mtime, or 0 while there is no file; -1 before the first look. */
    @Volatile
    private var seenStamp: Long = -1L

    init {
        declared.forEach(::announceAddress)
    }

    /**
     * Whatever we know right now. Called per connection, so a hidden service
     * that comes up — or changes — after the relay did still gets its clients
     * authenticated, without a restart nobody knew to perform.
     */
    fun alternates(): Set<NormalizedRelayUrl> {
        refresh()
        return addresses
    }

    /**
     * What to put in the `Onion-Location` header on the clearnet endpoint, so
     * a Tor-capable client that arrived the ordinary way learns this relay is
     * also a hidden service and can move its connection inside the network.
     * Null when we have no address to name.
     */
    fun onionLocation(): String? {
        refresh()
        return advertised
    }

    private fun refresh() {
        val file = hostnameFile ?: return
        val stamp = file.toFile().lastModified()
        if (stamp != seenStamp) adopt(file, stamp)
    }

    /**
     * Re-read the published hostname. Synchronized and re-checked because the
     * caller's test is deliberately lock-free: two connections arriving
     * together would otherwise both read and both announce.
     *
     * A file that is not there says nothing — that is the ordinary state of a
     * relay whose hidden service has not started, and a warning on every boot
     * of every deployment without Tor would cry wolf. Content that is not an
     * address is different: something wrote a file we cannot use, and saying so
     * once per change is the only way anyone learns of it.
     */
    @Synchronized
    private fun adopt(
        file: Path,
        stamp: Long,
    ) {
        if (stamp == seenStamp) return
        seenStamp = stamp
        // Gone, or never there. Keep the address we already had: a hidden
        // service does not stop existing because a mount blinked.
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
            // The published address wins the advertisement: it is the service
            // this deployment actually runs, and a declared one may be a
            // second door someone else's Tor holds open.
            advertised = (if (url.isOnion()) url.toHttp() else null) ?: onionAmong(next)
            addresses = next
            announceAddress(url)
        }
    }

    /** The first `.onion` in [from], as an http url — the only thing the header may name. */
    private fun onionAmong(from: Set<NormalizedRelayUrl>): String? = from.firstOrNull { it.isOnion() }?.toHttp()

    private fun announceAddress(url: NormalizedRelayUrl) = announce("onion: this relay also answers at ${url.url} — NIP-42 AUTH is accepted for it")
}

/**
 * `RELAY_ONION_URL` — a second address this relay answers at, declared by hand;
 * for a hidden service run outside this repo's compose file, where nothing
 * publishes a hostname file. Malformed is fatal, like `RELAY_URL`: an address
 * the operator typed and we then ignored is a Tor endpoint whose clients lose
 * their ranking lens for a reason nothing reports.
 *
 * `RELAY_ONION_HOSTNAME_FILE` — where the hidden service's container writes its
 * hostname (`/var/lib/onion/hostname` under compose). Absent is normal; see
 * [RelayAddresses].
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

    return RelayAddresses(declared, hostnameFile)
}
