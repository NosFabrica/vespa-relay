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
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

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
 * the relay boots there may be no address to configure yet — and a container
 * restart that loses the key changes it. [hostnameFile] is the file Tor's
 * container publishes the hostname into (a volume shared with this one), read
 * on demand and cached once it parses: an address that appears an hour after
 * boot is picked up by the next connection instead of waiting for a relay
 * restart nobody knew to perform.
 *
 * "On demand" is once per websocket connect, and only until the read succeeds —
 * one failed `open` against a page-cached directory entry, beside a websocket
 * handshake that costs several orders of magnitude more. A relay with no hidden
 * service configures no path at all and does not even do that.
 */
class RelayAddresses(
    private val declared: Set<NormalizedRelayUrl> = emptySet(),
    private val hostnameFile: Path? = null,
    private val announce: (String) -> Unit = { System.err.println(it) },
) {
    @Volatile
    private var published: NormalizedRelayUrl? = null

    @Volatile
    private var complainedAbout: String? = null

    private val announced = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /**
     * Whatever we know right now. Called per connection, so a hidden service
     * that comes up after the relay did still gets its clients authenticated.
     *
     * Each address is announced the first time it enters the set: an operator
     * who cannot see the address cannot hand it to anyone, and for the
     * discovered one that line is the moment the endpoint became usable.
     */
    fun alternates(): Set<NormalizedRelayUrl> {
        val hidden = published ?: readPublished()
        val all = if (hidden == null) declared else declared + hidden
        all.forEach {
            if (announced.add(it)) {
                announce("onion: this relay also answers at ${it.url} — NIP-42 AUTH is accepted for it")
            }
        }
        return all
    }

    /**
     * The hostname Tor wrote, as a relay url, cached once it parses.
     *
     * A missing file is the ordinary state of a relay whose hidden service has
     * not started yet — silence, not a warning that would cry wolf on every
     * boot of every deployment that has no Tor at all. Content that is not a
     * relay address is different: something wrote a file we cannot use, and
     * that is worth saying (once per distinct value, so a stuck file does not
     * repeat itself on every connection).
     */
    private fun readPublished(): NormalizedRelayUrl? {
        val file = hostnameFile ?: return null
        val raw =
            runCatching { Files.readString(file) }
                .getOrNull()
                ?.trim()
                ?.takeIf { it.isNotEmpty() }
                ?: return null

        val url = RelayUrlNormalizer.normalizeOrNull(raw)
        if (url == null) {
            if (raw != complainedAbout) {
                complainedAbout = raw
                announce("onion: $file holds \"$raw\", which is not a relay address — no second address in use")
            }
            return null
        }

        published = url
        return url
    }
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
