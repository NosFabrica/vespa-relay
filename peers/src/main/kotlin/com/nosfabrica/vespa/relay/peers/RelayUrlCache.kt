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
package com.nosfabrica.vespa.relay.peers

import com.nosfabrica.vespa.relay.config.withoutDefaultPort
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.util.concurrent.ConcurrentHashMap

/**
 * Raw tag value to the url a relay-list read makes of it, memoized per
 * distinct spelling. The parse is a pure function of the string; the onion
 * gate is a property of the deployment, so it is applied on the way out and
 * is not part of the key. Keys come from strangers' relay lists, so the map
 * is dropped whole at [maxEntries] rather than allowed to grow.
 */
internal class RelayUrlCache(
    private val maxEntries: Int = MAX_ENTRIES,
) {
    /** One decided spelling. [url] is null when the string is not a relay url; [onion] says a deployment without Tor must still refuse it. */
    private class Decided(
        val url: NormalizedRelayUrl?,
        val onion: Boolean,
    )

    private val cache = ConcurrentHashMap<String, Decided>()

    /** Distinct spellings held. Diagnostics and tests only. */
    fun size(): Int = cache.size

    fun clear() = cache.clear()

    /** [raw] as a dialable url, or null. Identical to computing it every time. */
    fun normalize(
        raw: String,
        allowOnion: Boolean,
    ): NormalizedRelayUrl? {
        val decided =
            cache[raw] ?: decide(raw).also {
                // Racing writers compute the same value, so whichever lands is
                // right; the size test is why this is not putIfAbsent.
                if (cache.size >= maxEntries) cache.clear()
                cache[raw] = it
            }
        if (!allowOnion && decided.onion) return null
        return decided.url
    }

    /** The rules, once per distinct spelling; everything here is a function of [raw] alone. */
    private fun decide(raw: String): Decided {
        val trimmed = raw.trim()
        if (trimmed.isEmpty() || trimmed.any { it.isWhitespace() }) return REFUSED
        if (!trimmed.startsWith("ws://", true) && !trimmed.startsWith("wss://", true)) return REFUSED
        val url = RelayUrlNormalizer.normalizeOrNull(trimmed) ?: return REFUSED
        if (!url.url.startsWith("ws://", true) && !url.url.startsWith("wss://", true)) return REFUSED
        if (RelayUrlNormalizer.isLocalHost(url.url)) return REFUSED
        return Decided(withoutDefaultPort(url), RelayUrlNormalizer.isOnion(url.url))
    }

    companion object {
        /** Distinct spellings held before the map is dropped whole. */
        const val MAX_ENTRIES = 65_536

        /** Not a relay url, whatever the transport; cached like any other answer. */
        private val REFUSED = Decided(null, false)

        /** The process-wide cache: the sweep and the fast lane read the same relay lists minutes apart. */
        val Default = RelayUrlCache()
    }
}
