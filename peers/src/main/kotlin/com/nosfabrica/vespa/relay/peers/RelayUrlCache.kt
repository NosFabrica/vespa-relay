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
 * Raw tag value -> the url a relay-list read makes of it, memoized.
 *
 * THE SAME STRINGS ARRIVE MILLIONS OF TIMES. A relay-list scan sees one tag per
 * relay per event, and the population of relays is tiny beside the population of
 * events naming them — a corpus with 19,844 known relay urls (#182's own
 * number) hands this the same few thousand spellings once per author. Every one
 * of them used to pay a trim, a whitespace scan, two prefix tests, a full parse
 * through [RelayUrlNormalizer], an onion test, a loopback test and a port strip.
 * The parse is the expensive part and it is entirely a function of the input
 * string, so it is computed once per DISTINCT spelling here instead of once per
 * occurrence.
 *
 * QUARTZ HAS NO SUCH CACHE — `RelayUrlNormalizer` is a stateless companion of
 * pure functions, and the only interning it ships is `EventInterner`
 * (`ConcurrentHashMap<String, WeakReference<Event>>`, keyed by event id, for
 * whole events). This is that pattern pointed at urls, and it lives here rather
 * than upstream because the reject rules it memoizes are OURS: the scheme
 * demand, the second scheme check after the normalizer's repairs, the loopback
 * refusal. See [RelayDiscovery.normalize] for what each is worth.
 *
 * **[allowOnion] IS NOT PART OF THE KEY, and must not be.** It is a property of
 * the DEPLOYMENT (does this router have a Tor transport), not of the string, so
 * keying on it would double every entry to record a decision that is the same
 * for every caller in a process. Instead the entry carries whether the url is an
 * onion and the gate is applied on the way out — which also means one cache
 * serves a Tor-enabled read and a clearnet one without either seeing the
 * other's answer.
 *
 * BOUNDED, BECAUSE THE KEYS COME FROM STRANGERS. Anyone's kind 10002 can name
 * urls nobody has ever seen, so an unbounded map here is a memory leak with a
 * publish button on it. At [MAX_ENTRIES] the whole map is dropped rather than
 * evicted one at a time: this is a cache, and the cost of a miss is the parse it
 * was avoiding — the correctness of the answer never depends on a hit. A clear
 * costs one pass of re-parsing over urls that are still live, which is bounded
 * by the same number.
 */
internal class RelayUrlCache(
    private val maxEntries: Int = MAX_ENTRIES,
) {
    /**
     * One decided spelling. [url] is null when the string is not a relay url at
     * all; [onion] says whether a deployment WITHOUT Tor must still refuse it.
     */
    private class Decided(
        val url: NormalizedRelayUrl?,
        val onion: Boolean,
    )

    private val cache = ConcurrentHashMap<String, Decided>()

    /** How many distinct spellings are held. Diagnostics and tests only. */
    fun size(): Int = cache.size

    fun clear() = cache.clear()

    /**
     * [raw] as a dialable url, or null. Byte-identical to computing it every
     * time — the memo is over the string, and the only per-caller input
     * ([allowOnion]) is applied after the lookup.
     */
    fun normalize(
        raw: String,
        allowOnion: Boolean,
    ): NormalizedRelayUrl? {
        val decided =
            cache[raw] ?: decide(raw).also {
                // Racing writers both compute the same value, so whichever
                // lands is right; the size test is why this is not putIfAbsent.
                if (cache.size >= maxEntries) cache.clear()
                cache[raw] = it
            }
        if (!allowOnion && decided.onion) return null
        return decided.url
    }

    /**
     * The rules, once per distinct spelling. Everything here is a function of
     * [raw] alone — see the class KDoc for why the onion gate is not.
     */
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
        /**
         * Distinct spellings held before the map is dropped whole. Comfortably
         * over the 19,844 relay urls a real deployment knows of, and small
         * enough that a flood of invented ones costs a few MB and one re-parse
         * pass rather than a heap.
         */
        const val MAX_ENTRIES = 65_536

        /** Not a relay url, whatever the deployment's transport — cached like any other answer. */
        private val REFUSED = Decided(null, false)

        /**
         * The process-wide cache, in the spirit of quartz's `EventInterner.Default`.
         * Shared on purpose: the sweep and the fast lane read the same relay
         * lists minutes apart, and a per-call map would throw the answer away
         * between them.
         */
        val Default = RelayUrlCache()
    }
}
