/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.RelayReachabilityStore

/**
 * Relay liveness that survives a restart, kept as NIP-66 kind:30166 in the very
 * store this relay serves.
 *
 * [RelayHealth] forgets everything when a cycle ends, so the next one re-dials
 * the same corpses. This is the memory: quartz's [RelayReachabilityStore] keeps
 * one replaceable 30166 per (monitor, relay) — addressable by the normalized url
 * — with `created_at` giving a free TTL. Liveness is inferred exactly as NIP-66
 * defines it: a fresh record WITH `rtt-open` is reachable, one WITHOUT is
 * checked-and-dead, and no record at all means never checked. A recent success
 * beats an earlier failure, whether both came from us or from two monitors.
 *
 * ## Reading is free, writing needs an identity
 *
 * [snapshot] only reads, so it runs with a throwaway key and is always on. That
 * matters more than it sounds: 30166s published by OTHER monitors — nostr.watch
 * and friends — are ordinary events, so any relay whose streams mirror kind
 * 30166 inherits the network's census without dialling anything. Twenty thousand
 * relays pre-classified before the first cycle, for the cost of a stream entry.
 *
 * Writing our own findings back needs [RelayIdentity] — this relay's own key,
 * the same one it authenticates with and advertises as its NIP-11 `self`. That
 * is what makes a record checkable: a reader can tell the 30166 came from the
 * relay whose NIP-11 they just read, rather than from an anonymous stranger. An
 * ephemeral key would be worse than none, minting a fresh author for every
 * record on every restart.
 *
 * A dead mark only ever lasts [ttlSeconds]. Skipping is "not now", never "never
 * again": relays come back, and a permanent blacklist would quietly amputate an
 * author's only home.
 */
class RelayReachability(
    store: IEventStore,
    private val writer: NostrSigner?,
    ttlSeconds: Long = RelayReachabilityStore.DEFAULT_TTL_SECONDS,
) {
    // Reads honour records from ANY monitor; only writes go out under [writer].
    private val reads = RelayReachabilityStore(store, writer ?: NostrSignerInternal(KeyPair()), ttlSeconds)

    val publishes: Boolean get() = writer != null

    /** Relays proven unreachable within the TTL and not seen live since. */
    suspend fun deadSet(): Set<NormalizedRelayUrl> = runCatching { reads.snapshot().dead }.getOrDefault(emptySet())

    /**
     * Persist what [RelayObserver] measured. A no-op without an identity to sign
     * with — the run still benefited from everyone else's records, it just adds
     * none of its own.
     *
     * The rtt is the MEASURED open time, never a placeholder. Aggregators rank
     * relays by that field, so a hard-coded zero published to a store other
     * people sync from would be telling the network every relay answers
     * instantly. A relay we somehow reached without timing it is therefore
     * recorded as reachable with no claim about speed, not as reachable in 0ms.
     *
     * Never throws: liveness bookkeeping failing is not a reason to fail syncs
     * that already worked.
     */
    suspend fun record(observations: Map<NormalizedRelayUrl, RelayObserver.Observation>): Int {
        if (writer == null || observations.isEmpty()) return 0
        val reachable = HashMap<NormalizedRelayUrl, Long>()
        val dead = HashSet<NormalizedRelayUrl>()
        for ((url, o) in observations) {
            when {
                o.reachable -> reachable[url] = o.rttOpenMs ?: continue

                // Only a connection we actually attempted and that actually failed.
                // Silence is not evidence: a relay nobody dialled this round has
                // nothing to report, and saying otherwise would refresh a TTL on a
                // claim we never tested.
                o.error != null -> dead += url
            }
        }
        if (reachable.isEmpty() && dead.isEmpty()) return 0
        runCatching { reads.recordProbed(reachable, dead, nowSeconds()) }
            .onFailure { System.err.println("router: could not record relay reachability: ${it.message}") }
        return reachable.size + dead.size
    }

    companion object {
        private fun nowSeconds(): Long = System.currentTimeMillis() / 1000
    }
}
