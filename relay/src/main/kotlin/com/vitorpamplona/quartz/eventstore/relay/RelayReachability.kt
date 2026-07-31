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
 * Writing our own findings back is opt-in ([ROUTER_MONITOR_NSEC]) because it
 * mints a persistent identity: per NIP-66 a monitor is its own pubkey, distinct
 * from the observer whose web of trust this relay ranks by, and distinct from
 * [RouterIdentity]'s NIP-42 key. An ephemeral key would be worse than none — a
 * new author every restart, one more copy of every record, forever.
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
     * Persist a cycle's findings. A no-op without a monitor key — the run still
     * benefited from everyone else's records, it just adds none of its own.
     *
     * Never throws: liveness bookkeeping failing is not a reason to fail a sync
     * that already worked.
     */
    suspend fun record(
        reachable: Set<NormalizedRelayUrl>,
        dead: Set<NormalizedRelayUrl>,
    ) {
        if (writer == null) return
        runCatching { reads.record(reachable, dead - reachable, 0L, nowSeconds()) }
            .onFailure { System.err.println("router: could not record relay reachability: ${it.message}") }
    }

    companion object {
        const val ENV_VAR = "ROUTER_MONITOR_NSEC"

        private fun nowSeconds(): Long = System.currentTimeMillis() / 1000

        /**
         * The monitor identity from [ENV_VAR], or null to read-only. Shares
         * [RouterIdentity]'s parsing (and its refusal to start on a malformed
         * key) but deliberately NOT its key: NIP-42 authentication and relay
         * monitoring are different claims about who we are.
         */
        fun fromEnv(env: (String) -> String? = System::getenv): NostrSigner? {
            val raw = env(ENV_VAR)?.trim()?.ifEmpty { null } ?: return null
            return RouterIdentity.signerFor(raw, ENV_VAR)
        }
    }
}
