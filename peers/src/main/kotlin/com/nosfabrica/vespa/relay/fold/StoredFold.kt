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
package com.nosfabrica.vespa.relay.fold

import com.nosfabrica.vespa.relay.peers.RelayDiscovery
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.Volatile

/**
 * The fold as a monitor's records hold it, for a roster with no monitor in its process. Its world
 * is every url those records are about, less the ones they call dead: the monitor's set, short of
 * the urls it found this round and has not written a record for yet.
 */
class StoredFold(
    private val record: RelayVerdictRecord,
    /** The urls the world is built from; read again once [worldRefreshMs] has passed. */
    private val readWorld: suspend () -> Set<NormalizedRelayUrl>,
    private val worldRefreshMs: Long = WORLD_REFRESH_MS,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    /** One world read and when it was taken, swapped whole so no reader sees one without the other. */
    private class World(
        val index: ServerIndex,
        val readAtMs: Long,
    )

    @Volatile private var world: World? = null

    /** Each folded url's last answer, so a store that cannot answer leaves the roster as it was. */
    private val last = ConcurrentHashMap<NormalizedRelayUrl, NormalizedRelayUrl>()

    /**
     * Which of [urls] are folded away, and onto what, so the roster never builds a leg for another
     * url's relay. Stand-ins and urls waiting on the fold are included: this caller only decides
     * which socket to open.
     */
    suspend fun foldedAway(urls: List<NormalizedRelayUrl>): Map<NormalizedRelayUrl, NormalizedRelayUrl> {
        val collapsed = FoldReading.readBack(record, urls, world()) ?: return urls.mapNotNull { url -> last[url]?.let { url to it } }.toMap()
        val folded = collapsed.aliases + collapsed.standIns + collapsed.waiting
        for (url in urls) {
            val onto = folded[url]
            if (onto == null) last.remove(url) else last[url] = onto
        }
        return folded
    }

    private suspend fun world(): ServerIndex {
        val held = world
        val now = clock()
        if (held != null && now - held.readAtMs < worldRefreshMs) return held.index
        return try {
            ServerIndex(readWorld()).also { world = World(it, now) }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // The last world stands; the next call reads again.
            System.err.println("router: could not read the fold's world from our records: ${e.message}")
            held?.index ?: ServerIndex.EMPTY
        }
    }

    companion object {
        /** How long a world read stands; the monitor's own rebuilds it once per derivation. */
        const val WORLD_REFRESH_MS = 5 * 60 * 1000L

        /** The fold [signers]' records hold in [store], their own `dead` verdicts included. */
        fun over(
            store: IEventStore,
            signers: List<String>,
            allowOnion: Boolean,
        ): StoredFold =
            StoredFold(
                RelayVerdictRecord(store, signer = null, readFrom = signers),
                readWorld = {
                    val recorded = RelayDiscovery.recorded(store, signers, RelayVerdictRecord.DEFAULT_TTL_SECONDS, allowOnion)
                    recorded - RelayDiscovery.undialable(store, signers, RelayVerdictRecord.DEAD_TTL_SECONDS, allowOnion)
                },
            )
    }
}
