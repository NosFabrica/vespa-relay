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
package com.nosfabrica.vespa.relay.status

import com.nosfabrica.vespa.relay.sync.SyncBands
import com.nosfabrica.vespa.relay.web.PubkeyDocuments
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * How far this mirror has got with one author's events: every stream that asks a relay for them, and
 * each such pair's walk and reconcile, as the status page's relay table words them. The page under the
 * search box asks it (at `/coverage/{pubkey}`, see `pubkeyDocument`), so it is public, and it carries
 * only what the author's own relay list and this mirror's config already make public. Answers are
 * cached briefly and fresh ones are rate-limited across all authors: each walks the band snapshot.
 */
class AuthorCoverage internal constructor(
    private val bands: () -> JsonObject?,
    /** The units reading an author's events: bound to them, or bound to nobody on the given relays. */
    private val units: (String, Set<NormalizedRelayUrl>) -> List<RelayStatusReport.PrimeUnit>,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
    private val cacheSeconds: Long = CACHE_SECONDS,
    private val gate: RateGate = RateGate(PER_SECOND, BURST),
) : PubkeyDocuments {
    internal constructor(
        bands: SyncBands,
        units: (String, Set<NormalizedRelayUrl>) -> List<RelayStatusReport.PrimeUnit>,
    ) : this({ bands.snapshot() }, units)

    private class Cached(
        val atSec: Long,
        val doc: JsonObject,
    )

    private val cache = ConcurrentHashMap<String, Cached>()

    /**
     * [pubkey]'s document, or null when over the rate with nothing fresh cached. `relay` in [query] names
     * the author's own write relays, at most [MAX_RELAYS]: a stream bound to nobody reads everyone's events
     * on a relay, so it reads theirs wherever they write, and only they can say where that is.
     */
    override fun answer(
        pubkey: String,
        query: Map<String, List<String>>,
    ): JsonObject? {
        val relays =
            query["relay"]
                .orEmpty()
                .asSequence()
                .mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }
                .distinct()
                .take(MAX_RELAYS)
                .sortedBy { it.url }
                .toList()
        val key = pubkey + relays.joinToString("") { " " + it.url }
        val now = clock()
        cache[key]?.takeIf { now - it.atSec < cacheSeconds }?.let { return it.doc }
        if (!gate.take()) return null
        val doc = document(pubkey, relays, now)
        // Bounded by dropping the lot: the entries are seconds old, so a refill costs little.
        if (cache.size >= MAX_CACHED) cache.clear()
        cache[key] = Cached(now, doc)
        return doc
    }

    private fun document(
        pubkey: String,
        relays: List<NormalizedRelayUrl>,
        now: Long,
    ): JsonObject {
        val units = units(pubkey, relays.toSet())
        val report = if (units.isEmpty()) null else RelayStatusReport.build(bands(), units, now)
        val rows = (report?.get("rows") as? JsonArray).orEmpty().map { it.jsonObject }
        val read = rows.mapTo(HashSet()) { (it["relay"] as? JsonPrimitive)?.content }
        return buildJsonObject {
            put("pubkey", pubkey)
            put("generatedAt", now)
            // False is "nothing here reads this author's events", not "nothing found".
            put("followed", rows.isNotEmpty())
            putJsonArray("relays") {
                for (row in rows) add(publicRow(row))
            }
            // The author's own relays no stream reads: not rated working, or not asked for these kinds.
            putJsonArray("unread") {
                for (url in relays) if (url.url !in read) add(url.url)
            }
        }
    }

    /** One relay-table row, without what is the operator's: abort text, kind caps, monitor drift. */
    private fun publicRow(row: JsonObject): JsonObject = JsonObject(row.filterKeys { it in PUBLIC_MEMBERS })

    /** A token bucket over the whole route: [perSecond] answers a second, [burst] at once. */
    class RateGate(
        private val perSecond: Int,
        private val burst: Int,
        private val nowMs: () -> Long = System::currentTimeMillis,
    ) {
        /** When the answers given so far are paid for, in milliseconds; one more costs a second over [perSecond]. */
        private val paidUntil = AtomicLong(Long.MIN_VALUE / 2)

        fun take(): Boolean {
            val costMs = 1000L / perSecond
            while (true) {
                val now = nowMs()
                val seen = paidUntil.get()
                // Never further back than a full bucket's worth: idle time does not bank past [burst].
                val from = maxOf(seen, now - burst * costMs)
                if (from + costMs > now) return false
                if (paidUntil.compareAndSet(seen, from + costMs)) return true
            }
        }
    }

    companion object {
        /** How long one author's answer is served again; the status page itself refreshes on a similar clock. */
        const val CACHE_SECONDS = 30L

        const val PER_SECOND = 20

        const val BURST = 40

        const val MAX_CACHED = 4_096

        /** Relays one request may name; a relay list longer than this is not one a person writes to. */
        const val MAX_RELAYS = 20

        /** The row members a reader may see: the walk and the reconcile, never why a relay refused us. */
        private val PUBLIC_MEMBERS =
            setOf("relay", "stream", "syncStatus", "behind", "behindSec", "coveredFrom", "coveredTo", "verifiedAgoSec", "tailed")
    }
}
