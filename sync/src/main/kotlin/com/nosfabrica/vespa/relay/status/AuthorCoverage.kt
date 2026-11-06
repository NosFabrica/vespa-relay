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

/**
 * How far this mirror has got with one author's events of some kinds (kind 1 unless asked): every
 * stream that asks a relay for them, and each such pair's walk, as the status page's relay table words
 * them. The page under the search box asks it (at `/coverage/{pubkey}`, see `pubkeyDocument`), so it is
 * public, and it carries only what the author's own relay list and this mirror's config already make
 * public. An answer costs the author's own units and the relays named, so it needs no rate limit of its
 * own; answers are still cached briefly, since the page asks again on every visit.
 */
class AuthorCoverage internal constructor(
    private val bands: () -> JsonObject?,
    /** The units reading an author's events of the given kinds: bound to them, or bound to nobody on the given relays. */
    private val units: (String, Set<NormalizedRelayUrl>, Set<Int>) -> List<RelayStatusReport.PrimeUnit>,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
    private val cacheSeconds: Long = CACHE_SECONDS,
) : PubkeyDocuments {
    internal constructor(
        bands: SyncBands,
        units: (String, Set<NormalizedRelayUrl>, Set<Int>) -> List<RelayStatusReport.PrimeUnit>,
    ) : this({ bands.snapshot() }, units)

    private class Cached(
        val atSec: Long,
        val doc: JsonObject,
    )

    private val cache = ConcurrentHashMap<String, Cached>()

    /**
     * [pubkey]'s document. `relay` in [query] names the author's own write relays, at most [MAX_RELAYS]:
     * a stream bound to nobody reads everyone's events on a relay, so it reads theirs wherever they write,
     * and only they can say where that is. `kind` names the kinds asked about, at most [MAX_KINDS],
     * [DEFAULT_KINDS] when none is: a stream mirroring only profiles says nothing about someone's posts.
     */
    override fun answer(
        pubkey: String,
        query: Map<String, List<String>>,
    ): JsonObject {
        val relays =
            query["relay"]
                .orEmpty()
                .asSequence()
                .mapNotNull { RelayUrlNormalizer.normalizeOrNull(it.trim()) }
                .distinct()
                .take(MAX_RELAYS)
                .sortedBy { it.url }
                .toList()
        val kinds =
            query["kind"]
                .orEmpty()
                .asSequence()
                .mapNotNull { it.trim().toIntOrNull()?.takeIf { k -> k in 0..MAX_KIND } }
                .distinct()
                .take(MAX_KINDS)
                .sorted()
                .toList()
                .ifEmpty { DEFAULT_KINDS }
        val key = pubkey + kinds.joinToString(",", " ") + relays.joinToString("") { " " + it.url }
        val now = clock()
        cache[key]?.takeIf { now - it.atSec < cacheSeconds }?.let { return it.doc }
        val doc = document(pubkey, relays, kinds.toSet(), now)
        // Bounded by dropping the lot: the entries are seconds old, so a refill costs little.
        if (cache.size >= MAX_CACHED) cache.clear()
        cache[key] = Cached(now, doc)
        return doc
    }

    private fun document(
        pubkey: String,
        relays: List<NormalizedRelayUrl>,
        kinds: Set<Int>,
        now: Long,
    ): JsonObject {
        val units = units(pubkey, relays.toSet(), kinds)
        val report = if (units.isEmpty()) null else RelayStatusReport.build(bands(), units, now)
        val rows = (report?.get("rows") as? JsonArray).orEmpty().map { it.jsonObject }
        val read = rows.mapTo(HashSet()) { (it["relay"] as? JsonPrimitive)?.content }
        return buildJsonObject {
            put("pubkey", pubkey)
            put("generatedAt", now)
            // False is "nothing here reads this author's events of these kinds", not "nothing found".
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

    /** One relay-table row, with only what the page draws: never abort text, kind caps or monitor drift. */
    private fun publicRow(row: JsonObject): JsonObject = JsonObject(row.filterKeys { it in PUBLIC_MEMBERS })

    companion object {
        /** How long one answer is served again; the status page itself refreshes on a similar clock. */
        const val CACHE_SECONDS = 30L

        const val MAX_CACHED = 4_096

        /** Relays one request may name; a relay list longer than this is not one a person writes to. */
        const val MAX_RELAYS = 20

        /** Kinds one request may name. */
        const val MAX_KINDS = 20

        private const val MAX_KIND = 65_535

        /** A reader's posts: what the search page asks about. */
        val DEFAULT_KINDS = listOf(1)

        /** The row members the page reads: which relay, which stream, and how far its walk got. */
        private val PUBLIC_MEMBERS = setOf("relay", "stream", "syncStatus")
    }
}
