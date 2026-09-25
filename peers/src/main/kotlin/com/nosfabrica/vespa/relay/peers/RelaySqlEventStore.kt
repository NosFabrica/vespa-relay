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

import com.nosfabrica.vespa.relay.ingest.AddressVersion
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.sqlCount
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.sqlIdsAndTimes
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.sqlQuery
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.sqlStream
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime

/**
 * The store as the mirror and the monitor read it: every read goes to [relay] as SQL over
 * the websocket, every write still lands in [writes]. SQL rather than REQ because a REQ
 * from a signed-in connection is ranked through that identity's web of trust, and these
 * planes need the raw corpus, uncapped: SQL reads the store underneath the lens.
 *
 * Filters with a NIP-50 `search` have no SQL spelling and are read from [writes].
 */
class RelaySqlEventStore(
    private val client: INostrClient,
    /** The relay's own url, as its NIP-42 challenge names it. */
    private val readRelay: NormalizedRelayUrl,
    private val writes: IEventStore,
    /** How long a query may stay silent. Generous: the relay fetches every reference before its first row. */
    private val idleTimeoutMs: Long = DEFAULT_IDLE_TIMEOUT_MS,
) : IEventStore by writes {
    override suspend fun <T : Event> query(filter: Filter): List<T> {
        val out = ArrayList<T>()
        query<T>(filter) { out.add(it) }
        return out
    }

    override suspend fun <T : Event> query(filters: List<Filter>): List<T> {
        val out = ArrayList<T>()
        query<T>(filters) { out.add(it) }
        return out
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(
        filter: Filter,
        onEach: (T) -> Unit,
    ) {
        if (filter.search != null) return writes.query(filter, onEach)
        client.sqlQuery(readRelay, filter, idleTimeoutMs) { onEach(it as T) }
    }

    /** Union of [filters], each event once, as the store's own multi-filter query answers. */
    override suspend fun <T : Event> query(
        filters: List<Filter>,
        onEach: (T) -> Unit,
    ) {
        val seen = HashSet<String>()
        for (filter in filters) query<T>(filter) { if (seen.add(it.id)) onEach(it) }
    }

    override suspend fun count(filter: Filter): Int {
        if (filter.search != null) return writes.count(filter)
        // The store's rule: a count honours its filter's limit, and a limit <= 0 matches nothing.
        val limit = filter.limit
        if (limit != null && limit <= 0) return 0
        val n = client.sqlCount(readRelay, filter, idleTimeoutMs).toInt()
        return if (limit != null) minOf(n, limit) else n
    }

    /** As the store counts several filters: the union of each one's ids, its newest `limit` when it has one. */
    override suspend fun count(filters: List<Filter>): Int =
        when (filters.size) {
            0 -> 0
            1 -> count(filters[0])
            else -> filters.flatMapTo(HashSet()) { ids(it) }.size
        }

    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
        onProgress: ((collected: Int) -> Unit)?,
    ): List<IdAndTime> {
        val seen = HashSet<String>()
        val all = ArrayList<IdAndTime>()
        for (filter in filters) {
            // Past maxEntries + 1 the caller only needs to know it overflowed.
            val capped = if (maxEntries == null) filter else filter.copy(limit = minOf(filter.limit ?: Int.MAX_VALUE, maxEntries + 1))
            for (entry in client.sqlIdsAndTimes(readRelay, capped, idleTimeoutMs = idleTimeoutMs)) {
                if (seen.add(entry.id)) all.add(entry)
            }
            onProgress?.invoke(all.size)
        }
        return if (maxEntries != null && all.size > maxEntries + 1) all.subList(0, maxEntries + 1) else all
    }

    override suspend fun sql(
        query: String,
        params: List<Any?>,
        named: Map<String, Any?>,
        onColumns: (List<String>) -> Unit,
        onRow: (List<Any?>) -> Unit,
    ) = client.sqlStream(readRelay, query, params, named, idleTimeoutMs = idleTimeoutMs, onColumns = onColumns, onRow = onRow)

    /** Which of [ids] the store holds: ingest's duplicate gate. */
    suspend fun existingIds(ids: List<String>): Set<String> {
        val found = HashSet<String>()
        for (chunk in ids.distinct().chunked(KEY_CHUNK)) {
            client.sqlStream(readRelay, "SELECT id FROM events WHERE id IN (${marks(chunk)})", chunk, idleTimeoutMs = idleTimeoutMs) { found.add(it[0] as String) }
        }
        return found
    }

    /**
     * The newest stored version of each author's [kind] event: newest `created_at`, ties to
     * the lower id, the store's own replaceable rule. Ingest's stale-version gate.
     */
    suspend fun newestVersions(
        kind: Int,
        authors: List<String>,
    ): Map<String, AddressVersion> {
        val out = HashMap<String, AddressVersion>()
        for (chunk in authors.distinct().chunked(KEY_CHUNK)) {
            client.sqlStream(
                readRelay,
                "SELECT pubkey, created_at, id FROM events WHERE kind = ? AND pubkey IN (${marks(chunk)})",
                listOf<Any?>(kind.toLong()) + chunk,
                idleTimeoutMs = idleTimeoutMs,
            ) { row ->
                val v = AddressVersion((row[1] as Number).toLong(), row[2] as String)
                out.merge(row[0] as String, v) { a, b -> if (b.createdAt > a.createdAt || (b.createdAt == a.createdAt && b.id < a.id)) b else a }
            }
        }
        return out
    }

    /**
     * Distinct non-empty first values of [tagName] tags on the events [filter] matches: the
     * relay-list roll-up discovery reads, which the store answers natively as a tag grouping
     * when the filter is plain enough.
     */
    suspend fun distinctTagValues(
        filter: Filter,
        tagName: String,
    ): Set<String> {
        val (where, args) = tagRowConditions(filter)
        val out = HashSet<String>()
        client.sqlStream(
            readRelay,
            "SELECT DISTINCT t.value FROM tags t WHERE t.name = ? AND t.value <> ''" + where.joinToString("") { " AND $it" },
            listOf<Any?>(tagName) + args,
            idleTimeoutMs = idleTimeoutMs,
        ) { out.add(it[0] as String) }
        return out
    }

    private suspend fun ids(filter: Filter): List<String> = client.sqlIdsAndTimes(readRelay, filter, idleTimeoutMs = idleTimeoutMs).map { it.id }

    /** [filter]'s event conditions on a `tags` row `t`, its tag conditions as subqueries carrying the same bounds. */
    private fun tagRowConditions(filter: Filter): Pair<List<String>, List<Any?>> {
        require(filter.search == null) { "a search filter has no SQL spelling" }
        val (conditions, args) = eventConditions("t", filter)
        val tagged = filter.tags.orEmpty().map { (k, v) -> k to v } + filter.tagsAll.orEmpty().flatMap { (k, vs) -> vs.map { k to listOf(it) } }
        tagged.forEachIndexed { i, (name, values) ->
            val x = "x$i"
            val (bounds, boundArgs) = eventConditions(x, filter)
            val inner = listOf("$x.name = ? AND $x.value IN (${marks(values)})") + bounds
            conditions += "t.event_id IN (SELECT $x.event_id FROM tags $x WHERE ${inner.joinToString(" AND ")})"
            args.add(name)
            args.addAll(values)
            args.addAll(boundArgs)
        }
        return conditions to args
    }

    /** `ids` / `authors` / `kinds` / `since` / `until` of [filter] on the `tags` row [alias]. */
    private fun eventConditions(
        alias: String,
        filter: Filter,
    ): Pair<MutableList<String>, MutableList<Any?>> {
        val c = ArrayList<String>()
        val args = ArrayList<Any?>()
        filter.ids?.let {
            c += "$alias.event_id IN (${marks(it)})"
            args.addAll(it)
        }
        filter.authors?.let {
            c += "$alias.pubkey IN (${marks(it)})"
            args.addAll(it)
        }
        filter.kinds?.let {
            c += "$alias.kind IN (${marks(it)})"
            args.addAll(it.map(Int::toLong))
        }
        filter.since?.let {
            c += "$alias.created_at >= ?"
            args.add(it)
        }
        filter.until?.let {
            c += "$alias.created_at <= ?"
            args.add(it)
        }
        return c to args
    }

    private fun marks(values: Collection<*>) = values.joinToString(",") { "?" }

    companion object {
        const val DEFAULT_IDLE_TIMEOUT_MS = 120_000L

        /** Keys bound per statement. */
        private const val KEY_CHUNK = 500
    }
}
