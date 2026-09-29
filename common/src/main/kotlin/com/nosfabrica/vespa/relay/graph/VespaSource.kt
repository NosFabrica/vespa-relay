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
package com.nosfabrica.vespa.relay.graph

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.vitorpamplona.neo4j.eventstore.reconcile.SourceOfTruth
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime

/**
 * The Vespa store as the graph's source of truth: raw engine reads (un-lensed, expired included),
 * since the graph mirrors what is stored. Bound after the store opens — the store needs the
 * graph's observer first, and nothing reads the source before the reconcile loop starts.
 */
class VespaSource : SourceOfTruth {
    @Volatile private var store: VespaEventStore? = null

    fun bind(store: VespaEventStore) {
        this.store = store
    }

    private fun engine() = (store ?: error("the graph source was read before the store opened")).engine

    // The reconciler's sweep asks for Long.MIN_VALUE..Long.MAX_VALUE at its edges: those are the
    // store's OPEN bounds (null), which its walk handles — its window arithmetic would overflow on
    // the extremes, and a null `until` is what keeps events dated past the clock in the walk.
    override suspend fun visitIds(
        since: Long,
        until: Long,
        onPage: suspend (List<IdAndTime>) -> Boolean,
    ) = engine().visitIds(
        EventQuery(since = since.takeIf { it != Long.MIN_VALUE }, until = until.takeIf { it != Long.MAX_VALUE }),
    ) { refs -> onPage(refs.map { IdAndTime(it.createdAt, it.id) }) }

    override suspend fun fetch(ids: List<String>): List<Event> = fetch(ids, notExpiredAt = null)

    /**
     * What callers' Cypher results are filled from: like [fetch], minus NIP-40-expired events —
     * the relay never serves those, so the graph endpoint must not either. (The graph still
     * holds them: it mirrors what is stored, and the reconciler must see everything.)
     */
    suspend fun fetchServable(ids: List<String>): List<Event> = fetch(ids, notExpiredAt = System.currentTimeMillis() / 1000)

    private suspend fun fetch(
        ids: List<String>,
        notExpiredAt: Long?,
    ): List<Event> {
        if (ids.isEmpty()) return emptyList()
        return engine().search(EventQuery(ids = ids, notExpiredAt = notExpiredAt)).map { d ->
            Event(d.id, d.pubkey, d.createdAt, d.kind, d.tags.map { it.toTypedArray() }.toTypedArray(), d.content, d.sig)
        }
    }
}
