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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip01Core.store.StoreQueryContext
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import kotlin.coroutines.coroutineContext

/**
 * The store, with [SearchReferenceExpansion] spliced into the two methods a
 * REQ's stored replay actually rides.
 *
 * ## Why this is the seam, and not the session backend
 *
 * The expansion has to do a STORE LOOKUP in the middle of a page, and every
 * delivery callback above this line is non-suspending — `SessionBackend.query`
 * hands rows to a `(Event) -> Unit` from inside the store's own loop, and then
 * parks at `awaitCancellation()` for the life of the subscription rather than
 * returning. An earlier version of this feature lived up there and paid for it:
 * a `ReplayGate` buffering rows under a lock, a flush coroutine launched
 * undispatched from inside the EOSE callback, a sealed row type to carry the
 * stored and live halves, and a drain loop for events that landed mid-flush —
 * about 150 lines of machinery, all of it plumbing, and a frame-ordering race
 * to reason about every time anyone touched it.
 *
 * Down here the same two calls are ORDINARY SUSPEND FUNCTIONS THAT RETURN.
 * `LiveEventStore` calls `rawQuery`, waits for it, and only then signals EOSE
 * and starts the live tail. So the expansion is: collect the page, look the
 * subjects up, write the page out. The machinery is gone because the seam
 * never needed it.
 *
 * `StoreQueryContext` is what makes it work from here — its own KDoc says it
 * crosses "any decorator in between", and it carries the NIP-42 identity this
 * needs to resolve the reader's enrolment.
 *
 * ## Why overriding exactly these two is the whole scope
 *
 * `LiveEventStore` reaches the store in six places, and they are cleanly
 * split: the REQ replay takes `query(filters, onEach)` and `rawQuery`; NIP-77
 * takes the LIST-returning `query(filter)` and `snapshotIdsForNegentropy`;
 * COUNT takes `count`; the deferred-FTS drain takes `needsFtsCatchUp` /
 * `ftsCatchUp`. Overriding the two callback shapes therefore reaches the
 * stored replay of a REQ and NOTHING else — a negentropy reconcile, a COUNT,
 * an expiry sweep, the reindexer and every maintenance read are untouched BY
 * CONSTRUCTION rather than by a gate that has to remember them. `by inner`
 * forwards the rest, which is also what the store's own decorator rule asks
 * for: delegate, never ride an interface default.
 *
 * ## It stays honest about the interface it implements
 *
 * A decorator that made `query` emit events the filters do not match would be
 * lying to everything above it. This one cannot: the admission rule is
 * `filters.any { it.match(candidate) }`, so every spliced subject matches the
 * REQ exactly as the hits do. What changes is COMPLETENESS, not soundness —
 * and a ranked, `limit`-bounded search was never complete to begin with. The
 * events the expansion adds are ones the filters always admitted and ranking
 * alone would never have surfaced.
 *
 * The expansion's own lookups go to [inner], never through this decorator, so
 * a subject lookup cannot recurse into another expansion. It uses the
 * list-returning `query`, which is not overridden here, so that holds even if
 * someone later routes it differently.
 */
internal class ExpandingEventStore(
    private val inner: IEventStore,
    private val limits: SearchExpansionLimits,
    private val enrolment: EnrolledSigners,
) : IEventStore by inner {
    /**
     * The screened replay: every row materialized because the session's policy
     * filters outgoing events. Rows and subjects go out through one callback,
     * so the policy sees a subject exactly as it sees a hit.
     */
    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(
        filters: List<Filter>,
        onEach: (T) -> Unit,
    ) {
        val expansion = expansionFor(filters) ?: return inner.query(filters, onEach)
        val rows = ArrayList<Event>()
        inner.query<Event>(filters) { rows.add(it) }
        val subjects = expansion.expand(rows, Event::id) { it }
        rows.forEachIndexed { i, row ->
            onEach(row as T)
            subjects[i].forEach { onEach(it as T) }
        }
    }

    /**
     * The zero-decode replay, and it stays zero-decode for every row that
     * cannot point at anything: `kind` is a field on a [RawEvent], so the tags
     * parse and the `EventFactory` dispatch the expansion needs are paid only
     * by the handful of rows whose kind nominates a subject.
     */
    override suspend fun rawQuery(
        filters: List<Filter>,
        onEach: (RawEvent) -> Unit,
    ) {
        val expansion = expansionFor(filters) ?: return inner.rawQuery(filters, onEach)
        val rows = ArrayList<RawEvent>()
        inner.rawQuery(filters) { rows.add(it) }
        val subjects =
            expansion.expand(rows, RawEvent::id) { raw ->
                if (raw.kind in SearchReferences.KINDS) raw.toEvent() else null
            }
        rows.forEachIndexed { i, row ->
            onEach(row)
            // A subject IS a stored event — the lookup that found it is a store
            // recall — so it rides the same frame path as the rest of the page.
            subjects[i].forEach { onEach(RawEvent.fromEvent(it)) }
        }
    }

    /**
     * The write path, and the one exact invalidation signal there is: a reader
     * publishing a provider list HERE has it applied on their very next search
     * instead of within [EnrolledSigners]'s TTL. Only on `Accepted` — a
     * rejected 10040 (a duplicate, a bad signature, a banned author) changed
     * nothing to invalidate.
     */
    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
        val outcomes = inner.batchInsert(events)
        events.forEachIndexed { i, event ->
            if (event.kind == TrustProviderListEvent.KIND && outcomes.getOrNull(i) is IEventStore.InsertOutcome.Accepted) {
                enrolment.invalidate(event.pubKey)
            }
        }
        return outcomes
    }

    /** [batchInsert]'s single-event sibling, for the scripted paths that use it. A throw IS the rejection. */
    override suspend fun insert(event: Event) {
        inner.insert(event)
        if (event.kind == TrustProviderListEvent.KIND) enrolment.invalidate(event.pubKey)
    }

    /**
     * The expansion this read gets, or null for the plain delegating path.
     *
     * Three gates, cheapest first, and each rules out a whole class of traffic:
     * a read with no SEARCHING filter (a mirror's paging, a NIP-77 catch-up,
     * every plain reference read the web page makes — all of which carry
     * `include:spam` and so would pass a "has a search field" test); a search
     * whose `kinds` hold no pointer kind; and, per row inside [expand], a hit
     * that only the plain half of a mixed subscription could have produced.
     */
    private suspend fun expansionFor(filters: List<Filter>): SearchReferenceExpansion? {
        if (!limits.enabled) return null
        val searching = SearchReferenceExpansion.searching(filters)
        if (searching.isEmpty()) return null
        if (!SearchReferenceExpansion.couldPoint(searching)) return null
        val observers = SearchReferenceExpansion.observersOf(filters, coroutineContext[StoreQueryContext]?.observer)
        return SearchReferenceExpansion(filters, searching, observers, enrolment, limits) { inner.query<Event>(it) }
    }
}
