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

import com.nosfabrica.vespa.eventstore.engine.observe.IndexObserver
import com.vitorpamplona.neo4j.eventstore.GraphProjection
import com.vitorpamplona.neo4j.eventstore.engine.schema.GraphPolicy
import com.vitorpamplona.neo4j.eventstore.reconcile.FileCursorStore
import com.vitorpamplona.quartz.nip01Core.core.Event

/**
 * Opens the projection before the store (the store takes its observer at open), for one process.
 * [reconciles]: only the relay process runs the reconcile loop; the sync process only feeds.
 */
fun openGraphProjection(
    settings: GraphSettings,
    source: VespaSource,
    reconciles: Boolean,
): GraphProjection =
    GraphProjection.open(
        url = settings.url,
        user = settings.user,
        password = settings.password,
        source = source,
        database = settings.database,
        policy = GraphPolicy(tagNodeNames = settings.tagNodes ?: GraphPolicy.DEFAULT_TAG_NODES, excludedKinds = settings.excludedKinds),
        cursor = if (reconciles) FileCursorStore(settings.cursorFile) else null,
        queueCapacity = settings.queueCapacity,
    )

/** The store's observer shape, delivering to the projection's feed (the two have the same two calls). */
fun GraphProjection.asIndexObserver(): IndexObserver {
    val feed = listener
    return object : IndexObserver {
        override fun onPut(events: List<Event>) = feed.onPut(events)

        override fun onRemove(ids: List<String>) = feed.onRemove(ids)
    }
}
