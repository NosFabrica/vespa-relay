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
import com.vitorpamplona.neo4j.eventstore.cypher.Hydrator
import com.vitorpamplona.neo4j.eventstore.engine.schema.GraphPolicy
import com.vitorpamplona.neo4j.eventstore.reconcile.FileCursorStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import java.io.File

/** Which process a projection is opened in: both feed it; only the relay serves and fully reconciles it. */
enum class GraphRole { RELAY, SYNC }

/**
 * Opens the projection before the store (the store takes its observer at open), for one process.
 *
 * The RELAY runs every reconcile stage and serves Cypher, so it alone keeps the sweep cursor and
 * insists the server is safe to expose. The SYNC process only repairs its OWN dropped deliveries
 * ([startGraphRepairs] with `dirtyOnly`): its dirty marks live in its memory, and nobody else can
 * see them. Each process saves what it still owes to its own dirty file on shutdown.
 */
fun openGraphProjection(
    settings: GraphSettings,
    source: VespaSource,
    role: GraphRole,
): GraphProjection =
    GraphProjection.open(
        url = settings.url,
        user = settings.user,
        password = settings.password,
        source = source,
        database = settings.database,
        policy = GraphPolicy(tagNodeNames = settings.tagNodes ?: GraphPolicy.DEFAULT_TAG_NODES, excludedKinds = settings.excludedKinds),
        cursor = if (role == GraphRole.RELAY) FileCursorStore(settings.cursorFile) else null,
        requireSafeServer = role == GraphRole.RELAY,
        queueCapacity = settings.queueCapacity,
        dirtyFile = File(settings.cursorFile.absoluteFile.parentFile, "graph-dirty-${role.name.lowercase()}.txt"),
        hydrator = Hydrator { ids -> source.fetchServable(ids) },
    )

/** Starts this process's reconcile loop: every stage in the relay, only its own drops in sync. */
fun GraphProjection.startGraphRepairs(
    settings: GraphSettings,
    role: GraphRole,
    scope: CoroutineScope,
): Job =
    reconcileLoop.start(scope, settings.reconcileEverySeconds, dirtyOnly = role == GraphRole.SYNC) { e ->
        System.err.println("graph: reconcile failed: ${e::class.simpleName}: ${e.message}")
    }

/** The store's observer shape, delivering to the projection's feed (the two have the same calls). */
fun GraphProjection.asIndexObserver(): IndexObserver {
    val feed = listener
    return object : IndexObserver {
        override fun onPut(events: List<Event>) = feed.onPut(events)

        override fun onRemove(ids: List<String>) = feed.onRemove(ids)

        override fun onUncertain(
            events: List<Event>,
            ids: List<String>,
        ) = feed.onUncertain(events, ids)
    }
}
