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
package com.nosfabrica.vespa.relay.pulse

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.engine.IngestStats
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.time.Instant

/**
 * This process's store resources as one document for the pulse page: the store's `metrics()`
 * and the ingest pipeline's stage split. Every total is a cumulative counter the page differences
 * between two polls; `gauges` are instantaneous and named apart so nobody differences them.
 */
object PulseDocument {
    /** Bumped when a member changes meaning; the page says so rather than misreading it. */
    const val SCHEMA = 1

    /**
     * The document. [clientDerived] adds the sections that describe the people using this relay,
     * the load hotspots and the slow-read log that quotes queries; off by default, and never to be
     * served where `/stats.json` is served.
     */
    fun of(
        store: VespaEventStore,
        title: String,
        scope: String,
        startedAtMillis: Long,
        clientDerived: Boolean = false,
        nowMillis: Long = System.currentTimeMillis(),
    ): JsonObject = of(store.metrics(), store.runCatching { feedStatus() }.getOrNull(), title, scope, startedAtMillis, clientDerived, nowMillis)

    /**
     * A reader bound to one store, for the route to call per request. [startedAtMillis] is when
     * the store's counters began, not when this reader was built, so `uptimeSeconds` covers the
     * whole window the totals do.
     */
    fun reader(
        store: VespaEventStore,
        startedAtMillis: Long,
        title: String,
        scope: String,
        clientDerived: Boolean = false,
    ): () -> JsonObject = { of(store, title, scope, startedAtMillis, clientDerived) }

    /** The pure form, so the shape can be asserted without a store. */
    fun of(
        metrics: CostLedger.Snapshot,
        feed: String?,
        title: String,
        scope: String,
        startedAtMillis: Long,
        clientDerived: Boolean = false,
        nowMillis: Long = System.currentTimeMillis(),
        // Read once here rather than three times below, so every lock member describes one instant.
        held: List<IngestStats.Held> = IngestStats.heldAll(),
        stages: Map<String, IngestStats.Stage> = IngestStats.snapshot(),
        blocked: Map<String, Map<String, Long>> = IngestStats.blockedSplit(),
    ): JsonObject =
        buildJsonObject {
            put("schema", SCHEMA)
            put("title", title)
            put("scope", scope)
            put("generatedAt", Instant.ofEpochMilli(nowMillis).toString())
            put("uptimeSeconds", (nowMillis - startedAtMillis) / 1000)
            // Said outright: "no client sections in this build" and "nobody has searched yet" produce
            // the same empty arrays.
            put("clientDerived", clientDerived)

            putActivities(metrics)
            putOutcomes(metrics)
            putEngine(metrics)
            putGauges(metrics)
            putLocks(held, blocked)
            putStages(stages)
            feed?.takeIf { it.isNotBlank() }?.let { put("feed", it) }
            if (clientDerived) {
                putHotspots(metrics)
                putSlowReads(metrics)
            }
        }

    /**
     * Port calls grouped by the activity that made them. `callsPerDoc` is computed here rather
     * than by the page because the two numbers it divides come off one slot.
     */
    private fun kotlinx.serialization.json.JsonObjectBuilder.putActivities(m: CostLedger.Snapshot) {
        val byActivity = m.ports.groupBy { it.activity }
        if (byActivity.isEmpty()) return
        putJsonArray("activities") {
            byActivity.entries
                .sortedByDescending { (_, ports) -> ports.sumOf { it.nanos } }
                .forEach { (activity, ports) ->
                    add(
                        buildJsonObject {
                            put("activity", activity.name)
                            put("calls", ports.sumOf { it.calls })
                            put("ms", ports.sumOf { it.nanos } / 1_000_000)
                            put("docs", ports.sumOf { it.docs })
                            putJsonArray("ports") {
                                ports.sortedByDescending { it.nanos }.forEach { p ->
                                    add(
                                        buildJsonObject {
                                            put("call", p.call.name)
                                            put("calls", p.calls)
                                            put("ms", p.nanos / 1_000_000)
                                            put("docs", p.docs)
                                            put("callsPerDoc", p.callsPerDoc)
                                            // Absent, not zero, where no histogram is kept for this call
                                            // shape: "p50 0.00ms" reads as instant when it means unmeasured.
                                            p.latency?.let { l ->
                                                put("p50Ms", l.p50Nanos / 1_000_000.0)
                                                put("p99Ms", l.p99Nanos / 1_000_000.0)
                                                put("measured", l.count)
                                            }
                                        },
                                    )
                                }
                            }
                        },
                    )
                }
        }
    }

    /**
     * What became of the events this process was offered, per activity and per reason, with the
     * denominator that makes them a rate. A refused event never reaches a port counter.
     */
    private fun kotlinx.serialization.json.JsonObjectBuilder.putOutcomes(m: CostLedger.Snapshot) {
        if (m.outcomes.isEmpty()) return
        putJsonObject("outcomes") {
            put("admitted", m.admitted)
            put("offered", m.offered)
            putJsonArray("byActivity") {
                m.outcomes.entries
                    .sortedByDescending { (_, row) -> row.values.sum() }
                    .forEach { (activity, row) ->
                        add(
                            buildJsonObject {
                                put("activity", activity.name)
                                put("offered", row.values.sum())
                                // Rows, not a member per reason: a dynamic member name is one
                                // the glossary can never define.
                                putJsonArray("reasons") {
                                    row.entries.sortedByDescending { it.value }.forEach { (reason, n) ->
                                        add(
                                            buildJsonObject {
                                                put("reason", reason)
                                                put("events", n)
                                            },
                                        )
                                    }
                                }
                            },
                        )
                    }
            }
        }
    }

    /**
     * What the engine did, per rank profile: its own time, documents matched against hits served,
     * and how often it degraded.
     */
    private fun kotlinx.serialization.json.JsonObjectBuilder.putEngine(m: CostLedger.Snapshot) {
        if (m.engine.isEmpty()) return
        putJsonArray("engine") {
            m.engine.sortedByDescending { it.engineNanos }.forEach { e ->
                add(
                    buildJsonObject {
                        put("profile", e.profile)
                        put("queries", e.queries)
                        // Decimal milliseconds: the page divides by the query count, and a sub-millisecond
                        // engine would otherwise publish every profile as a flat zero.
                        put("engineMs", e.engineNanos / 1_000_000.0)
                        put("summaryMs", e.summaryNanos / 1_000_000.0)
                        put("docsMatched", e.docsMatched)
                        put("hitsServed", e.hitsServed)
                        put("degraded", e.degraded)
                        put("rungs", e.rungs)
                    },
                )
            }
        }
    }

    /** The instantaneous readings, named apart from every counter so a reader cannot difference them. */
    private fun kotlinx.serialization.json.JsonObjectBuilder.putGauges(m: CostLedger.Snapshot) {
        if (m.gauges.isEmpty()) return
        putJsonArray("gauges") {
            m.gauges.entries.sortedBy { it.key }.forEach { (name, value) ->
                add(
                    buildJsonObject {
                        put("gauge", name)
                        put("value", value)
                    },
                )
            }
        }
    }

    /**
     * `held` is what holds a store mutex at this instant and what it says it is doing; `wait` is
     * cumulative wait per lock stage, split by what was holding when each waiter arrived. A whole
     * wait is charged to that first holder even where the lock changed hands.
     */
    private fun kotlinx.serialization.json.JsonObjectBuilder.putLocks(
        held: List<IngestStats.Held>,
        blocked: Map<String, Map<String, Long>>,
    ) {
        if (held.isEmpty() && blocked.isEmpty()) return
        putJsonObject("locks") {
            if (held.isNotEmpty()) {
                putJsonArray("held") {
                    held.forEach { h ->
                        add(
                            buildJsonObject {
                                put("stage", h.stage)
                                put("heldMs", h.heldForMillis())
                                h.detail?.let { put("doing", it) }
                                h.lock.takeIf { it.isNotEmpty() }?.let { put("mutex", it) }
                            },
                        )
                    }
                }
            }
            if (blocked.isNotEmpty()) {
                putJsonArray("wait") {
                    blocked.entries
                        .map { (stage, row) -> Triple(stage, row, row.values.sum()) }
                        .sortedByDescending { it.third }
                        .forEach { (stage, row, total) ->
                            add(
                                buildJsonObject {
                                    put("stage", stage)
                                    put("ms", total / 1_000_000)
                                    putJsonArray("behind") {
                                        row.entries.sortedByDescending { it.value }.forEach { (holder, nanos) ->
                                            add(
                                                buildJsonObject {
                                                    put("holder", holder)
                                                    put("ms", nanos / 1_000_000)
                                                },
                                            )
                                        }
                                    }
                                },
                            )
                        }
                }
            }
        }
    }

    /**
     * The write path's own stage split, busiest first, with the shape of each stage's time beside
     * its total: one pathological call and a hundred thousand ordinary ones sum the same.
     */
    private fun kotlinx.serialization.json.JsonObjectBuilder.putStages(stages: Map<String, IngestStats.Stage>) {
        if (stages.isEmpty()) return
        putJsonArray("stages") {
            stages.entries
                .sortedByDescending { it.value.totalNanos }
                .forEach { (name, st) ->
                    add(
                        buildJsonObject {
                            put("stage", name)
                            put("ms", st.totalNanos / 1_000_000)
                            // Only where the store timed the stage as calls; a lock's wait/hold pair has no
                            // denominator.
                            if (st.calls > 0) {
                                put("calls", st.calls)
                                put("meanMs", st.meanNanos / 1_000_000.0)
                                put("maxMs", st.maxNanos / 1_000_000.0)
                            }
                        },
                    )
                }
        }
    }

    /**
     * Who and what is driving the load, from a bounded sketch that keeps the heavy hitters and
     * forgets the tail; `error` is the sketch's overestimate bound for the row. Client-derived,
     * see [of].
     */
    private fun kotlinx.serialization.json.JsonObjectBuilder.putHotspots(m: CostLedger.Snapshot) {
        if (m.topObservers.isEmpty() && m.topTerms.isEmpty()) return
        putJsonObject("hotspots") {
            putJsonArray("observers") {
                m.topObservers.forEach { h ->
                    add(
                        buildJsonObject {
                            put("key", h.key)
                            put("weight", h.weight)
                            put("error", h.error)
                        },
                    )
                }
            }
            putJsonArray("terms") {
                m.topTerms.forEach { h ->
                    add(
                        buildJsonObject {
                            put("key", h.key)
                            put("weight", h.weight)
                            put("error", h.error)
                        },
                    )
                }
            }
        }
    }

    /**
     * The reads that beat the slow threshold, newest first, bounded by the store's ring rather
     * than by how many distinct queries exist. Client-derived: `detail` quotes the query.
     */
    private fun kotlinx.serialization.json.JsonObjectBuilder.putSlowReads(m: CostLedger.Snapshot) {
        if (m.slowReads.isEmpty()) return
        putJsonArray("slowReads") {
            m.slowReads.forEach { s ->
                add(
                    buildJsonObject {
                        put("at", s.atMillis / 1000)
                        put("activity", s.activity.name)
                        put("profile", s.profile)
                        put("wallMs", s.wallNanos / 1_000_000)
                        put("engineMs", s.engineNanos / 1_000_000)
                        put("summaryMs", s.summaryNanos / 1_000_000)
                        put("hits", s.hits)
                        put("docsMatched", s.docsMatched)
                        put("detail", s.detail)
                    },
                )
            }
        }
    }
}
