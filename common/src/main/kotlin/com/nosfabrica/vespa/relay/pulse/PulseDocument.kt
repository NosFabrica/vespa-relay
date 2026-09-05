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
 * WHERE THIS PROCESS'S STORE RESOURCES GO, as one document — the store's
 * `metrics()` and the ingest pipeline's stage split, rendered for the pulse
 * page.
 *
 * COUNTERS AND GAUGES, NEVER RATES. Every total here is cumulative since the
 * process started, so the page differences two consecutive polls to recover a
 * rate and any number of readers may poll without consuming anything. The one
 * exception is `gauges`, which are instantaneous by nature (a queue depth,
 * calls in flight) and which a reader must never difference — they are named
 * apart for exactly that reason. See docs/telemetry.md §4 and §10.1 in the
 * store.
 *
 * BUILT ON DEMAND, not on a rollup clock. Unlike `/stats.json`, whose numbers
 * cost Vespa queries, everything here is a read of in-process counters: 96
 * ordinal-indexed slots, a handful of histograms and a few gauge lambdas.
 * There is no rollup thread and no `stale` member, because there is nothing
 * that can go stale — a document that answers at all is current as of the
 * request.
 *
 * PER PROCESS, NOT PER CLUSTER. The relay and the mirror hold separate stores
 * over one Vespa, so each has its own ledger and its own page; the mirror's
 * ingest does not appear on the relay's. Vespa's own resource use — memory,
 * disk, transaction log — is not here at all: it has a metrics proxy that
 * already reports it, and a second, worse source of truth for it would be the
 * wrong thing to build (telemetry.md §9).
 */
object PulseDocument {
    /** Bumped when a member changes meaning; the page says so rather than misreading it. */
    const val SCHEMA = 1

    /**
     * The document.
     *
     * [clientDerived] carries the two sections that describe the people using
     * this relay rather than the relay itself — which observer lenses and
     * which search terms are driving the load, and the slow-read log, whose
     * `detail` quotes the query. They are the most useful sections an operator
     * has and they must never be served where `/stats.json` is served: that
     * document's rule is that every field is a fact about stored events and
     * nothing about clients belongs in it. Off unless the caller says
     * otherwise, so the unsafe direction is never the default.
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
     * A reader bound to one store, for the route to call per request.
     *
     * [startedAtMillis] is WHEN THE STORE'S COUNTERS BEGAN, and the caller must
     * say: the page states every total as cumulative over `uptimeSeconds`, and
     * defaulting it to the moment this reader was built would name a window
     * shorter than the one the counters actually cover — by two minutes on a
     * relay that deployed a schema first. It is stamped once rather than read
     * per call so the window only ever grows.
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
            // Said in the document rather than inferred from a missing member:
            // "this build serves no client sections" and "nobody has searched
            // yet" produce the same empty arrays and are different facts.
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
     * Port calls, grouped by the ACTIVITY that made them — the model's first
     * question, "what was the store doing", answered before "which method did
     * it call". `callsPerDoc` is the ratio the store's own contract is written
     * in ("never ingest in a loop over insert()"), computed here rather than
     * by the page because the two numbers it divides come off one slot.
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
                                            // Absent, not zero, where no histogram is kept for this
                                            // call shape: a bulk put reporting "p50 0.00ms" reads as
                                            // instant when it means unmeasured (telemetry.md §14.4).
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
     * What became of the events this process was offered, per activity and per
     * reason, over the denominator that makes them a rate. "81% of what this
     * node is offered is already stored" is the number that tells an operator
     * to narrow a sync, and it is the one thing no port-level counter can see:
     * a refused event never reaches the index at all.
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
                                // Rows, not a member per reason: the reason set is
                                // the store's closed one, but a dynamic member name
                                // is one this document's glossary can never define.
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
     * What the ENGINE did, per rank profile: its own time (which is not our
     * wall time), how many documents it matched against how many it served,
     * and how often it degraded. `matched` far above `served` is a query doing
     * work the client never sees; the two are the recall-versus-page picture
     * the trust gate moves most.
     */
    private fun kotlinx.serialization.json.JsonObjectBuilder.putEngine(m: CostLedger.Snapshot) {
        if (m.engine.isEmpty()) return
        putJsonArray("engine") {
            m.engine.sortedByDescending { it.engineNanos }.forEach { e ->
                add(
                    buildJsonObject {
                        put("profile", e.profile)
                        put("queries", e.queries)
                        // Milliseconds as a DECIMAL, not an integer. The page
                        // divides these by the query count, and a store whose
                        // engine answers in under a millisecond would otherwise
                        // publish every profile as a flat zero — the same
                        // precision the health loop used to lose rounding
                        // `%.2fs`, one boundary further on.
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

    /**
     * The instantaneous readings, named apart from every counter above so a
     * reader cannot difference them into nonsense. Pulled at snapshot time, so
     * they cost nothing until this document is built.
     */
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
     * THE PRESENT TENSE AND ITS CAUSE. `held` is what holds a store mutex at
     * this instant and what it says it is doing; `wait` is cumulative wait per
     * lock stage split by what was holding when each waiter arrived.
     *
     * The split is the point. `lock.ingest.wait 41s` only prompts a question;
     * `38.4s of it behind "derive 500 subject(s) in 10 chunk(s)"` names a fix.
     * First-holder attribution, and the page says so: over a long wait the
     * lock may change hands, and all of that wait is charged to whoever held
     * it when the waiter arrived.
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
     * The write path's own stage split, busiest first, with the shape of each
     * stage's time beside its total. The same rows the mirror's status page
     * draws, on the same read: one pathological call and a hundred thousand
     * ordinary ones sum the same and need different fixes.
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
                            // Only where the store timed the stage as calls: a lock's
                            // wait/hold pair is booked from a duration measured
                            // elsewhere, and a mean over no denominator is a fiction.
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
     * WHO AND WHAT IS DRIVING THE LOAD — the dimension a metrics system is
     * normally forbidden (an observer pubkey and a search term are both
     * unbounded key spaces), made safe by a bounded sketch: weighted
     * Space-Saving over a fixed number of slots, which keeps the heavy hitters
     * and forgets the tail. `error` is the sketch's own overestimate bound for
     * that row, published rather than hidden: a row whose error approaches its
     * weight is a row that may not belong in the list at all.
     *
     * CLIENT-DERIVED. See [of].
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
     * The reads that beat the slow threshold, newest first: wall time against
     * the engine's own, what was matched against what was served, and the
     * store's own sentence about the query. A ring, so this is bounded by the
     * ring and never by how many distinct queries exist — which is what keeps
     * a retained query string inside the cardinality rule.
     *
     * CLIENT-DERIVED, and the most obviously so: `detail` quotes the query.
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
