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
package com.nosfabrica.vespa.relay.progress

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

/**
 * The progress document: what each stream and processor is doing right now,
 * rebuilt on the progress tick and served by this process's own status site.
 *
 * `health` and `store` sit at the root because they are facts about the
 * process, not about any stream. `live` is at the root because it is one
 * table of the pool's steady state; every row names its `stream`, and `pool`
 * on a held row says which of the mirror's four workloads the relay is in.
 * There is no heartbeat member: the process that serves the document is the
 * one that builds it. A member that would only appear on damage is published
 * at zero too, so it can be told from a router too old to say.
 */
class SyncProgress {
    /** Where the constraint is, and the numbers behind it. */
    class Health(
        /** `ingest` / `downloads` / `upstream` / `mixed`, decided in `SyncEngine.healthLoop`. */
        val bottleneck: String,
        /** Events leaving ingest per second over the last minute; the drain side of the queue. */
        val eventsPerSec: Int,
        /** Events handed to ingest per second over the same minute; the arrival side. */
        val arrivingPerSec: Int,
        /**
         * Every ingest stage since boot, busiest first: the cumulative
         * milliseconds AND the shape of that time — calls, mean, worst single
         * call. One pathological call and a hundred thousand ordinary ones sum
         * the same and need different fixes.
         *
         * Cumulative because `IngestStats.statusLine` is destructive; the page
         * differences consecutive polls to recover a rate. One list rather
         * than a total list beside a detail list: they came off one
         * `IngestStats.snapshot()` at one instant, and two lists invited a row
         * whose `ms` and `calls` were read seconds apart.
         */
        val stageDetail: List<StageDetail> = emptyList(),
        /**
         * What holds the store's write lock AT THIS INSTANT — null when
         * nothing does. The stages are a history; this is the present tense,
         * which is what an operator wants while ingest is stalled.
         */
        val lockHeld: LockHeld? = null,
        /** The store's own feed-health line, quoted verbatim rather than parsed. */
        val feed: String? = null,
        val heapUsedMb: Long,
        val heapMaxMb: Long,
        /** Websockets open, against the dispatcher budget. */
        val sockets: Int,
        val socketCeiling: Int,
        val socketsRunning: Int,
        /** Calls OkHttp is holding behind the socket budget; the only direct evidence the budget is the constraint. */
        val socketsQueued: Int,
        /** The relay's mean client read latency, which this router yields to. Null with no pressure feed. */
        val servingMs: Long?,
    )

    /**
     * One stage's time with its shape: [ms] over [calls] calls, worst single
     * call [maxMs]. `calls = 0` means the stage was booked from a duration
     * measured elsewhere (a lock's wait/hold pair), where a mean would be a
     * fiction — so the page shows none.
     */
    class StageDetail(
        val stage: String,
        val ms: Long,
        val calls: Long,
        val meanMs: Long,
        val maxMs: Long,
    )

    /**
     * The store's write lock in the present tense: which stage holds it, how
     * long for, and the holder's own sentence about the work ([detail],
     * quoted rather than parsed).
     */
    class LockHeld(
        val stage: String,
        val heldMs: Long,
        val detail: String?,
    )

    /**
     * The last document published, or null before the first tick. One writer
     * on the progress tick, readers on Netty threads, swapped whole.
     */
    @Volatile
    var latest: JsonObject? = null
        private set

    fun publish(
        streams: List<StreamPhases.Stream>,
        processors: List<Processors.Snapshot> = emptyList(),
        health: Health? = null,
        /** Every relay holding a tail right now; see `VisitPool.livePool`. */
        live: InFlight? = null,
        /** VirtualMachineErrors this process has survived; an OOM kills one thread and is caught by nobody. */
        fatals: Long = 0,
        /** Null on a process with no registry installed, which publishes no section rather than an empty one. */
        store: StoreCalls.Snapshot? = null,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ) {
        latest = document(streams, processors, fatals, health, live, store, nowSeconds)
    }

    companion object {
        private val json = Json

        /** The document, pure, so it can be asserted without a filesystem. */
        fun document(
            streams: List<StreamPhases.Stream>,
            processors: List<Processors.Snapshot> = emptyList(),
            fatals: Long = 0,
            health: Health? = null,
            live: InFlight? = null,
            store: StoreCalls.Snapshot? = null,
            nowSeconds: Long,
        ): JsonObject =
            buildJsonObject {
                put("fatals", fatals)
                health?.let { h ->
                    put(
                        "health",
                        buildJsonObject {
                            put("bottleneck", h.bottleneck)
                            put("eventsPerSec", h.eventsPerSec)
                            put("arrivingPerSec", h.arrivingPerSec)
                            put("heapUsedMb", h.heapUsedMb)
                            put("heapMaxMb", h.heapMaxMb)
                            put("sockets", h.sockets)
                            put("socketCeiling", h.socketCeiling)
                            put("socketsRunning", h.socketsRunning)
                            put("socketsQueued", h.socketsQueued)
                            h.servingMs?.let { put("servingMs", it) }
                            h.feed?.takeIf { it.isNotBlank() }?.let { put("feed", it) }
                            // The present-tense holder goes FIRST, above the
                            // history: when ingest is stalled this is the row
                            // that answers it, and a reader should not have to
                            // scroll a cumulative table to learn the gate is
                            // held right now by something else.
                            h.lockHeld?.let { held ->
                                put(
                                    "lockHeldBy",
                                    buildJsonObject {
                                        put("stage", held.stage)
                                        put("heldMs", held.heldMs)
                                        held.detail?.let { put("doing", it) }
                                    },
                                )
                            }
                            // Rows, not a member per stage: the names are the
                            // store's, and a dynamic member name is one the
                            // glossary can never define.
                            if (h.stageDetail.isNotEmpty()) {
                                putJsonArray("stages") {
                                    for (d in h.stageDetail) {
                                        add(
                                            buildJsonObject {
                                                put("stage", d.stage)
                                                put("ms", d.ms)
                                                // Only where the store timed the stage as
                                                // calls: a lock's wait/hold pair has no call
                                                // count, and inventing one would put a mean
                                                // over a denominator that does not exist.
                                                if (d.calls > 0) {
                                                    put("calls", d.calls)
                                                    put("meanMs", d.meanMs)
                                                    put("maxMs", d.maxMs)
                                                }
                                            },
                                        )
                                    }
                                }
                            }
                        },
                    )
                }
                putJsonArray("streams") {
                    for (s in streams) {
                        add(
                            buildJsonObject {
                                put("name", s.name)
                                put("phase", s.phase)
                                put("phaseForSec", s.phaseForSec)
                                s.roster?.let { put("roster", it) }
                                s.tails?.let { put("liveHeld", it) }
                                // The same word the pool's own row uses for the same quantity.
                                s.queued?.let { put("awaitingVisit", it) }
                                s.inFlight?.takeIf { it.relays.isNotEmpty() }?.let { put("inFlight", held(it)) }
                                // `schedule` and `limits` are omitted rather
                                // than empty: an empty list would claim the
                                // stream has no such jobs.
                                s.schedule.takeIf { it.isNotEmpty() }?.let { rows ->
                                    putJsonArray("schedule") {
                                        for (r in rows) {
                                            add(
                                                buildJsonObject {
                                                    put("job", r.job)
                                                    put("everySec", r.everySec)
                                                    put("due", r.due)
                                                    put("neverRun", r.neverRun)
                                                    put("waiting", r.waiting)
                                                    // Absent when nothing is waiting; a 0 would read as "due now".
                                                    r.nextInSec?.let { put("nextInSec", it) }
                                                },
                                            )
                                        }
                                    }
                                }
                                s.limits.takeIf { it.isNotEmpty() }?.let { rows ->
                                    putJsonArray("limits") {
                                        for (l in rows) {
                                            add(
                                                buildJsonObject {
                                                    put("job", l.job)
                                                    // Absent where nothing caps it; a 0 would read as "may do none".
                                                    l.cap?.let { put("streamCap", it) }
                                                    l.inUse?.let { put("inUse", it) }
                                                    put("deferred", l.deferred)
                                                },
                                            )
                                        }
                                    }
                                }
                            },
                        )
                    }
                }
                live?.takeIf { it.relays.isNotEmpty() }?.let { put("live", held(it)) }
                // Omitted when nothing registered: an empty array would claim
                // this router runs no processors.
                processors.takeIf { it.isNotEmpty() }?.let { rows ->
                    putJsonArray("processors") { for (p in rows) add(Processors.published(p)) }
                }
                store?.let { put("store", storeCalls(it)) }
            }

        /**
         * The store section; see [StoreCalls]. Every count is written with
         * its zeroes so `issued = returned + failed + cancelled + outstanding`
         * and the age bands summing to `outstanding` can always be checked.
         */
        private fun storeCalls(s: StoreCalls.Snapshot): JsonObject =
            buildJsonObject {
                put("outstanding", s.outstanding)
                // The operator's own threshold (`SYNC_STORE_SLOW_SEC`), so the
                // page marks rows by it rather than a copy of the default.
                put("slowAfterSec", s.slowAfterSec)
                put("issued", s.issued)
                put("returned", s.returned)
                put("failed", s.failed)
                put("cancelled", s.cancelled)
                s.calls.takeIf { it.isNotEmpty() }?.let { rows ->
                    putJsonArray("calls") {
                        for (c in rows) {
                            add(
                                buildJsonObject {
                                    put("caller", c.caller)
                                    put("op", c.op)
                                    // A summary of the ask, never the ask; see [StoreCalls.Call.asked].
                                    c.asked?.let { put("asked", it) }
                                    put("issuedAt", c.issuedAt)
                                    put("elapsedSec", c.elapsedSec)
                                    put("outstandingAtIssue", c.outstandingAtIssue)
                                },
                            )
                        }
                    }
                }
                put("omitted", s.omitted)
                s.callers.takeIf { it.isNotEmpty() }?.let { rows ->
                    putJsonArray("callers") {
                        for (c in rows) {
                            add(
                                buildJsonObject {
                                    put("caller", c.caller)
                                    put("issued", c.issued)
                                    put("returned", c.returned)
                                    put("failed", c.failed)
                                    put("cancelled", c.cancelled)
                                    put("outstanding", c.outstanding)
                                    c.oldestOutstandingSec?.let { put("oldestOutstandingSec", it) }
                                },
                            )
                        }
                    }
                }
                s.ages.takeIf { rows -> rows.any { it.calls > 0 } }?.let { rows ->
                    putJsonArray("ages") {
                        for (a in rows) {
                            add(
                                buildJsonObject {
                                    put("fromSec", a.fromSec)
                                    put("calls", a.calls)
                                },
                            )
                        }
                    }
                }
            }

        /**
         * A list of held relays, one shape for a stream's `inFlight` and the
         * root's `live`, so the page reads both with one renderer.
         */
        private fun held(f: InFlight): JsonObject =
            buildJsonObject {
                putJsonArray("relays") {
                    for (r in f.relays) {
                        add(
                            buildJsonObject {
                                put("relay", r.relay)
                                // Only on the root `live` list; inside a stream's own `inFlight` it would repeat the row above.
                                r.stream?.let { put("stream", it) }
                                put("heldForSec", r.heldForSec)
                                // Absent when the worker has no transfer slot.
                                r.transferringForSec?.let { put("transferringForSec", it) }
                                put("events", r.events)
                                put("quietForSec", r.quietForSec)
                                r.stage?.let { put("doing", it) }
                                // Absent for a visit in none of the four pools; see [InFlight.Relay.pool].
                                r.pool?.let { put("pool", it) }
                                r.pagingUntil?.let { put("pagingUntil", it) }
                            },
                        )
                    }
                }
                put("omitted", f.omitted)
            }

        /**
         * `SYNC_PROGRESS_FILE` named where this document was written for the
         * relay to read. Refused rather than ignored, like every removed
         * setting: a router configured with it expects a card that has moved.
         */
        fun refuseRemovedEnv(env: Map<String, String>) {
            env["SYNC_PROGRESS_FILE"]?.trim()?.takeIf { it.isNotEmpty() }?.let {
                error(
                    "SYNC_PROGRESS_FILE is no longer read — the sync process serves its own status page now " +
                        "(SYNC_STATUS_PORT, default 7778). Remove it, and read the mirror at http://<sync-host>:7778/.",
                )
            }
        }
    }
}
