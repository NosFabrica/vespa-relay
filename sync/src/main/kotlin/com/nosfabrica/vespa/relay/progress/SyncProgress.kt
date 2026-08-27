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
 * WHAT EACH STREAM IS DOING RIGHT NOW, as this process's own status site draws it.
 *
 * ## Why this file exists beside the other three
 *
 * The router already writes bands (what has been COVERED), sweep cursors (where
 * a reconcile is mid-window) and a manifest (what it is CONFIGURED to mirror).
 * None of them can answer the question an operator actually asks first — is it
 * working, and what happened to everything it took on this cycle. That state
 * lived only in `StreamPhases`' log lines, i.e. in whatever a container's stderr
 * had not yet rotated away.
 *
 * The consequences were the ones a missing heartbeat always has. A stream that
 * finished its cycle and a stream that died mid-fan-out looked identical from
 * `/stats.json`, because neither says anything there. A fan-out that discovered
 * 16,752 relays and left 5,323 with a band published no account of the other
 * ~11,400. And a rollup that ran while the router was down published the same
 * card as one that ran while it was busy.
 *
 * So: same terms as the manifest — the router is the only writer, the relay's
 * read is best-effort, nothing here may cost either process anything (see
 * `SyncCoverageReport` on the relay side for the full argument). Unlike the
 * manifest this is STATE, so it is rewritten on the progress tick rather than
 * once at boot; unlike the bands it is small and bounded by the stream count,
 * so rewriting it every tick costs nothing worth measuring.
 *
 * ## The shape
 *
 * ```json
 * {
 *   "streams": [
 *     {
 *       "name": "content",
 *       "phase": "fetching",
 *       "phaseForSec": 412,
 *       "inFlight": {"relays": [{"relay": "wss://slow.example/", "heldForSec": 41400,
 *                                "transferringForSec": 41390, "events": 2, "quietForSec": 41000,
 *                                "doing": "catching up (paging)", "pool": "catching-up",
 *                                "pagingUntil": 1689857148}],
 *                    "omitted": 118},
 *       "cycle": {
 *         "number": 12, "owner": "dynamic", "startedAt": 1769999000, "outcome": "running",
 *         "urls":  {"discovered": 16752, "foldedOntoAnother": 11429, "refusedUnstable": 12, "excluded": 0, "taken": 5311},
 *         "hosts": 850,
 *         "taken": {"delivered": 2200, "nothingNew": 900, "unreachable": 800,
 *                   "transferFailed": 100, "noRoute": 1100, "hostStruckOut": 200,
 *                   "knownDead": 100, "torUnavailable": 0, "busy": 12, "pending": 23},
 *         "foldedOnto": {"relays": [{"relay": "wss://nostr.oxtr.dev/", "urls": 55,
 *                                    "examples": ["wss://nostr.oxtr.dev/alpha"]}],
 *                        "omitted": 480},
 *         "balanced": true,
 *         "received": 481203
 *       },
 *       "passes": [{"number": 11, "outcome": "completed", "…": "the walk before it, still finishing"},
 *                  {"number": 12, "outcome": "running",   "…": "the one `cycle` carries"}]
 *     },
 *     {"name": "visits", "phase": "rotating", "phaseForSec": 3480, "roster": 412, "tails": 300}
 *   ],
 *   "live": {"relays": [{"relay": "wss://nos.lol/", "heldForSec": 41400, "transferringForSec": 41400,
 *                        "events": 91002, "quietForSec": 3, "doing": "holding a live tail",
 *                        "pool": "live"}],
 *            "omitted": 0},
 *   "processors": [
 *     {"name": "aliasFold", "phase": "idle", "phaseForSec": 400, "passesRun": 3,
 *      "lastPassAt": 1769998000, "lastPassSec": 812, "nextInSec": 20800,
 *      "sourced": 17584, "heldOutDead": 832,
 *      "streams": [{"name": "all streams", "candidates": 16752, "newUrls": 4139, "foldedAway": 0,
 *                   "consistent": 0, "inconsistent": 0, "unmeasured": 4021,
 *                   "dialled": 2000, "decided": 118,
 *                   "undecided": {"reasons": [{"reason": "cooling down from an earlier failed pass",
 *                                              "urls": 0, "hosts": 214, "examples": ["relay.example"]}],
 *                                 "omitted": 0}}]},
 *     {"name": "consistency", "phase": "measuring", "phaseForSec": 400, "passesRun": 3,
 *      "lastPassAt": 1769998000, "lastPassSec": 9720,
 *      "measuring": {"unit": "url", "attempted": 604, "toProbe": 4728, "etaSec": 2724,
 *                    "quietForSec": 3},
 *      "inFlight": {"relays": [{"relay": "wss://slow.example/", "heldForSec": 214,
 *                               "stage": "paired walk"}], "omitted": 0},
 *      "sourced": 17584, "heldOutDead": 832,
 *      "streams": [{"name": "all streams", "candidates": 16752, "foldedAway": 11429,
 *                   "consistent": 583, "inconsistent": 12, "unmeasured": 4728,
 *                   "dialled": 4728, "decided": 74,
 *                   "undecided": {"reasons": [{"reason": "never answered a REQ", "urls": 3902, "hosts": 2201,
 *                                              "top": [{"host": "dead.example", "urls": 12}]}],
 *                                 "omitted": 0}}]},
 *     {"name": "ingest", "phase": "running", "queued": 12, "capacity": 20000,
 *      "accepted": 3910233, "rejected": 41002}
 *   ]
 * }
 * ```
 *
 * `passes` is the half `cycle` could not say. A walk ends when its last url is
 * handed out, not when its last worker returns, so a rotation normally has the
 * previous pass finishing while the new one walks — and with one slot to
 * describe them in, the old pass simply stopped being published the moment the
 * new one opened. `processors` is the work that is not a stream at all: the
 * alias fold, the stability gate, the NIP-66 monitor, ingest, the healer, the
 * push. See [Processors].
 *
 * `measuring` is the live position of a probe pass and the only member of a
 * processor row that moves while one runs — every other number there describes
 * the pass that ENDED. On a sweep it stands where `nextInSec` would be, because
 * a countdown to the next pass is a promise nobody has computed until this one
 * returns; a fast-lane pass carries both, and both are true. See
 * [Processors.Measuring].
 *
 * `quietForSec` beside it is what separates a pass about to finish from one that
 * has stopped: `etaSec` is honest arithmetic on the rate so far, so a pass whose
 * last url has wedged reports `0` and every number on the row agrees with every
 * other one. A processor's `inFlight` is the same disclosure a stream's is —
 * WHICH urls it is holding rather than how many — with its own shape and its own
 * order, longest-held first, because a probe leg is bounded by construction and
 * a long one is the anomaly. See [Processors.Holding].
 *
 * `pool` on a held row and `live` at the root are the two halves of one
 * answer: WHICH OF THE MIRROR'S FOUR WORKLOADS each relay is in. One rotating
 * pool runs all of them, so `visiting` counted a catch-up, a history audit and
 * a whole-corpus re-walk as one number while `tails` counted the fourth and
 * named nobody. `pool` is the machine word rows are grouped by — `live`,
 * `catching-up`, `re-fetching`, `auditing`, and absent for a visit between
 * them, which is drawn under its own `doing` rather than dropped. `live` is
 * the fourth list itself: the tails are pool-wide by construction (one
 * subscription per relay carries every wanting stream's filter), so they sit at
 * the root rather than being divided per stream, and the stream rows keep the
 * `tails` COUNT that is their own share. See `VisitPool.POOL_LIVE` and its
 * neighbours.
 *
 * There is NO heartbeat member, and there used to be. This document was a file
 * on a volume the serving relay read, so it had to carry a `writtenAt` the
 * reader turned into a `staleForSec`: a file cannot say whether the process
 * writing it still exists, and a mirror down for a day published exactly the
 * card a mirror mid-cycle did. The document is served by the process that
 * builds it now, so the question is answered by whether the request answers,
 * and every timestamp left in here says when something HAPPENED.
 *
 * `urls` and `taken` are a PARTITION and the members are chosen to sum — see
 * the pass tallies for the two identities and for why `pending` was derived. `balanced`
 * is the writer's own check on them, published rather than asserted.
 */
class SyncProgress {
    /**
     * WHERE THE CONSTRAINT IS, and the numbers behind it.
     *
     * The router works this out every sixty seconds and prints it: a full ingest
     * queue and an empty one are opposite diagnoses that look identical from
     * every other number, and the pair is what tells them apart. It reached
     * stderr and stopped there, so the one question this whole document exists
     * to answer — why is it slow — was the one thing it did not carry.
     *
     * These are facts about the PROCESS rather than about any stream, which is
     * why they sit at the document's root beside the heartbeat.
     */
    class Health(
        /**
         * `ingest` / `downloads` / `upstream` / `mixed` — read the router's own
         * words in `SyncEngine.healthLoop`, which is where this is decided.
         */
        val bottleneck: String,
        /** Events reaching ingest per second, averaged over the last minute. */
        val eventsPerSec: Int,
        val heapUsedMb: Long,
        val heapMaxMb: Long,
        /** Websockets open, against the dispatcher budget that is the real concurrency ceiling. */
        val sockets: Int,
        val socketCeiling: Int,
        /**
         * The relay's mean client read latency, which this router YIELDS to.
         *
         * A mirror that is deliberately slowing down and one that is stuck look
         * the same from throughput alone, and only this number tells them apart.
         * Null where no pressure feed is configured.
         */
        val servingMs: Long?,
    )

    /**
     * The last document published, or null before the first tick.
     *
     * Read by the status site on the same heap that wrote it. This used to be a
     * FILE the serving relay read off a shared volume, which is why the document
     * carried a `writtenAt` heartbeat: a file cannot say whether the process
     * writing it is still running, so the reader had to infer it from a
     * timestamp that stopped advancing. Nothing infers it now — the page is
     * served by this process, so a page that renders is a process that is alive.
     *
     * Volatile rather than locked: one writer on the progress tick, readers on
     * Netty threads, and the reference is swapped whole.
     */
    @Volatile
    var latest: JsonObject? = null
        private set

    /**
     * Publish [streams] as the current document.
     */
    fun publish(
        streams: List<StreamPhases.Stream>,
        processors: List<Processors.Snapshot> = emptyList(),
        /** Where the constraint is, and the numbers behind it — see [Health]. */
        health: Health? = null,
        /**
         * THE LIVE POOL — every relay holding a tail right now, pool-wide.
         *
         * Beside the streams rather than inside them because a tail is not a
         * stream's: one subscription per relay carries every wanting stream's
         * filter and its arrivals are counted at the url. See
         * `VisitPool.livePool`.
         */
        live: InFlight? = null,
        /**
         * VirtualMachineErrors this process has survived — an OutOfMemoryError
         * kills whichever thread allocates next and is caught by nobody, so the
         * router carries on looking merely quiet. Four of them once passed
         * unnoticed. It reached a stderr line and stopped there.
         */
        fatals: Long = 0,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ) {
        latest = document(streams, processors, fatals, health, live, nowSeconds)
    }

    companion object {
        // NOT pretty-printed, unlike the manifest. That one is a handful of
        // lines an operator reads by hand; this is rewritten every tick and is
        // read by the relay, and the newlines would be the larger half of it.
        private val json = Json

        /** The document, pure, so it can be asserted without a filesystem. */
        fun document(
            streams: List<StreamPhases.Stream>,
            processors: List<Processors.Snapshot> = emptyList(),
            fatals: Long = 0,
            health: Health? = null,
            live: InFlight? = null,
            nowSeconds: Long,
        ): JsonObject =
            buildJsonObject {
                // Always, including zero: "no thread has been killed" is the
                // claim worth publishing, and a member that appears only on
                // damage cannot be distinguished from a router too old to say.
                put("fatals", fatals)
                // The constraint, at the root: it is a fact about the process,
                // and every stream's slowness is downstream of it.
                health?.let { h ->
                    put(
                        "health",
                        buildJsonObject {
                            put("bottleneck", h.bottleneck)
                            put("eventsPerSec", h.eventsPerSec)
                            put("heapUsedMb", h.heapUsedMb)
                            put("heapMaxMb", h.heapMaxMb)
                            put("sockets", h.sockets)
                            put("socketCeiling", h.socketCeiling)
                            h.servingMs?.let { put("servingMs", it) }
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
                                // WHAT THE PHASE KNOWS, flat beside the word it
                                // qualifies rather than in a container of its
                                // own: both members are about the phase and
                                // nothing else, and a wrapper would be a name
                                // to look up for no gain.
                                //
                                // A rotating stream's whole state, and the one
                                // phase that published nothing at all until it
                                // was added: the card drew `rotating for 58m`
                                // beside a stream riding four hundred relays,
                                // and beside one riding none.
                                s.roster?.let { put("roster", it) }
                                s.tails?.let { put("liveHeld", it) }
                                // WHICH relays are running, beside the cycle
                                // rather than inside it: a worker outlives the
                                // pass that handed it out, so this set spans
                                // passes and the cycle's `pending`/`busy` split
                                // it between them. It is what those counts never
                                // said — a stream held on two relays for eleven
                                // hours published the number 2 and no url. See
                                // [InFlight], and note it is not "the pending
                                // urls": the walk has not reached some of them.
                                s.inFlight?.takeIf { it.relays.isNotEmpty() }?.let { put("inFlight", held(it)) }
                                // WHAT THIS STREAM MAY SPEND on each of the
                                // pool's jobs, and what it has spent. Omitted
                                // on a router with no caps machinery at all,
                                // rather than published as an empty array —
                                // an empty list is a claim that this stream
                                // has no jobs.
                                // WHEN THE SCHEDULED RE-READS COME DUE — the
                                // half `auditsRun` could never say. Omitted
                                // for a stream that schedules neither, which
                                // is a stream that never re-reads its past.
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
                                                    // Absent when nothing is waiting — there is
                                                    // no next one to count down to, and a 0
                                                    // would read as "due now".
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
                                                    // Absent where nothing caps it: null is
                                                    // "bounded by the dial width alone", and a
                                                    // 0 would read as "may do none of this".
                                                    l.cap?.let { put("streamCap", it) }
                                                    l.inUse?.let { put("inUse", it) }
                                                    // ALWAYS, including zero: a cap that has
                                                    // turned nothing away is the claim worth
                                                    // publishing, and a member appearing only
                                                    // on damage cannot be told from a router
                                                    // too old to say.
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
                // THE LIVE POOL, at the ROOT and not under a stream: a tail
                // carries every wanting stream's filter and counts its
                // arrivals at the url, so dividing it per stream would publish
                // one undivided number once per stream. Omitted entirely when
                // nothing is tailed — a router holding no tails and one too old
                // to say are told apart the same way every other absent member
                // here is, by the rest of the document.
                live?.takeIf { it.relays.isNotEmpty() }?.let { put("live", held(it)) }
                // THE WORK THAT IS NOT A STREAM — see [Processors]. Omitted
                // entirely when nothing registered, rather than published as an
                // empty array: an empty list is a claim that this router runs
                // none of them, and a router built before this existed makes no
                // such claim.
                processors.takeIf { it.isNotEmpty() }?.let { rows ->
                    putJsonArray("processors") { for (p in rows) add(Processors.published(p)) }
                }
            }

        /**
         * A LIST OF HELD RELAYS, wherever it is published from — a stream's
         * `inFlight` and the root's `live` alike.
         *
         * One builder because they are one shape, deliberately: the live pool's
         * rows carry the same clocks, the same `doing` sentence and the same
         * `pool` word as a visiting leg's, so the page reads both with one
         * renderer and the glossary defines each member once. Two builders
         * would be two places for a member to be forgotten in, and the one that
         * would be forgotten is `omitted` — the promise that a list says what
         * it left out.
         */
        private fun held(f: InFlight): JsonObject =
            buildJsonObject {
                putJsonArray("relays") {
                    for (r in f.relays) {
                        add(
                            buildJsonObject {
                                put("relay", r.relay)
                                // WHOSE row it is, on a list that is not
                                // already one stream's — the root `live` list.
                                // Absent inside a stream's own `inFlight`,
                                // where it would repeat the row above it.
                                r.stream?.let { put("stream", it) }
                                put("heldForSec", r.heldForSec)
                                // Absent when the worker has no transfer slot —
                                // in the guards, or queued behind other legs,
                                // which is where most of a fan-out's workers
                                // are.
                                r.transferringForSec?.let { put("transferringForSec", it) }
                                // The pair that separates a real backlog from a
                                // walk that will not end — and, on a live row, a
                                // relay with nothing to say from a tail that has
                                // silently died.
                                put("events", r.events)
                                put("quietForSec", r.quietForSec)
                                // WHAT IT IS DOING. Absent until the leg reaches
                                // a stage worth a word, which is a state and not
                                // a gap.
                                r.stage?.let { put("doing", it) }
                                // …and WHICH POOL that puts it in, which is the
                                // half a reader may group by. Absent for a visit
                                // in none of the four; see [InFlight.Relay.pool].
                                r.pool?.let { put("pool", it) }
                                // Absent when no walk is running.
                                r.pagingUntil?.let { put("pagingUntil", it) }
                            },
                        )
                    }
                }
                // Never silent, for the same reason as the fold's.
                put("omitted", f.omitted)
            }

        /**
         * `SYNC_PROGRESS_FILE` named where this document was WRITTEN, for the
         * serving relay to read off a shared volume. It is gone: the sync
         * process serves the document itself now, on its own status site, so a
         * path here would be a file nobody ever opens.
         *
         * Refused rather than ignored, the way every removed setting in this
         * repo is. A router configured with it is a deployment expecting the
         * relay's `/stats.html` to carry a sync card, and that card has moved —
         * silently accepting the value would leave the operator watching a
         * panel that is never going to appear.
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
