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
package com.nosfabrica.vespa.relay.router.progress

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
 * WHAT EACH STREAM IS DOING RIGHT NOW, written where the relay can read it.
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
 *   "writtenAt": 1770000000,
 *   "streams": [
 *     {
 *       "name": "content",
 *       "phase": "fetching",
 *       "phaseForSec": 412,
 *       "inFlight": {"relays": [{"relay": "wss://slow.example/", "heldForSec": 41400,
 *                                "transferringForSec": 41390, "events": 2, "quietForSec": 41000,
 *                                "doing": "paging", "pagingUntil": 1689857148}],
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
 *     }
 *   ],
 *   "processors": [
 *     {"name": "aliasFold", "phase": "idle", "phaseForSec": 400, "passesRun": 3,
 *      "lastPassAt": 1769998000, "lastPassSec": 812, "nextInSec": 20800,
 *      "sourced": 17584, "heldOutDead": 832,
 *      "streams": [{"name": "all streams", "candidates": 16752, "foldedAway": 0,
 *                   "consistent": 0, "inconsistent": 0, "unmeasured": 4021,
 *                   "dialled": 2000, "decided": 118,
 *                   "undecided": {"reasons": [{"reason": "cooling down from an earlier failed pass",
 *                                              "urls": 0, "hosts": 214, "examples": ["relay.example"]}],
 *                                 "omitted": 0}}]},
 *     {"name": "consistency", "phase": "idle", "phaseForSec": 400, "passesRun": 3,
 *      "lastPassAt": 1769998000, "lastPassSec": 9720, "nextInSec": 11880,
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
 * `writtenAt` is the HEARTBEAT and is the most load-bearing member here: it is
 * rewritten on every tick whatever the streams are doing, so a reader can tell a
 * quiet router from a stopped one — which nothing on the other side could do
 * before. Every other timestamp in this document says when something happened;
 * this one says the process was alive to say so.
 *
 * `urls` and `taken` are a PARTITION and the members are chosen to sum — see
 * [CycleTally] for the two identities and for why `pending` is derived. `balanced`
 * is the writer's own check on them, published rather than asserted.
 */
class SyncProgress(
    /** Where the document is written; null publishes nothing — see [write]. */
    private val file: File?,
) {
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

    /** Whether this router publishes progress at all, i.e. whether `SYNC_PROGRESS_FILE` named a path. */
    val publishes: Boolean get() = file != null

    /**
     * Write [streams] out. Returns whether anything was written.
     *
     * Never throws, and — unlike the manifest — never LOGS on failure either.
     * This runs on the progress tick, so a read-only volume would otherwise mint
     * an error line every thirty seconds forever, burying the phase report this
     * document exists to complement. The failure is disclosed on the other side
     * instead: with no write, `writtenAt` stops advancing and the relay reports
     * the file as stale, which is the same signal a stopped router gives and
     * wants the same look.
     */
    fun write(
        streams: List<StreamPhases.Stream>,
        processors: List<Processors.Snapshot> = emptyList(),
        /** Where the constraint is, and the numbers behind it — see [Health]. */
        health: Health? = null,
        /**
         * VirtualMachineErrors this process has survived — an OutOfMemoryError
         * kills whichever thread allocates next and is caught by nobody, so the
         * router carries on looking merely quiet. Four of them once passed
         * unnoticed. It reached a stderr line and stopped there.
         */
        fatals: Long = 0,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ): Boolean {
        val f = file ?: return false
        return runCatching {
            f.parentFile?.mkdirs()
            val tmp = File(f.parentFile ?: File("."), "${f.name}.tmp")
            tmp.writeText(json.encodeToString(JsonObject.serializer(), document(streams, processors, fatals, health, nowSeconds)))
            // Temp file plus an atomic move, for the same reason every other
            // file here is written that way: the relay reads this on its own
            // schedule and a half-written document parses as nothing.
            try {
                Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            } catch (_: AtomicMoveNotSupportedException) {
                Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING)
            }
        }.isSuccess
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
            nowSeconds: Long,
        ): JsonObject =
            buildJsonObject {
                put("writtenAt", nowSeconds)
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
                                // own: every member here is about the phase and
                                // nothing else, and a wrapper would be a name to
                                // look up for no gain. Only what this phase can
                                // answer is written — see [StreamPhases.Detail],
                                // including the two members deliberately left
                                // out because the document already has them.
                                s.detail.let { d ->
                                    d.returned?.let { put("returned", it) }
                                    d.running?.let { put("running", it) }
                                    d.transferring?.let { put("transferring", it) }
                                    // Two decimals: this is a percentage a human
                                    // reads, and a full double publishes sixteen
                                    // digits of noise on every tick.
                                    d.fraction?.let { put("fraction", Math.round(it * 10_000) / 10_000.0) }
                                    d.etaMs?.let { put("etaSec", it / 1000) }
                                    d.reachedSeconds?.let { put("reached", it) }
                                    d.collected?.let { put("collected", it) }
                                    d.collectedTotal?.let { put("collectedTotal", it) }
                                    d.free?.let { put("slotsFree", it) }
                                    d.needed?.let { put("slotsNeeded", it) }
                                    d.nextInSec?.let { put("nextInSec", it) }
                                    d.retrySec?.let { put("retryInSec", it) }
                                    d.reason?.let { put("reason", it) }
                                }
                                // WHICH relays are running, beside the cycle
                                // rather than inside it: a worker outlives the
                                // pass that handed it out, so this set spans
                                // passes and the cycle's `pending`/`busy` split
                                // it between them. It is what those counts never
                                // said — a stream held on two relays for eleven
                                // hours published the number 2 and no url. See
                                // [InFlight], and note it is not "the pending
                                // urls": the walk has not reached some of them.
                                s.inFlight?.takeIf { it.relays.isNotEmpty() }?.let { f ->
                                    put(
                                        "inFlight",
                                        buildJsonObject {
                                            putJsonArray("relays") {
                                                for (r in f.relays) {
                                                    add(
                                                        buildJsonObject {
                                                            put("relay", r.relay)
                                                            // WHICH WALK handed
                                                            // this url out. With
                                                            // two passes live the
                                                            // table could not say,
                                                            // and the rotation
                                                            // knew all along.
                                                            r.pass?.let { put("pass", it) }
                                                            put("heldForSec", r.heldForSec)
                                                            // Absent when the worker has no transfer
                                                            // slot — in the guards, or queued behind
                                                            // other legs, which is where most of a
                                                            // fan-out's workers are.
                                                            r.transferringForSec?.let { put("transferringForSec", it) }
                                                            // The pair that separates a real backlog
                                                            // from a walk that will not end.
                                                            put("events", r.events)
                                                            put("quietForSec", r.quietForSec)
                                                            // WHAT IT IS DOING.
                                                            // Absent until the
                                                            // leg reaches a
                                                            // stage worth a
                                                            // word, which is a
                                                            // state and not a
                                                            // gap.
                                                            r.doing?.let { put("doing", it) }
                                                            // Absent when no
                                                            // walk is running.
                                                            r.pagingUntil?.let { put("pagingUntil", it) }
                                                        },
                                                    )
                                                }
                                            }
                                            // Never silent, for the same reason
                                            // as the fold's.
                                            put("omitted", f.omitted)
                                        },
                                    )
                                }
                                // THE NEWEST PASS, under the name it has always
                                // had. Every reader of this document reads
                                // `cycle`, and the passes beside it are an
                                // addition rather than a replacement: a rollup
                                // or a page that knows nothing about `passes`
                                // goes on describing the current walk exactly
                                // as it did.
                                s.newest?.let { put("cycle", cycle(it)) }
                                // …AND EVERY PASS STILL RUNNING, oldest first.
                                // A walk ends when its last url is handed out,
                                // not when its last worker returns, so the
                                // ordinary state of a rotation is one pass
                                // walking while the previous one's legs finish.
                                // Published only when there is more than one:
                                // a single-pass stream would otherwise carry a
                                // verbatim copy of `cycle` on every tick, for
                                // nothing.
                                s.cycles.takeIf { it.size > 1 }?.let { cycles ->
                                    putJsonArray("passes") { for (c in cycles) add(cycle(c)) }
                                }
                            },
                        )
                    }
                }
                // THE WORK THAT IS NOT A STREAM — see [Processors]. Omitted
                // entirely when nothing registered, rather than published as an
                // empty array: an empty list is a claim that this router runs
                // none of them, and a router built before this existed makes no
                // such claim.
                processors.takeIf { it.isNotEmpty() }?.let { rows ->
                    putJsonArray("processors") { for (p in rows) add(processor(p)) }
                }
            }

        /** One pass, as the document carries it — both under `cycle` and inside `passes`. */
        private fun cycle(c: StreamPhases.Cycle): JsonObject =
            c.tally.let { t ->
                buildJsonObject {
                    // The pass number and who opened it. Without them two rows
                    // of `passes` are two anonymous partitions, and the question
                    // they exist to answer — is the old walk still finishing —
                    // needs to know which is which.
                    put("number", c.number)
                    put("owner", c.owner)
                    put("startedAt", c.startedSec)
                    c.endedSec?.let { put("endedAt", it) }
                    put("outcome", c.outcome)
                    put(
                        "urls",
                        buildJsonObject {
                            put("discovered", t.discovered)
                            put("foldedOntoAnother", t.foldedOntoAnother)
                            put("refusedUnstable", t.refusedUnstable)
                            put("excluded", t.excluded)
                            put("taken", t.taken)
                        },
                    )
                    // Beside the url counts, never instead of them: the gap
                    // between the two IS the disclosure — 3,272 urls on 850
                    // hosts, in the run that motivated this.
                    put("hosts", t.hosts)
                    // How old the list those urls came from was when this cycle
                    // started. Without it `discovered` changes meaning silently
                    // on a stream that recycles its relay list: the count can
                    // describe a store walk from hours ago, and two identical
                    // documents cannot be told from a mirror that stopped
                    // looking.
                    put("relayListAgeSec", t.listAgeSec)
                    put(
                        "taken",
                        buildJsonObject {
                            put("delivered", t.delivered.get())
                            put("nothingNew", t.nothingNew.get())
                            put("unreachable", t.unreachable.get())
                            put("transferFailed", t.transferFailed.get())
                            put("noRoute", t.noRoute.get())
                            put("hostStruckOut", t.hostStruckOut.get())
                            put("knownDead", t.knownDead.get())
                            put("torUnavailable", t.torUnavailable.get())
                            // Not dialled because a worker from an earlier pass
                            // still had it. Its own member because "the rotation
                            // is overlapping" and "the relay is dead" are
                            // opposite findings.
                            put("busy", t.busy.get())
                            // Derived, and it is what makes the eight members
                            // sum to `urls.taken` while the cycle is still
                            // running.
                            put("pending", t.pending())
                        },
                    )
                    put("balanced", t.balanced())
                    put("received", t.received.get())
                    // WHICH urls were folded, not only how many. The count
                    // answers "how much of the fan-out was duplication"; this
                    // answers "which server is wearing forty urls", which is the
                    // one an operator can act on. Bounded, and it says what it
                    // left out — see [CycleTally.foldedOnto].
                    t.foldedOnto().takeIf { it.onto.isNotEmpty() }?.let { fold ->
                        put(
                            "foldedOnto",
                            buildJsonObject {
                                putJsonArray("relays") {
                                    for (row in fold.onto) {
                                        add(
                                            buildJsonObject {
                                                put("relay", row.relay)
                                                put("urls", row.urls)
                                                putJsonArray("examples") { for (u in row.examples) add(u) }
                                            },
                                        )
                                    }
                                }
                                // Never silent: a truncated list that does not
                                // say so reads as the whole answer.
                                put("omitted", fold.omitted)
                            },
                        )
                    }
                }
            }

        /**
         * One processor, as the document carries it — see [Processors].
         *
         * The counters are put as MEMBERS rather than as a `{name, value}` list,
         * because that is how a reader of this document reads every other number
         * in it and because each of them has an entry in the relay's published
         * glossary. The set is fixed by the wiring in `SyncEngine`, not open to
         * whatever a caller passes: `SyncVocabularyTest` fails the build for a
         * published count with no term.
         */
        private fun processor(p: Processors.Snapshot): JsonObject =
            buildJsonObject {
                put("name", p.name)
                put("phase", p.phase)
                put("phaseForSec", p.phaseForSec)
                // `passesRun`, not `passes`: a stream's `passes` is the LIST of
                // walks still running, and one name for a list and a count is
                // the kind of overload this document exists to stop making.
                p.passes?.let { put("passesRun", it) }
                p.lastPassAt?.let { put("lastPassAt", it) }
                p.lastPassSec?.let { put("lastPassSec", it) }
                // The countdown, and the reason a processor needs one at all:
                // "the fold has decided nothing about this host" reads as broken
                // until you know its clock is six hours long and the next turn
                // is four of them away.
                p.nextInSec?.let { put("nextInSec", it) }
                for (c in p.counts) put(c.name, c.value)
                // WHAT A TOTAL IS MADE OF. `rejected` is the largest number this
                // router publishes and the least readable one: a mirror is
                // offered the same event once per relay holding it, so rejecting
                // most of what arrives is the pipeline working. The split is
                // what says so.
                p.reasons.takeIf { it.isNotEmpty() }?.let { rows ->
                    put(
                        "rejections",
                        buildJsonObject {
                            putJsonArray("reasons") {
                                for (r in rows) {
                                    add(
                                        buildJsonObject {
                                            put("reason", r.reason)
                                            put("events", r.events)
                                        },
                                    )
                                }
                            }
                        },
                    )
                }
                p.work.takeIf { it.isNotEmpty() }?.let { rows ->
                    putJsonArray("streams") {
                        for (w in rows) {
                            add(
                                buildJsonObject {
                                    put("name", w.stream)
                                    put("candidates", w.candidates)
                                    // THE PARTITION, in precedence order:
                                    // `candidates = foldedAway + consistent +
                                    // inconsistent + unmeasured`, and
                                    // `unmeasured` is the sum of the `undecided`
                                    // rows' `urls`. Written at zero, because a
                                    // member that appears only when non-zero
                                    // cannot be summed by a reader that has not
                                    // memorised the schema — but ABSENT from a
                                    // pass that does not measure them at all,
                                    // which is the fold. See [Processors.Work].
                                    w.foldedAway?.let { put("foldedAway", it) }
                                    w.consistent?.let { put("consistent", it) }
                                    w.inconsistent?.let { put("inconsistent", it) }
                                    // The one that says whether it is getting
                                    // anywhere. See [Processors.Work].
                                    put("unmeasured", w.unmeasured)
                                    put("dialled", w.dialled)
                                    put("decided", w.decided)
                                    w.undecided.takeIf { it.isNotEmpty() }?.let { reasons ->
                                        put(
                                            "undecided",
                                            buildJsonObject {
                                                putJsonArray("reasons") {
                                                    for (u in reasons) {
                                                        add(
                                                            buildJsonObject {
                                                                put("reason", u.reason)
                                                                // Urls first: it is the count that
                                                                // sums back to `unmeasured`, and
                                                                // `hosts` beside it is the count
                                                                // that names who to chase.
                                                                put("urls", u.urls)
                                                                put("hosts", u.hosts)
                                                                u.examples.takeIf { it.isNotEmpty() }?.let { names ->
                                                                    putJsonArray("examples") { for (h in names) add(h) }
                                                                }
                                                                // The widest few WITH counts, where the pass
                                                                // counts urls. Ranked, and deliberately not
                                                                // summing to the row — see [Processors.Undecided.top].
                                                                u.top.takeIf { it.isNotEmpty() }?.let { rows ->
                                                                    putJsonArray("top") {
                                                                        for (h in rows) {
                                                                            add(
                                                                                buildJsonObject {
                                                                                    put("host", h.host)
                                                                                    put("urls", h.urls)
                                                                                },
                                                                            )
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                        )
                                                    }
                                                }
                                                // Bounded like every other list
                                                // here, and never silently.
                                                put("omitted", w.undecidedOmitted)
                                            },
                                        )
                                    }
                                },
                            )
                        }
                    }
                }
            }

        /**
         * `SYNC_PROGRESS_FILE` — where the document is written. Unset publishes
         * nothing, which is right for a router with no relay beside it.
         *
         * No `ROUTER_*` spelling: those exist for settings that predate the
         * rename, and this one never had one.
         */
        fun fromEnv(env: Map<String, String>): SyncProgress =
            SyncProgress(
                env["SYNC_PROGRESS_FILE"]
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
                    ?.let(::File),
            )
    }
}
