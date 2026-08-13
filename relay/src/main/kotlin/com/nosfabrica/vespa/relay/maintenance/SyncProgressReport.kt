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
package com.nosfabrica.vespa.relay.maintenance

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray

/**
 * WHAT THE ROUTER IS DOING, as `/stats.json` publishes it — the progress file
 * (`SyncProgress`, written by the sync process on its progress tick) read off
 * the shared volume and reduced to what a reader can act on.
 *
 * ## The three things this answers that nothing else could
 *
 * **Is it alive.** `writtenAt` is a heartbeat: the router rewrites it every tick
 * whatever its streams are doing. Beside the rollup's own clock it becomes
 * `staleForSec` — how long the file has gone unwritten — which is the one number
 * that separates a quiet mirror from a stopped one. A mirror that has been down
 * for a day used to publish exactly the same card as one mid-cycle.
 *
 * **What happened to everything it took on.** The cycle's `urls`/`taken` objects
 * are a PARTITION, and this object republishes them as one: a production run
 * reported 16,752 discovered against 5,323 band-bearing and nothing accounted
 * for the ~11,400 in between. `accountedFor` is this side's own check that they
 * still sum after the read — the writer's `balanced` says the router thought so,
 * this says the document does.
 *
 * **Whether the last cycle worked.** `outcome` is `running`, `completed` or
 * `failed`. A cycle that aborted at 80% and one that finished left the identical
 * trace before it existed: both simply stopped saying anything.
 *
 * ## Read, not relayed
 *
 * Rebuilt member by member, never passed through, for the reason [MirrorReport]
 * gives at length: the file is another process's, and a hand-edited or
 * half-migrated one must not be able to put arbitrary JSON into a document
 * served under this relay's name. Everything unreadable is simply absent, and no
 * failure here may cost the relay its rollup.
 */
internal object SyncProgressReport {
    private val lenient =
        Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

    /**
     * The ten outcomes of a url the cycle took on, in the order the card reads
     * them: what worked, then what did not, then what our own rotation declined
     * to hand out, then what is still going.
     *
     * A fixed list, spelled here rather than taken from the file's key order.
     * The document is a partition and a reader adds these up; taking the members
     * from whatever the writer happened to emit would let a future router widen
     * the sum silently, which is exactly the failure — counts that do not add up
     * to their own total — this whole object exists to end.
     */
    private val OUTCOMES =
        listOf(
            "delivered",
            "nothingNew",
            "unreachable",
            "transferFailed",
            "noRoute",
            "hostStruckOut",
            "knownDead",
            "torUnavailable",
            "busy",
            "pending",
        )

    /**
     * Fold the progress file into the `progress` object of the `sync` section,
     * or null when there is nothing to say.
     *
     * [nowSeconds] is the rollup's clock, not the file's, and that asymmetry is
     * the point: the staleness of the file is measured against the reader, so a
     * router that stopped writing an hour ago says so however recent its own
     * last timestamp looked.
     */
    fun build(
        progressJson: String?,
        nowSeconds: Long,
    ): JsonObject? {
        val doc = parse(progressJson) ?: return null
        val writtenAt = (doc["writtenAt"] as? JsonPrimitive)?.longOrNull
        val streams = (doc["streams"] as? JsonArray)?.filterIsInstance<JsonObject>().orEmpty()
        // A file with neither a heartbeat nor a stream is not a router being
        // quiet, it is a document this parser could not read — and publishing an
        // empty `progress` object would be a claim that the router said nothing,
        // which is a different and stronger statement.
        if (writtenAt == null && streams.isEmpty()) return null

        return buildJsonObject {
            writtenAt?.let {
                put("writtenAt", it)
                // Never negative. The two processes share a host clock but not a
                // guarantee, and a file stamped a second into the future would
                // otherwise publish "-1s stale", which reads as a bug in the
                // relay rather than as skew.
                put("staleForSec", (nowSeconds - it).coerceAtLeast(0))
            }
            // Zero included: "no thread has been killed" is the claim, and a
            // member that appears only on damage cannot be told from a router
            // too old to say either way.
            num(doc["fatals"])?.let { put("fatals", it) }
            putJsonArray("streams") {
                for (s in streams) stream(s)?.let { add(it) }
            }
            // THE WORK THAT IS NOT A STREAM — the alias fold, the stability
            // gate, the NIP-66 monitor, ingest, the healer, the push. Absent
            // rather than empty when the file carries none: a router built
            // before this existed makes no claim about them, and an empty array
            // would be the claim that it runs none.
            (doc["processors"] as? JsonArray)
                ?.filterIsInstance<JsonObject>()
                ?.take(MAX_PROCESSORS)
                ?.mapNotNull { processor(it) }
                ?.takeIf { it.isNotEmpty() }
                ?.let { rows -> putJsonArray("processors") { for (r in rows) add(r) } }
        }
    }

    /** One stream's line, or null when it carries no name — without which it says nothing. */
    private fun stream(o: JsonObject): JsonObject? {
        val name = text(o["name"]) ?: return null
        return buildJsonObject {
            put("name", name)
            text(o["phase"])?.let { put("phase", it) }
            (o["phaseForSec"] as? JsonPrimitive)?.longOrNull?.let { put("phaseForSec", it) }
            // WHAT THE PHASE KNOWS. Only what a phase can answer is present, so
            // each is copied when it is there and absent when it is not — an
            // absent `reached` is a walk that has not reported a depth, and a
            // zero would be a claim about 1970.
            for (member in PHASE_DETAIL) num(o[member])?.let { put(member, it) }
            // The one that is not an integer: a fraction of the window walked.
            (o["fraction"] as? JsonPrimitive)?.doubleOrNull?.let { put("fraction", it) }
            text(o["reason"])?.let { put("reason", it) }
            // WHICH relays this stream has workers on. Beside the cycle rather
            // than inside it because a worker outlives the pass that handed it
            // out — see the router's [InFlight] for the full account, and for
            // why this is not simply "the pending urls".
            inFlight(o["inFlight"] as? JsonObject)?.let { put("inFlight", it) }
            (o["cycle"] as? JsonObject)?.let { c -> cycle(c)?.let { put("cycle", it) } }
            // EVERY PASS STILL RUNNING, not just the newest.
            //
            // A walk ends when its last url is handed out and its slowest legs
            // run on past it, so the ordinary state of a rotation is one pass
            // walking while the one before it finishes. With a single `cycle`
            // the older pass stopped being published the moment the new one
            // opened — its events went on arriving against a partition nothing
            // was showing, and its still-running legs appeared only as the new
            // pass's `busy`. Each row is rebuilt by the same [cycle] reader, so
            // the arithmetic is checked per pass rather than per stream.
            (o["passes"] as? JsonArray)
                ?.filterIsInstance<JsonObject>()
                ?.take(MAX_PASSES)
                ?.mapNotNull { cycle(it) }
                ?.takeIf { it.size > 1 }
                ?.let { rows -> putJsonArray("passes") { for (r in rows) add(r) } }
        }
    }

    /**
     * One processor, rebuilt member by member like everything else here.
     *
     * The counters are an ALLOWLIST rather than whatever the file happens to
     * carry, which is the same rule the ten `taken` outcomes follow and for a
     * stronger reason: these become members of a document served under this
     * relay's name, every one of them has an entry in the published glossary,
     * and a hand-edited file must not be able to put a new name into either.
     */
    private fun processor(o: JsonObject): JsonObject? {
        val name = text(o["name"]) ?: return null
        return buildJsonObject {
            put("name", name)
            text(o["phase"])?.let { put("phase", it) }
            num(o["phaseForSec"])?.let { put("phaseForSec", it) }
            num(o["passesRun"])?.let { put("passesRun", it) }
            num(o["lastPassAt"])?.let { put("lastPassAt", it) }
            num(o["lastPassSec"])?.let { put("lastPassSec", it) }
            num(o["nextInSec"])?.let { put("nextInSec", it) }
            for (counter in COUNTERS) num(o[counter])?.let { put(counter, it) }
            rejections(o["rejections"] as? JsonObject)?.let { put("rejections", it) }
            (o["streams"] as? JsonArray)
                ?.filterIsInstance<JsonObject>()
                ?.take(MAX_PROCESSOR_STREAMS)
                ?.mapNotNull { processorWork(it) }
                ?.takeIf { it.isNotEmpty() }
                ?.let { rows -> putJsonArray("streams") { for (r in rows) add(r) } }
        }
    }

    /**
     * What a total is made of, rebuilt row by row and bounded again here.
     *
     * Ingest's `rejected` is the largest number this document carries and the
     * least readable: rejecting most of what arrives is a mirror working, and
     * only the split says so.
     */
    private fun rejections(o: JsonObject?): JsonObject? {
        val rows = (o?.get("reasons") as? JsonArray)?.filterIsInstance<JsonObject>().orEmpty()
        if (rows.isEmpty()) return null
        return buildJsonObject {
            putJsonArray("reasons") {
                for (row in rows.take(MAX_REJECTION_ROWS)) {
                    val reason = text(row["reason"]) ?: continue
                    add(
                        buildJsonObject {
                            put("reason", reason)
                            put("events", num(row["events"]) ?: 0)
                        },
                    )
                }
            }
        }
    }

    /** How far one processor's pass got over one stream's candidates. */
    private fun processorWork(o: JsonObject): JsonObject? {
        val name = text(o["name"]) ?: return null
        return buildJsonObject {
            put("name", name)
            put("candidates", num(o["candidates"]) ?: 0)
            // The progress number: what still has no verdict. Defaulted to
            // `candidates` rather than to 0 when a file does not say — "nothing
            // left to measure" is a strong claim and an unreadable row must not
            // make it.
            put("unmeasured", num(o["unmeasured"]) ?: num(o["candidates"]) ?: 0)
            put("dialled", num(o["dialled"]) ?: 0)
            put("decided", num(o["decided"]) ?: 0)
            undecided(o["undecided"] as? JsonObject)?.let { put("undecided", it) }
        }
    }

    /**
     * Why a pass left hosts undecided, bounded again on this side.
     *
     * Same contract as [foldedOnto] and [inFlight]: the router bounds its list,
     * this bounds it a second time rather than trusting that it did, and
     * `omitted` carries through whatever either side dropped.
     */
    private fun undecided(o: JsonObject?): JsonObject? {
        if (o == null) return null
        val rows = (o["reasons"] as? JsonArray)?.filterIsInstance<JsonObject>().orEmpty()
        if (rows.isEmpty()) return null
        val kept = rows.take(MAX_UNDECIDED_ROWS)
        var unreadable = 0
        return buildJsonObject {
            putJsonArray("reasons") {
                for (row in kept) {
                    val reason =
                        text(row["reason"]) ?: run {
                            unreadable++
                            null
                        } ?: continue
                    add(
                        buildJsonObject {
                            put("reason", reason)
                            put("hosts", num(row["hosts"]) ?: 0)
                            putJsonArray("examples") {
                                for (h in (row["examples"] as? JsonArray).orEmpty().take(MAX_UNDECIDED_EXAMPLES)) {
                                    text(h)?.let { add(it) }
                                }
                            }
                        },
                    )
                }
            }
            put("omitted", (num(o["omitted"]) ?: 0) + (rows.size - kept.size) + unreadable)
        }
    }

    /**
     * The longest-held legs, rebuilt row by row and capped again on this side.
     *
     * Same terms as [foldedOnto]: the router already bounds its list, and this
     * bounds it a second time rather than trusting that it did. `omitted` is
     * carried through and ADDED to whatever this side drops, because a
     * truncated list that does not say it is truncated reads as the whole
     * answer — and here the whole answer is what an operator is chasing.
     */
    private fun inFlight(o: JsonObject?): JsonObject? {
        if (o == null) return null
        val rows = (o["relays"] as? JsonArray)?.filterIsInstance<JsonObject>().orEmpty()
        if (rows.isEmpty()) return null
        val kept = rows.take(MAX_IN_FLIGHT_ROWS)
        // Rows this side could not read are DROPPED, so they have to be counted
        // — see `omitted` below. A row with no url says nothing and cannot be
        // published, but letting it vanish silently is the exact failure the
        // `omitted` member exists to prevent.
        var unreadable = 0
        return buildJsonObject {
            putJsonArray("relays") {
                for (row in kept) {
                    val relay =
                        text(row["relay"]) ?: run {
                            unreadable++
                            null
                        } ?: continue
                    add(
                        buildJsonObject {
                            put("relay", relay)
                            // Which walk handed it out. Absent on a router that
                            // predates the stamp, where the honest answer is
                            // nothing rather than a guess.
                            num(row["pass"])?.let { put("pass", it) }
                            put("heldForSec", num(row["heldForSec"]) ?: 0)
                            // Absent means "holds no transfer slot", which is a
                            // statement. Defaulting it to 0 would turn a worker
                            // queued behind a saturated pool into one that just
                            // took a slot — the opposite reading.
                            num(row["transferringForSec"])?.let { put("transferringForSec", it) }
                            put("events", num(row["events"]) ?: 0)
                            put("quietForSec", num(row["quietForSec"]) ?: 0)
                        },
                    )
                }
            }
            put("omitted", (num(o["omitted"]) ?: 0) + (rows.size - kept.size) + unreadable)
        }
    }

    /**
     * One cycle's disposition, re-derived rather than copied.
     *
     * The sums are recomputed HERE from the members this object names, so that
     * `accountedFor` is a statement about the document being served rather than
     * a boolean forwarded from a file nobody on this side has checked. A
     * mismatch is published, not hidden: the counts are still worth having, and
     * the flag is what stops a reader treating a broken partition as a whole
     * one.
     */
    private fun cycle(o: JsonObject): JsonObject? {
        val urls = o["urls"] as? JsonObject ?: return null
        val discovered = num(urls["discovered"]) ?: return null
        val folded = num(urls["foldedOntoAnother"]) ?: 0
        val excluded = num(urls["excluded"]) ?: 0
        // Defaulted to 0 rather than required: a router that predates the
        // stability gate wrote no such member, and reading its absence as
        // anything but "none were refused" would break the partition on every
        // document written before this shipped.
        val refusedUnstable = num(urls["refusedUnstable"]) ?: 0
        val taken = num(urls["taken"]) ?: (discovered - folded - refusedUnstable - excluded)
        val outcomes = o["taken"] as? JsonObject ?: JsonObject(emptyMap())
        val byOutcome = OUTCOMES.associateWith { num(outcomes[it]) ?: 0L }
        val settled = byOutcome.values.sum()
        return buildJsonObject {
            // Which pass this is, and which half of the router opened it. Absent
            // on a file written before passes were published, where the single
            // cycle needed no name to be told from the others.
            num(o["number"])?.let { put("number", it) }
            text(o["owner"])?.let { put("owner", it) }
            num(o["startedAt"])?.let { put("startedAt", it) }
            num(o["endedAt"])?.let { put("endedAt", it) }
            text(o["outcome"])?.let { put("outcome", it) }
            put(
                "urls",
                buildJsonObject {
                    put("discovered", discovered)
                    put("foldedOntoAnother", folded)
                    put("refusedUnstable", refusedUnstable)
                    put("excluded", excluded)
                    put("taken", taken)
                },
            )
            // Beside the urls, never instead of them. 3,272 urls resolved to 850
            // hosts in the run that motivated this: every count taken over urls
            // is inflated by whatever the alias fold has not decided yet, and the
            // gap between the two numbers IS the disclosure.
            num(o["hosts"])?.let { put("hosts", it) }
            // Absent only on a file written before this member existed, where it
            // is omitted rather than defaulted to 0 — "the list was derived for
            // this cycle" is a claim, and an older router made no such claim
            // either way. A current one always writes it, 0 included.
            num(o["relayListAgeSec"])?.let { put("relayListAgeSec", it) }
            put("taken", buildJsonObject { for ((k, v) in byOutcome) put(k, v) })
            put("received", num(o["received"]) ?: 0L)
            // Both halves of the partition, checked on this side.
            // WHICH urls folded onto which survivor. Rebuilt row by row like
            // everything else here — a file this process did not write must not
            // be able to put arbitrary JSON, or an unbounded array, into a
            // document served under this relay's name.
            foldedOnto(o["foldedOnto"] as? JsonObject)?.let { put("foldedOnto", it) }
            put("accountedFor", folded + refusedUnstable + excluded + taken == discovered && settled == taken)
            // What the WRITER thought, kept separately. The two disagreeing
            // localises the fault to the read or to the router, which one
            // merged flag could never do.
            (o["balanced"] as? JsonPrimitive)?.booleanOrNull?.let { put("balanced", it) }
        }
    }

    /**
     * The fold's biggest survivors, capped again on this side.
     *
     * The router already bounds its list, and this bounds it a second time
     * rather than trusting that it did: the cap is the only thing standing
     * between a hand-edited file and an unbounded array in a document served on
     * every poll. `omitted` is carried through whatever happens, because a
     * truncated list that does not say it is truncated reads as the whole
     * answer.
     */
    private fun foldedOnto(o: JsonObject?): JsonObject? {
        if (o == null) return null
        val rows = (o["relays"] as? JsonArray)?.filterIsInstance<JsonObject>().orEmpty()
        if (rows.isEmpty()) return null
        val kept = rows.take(MAX_FOLD_ROWS)
        // Counted, not dropped — see [inFlight] for the argument. Same contract,
        // same failure if it is broken.
        var unreadable = 0
        return buildJsonObject {
            putJsonArray("relays") {
                for (row in kept) {
                    val relay =
                        text(row["relay"]) ?: run {
                            unreadable++
                            null
                        } ?: continue
                    add(
                        buildJsonObject {
                            put("relay", relay)
                            put("urls", num(row["urls"]) ?: 0)
                            putJsonArray("examples") {
                                for (u in (row["examples"] as? JsonArray).orEmpty().take(MAX_FOLD_EXAMPLES)) {
                                    text(u)?.let { add(it) }
                                }
                            }
                        },
                    )
                }
            }
            put("omitted", (num(o["omitted"]) ?: 0) + (rows.size - kept.size) + unreadable)
        }
    }

    /**
     * The gauges a processor may publish, and the whole of them.
     *
     * Not "whatever numbers the file carries": these are members of a document
     * served under this relay's name and each one is defined in
     * [SyncVocabulary], so a name that is not here is a name no reader could
     * look up. Adding one is a two-file change on purpose.
     */
    internal val COUNTERS =
        listOf(
            // ingest
            "queued",
            "capacity",
            "accepted",
            "rejected",
            // the healer and the upstream push
            "pushed",
            "dropped",
            // the NIP-66 monitor
            "observed",
            "knownDead",
            // ingest's one loss counter
            "lostToStore",
        )

    /**
     * The phase members a stream may publish beside its phase word.
     *
     * An allowlist for the reason every list here is one: these become members
     * of a document served under this relay's name, and each has a glossary
     * entry. `fraction` and `reason` are handled apart — one is not an integer
     * and the other is not a number at all.
     */
    private val PHASE_DETAIL =
        listOf(
            "returned",
            "running",
            "transferring",
            "etaSec",
            "reached",
            "collected",
            "collectedTotal",
            "slotsFree",
            "slotsNeeded",
            "nextInSec",
            "retryInSec",
        )

    /** This side's own ceilings — see [foldedOnto] for why they are restated here. */
    private const val MAX_FOLD_ROWS = 20
    private const val MAX_FOLD_EXAMPLES = 2
    private const val MAX_IN_FLIGHT_ROWS = 20

    /** Matches the router's own `StreamPhases.MAX_TRACKED_CYCLES`, restated rather than trusted. */
    private const val MAX_PASSES = 4

    /** Six today (fold, stability, reachability, ingest, heal, push), with room to grow. */
    private const val MAX_PROCESSORS = 12

    /** A processor reports per stream, and a router runs a handful of them. */
    private const val MAX_PROCESSOR_STREAMS = 12

    private const val MAX_UNDECIDED_ROWS = 6
    private const val MAX_REJECTION_ROWS = 4
    private const val MAX_UNDECIDED_EXAMPLES = 3

    private fun num(value: JsonElement?): Long? = (value as? JsonPrimitive)?.longOrNull

    /**
     * A member's text, or null when absent, blank — or not a primitive at all.
     *
     * The cast is load-bearing for the reason [MirrorReport.text] spells out:
     * `jsonPrimitive` throws on an object, and one `"name": {}` in a file this
     * process did not write would cost the whole sync section.
     */
    private fun text(value: JsonElement?): String? = (value as? JsonPrimitive)?.contentOrNull?.takeIf { it.isNotBlank() }

    /** A corrupt or half-written file costs this object, not the rollup. `Exception`, never `Throwable` — see [SyncCoverageReport.parse]. */
    private fun parse(text: String?): JsonObject? {
        if (text.isNullOrBlank()) return null
        return try {
            lenient.parseToJsonElement(text).jsonObject
        } catch (e: Exception) {
            null
        }
    }
}
