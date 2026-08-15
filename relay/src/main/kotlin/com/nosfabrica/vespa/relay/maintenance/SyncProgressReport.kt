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
import kotlinx.serialization.json.JsonNull
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
        previous: JsonObject? = null,
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
            // WHERE THE CONSTRAINT IS. The router decides this itself, from the
            // pair that separates a full ingest queue from an empty one, and it
            // is the first thing an operator asks of a mirror that feels slow.
            val health = health(doc["health"] as? JsonObject)
            health?.let { put("health", it) }
            // …AND HOW IT GOT HERE. Every gauge above is an instant, and every
            // question asked of them is differential — see [series].
            series(previous, health, doc, nowSeconds)?.let { put("series", it) }
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

    /**
     * The router's own account of why it is going at the speed it is.
     *
     * Rebuilt member by member like everything else, and `bottleneck` is checked
     * against the words the router can actually emit — a value this side does
     * not recognise is dropped rather than served, because the card colours a
     * state from it.
     */
    private fun health(o: JsonObject?): JsonObject? {
        if (o == null) return null
        // …and EMPTY is not a health object. Every member here is allowlisted,
        // so a `health` this side recognises nothing in — a word `bottleneckOf`
        // cannot emit, a router older than these gauges — rebuilt to `{}` and
        // was published anyway. `{}` is a claim that the router reported its
        // constraint, and the card believed it: it drew a chip with no text in
        // it, beside the live one. Absent is the honest answer.
        return buildJsonObject {
            text(o["bottleneck"])?.takeIf { it in BOTTLENECKS }?.let { put("bottleneck", it) }
            for (member in HEALTH_NUMBERS) num(o[member])?.let { put(member, it) }
        }.takeIf { it.isNotEmpty() }
    }

    /**
     * HOW IT GOT HERE — the last hour of the four process gauges, appended one
     * sample per rollup.
     *
     * ## Why an instant was not enough
     *
     * Every gauge on this card is a level, and not one operator question about
     * a level is answerable from one reading. `heap 45%` says nothing; heap 45%
     * and climbing three points a minute says everything. A queue at 4,101 of
     * 4,096 is the constraint if it has been there for ten minutes and is noise
     * if it filled this second. "Is it stuck" is a derivative, and the card was
     * answering it with thresholds — which is the wrong instrument, and is why
     * the thresholds always felt arbitrary.
     *
     * ## Where it is kept
     *
     * In the DOCUMENT, appended to whatever the previously served one carried.
     * Nothing new holds it: `StatsSnapshot` already merges each tier into the
     * document it is serving and already persists that document to `STATS_FILE`,
     * so a series that lives there is carried across rollups by the merge and
     * across restarts by the file, with no ring buffer, no scheduler and no
     * second lifetime to reason about. A relay that has just started serves the
     * history its last run wrote.
     *
     * ## What is in it, and what is deliberately not
     *
     * The four PROCESS gauges, because they are the ones that decide whether
     * there is a problem at all, and because they are four scalars rather than
     * a shape that changes with the deployment. Per-stream and per-processor
     * series are not here and are not an oversight: the alias fold runs on a
     * six-hour clock, so at this cadence an hour of samples would not contain
     * one of its passes. That needs a different window and is a different
     * feature.
     *
     * `at` is published beside the values rather than an interval being
     * assumed. The rollup cadence is an operator's env var, a restart leaves a
     * hole, and a reader drawing evenly spaced points over an uneven series
     * would draw a smooth line through a gap.
     */
    private fun series(
        previous: JsonObject?,
        health: JsonObject?,
        doc: JsonObject,
        nowSeconds: Long,
    ): JsonObject? {
        val prior = previous?.get("series") as? JsonObject
        // The one thing that must not happen: a sample per REQUEST rather than
        // per rollup. `build` is called once per counters tick, but a document
        // republished without a new reading would still append — so a sample
        // whose clock has not moved past the last one is the same instant and
        // is dropped.
        val lastAt = (prior?.get("at") as? JsonArray)?.lastOrNull()?.let { num(it) }
        if (lastAt != null && nowSeconds <= lastAt) return prior
        val sample =
            SERIES.associateWith { member ->
                when (member) {
                    "heapPct" -> {
                        val used = num(health?.get("heapUsedMb"))
                        val max = num(health?.get("heapMaxMb"))
                        if (used != null && max != null && max > 0) used * 100 / max else null
                    }

                    "queued" -> {
                        num(
                            (doc["processors"] as? JsonArray)
                                ?.filterIsInstance<JsonObject>()
                                ?.firstOrNull { text(it["name"]) == "ingest" }
                                ?.get("queued"),
                        )
                    }

                    else -> {
                        num(health?.get(member))
                    }
                }
            }
        // Nothing to sample is not a zero sample. A router too old to publish
        // health, or one whose first health tick has not fired, would otherwise
        // lay down an hour of flat zeroes that read as a dead mirror.
        if (sample.values.all { it == null } && prior == null) return null
        return buildJsonObject {
            putJsonArray("at") {
                for (t in tail((prior?.get("at") as? JsonArray).orEmpty().mapNotNull { num(it) } + nowSeconds)) add(t)
            }
            for (member in SERIES) {
                putJsonArray(member) {
                    // A gap is published as a NULL, never as a zero or a
                    // carried-forward value: the reader has to be able to tell
                    // "the router said nothing" from "the router said none".
                    val kept = (prior?.get(member) as? JsonArray).orEmpty().map { num(it) }
                    for (v in tail(kept + sample[member])) {
                        if (v == null) add(JsonNull) else add(v)
                    }
                }
            }
        }
    }

    /** The last [MAX_SAMPLES] of a series, so the ring is bounded by the document rather than by a clock. */
    private fun <T> tail(values: List<T>): List<T> = if (values.size <= MAX_SAMPLES) values else values.takeLast(MAX_SAMPLES)

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
            // The other three members of the partition, carried only where the
            // router wrote them. NOT defaulted to zero: the alias fold measures
            // no stability verdicts and a router older than the partition
            // measured none either, and in both cases a zero here would be a
            // measurement neither of them took — the card draws "0 refused as
            // inconsistent" from it, which is a claim. Absent, the funnel reads
            // it as an unattributed slice and says so.
            num(o["foldedAway"])?.let { put("foldedAway", it) }
            num(o["consistent"])?.let { put("consistent", it) }
            num(o["inconsistent"])?.let { put("inconsistent", it) }
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
                            put("urls", num(row["urls"]) ?: 0)
                            put("hosts", num(row["hosts"]) ?: 0)
                            (row["examples"] as? JsonArray)?.takeIf { it.isNotEmpty() }?.let { names ->
                                putJsonArray("examples") {
                                    for (h in names.take(MAX_UNDECIDED_EXAMPLES)) text(h)?.let { add(it) }
                                }
                            }
                            // The ranked hosts under this reason, rebuilt row by
                            // row and capped again here — the same contract
                            // `foldedOnto` and `inFlight` are held to. A row
                            // with no host is dropped rather than published as
                            // an anonymous count: it is the NAME that makes this
                            // level worth drawing.
                            topHosts(row["top"] as? JsonArray)?.let { put("top", it) }
                        },
                    )
                }
            }
            put("omitted", (num(o["omitted"]) ?: 0) + (rows.size - kept.size) + unreadable)
        }
    }

    /**
     * The widest hosts under one reason, ranked as the router ranked them.
     *
     * The order is NOT re-sorted here. It is the router's ranking, taken over
     * the whole set it measured, and re-sorting a capped list would order the
     * head by a criterion that was never applied to the tail — which reads as a
     * top-N and is not one.
     */
    private fun topHosts(rows: JsonArray?): JsonArray? {
        val kept =
            rows
                .orEmpty()
                .filterIsInstance<JsonObject>()
                .take(MAX_UNDECIDED_HOSTS)
                .mapNotNull { row ->
                    val host = text(row["host"]) ?: return@mapNotNull null
                    buildJsonObject {
                        put("host", host)
                        put("urls", num(row["urls"]) ?: 0)
                    }
                }
        return if (kept.isEmpty()) null else JsonArray(kept)
    }

    /**
     * The quietest legs, rebuilt row by row and capped again on this side.
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
                            // The stage the leg reached. Copied as written and
                            // never defaulted: a router that predates it says
                            // nothing, which reads as "not known" rather than
                            // as a wrong stage.
                            text(row["doing"])?.let { put("doing", it) }
                            // Never defaulted: the stream's `reached` or a zero
                            // would date a leg from a walk it is not on.
                            num(row["pagingUntil"])?.let { put("pagingUntil", it) }
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
            // where the two probe passes' candidate set came from — the funnel's
            // mouth, above everything their `streams` rows partition
            "sourced",
            "heldOutDead",
        )

    /**
     * The gauges kept as a series, in draw order — see [series].
     *
     * `heapPct` is derived rather than copied: a percentage is what a reader
     * compares across samples, and publishing both halves of the pair sixty
     * times over to let the page divide them would triple the cost of the
     * feature for nothing.
     */
    internal val SERIES = listOf("eventsPerSec", "queued", "heapPct", "sockets")

    /**
     * How many samples the ring holds — an hour at the stock 60s counters
     * cadence, and however long that many ticks is at any other.
     *
     * Bounded by COUNT rather than by age on purpose: the bound has to hold
     * whatever an operator sets `STATS_COUNTERS_INTERVAL_SECONDS` to, and this
     * one costs the document about 1.2KB whatever that is.
     */
    internal const val MAX_SAMPLES = 60

    /** The four words `SyncEngine.bottleneckOf` can produce, and no others. */
    private val BOTTLENECKS = setOf("ingest", "downloads", "upstream", "mixed")

    private val HEALTH_NUMBERS =
        listOf("eventsPerSec", "heapUsedMb", "heapMaxMb", "sockets", "socketCeiling", "servingMs")

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

    /**
     * Undecided reasons kept per row — EIGHT, matching the router's own
     * `Processors.MAX_UNDECIDED_REASONS`.
     *
     * It was six, which was the fold's whole enumeration and one short of the
     * stability gate's seven. This side is supposed to bound a list the router
     * already bounded; cutting BELOW what the router publishes is a different
     * thing entirely, and here it would have dropped a reason whose urls the
     * page then draws as `not accounted for` — an arithmetic fault reported
     * against a document that was complete when it arrived.
     */
    private const val MAX_UNDECIDED_ROWS = 8
    private const val MAX_REJECTION_ROWS = 4
    private const val MAX_UNDECIDED_EXAMPLES = 3

    /** Ranked hosts kept under one reason, matching `Processors.MAX_UNDECIDED_HOSTS`. */
    private const val MAX_UNDECIDED_HOSTS = 6

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
