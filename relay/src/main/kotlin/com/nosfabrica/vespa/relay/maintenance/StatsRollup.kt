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

import com.nosfabrica.vespa.relay.server.StatsSnapshot
import com.vitorpamplona.quartz.kinds.KindNames
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject
import java.io.File
import java.time.Instant
import java.time.YearMonth

/**
 * The corpus statistics document this relay publishes at `GET /stats.json`,
 * and the background job that recomputes it.
 *
 * ## What this answers, and what it does not
 *
 * Every number here describes THIS RELAY'S STORE — what the router has actually
 * mirrored — not the Nostr network. The two are easy to confuse because the
 * charts look identical, and the confusion runs one way: a reader who compares
 * our event count against a network-wide dashboard's and finds it smaller will
 * read it as a broken relay rather than as a mirror's coverage. The document
 * says so in a `scope` field rather than leaving it to the page, because the
 * JSON is the artifact people will actually reuse.
 *
 * ## Anonymous, always
 *
 * These queries go to the raw engine, not through the trust projection, and
 * that is the only correct choice: the store gates an AUTHENTICATED reader to
 * authors that reader has scored, so the same pipeline run under an operator's
 * own lens would answer a smaller, different question under an identical label.
 * `observer_stats.html` makes the same call for the same reason.
 *
 * ## Sections fail independently
 *
 * Every section is computed by its own queries and carries its own status, so
 * one rejected pipeline costs one panel rather than the document. This is not
 * defensive habit — it is the only way this ships honestly. A grouping pipeline
 * is accepted or rejected WHOLE by Vespa, the four shapes borrowed from
 * `EventYql` are proven against this deployment and the rest are not, and a
 * single try/catch around the lot would turn one bad column into a blank page
 * with nothing to fix it from. A failed section carries the engine's own
 * message and the YQL that produced it, which is enough to correct the pipeline
 * without a debugger.
 */
internal class StatsRollup(
    private val vespa: StatsVespa,
    private val relayUrl: String,
    /** Wall clock in epoch seconds; injected so the window bounds are assertable. */
    private val nowSeconds: () -> Long = { System.currentTimeMillis() / 1000 },
    private val windowDays: Int = DEFAULT_WINDOW_DAYS,
    // In weeks, not days: this is what the page prints, and a window stated as
    // "the last 182 days" on a weekly chart is a number no reader converts. The
    // seconds are derived where the query is built.
    private val weekWindowWeeks: Int = DEFAULT_WEEK_WINDOW_WEEKS,
    /**
     * The first month the monthly series covers — an ANCHOR, not a length. See
     * [DEFAULT_MONTH_SERIES_START].
     */
    private val monthSeriesStart: YearMonth = DEFAULT_MONTH_SERIES_START,
    private val hourWindowDays: Int = DEFAULT_HOUR_WINDOW_DAYS,
    private val topRelays: Int = DEFAULT_TOP_RELAYS,
    private val kindSeries: Int = DEFAULT_KIND_SERIES,
    /**
     * The router's three files, read off the volume both containers mount. Null
     * in a serve-only deployment, and null is the normal case rather than an
     * error — see [SyncCoverageReport].
     *
     * Two are state (what the mirror has walked); the manifest is CONFIG (what
     * it is configured to mirror at all), which is why it is written once at the
     * router's boot rather than flushed — see [MirrorReport] and `SyncManifest`.
     */
    private val syncBandsFile: File? = null,
    private val syncSweepsFile: File? = null,
    private val syncManifestFile: File? = null,
) {
    /** Compute the whole document. Never throws: a section that fails says so in the document. */
    suspend fun compute(): JsonObject {
        val startedMs = System.currentTimeMillis()
        // Kinds first: its per-kind spans are where the corpus-wide newest event
        // comes from, since Vespa cannot answer a bare max() without a grouping.
        val kinds = kindsSection()
        val corpus = corpusSection(newestOf(kinds), distinctKindsOf(kinds))
        val activity = activitySection()
        // The per-kind series follow the histogram the kinds section just
        // computed, so the panel tracks the corpus rather than a hardcoded list
        // that would go stale in the direction of hiding whatever grew.
        val kindActivity = kindActivitySection(topKindNumbers(kinds))
        val relays = relaysSection()
        val zaps = zapsSection()
        val trust = trustSection()
        val sync = syncSection()
        return buildJsonObject {
            put("schema", SCHEMA_VERSION)
            put("relay", relayUrl)
            put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
            put("tookMs", System.currentTimeMillis() - startedMs)
            put(
                "scope",
                "This relay's own store — the events it has mirrored and serves. NOT the Nostr network: " +
                    "a total below a network-wide dashboard's is this mirror's coverage, not a fault.",
            )
            put("countedAs", "anonymous")
            put("timezone", "UTC")
            put("corpus", corpus)
            put("kinds", kinds)
            put("activity", activity)
            put("kindActivity", kindActivity)
            put("relayDistribution", relays)
            put("zaps", zaps)
            put("trust", trust)
            // Absent, not empty, when there is no router: a serve-only relay has
            // no sync to report and a card saying "0 relays" would read as a
            // broken mirror rather than as no mirror.
            sync?.let { put("sync", it) }
        }
    }

    /**
     * The router's coverage and the kind set it mirrors, read off the shared
     * volume.
     *
     * The one section that queries NOTHING — it is three file reads and a fold,
     * and it is here rather than in its own endpoint because `/stats.json` is
     * where a reader already looks and because the rollup already has a timer,
     * a snapshot, and an ETag. [SyncCoverageReport] carries the argument for
     * reading the router's files at all.
     *
     * `mirrors` sits beside the coverage rather than in a section of its own
     * because the two are read as one thing: how far the mirror has walked, and
     * what it was ever going to hold. Either half can be absent — a router that
     * has walked nothing still knows its own filters, and a manifest is missing
     * entirely until the router is restarted on a build that writes one.
     *
     * Wrapped so that no failure here can cost the document: an unreadable file
     * (wrong permissions, a volume that is not mounted, a half-written temp) is
     * reported as a failed section beside working ones, which is the same
     * contract every queried section has.
     */
    private suspend fun syncSection(): JsonObject? {
        if (syncBandsFile == null && syncSweepsFile == null && syncManifestFile == null) return null
        var data: JsonObject? = null
        val section =
            section { attempts ->
                attempt(attempts, "sync") {
                    val coverage = SyncCoverageReport.build(readOrNull(syncBandsFile), readOrNull(syncSweepsFile), nowSeconds())
                    val mirrors = MirrorReport.build(readOrNull(syncManifestFile))
                    data =
                        if (coverage == null && mirrors == null) {
                            null
                        } else {
                            buildJsonObject {
                                coverage?.forEach { (member, value) -> put(member, value) }
                                mirrors?.let { put("mirrors", it) }
                            }
                        }
                    data
                }
                data ?: buildJsonObject { }
            }
        // Nothing read AND nothing failed means no router has ever written here.
        return if (data == null && section["errors"] == null) null else section
    }

    /** Missing is the normal case; unreadable is not, and is allowed to throw into [attempt]. */
    private fun readOrNull(file: File?): String? = file?.takeIf { it.isFile }?.readText()

    /**
     * The newest `created_at` anywhere in the store, taken as the maximum over
     * the per-kind spans [kindsSection] already computed.
     *
     * Derived rather than queried because a bare `max(created_at)` with no
     * grouping level is not something Vespa will answer — see
     * [StatsYql.NO_BARE_AGGREGATES]. The spans are bounded to the present, so
     * this is too, which is what makes it usable as a freshness signal instead
     * of a report on the corpus's worst-dated spam.
     */
    private fun newestOf(kinds: JsonObject): Long? =
        (kinds["data"] as? JsonObject)
            ?.get("all")
            ?.let { it as? JsonArray }
            ?.mapNotNull { entry -> (entry as? JsonObject)?.get("lastSeen")?.jsonPrimitive?.longOrNull }
            ?.maxOrNull()

    /** How many distinct kinds the histogram found — the same number `distinct("kind")` would have cost a query for. */
    private fun distinctKindsOf(kinds: JsonObject): Int? = (kinds["data"] as? JsonObject)?.get("total")?.jsonPrimitive?.intOrNull

    /**
     * The kinds to draw a per-kind series for: the largest few from the
     * histogram [kindsSection] just computed.
     *
     * Reads the section's own output rather than re-querying — the histogram is
     * already sorted and already paid for. An empty list when that section
     * failed is the right answer: no series is better than a series over kinds
     * we could not count.
     */
    private fun topKindNumbers(kinds: JsonObject): List<Int> =
        (kinds["data"] as? JsonObject)
            ?.get("all")
            ?.let { it as? JsonArray }
            ?.mapNotNull { entry -> (entry as? JsonObject)?.get("kind")?.jsonPrimitive?.intOrNull }
            ?.take(kindSeries)
            .orEmpty()

    // ---- sections -----------------------------------------------------------

    /** Corpus totals: independent distinct/count queries over everything. */
    private suspend fun corpusSection(
        newestEvent: Long?,
        distinctKinds: Int?,
    ): JsonObject =
        section { attempts ->
            val now = nowSeconds()
            val events = attempt(attempts, "events") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL)) }
            val pubkeys = attempt(attempts, "pubkeys") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"))) }
            // Events signed for a time that has not happened. Clock skew and
            // spam both land here, and the count is worth having on its own:
            // it is the reason every freshness number on this page is bounded,
            // and a corpus where it grows is one where something upstream is
            // publishing garbage that ordinary charts would silently absorb.
            val future = attempt(attempts, "futureDated") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL, StatsYql.after(now))) }
            buildJsonObject {
                events?.let { put("events", it) }
                pubkeys?.let { put("pubkeys", it) }
                // The histogram already counted these, and asking twice was not
                // only a wasted query: two counts of the same thing taken
                // seconds apart can disagree on a live corpus, and a page
                // showing "412 kinds" beside a table headed "All 413 kinds" has
                // no way to explain itself.
                distinctKinds?.let { put("kinds", it) }
                future?.let { put("futureDated", it) }
                // Derived from the per-kind spans rather than asked for: a bare
                // `max(created_at)` with no grouping level is not a query Vespa
                // can answer at all — see StatsYql.NO_BARE_AGGREGATES — and the
                // kinds histogram has already paid for this number.
                newestEvent?.let { put("newestEvent", it) }
                put("asOf", now)
            }
        }

    /**
     * The trust view's own health — the numbers that say whether ranked search
     * can rank at all.
     *
     * This is the one section that reads a SECOND document type. It exists
     * because the failure it detects is silent and total: `TrustReconcile`'s own
     * KDoc warns that "a corpus mirrored before its provider lists arrived stays
     * silently unprojected, and every ranked search comes back empty", and until
     * now that state looked identical to a healthy relay on every page we serve.
     * A `scoredPubkeys` of zero IS that failure, stated as a number.
     *
     * The four counts are a chain, and reading them together localises a break:
     * observers name providers, providers publish scores, scores project onto
     * pubkeys. Zero observers is a mirror that never fetched a kind-10040; zero
     * scores with observers present is a sync that has not reached kind 30382;
     * scores present with zero scored pubkeys is the projection itself being
     * behind, which is what a reconcile fixes.
     */
    private suspend fun trustSection(): JsonObject =
        section(
            note =
                "`scoredPubkeys` and the corpus's `pubkeys` are INDEPENDENT populations, not a ratio: the web of " +
                    "trust scores people whose events this relay may not hold, and holds events from people nobody " +
                    "has scored. Compare their magnitudes, not their quotient.",
        ) { attempts ->
            val scored = attempt(attempts, "scoredPubkeys") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL, source = StatsYql.REPUTATION)) }
            val observers = attempt(attempts, "observers") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"), "kind = $KIND_OBSERVER")) }
            val providers = attempt(attempts, "providers") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"), "kind = $KIND_SCORE")) }
            val scores = attempt(attempts, "scores") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL, "kind = $KIND_SCORE")) }
            buildJsonObject {
                scored?.let { put("scoredPubkeys", it) }
                observers?.let { put("observers", it) }
                providers?.let { put("providers", it) }
                scores?.let { put("scores", it) }
            }
        }

    /**
     * What a kind is CALLED, from Quartz's registry rather than this repo's.
     *
     * [KindNames] is the protocol-wide table Quartz maintains — 287 kinds and
     * their NIPs, updated whenever the pin moves. The web UI's `kinds.js`
     * carries about 117, which is the right size for what IT is: badge text
     * for the cards this relay can render, kept short and lowercase so a mixed
     * feed stays scannable. That set is a statement about our renderers; this
     * table's job is the opposite one — it ENUMERATES the store, so it holds
     * kinds nobody here has ever written a card for, and naming them from a
     * registry of renderers meant 180 of them could only ever read "kind N".
     *
     * Emitted INTO the document rather than resolved in the page, because
     * /stats.json is the artifact and a reader charting it elsewhere should
     * not have to carry a copy of this table to label an axis. The page still
     * falls back to `kinds.js` for the ten kinds Quartz does not name yet
     * (`1630`-`1633` git statuses, `30024`, `30040`…), so the two sources
     * compose instead of one replacing the other.
     */
    private fun kindName(kind: Int): String? = KindNames.nameFor(kind)

    /**
     * The per-kind table: documents, distinct authors, and the span of
     * `created_at`, as three queries rather than one combined pipeline.
     *
     * NO author count. It used to be here, split from the other two because it
     * was "by far the most expensive" — that split bounded the blast radius but
     * not the cost, and at 91.5M events the cost is the problem.
     *
     * `all(group(kind) each(all(group(pubkey) output(count()))))` partitions the
     * WHOLE corpus: every event has exactly one kind, so the union of the inner
     * groupings is every document in the store, and with
     * `grouping.defaultMaxGroups = -1` — correctly -1, a truncated histogram is
     * a wrong statistic — the engine materialises a pubkey set for each of 122
     * kinds, kind 1 alone being 39.7M events. Measured 2026-08-08: rollups
     * driving proton to allocate 2 GiB `PartialResult` buffers per match thread,
     * container RSS into the cgroup ceiling, jdisc refusing connections, and the
     * rollup's own later queries failing with java.net.ConnectException. Also
     * what OOMKilled the engine at a 46Gi limit, and then again at 64Gi.
     *
     * The time-bucketed authors in [series] stay: they look identical but are
     * WINDOWED, so they group a slice rather than the store. The monthly one is
     * the slice worth watching — [DEFAULT_MONTH_SERIES_START] anchors it to a
     * date, so it widens by a month every month, and it is this same
     * pubkey-set-per-group shape.
     *
     * There is no cheaper way to ask this question of the engine — a distinct
     * count over a high-cardinality field IS the group set. If the column comes
     * back it needs a different source (a counter maintained on write, or a
     * sketch), not a different query.
     *
     * EVERY kind, not a top-N. This is the table that replaced
     * `kind_stats.html`, and the reason it could is that the grouping ENUMERATES
     * what the store holds: that page asked one NIP-45 COUNT per kind it already
     * knew to name — the search UI's card registry, plus whatever an operator
     * typed in — so a kind nobody had registered was invisible to the only page
     * that would have revealed it. A histogram has no such blind spot, and
     * truncating it here would reintroduce one for exactly the long tail the
     * question "what does this relay hold" is asked about.
     *
     * The cost is bounded by the corpus's distinct kinds, not by its events —
     * order of thousands, one small object each, and the response gzips well
     * because the rows are near-identical. Sorted by volume so the page can
     * render the head first and the tail below it.
     */
    private suspend fun kindsSection(): JsonObject =
        section { attempts ->
            val counts =
                attempt(attempts, "events") { longsByGroup(StatsYql.countsBy("kind")) }
                    ?: return@section buildJsonObject { }
            // Bounded to now: an unbounded max(created_at) reports whatever the
            // most optimistically-dated spam in that kind claims, and a "newest"
            // of 2100 makes the whole column decorative. The future-dated events
            // are counted in `corpus` instead, where they are the finding rather
            // than the noise.
            val spans = attempt(attempts, "span") { spansByGroup(StatsYql.spanBy("kind"), StatsYql.upTo(nowSeconds())) }
            buildJsonObject {
                put("total", counts.size)
                putJsonArray("all") {
                    counts.entries
                        .sortedWith(compareByDescending<Map.Entry<String, Long>> { it.value }.thenBy { it.key.toIntOrNull() ?: 0 })
                        .forEach { (kind, events) ->
                            add(
                                buildJsonObject {
                                    val n = kind.toIntOrNull() ?: -1
                                    put("kind", n)
                                    kindName(n)?.let { put("name", it) }
                                    put("events", events)
                                    spans?.get(kind)?.let { (first, last) ->
                                        put("firstSeen", first)
                                        put("lastSeen", last)
                                    }
                                },
                            )
                        }
                }
            }
        }

    /**
     * The time series: events and distinct authors per UTC day, week and month,
     * plus the hour-of-day shape.
     *
     * Three granularities rather than one, because a coarser bucket is NOT the
     * finer one re-added. Events sum, but distinct authors do not: someone who
     * posts every day is one author in the week and seven in the sum of its
     * days. So a weekly "publishing pubkeys" has to be asked of the engine at
     * weekly granularity, and the page's Daily/Weekly/Monthly toggle switches
     * between three answers rather than re-aggregating one.
     *
     * The monthly series is the one anchored to a DATE rather than to a length —
     * see [DEFAULT_MONTH_SERIES_START] — which makes it the only one that is
     * both filled to its whole span (it deliberately reaches back past the day
     * this mirror started holding anything) and asked for A YEAR AT A TIME
     * rather than in one query (see [StatsYql.monthSlicesFrom]).
     */
    private suspend fun activitySection(): JsonObject =
        section { attempts ->
            val now = nowSeconds()
            val hourWindow = StatsYql.window(now - hourWindowDays * DAY_SECONDS, now)
            // Windows that trail the present, so one query each and nothing to
            // fill: 30 days and 26 weeks sit inside any live mirror's history.
            val days = series(attempts, "days", StatsYql.DAY, StatsYql::isoDay, listOf(Slice(now - windowDays * DAY_SECONDS, now)))
            val weeks =
                series(attempts, "weeks", StatsYql.WEEK, StatsYql::isoWeekStart, listOf(Slice(now - weekWindowWeeks * 7 * DAY_SECONDS, now)))
            // One slice per calendar year, on exact month boundaries — where the
            // old window was `monthWindowMonths * 31` days back from now
            // precisely because it could not land on one, and paid a partial
            // leading month for it.
            val monthSlices = StatsYql.monthSlicesFrom(monthSeriesStart, now)
            val months =
                series(
                    attempts,
                    "months",
                    StatsYql.MONTH,
                    StatsYql::isoMonth,
                    monthSlices.map { Slice(it.since, it.until, key = it.year.toString(), fill = it.months) },
                )
            val hours = attempt(attempts, "hours") { longsByGroup(StatsYql.countsBy(StatsYql.HOUR), hourWindow) }
            buildJsonObject {
                put("windowDays", windowDays)
                put("windowWeeks", weekWindowWeeks)
                // The months the WINDOW covers, which is not the same as the
                // number of bars when a year's query fails — that year's months
                // are then absent rather than drawn at zero, and the section's
                // errors name it. Derived from the slices rather than stated
                // beside them, so the two cannot disagree about the span.
                put("windowMonths", monthSlices.sumOf { it.months.size })
                // The anchor itself, because "44 months" is not what this window
                // means and a reader charting /stats.json elsewhere would have
                // to count backwards from `generatedAt` to recover the date the
                // series actually starts on.
                put("monthsSince", monthSeriesStart.toString())
                put("hourWindowDays", hourWindowDays)
                days?.let { put("days", it) }
                weeks?.let { put("weeks", it) }
                months?.let { put("months", it) }
                hours?.let { byHour ->
                    putJsonArray("hours") {
                        // Every hour, including the empty ones: a chart that
                        // silently drops 03:00 because nobody posted redraws
                        // 23 bars as if the day were shorter.
                        (0..23).forEach { hour ->
                            add(
                                buildJsonObject {
                                    put("hour", hour)
                                    put("events", byHour[hour.toString()] ?: 0L)
                                },
                            )
                        }
                    }
                }
            }
        }

    /**
     * The relay urls this store's NIP-65 lists name, and how many lists name
     * each — the "where do our users read and write" panel.
     *
     * One grouping over `tag_index` filtered to kind 10002, which is affordable
     * exactly because of what a relay list is: few tags per event, and the
     * values repeat across users, so the distinct set is relay urls (thousands)
     * rather than event ids (millions). The same pipeline on kind 1 would try to
     * return every `e` and `p` tag in the corpus.
     *
     * `lists` and not `users`: kind 10002 is replaceable, so the store holds one
     * per author and the two are equal today — but that equality is the store's
     * supersession behaving, not something this query establishes, and a column
     * headed "users" would be asserting it.
     */
    private suspend fun relaysSection(): JsonObject =
        section(
            note =
                "How many stored NIP-65 lists name each relay. NOT split by read/write: that marker is an `r` tag's " +
                    "THIRD element and tag_index holds only `<letter>:<value>`, so it is not queryable — it needs a " +
                    "walk over kind 10002.",
        ) { attempts ->
            val pairs =
                attempt(attempts, "relays") { longsByGroup(StatsYql.countsBy(StatsYql.TAG), "kind = 10002") }
                    ?: return@section buildJsonObject { }
            // A 10002 may carry tags other than `r`; keep the relay urls and
            // drop the rest rather than charting whatever else was on the event.
            //
            // SUMMED per canonical url, not `toMap()`. The grouping returns one
            // row per distinct string, and `wss://nos.lol` and `wss://nos.lol/`
            // are two strings for one relay — `toMap()` kept whichever came last
            // and threw the other's count away, so the relay both understated
            // its lists AND sat too low in a table sorted by them. See
            // [StatsYql.canonicalRelay] for why the normalizer is the one the
            // router dials with.
            val relays =
                pairs
                    .mapNotNull { (pair, count) -> StatsYql.tagValue(pair, 'r')?.let { StatsYql.canonicalRelay(it) to count } }
                    .groupingBy { it.first }
                    .fold(0L) { sum, (_, count) -> sum + count }
            buildJsonObject {
                put("total", relays.size)
                put("shown", minOf(relays.size, topRelays))
                putJsonArray("top") {
                    relays.entries
                        .sortedWith(compareByDescending<Map.Entry<String, Long>> { it.value }.thenBy { it.key })
                        .take(topRelays)
                        .forEach { (url, lists) ->
                            add(
                                buildJsonObject {
                                    put("relay", url)
                                    put("lists", lists)
                                },
                            )
                        }
                }
            }
        }

    /**
     * Zap receipts: how many, from how many wallets, and their shape over time.
     *
     * Deliberately NOT sats. The amount lives in the `bolt11` tag and in the
     * kind-9734 request nested in `description`, both multi-character tag names
     * that `tag_index` cannot address, and `content` is summary-only — so no
     * grouping query can reach a number of satoshis, and a total here would have
     * to be invented. Senders and recipients are the same story for a different
     * reason: they ARE addressable (`P:` and `p:`, cased and single-letter), but
     * grouping `tag_index` over kind 9735 would emit every `e:` tag with them,
     * which is one distinct value per zap.
     *
     * `wallets` is the receipt's own author — under NIP-57 that is the LNURL
     * service that settled the invoice, not the person who zapped. Worth having
     * on its own terms; it is not a user count and is not labelled as one.
     */
    private suspend fun zapsSection(): JsonObject =
        section(
            note =
                "Receipt counts only. Sats are unreachable by any grouping — the amount is in the `bolt11` and " +
                    "`description` tags, whose multi-character names are absent from tag_index — and senders/recipients " +
                    "would drag every `e:` tag along with them. Both need a walk over kind 9735.",
        ) { attempts ->
            val now = nowSeconds()
            val window = StatsYql.window(now - windowDays * DAY_SECONDS, now)
            val receipts = attempt(attempts, "receipts") { StatsYql.singleCount(vespa.group(StatsYql.TOTAL, "kind = 9735")) }
            val wallets = attempt(attempts, "wallets") { StatsYql.singleCount(vespa.group(StatsYql.distinct("pubkey"), "kind = 9735")) }
            val days =
                attempt(attempts, "days") {
                    bucketed(StatsYql.countsBy(StatsYql.DAY), "kind = 9735 and $window", StatsYql::isoDay, distinct = false)
                }
            buildJsonObject {
                receipts?.let { put("receipts", it) }
                wallets?.let { put("wallets", it) }
                put("windowDays", windowDays)
                days?.let { byDay ->
                    putJsonArray("days") {
                        byDay.entries.sortedBy { it.key }.forEach { (day, count) ->
                            add(
                                buildJsonObject {
                                    put("day", day)
                                    put("receipts", count)
                                },
                            )
                        }
                    }
                }
            }
        }

    /**
     * A daily series per kind, for the largest few — the mirror filling, broken
     * out by what it is filling with.
     *
     * The reference dashboard declares this panel and ships it empty. Ours is
     * one query per kind, which is why it is capped: the cost is linear in the
     * number of series and nobody reads twenty sparklines.
     *
     * Takes the kinds from [kindsSection]'s own histogram rather than a list
     * here, so the panel follows the corpus instead of an opinion about it that
     * would go stale in the direction of hiding whatever grew.
     */
    private suspend fun kindActivitySection(topByEvents: List<Int>): JsonObject =
        section { attempts ->
            val now = nowSeconds()
            val since = now - windowDays * DAY_SECONDS
            if (topByEvents.isEmpty()) return@section buildJsonObject { put("windowDays", windowDays) }
            // ONE query for every series, not one per kind. `group(kind)` with a
            // nested `group(time.date)` answers the whole panel in a single
            // round trip; this was eight sequential aggregations for eight
            // sparklines, and the engine was doing the same work either way.
            val perKind =
                attempt(attempts, "series") {
                    nestedBuckets(
                        StatsYql.nested("kind", StatsYql.DAY),
                        "kind in (${topByEvents.joinToString(", ")}) and ${StatsYql.window(since, now)}",
                        StatsYql::isoDay,
                    )
                }.orEmpty()
            buildJsonObject {
                put("windowDays", windowDays)
                putJsonArray("kinds") {
                    // In the histogram's order — largest first — rather than the
                    // engine's, so the panel reads the same way as the table.
                    topByEvents.forEach { kind ->
                        val byDay = perKind[kind.toString()] ?: return@forEach
                        add(
                            buildJsonObject {
                                put("kind", kind)
                                kindName(kind)?.let { put("name", it) }
                                putJsonArray("days") {
                                    byDay.entries.sortedBy { it.key }.forEach { (day, count) ->
                                        add(
                                            buildJsonObject {
                                                put("day", day)
                                                put("events", count)
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
     * One bucketed series — events and distinct authors per bucket — as the
     * array the page charts.
     *
     * [decode] turns the engine's bucket value into the label the page sorts and
     * prints. Every bucket pipeline here needs one and none of them agree: a day
     * arrives unpadded, a week as an epoch-relative integer, a month as
     * `year * 12 + month`. Routing all three through this parameter is what
     * keeps [StatsYql.isoDay]'s trap from having to be re-remembered per series.
     *
     * ONE QUERY PAIR PER SLICE, merged. A series whose window is a fixed trailing
     * length is one slice; the monthly one is a slice per calendar year, so no
     * single query's group set grows with the anchor — the argument is in
     * [StatsYql.monthSlicesFrom], and the reason the merge is a union of disjoint
     * keys rather than an addition is there too.
     *
     * Sequentially, deliberately. Firing the years concurrently would put the
     * same peak back into the engine at the same instant, which is the cost the
     * slicing exists to avoid; the rollup is a background timer with nothing
     * waiting on it.
     *
     * [Slice.fill] is the set of labels that must appear WHETHER OR NOT the
     * engine returned a bucket for them, emitted at zero. A grouping only returns
     * buckets that matched something, so an empty month is not a zero-height bar
     * — it is one fewer bar, and a chart asked for 44 months silently redraws as
     * however many of them had events, starting wherever the corpus happens to
     * begin rather than where the window does. Same argument the hour-of-day
     * series already makes for its 24 slots, and it weighs more here: the
     * monthly window deliberately reaches back past the day this mirror started
     * holding anything, so the empty buckets are part of the answer.
     *
     * ## What a failed slice costs, and why the two columns differ
     *
     * A slice whose events query fails contributes NOTHING — not even its fill.
     * Zero bars for a year we could not read would be a statement about the
     * corpus; absent bars are a gap, beside a section badge reading `partial`
     * and an error naming the year. The whole series is null only when every
     * slice failed, which is the same contract this had when there was one.
     *
     * The pubkeys column is ALL OR NOTHING across slices, because the page
     * cannot draw a hole in it: a point with no `pubkeys` key charts at zero
     * exactly like a real zero, so one unreadable year inside a readable series
     * would put twelve invented zeros in the middle of a chart with nothing to
     * mark them. Dropping the column omits the card instead, which is what a
     * column we could not read everywhere deserves.
     */
    private suspend fun series(
        attempts: Attempts,
        name: String,
        bucket: String,
        decode: (String) -> String?,
        slices: List<Slice>,
    ): JsonArray? {
        val events = LinkedHashMap<String, Long>()
        val authors = LinkedHashMap<String, Long>()
        var answered = 0
        var authorsWhole = true
        for (slice in slices) {
            // "months.2024.events" when there are years to tell apart, plain
            // "days.events" when there are not — an error key names the query
            // that failed, and for the monthly series that is one year of it.
            val key = slice.key?.let { "$name.$it" } ?: name
            val where = StatsYql.window(slice.since, slice.until)
            val counts = attempt(attempts, "$key.events") { bucketed(StatsYql.countsBy(bucket), where, decode, distinct = false) }
            if (counts == null) {
                // Both columns lose this slice: pubkeys without the events
                // beside them would chart a year we just said we cannot read.
                authorsWhole = false
                continue
            }
            answered++
            // The UNION, not the fill alone: a bucket the engine returned from
            // outside the labels we asked for is a disagreement between the
            // window and the enumeration, and dropping it here would hide it.
            val labels = counts.keys + slice.fill
            labels.forEach { events[it] = counts[it] ?: 0L }
            // EMPTY is not zero, and the difference is a sentence this page
            // prints. [bucketed] DROPS a group it cannot read rather than
            // throwing — deliberate, and it means an authors response whose
            // shape we stop understanding ([StatsYql.distinctCountOf] carries a
            // fallback for exactly that engine change) arrives as an empty map
            // rather than as a failed attempt. Zero-filling that writes
            // `"pubkeys": 0` onto every bucket, and stats.html states it: "No
            // publishing pubkeys in this window.", in the error colour, beside
            // an events chart full of bars.
            val byLabel =
                attempt(attempts, "$key.pubkeys") { bucketed(StatsYql.distinctAuthorsBy(bucket), where, decode, distinct = true) }
                    ?.takeIf { it.isNotEmpty() }
            if (byLabel == null) {
                authorsWhole = false
            } else {
                // Zero rather than absent once the column is readable: a bucket
                // holding no documents has no distinct authors either.
                labels.forEach { authors[it] = byLabel[it] ?: 0L }
            }
        }
        if (answered == 0) return null
        return buildJsonArray {
            events.keys.sorted().forEach { label ->
                add(
                    buildJsonObject {
                        put("period", label)
                        put("events", events[label] ?: 0L)
                        if (authorsWhole) authors[label]?.let { put("pubkeys", it) }
                    },
                )
            }
        }
    }

    /**
     * One query's worth of a series: the window to ask for, the labels that
     * window is responsible for, and a name for it when a failure has to be
     * reported.
     *
     * [key] is null for a series asked in one query — its errors are then
     * `days.events` as they always were, rather than growing a segment that
     * would mean nothing.
     */
    private data class Slice(
        val since: Long,
        val until: Long,
        val key: String? = null,
        val fill: List<String> = emptyList(),
    )

    // ---- section plumbing ---------------------------------------------------

    /**
     * Run [body], collecting per-query failures, and wrap the result in the
     * status envelope every section shares:
     *
     *   ok       everything this section asks for came back
     *   partial  some of it did; `data` holds that, `errors` names the rest
     *   failed   none of it did
     *
     * [note] is a caveat on a section that SUCCEEDED — what it deliberately does
     * not contain, and why. Distinct from `errors`, which is what broke. Zaps
     * and relay distribution both need one: each answers a real question while
     * leaving out the field a reader of the reference dashboard would come
     * looking for (satoshis; the read/write split), and an unannotated number is
     * how someone concludes we hold no zap amounts rather than that we cannot
     * query them.
     */
    private suspend fun section(
        note: String? = null,
        body: suspend (Attempts) -> JsonObject,
    ): JsonObject {
        val attempts = Attempts()
        val startedMs = System.currentTimeMillis()
        val data = body(attempts)
        return buildJsonObject {
            put("status", attempts.status())
            put("generatedAt", Instant.ofEpochMilli(startedMs).toString())
            put("tookMs", System.currentTimeMillis() - startedMs)
            note?.let { put("note", it) }
            put("data", data)
            if (attempts.errors.isNotEmpty()) {
                putJsonObject("errors") { attempts.errors.forEach { (k, v) -> put(k, v) } }
            }
        }
    }

    /**
     * What a section's queries did — how the status is decided.
     *
     * Counting SUCCESSES rather than inspecting `data`, which is the mistake
     * this replaced: the status was `data.isEmpty() -> "failed"`, and every
     * section writes some metadata of its own (`asOf`, `windowDays`, the window
     * spans) before a single query returns. So `data` was never empty, `failed`
     * was unreachable, and a section whose every query errored reported
     * `partial` — the status that means "some of this is real".
     */
    private class Attempts {
        val errors = LinkedHashMap<String, String>()
        var succeeded = 0
            private set

        fun ok() {
            succeeded++
        }

        fun status(): String =
            when {
                errors.isEmpty() -> "ok"
                succeeded == 0 -> "failed"
                else -> "partial"
            }
    }

    /**
     * Run one query, recording a failure under [key] instead of propagating it.
     *
     * `CancellationException` is rethrown: shutdown cancels the maintenance
     * scope, and a cancelled rollup that recorded itself as a Vespa error would
     * persist a document blaming the engine for the operator stopping the relay.
     */
    private suspend fun <T> attempt(
        attempts: Attempts,
        key: String,
        query: suspend () -> T?,
    ): T? =
        try {
            query().also { attempts.ok() }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            attempts.errors[key] = e.message ?: e.toString()
            null
        }

    // ---- readers ------------------------------------------------------------

    private suspend fun longsByGroup(
        pipeline: String,
        where: String = "true",
    ): Map<String, Long> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { g ->
                val value = StatsYql.valueOf(g) ?: return@mapNotNull null
                val count = StatsYql.aggOf(g, "count()") ?: return@mapNotNull null
                value to count
            }.toMap()
    }

    /**
     * A two-level pipeline: outer value -> inner label -> count.
     *
     * The inner labels go through [decode] for the same reason the flat ones do
     * — a `time.date` bucket is no more sortable nested than it is at the top.
     */
    private suspend fun nestedBuckets(
        pipeline: String,
        where: String,
        decode: (String) -> String?,
    ): Map<String, Map<String, Long>> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { outer ->
                val key = StatsYql.valueOf(outer) ?: return@mapNotNull null
                val inner =
                    StatsYql
                        .childGroups(outer)
                        .mapNotNull { g ->
                            val label = StatsYql.valueOf(g)?.let(decode) ?: return@mapNotNull null
                            val count = StatsYql.aggOf(g, "count()") ?: return@mapNotNull null
                            label to count
                        }.toMap()
                key to inner
            }.toMap()
    }

    /**
     * A bucketed pipeline, rekeyed by [decode] — the one path every time series
     * goes through.
     *
     * The decoder is not optional formatting. No bucket pipeline here returns a
     * value that can be used as a chart label or a sort key as it stands: a day
     * arrives unpadded ([StatsYql.isoDay]), a week as an epoch-relative bucket
     * index, a month as `year * 12 + month`. A bucket the decoder rejects is
     * DROPPED rather than passed through, so an engine that changes a format
     * loses points visibly instead of scrambling an axis.
     */
    private suspend fun bucketed(
        pipeline: String,
        where: String,
        decode: (String) -> String?,
        distinct: Boolean,
    ): Map<String, Long> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { g ->
                val label = StatsYql.valueOf(g)?.let(decode) ?: return@mapNotNull null
                val count = (if (distinct) StatsYql.distinctCountOf(g) else StatsYql.aggOf(g, "count()")) ?: return@mapNotNull null
                label to count
            }.toMap()
    }

    private suspend fun spansByGroup(
        pipeline: String,
        where: String = "true",
    ): Map<String, Pair<Long, Long>> {
        val root = vespa.group(pipeline, where)
        return StatsYql
            .topGroups(root)
            .mapNotNull { g ->
                val value = StatsYql.valueOf(g) ?: return@mapNotNull null
                val first = StatsYql.aggOf(g, "min(created_at)") ?: return@mapNotNull null
                val last = StatsYql.aggOf(g, "max(created_at)") ?: return@mapNotNull null
                value to (first to last)
            }.toMap()
    }

    companion object {
        /**
         * Bumped when a RELEASED field changes meaning or leaves, not when one
         * is added: a reader takes what it knows and ignores the rest, so
         * additions are already safe. A reader that sees a schema above the one
         * it was written for should say so rather than chart fields it is
         * guessing at.
         *
         * "Released" is the word that matters, and it was missing here for a
         * while. This endpoint has never shipped, and in the course of building
         * it the document lost `retention`, then `newUsers`, then `kinds.shown`
         * — each a field that LEAVES, each dutifully bumping the number, none of
         * them ever fetched by anyone. That would land the first public version
         * at 4 with no 1, 2 or 3 to point at, which teaches a reader that the
         * number tracks our editing rather than their contract. A version
         * describes what consumers can depend on; there are none until this
         * merges, so it stays at 1 until the first change AFTER that.
         */
        const val SCHEMA_VERSION = 1

        const val DEFAULT_WINDOW_DAYS = 30

        /** The span the reference dashboard's own Weekly toggle covers. */
        const val DEFAULT_WEEK_WINDOW_WEEKS = 26

        /**
         * The monthly chart's first bucket — a DATE, and deliberately not a
         * rolling count of months.
         *
         * The monthly view is the only one asked "how has this relay grown",
         * and a 24-month window answered a different question: it slid forward
         * every month, so the corpus's early years walked off the left edge and
         * the chart's leftmost bar was always ~two years ago rather than the
         * beginning of anything. January 2023 is where Nostr's own volume
         * begins — before it the buckets are noise a log scale would be needed
         * to see, and this page's charts are linear on purpose.
         *
         * Anchored, this window GROWS: a month is added each month and none
         * ever leaves, so the series length is a number the document states
         * (`windowMonths`) rather than a constant a reader can assume.
         *
         * THE COST WOULD GROW WITH IT, which is why the series is not asked for
         * in one query. `countsBy(MONTH)` is cheap at any width — one leaf group
         * per month, whatever the match set. `distinctAuthorsBy(MONTH)` is not:
         * it materialises a PUBKEY SET PER MONTH, the same shape that,
         * partitioning the whole corpus by kind, drove 2 GiB `PartialResult`
         * buffers per match thread and OOMKilled the engine at a 64Gi limit —
         * see [kindsSection], where the measurement is. Over an anchored window
         * that cost would climb every month forever, with nothing in the request
         * path to cap it ([StatsVespa] sets no read timeout, on purpose).
         *
         * [StatsYql.monthSlicesFrom] cuts it at calendar years, so no single
         * query ever holds more than twelve months of pubkey sets no matter how
         * far back the anchor sits, and moving the anchor buys bars rather than
         * risk. What grows is the NUMBER of queries — one more year, two more
         * groupings, once a year — and the total work over the corpus, which
         * would have been the same asked either way.
         */
        val DEFAULT_MONTH_SERIES_START: YearMonth = YearMonth.of(2023, 1)

        const val DEFAULT_HOUR_WINDOW_DAYS = 7
        const val DEFAULT_TOP_RELAYS = 50

        /**
         * How many kinds get their own daily series. One query each, so this is
         * the panel's whole cost — and past a handful nobody reads the
         * sparklines anyway.
         */
        const val DEFAULT_KIND_SERIES = 8
        private const val DAY_SECONDS = 86_400L

        /** NIP-85: the list naming which service scores which dimension for an observer. */
        private const val KIND_OBSERVER = 10040

        /** NIP-85: one published trust score. */
        private const val KIND_SCORE = 30382
    }
}

/**
 * Recompute the stats document every [everySeconds] into [snapshot], persisting
 * each result so a restart serves the last one instead of an empty page.
 *
 * Runs BEHIND the server like every other job in this package — the first
 * rollup on a large corpus is minutes of grouping, and no client should wait on
 * it. Until it finishes, `/stats.json` serves whatever the state file held.
 */
internal fun launchStatsRollup(
    scope: CoroutineScope,
    rollup: StatsRollup,
    snapshot: StatsSnapshot,
    everySeconds: Long,
) {
    scope.launch {
        while (true) {
            val startedMs = System.currentTimeMillis()
            runCatching { rollup.compute() }
                .onSuccess { doc ->
                    snapshot.publish(doc)
                    val secs = (System.currentTimeMillis() - startedMs) / 1000
                    // The failure count, not the failures: a section's own
                    // error text is in the document, where the operator can
                    // read it beside the query that produced it.
                    val failed = doc.entries.count { (_, v) -> (v as? JsonObject)?.statusOf() in setOf("failed", "partial") }
                    println(
                        "stats: rolled up in ${secs}s" +
                            (if (failed > 0) " — $failed section(s) incomplete, see /stats.json" else ""),
                    )
                }.onFailure { e ->
                    if (e is CancellationException) throw e
                    // compute() catches per query, so reaching here means the
                    // assembly itself broke — worth a loud line, and worth
                    // keeping the previous document rather than blanking it.
                    System.err.println("stats: rollup failed (${e.message}) — serving the previous document")
                }
            delay(everySeconds * 1000)
        }
    }
}

private fun JsonObject.statusOf(): String? = (this["status"] as? kotlinx.serialization.json.JsonPrimitive)?.content
