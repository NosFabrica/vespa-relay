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

import com.nosfabrica.vespa.relay.util.fmtDuration
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * The store calls this process has outstanding, who asked for each and for
 * how long: the store-side counterpart of [InFlight].
 *
 * A row is one call our code is suspended in, not a Vespa request; a call
 * can fan out into chunked reads inside the store. Every subsystem hits one
 * anonymous `IEventStore`, so attribution is carried by [Caller] and reached
 * through the coroutine context rather than plumbed through six signatures.
 * A call outside a scope carrying the element is untracked, never
 * misattributed: [storeCall] runs the block untouched.
 */
class StoreCalls(
    /**
     * How long a call runs before [warnSlow] names it (`SYNC_STORE_SLOW_SEC`,
     * in millis; 0 warns about nothing). Far below the ingest wedge bound on
     * purpose: this prints a line, that publishes the word `wedged`.
     */
    private val slowAfterMs: Long = DEFAULT_SLOW_AFTER_MS,
    /** How long before a call still running is named again. */
    private val rewarnAfterMs: Long = DEFAULT_REWARN_AFTER_MS,
    /** Where a row's issue stamp comes from; a parameter so a test can age a call. */
    private val now: () -> Long = System::currentTimeMillis,
) : AbstractCoroutineContextElement(StoreCalls) {
    /**
     * One call in flight. No `stage` and no `quietForSec`: a store call
     * delivers nothing until it delivers everything.
     */
    class Call(
        /** One of the `CALLER_*` constants. */
        val caller: String,
        /** One of the `OP_*` constants, the store method by name. */
        val op: String,
        /**
         * A summary of the filter, never the filter: kinds, how many authors,
         * how many ids, the window. Null for a call that carries no filter.
         */
        val asked: String?,
        /** Epoch seconds, so a log line can be lined up against a row. */
        val issuedAt: Long,
        val elapsedSec: Long,
        /**
         * How many store calls this process already had out when this one was
         * issued. Small beside a slow call means the wait was not on our side.
         */
        val outstandingAtIssue: Int,
    )

    /**
     * One subsystem's traffic through the store.
     *
     * [issued], [returned], [failed] and [cancelled] are lifetime counters;
     * [outstanding] and [oldestOutstandingSec] are live and come off the same
     * row snapshot as [Snapshot.calls] and [Snapshot.ages], so the three agree
     * exactly. `issued - returned - failed - cancelled` need not equal
     * [outstanding] on a busy router; it is stamped at different instants.
     */
    class Caller(
        val caller: String,
        val issued: Long,
        val returned: Long,
        val failed: Long,
        /** Shutdown, not a fault; folded into [failed] it would read as a store refusing work. */
        val cancelled: Long,
        val outstanding: Int,
        val oldestOutstandingSec: Long?,
    )

    /**
     * One band of the outstanding-age histogram: calls running at least
     * [fromSec] and less than the next band's. The bands partition the
     * outstanding set and sum to [Snapshot.outstanding]; the last is open-ended.
     */
    class Age(
        val fromSec: Long,
        val calls: Int,
    )

    /** Everything above, as of one instant. */
    class Snapshot(
        /** The [warnSlow] bound in seconds, 0 when off; published so the page colours rows at the router's own threshold. */
        val slowAfterSec: Long,
        val outstanding: Int,
        val issued: Long,
        val returned: Long,
        val failed: Long,
        val cancelled: Long,
        /** The outstanding calls, longest-running first. */
        val calls: List<Call>,
        /** How many more were outstanding and are not named here. See [MAX_CALL_ROWS]. */
        val omitted: Int,
        val callers: List<Caller>,
        val ages: List<Age>,
    )

    /** One call that has been issued and has not come back. */
    private class Open(
        val caller: String,
        val op: String,
        val asked: String?,
        val issuedMs: Long,
        val outstandingAtIssue: Int,
    ) {
        /** When [warnSlow] last named it. */
        @Volatile
        var warnedAtMs: Long = 0
    }

    /**
     * One caller's lifetime tally, filed under [name], which is
     * [OVERFLOW_CALLER] once the map is full. No `outstanding` counter here:
     * live counts come off the row snapshot so the callers, bands and total
     * agree by construction.
     */
    private class Tally(
        val name: String,
    ) {
        val issued = AtomicLong()
        val returned = AtomicLong()
        val failed = AtomicLong()
        val cancelled = AtomicLong()
    }

    /** The calls that are out, keyed by identity; [Open] overrides neither `equals` nor `hashCode`. */
    private val open = ConcurrentHashMap.newKeySet<Open>()
    private val tallies = ConcurrentHashMap<String, Tally>()
    private val outstanding = AtomicInteger()

    /**
     * Run [block] as one store call by [caller], booked from issue to return
     * however it returns. The removal is in a `finally`: a row that outlives
     * its call is a fault report about work that is not happening.
     */
    suspend fun <T> track(
        caller: String,
        op: String,
        filter: String? = null,
        block: suspend () -> T,
    ): T {
        val tally = tallyFor(caller)
        tally.issued.incrementAndGet()
        // One atomic for the admission and the reading both; a get then an
        // increment let two calls issued together each report the other absent.
        val found = outstanding.incrementAndGet() - 1
        val row = Open(tally.name, op, filter, now(), found)
        open.add(row)
        try {
            val answer = block()
            tally.returned.incrementAndGet()
            return answer
        } catch (e: CancellationException) {
            tally.cancelled.incrementAndGet()
            throw e
        } catch (e: Throwable) {
            tally.failed.incrementAndGet()
            throw e
        } finally {
            open.remove(row)
            outstanding.decrementAndGet()
        }
    }

    /**
     * The tally [caller] reports into, keeping at most [MAX_CALLERS]. Every
     * caller name is a constant today; the bound is against a call site that
     * one day interpolates a url into one.
     */
    private fun tallyFor(caller: String): Tally {
        tallies[caller]?.let { return it }
        // Racy by a caller or two at the boundary: the point is a bound, not an
        // exact size.
        if (tallies.size >= MAX_CALLERS) return tallies.computeIfAbsent(OVERFLOW_CALLER) { Tally(it) }
        return tallies.computeIfAbsent(caller) { Tally(it) }
    }

    /** Everything outstanding and every caller's tally, as of [nowMs]. */
    fun snapshot(nowMs: Long = now()): Snapshot {
        // One read of the set; the rows, per-caller counts, age bands and total
        // are all derived from it so they cannot disagree. Nothing below may
        // re-read `open`.
        val rows = open.toList()
        val named =
            rows
                // Longest-running first: a store call that has not come back is
                // the anomaly. Then caller and op, so one state rolls up one way.
                .sortedWith(compareBy({ it.issuedMs }, { it.caller }, { it.op }))
                .take(MAX_CALL_ROWS)
                .map {
                    Call(
                        caller = it.caller,
                        op = it.op,
                        asked = it.asked,
                        issuedAt = it.issuedMs / 1000,
                        elapsedSec = ((nowMs - it.issuedMs) / 1000).coerceAtLeast(0),
                        outstandingAtIssue = it.outstandingAtIssue,
                    )
                }
        // Both live members off the one row snapshot, so a row cannot report
        // `outstanding: 1` beside no age.
        val outPerCaller = HashMap<String, Int>()
        val oldestPerCaller = HashMap<String, Long>()
        for (row in rows) {
            outPerCaller.merge(row.caller, 1, Int::plus)
            oldestPerCaller.merge(row.caller, row.issuedMs, ::minOf)
        }
        val callers =
            tallies
                .map { (name, t) ->
                    Caller(
                        caller = name,
                        issued = t.issued.get(),
                        returned = t.returned.get(),
                        failed = t.failed.get(),
                        cancelled = t.cancelled.get(),
                        outstanding = outPerCaller[name] ?: 0,
                        oldestOutstandingSec = oldestPerCaller[name]?.let { ((nowMs - it) / 1000).coerceAtLeast(0) },
                    )
                }
                // Whoever is holding the most, first; ties by lifetime traffic
                // then name, so one state rolls up one way twice.
                .sortedWith(compareByDescending<Caller> { it.outstanding }.thenByDescending { it.issued }.thenBy { it.caller })
        // The four lifetime totals in one pass, so they describe one instant.
        var issued = 0L
        var returned = 0L
        var failed = 0L
        var cancelled = 0L
        for (t in tallies.values) {
            issued += t.issued.get()
            returned += t.returned.get()
            failed += t.failed.get()
            cancelled += t.cancelled.get()
        }
        return Snapshot(
            slowAfterSec = slowAfterMs / 1_000,
            outstanding = rows.size,
            issued = issued,
            returned = returned,
            failed = failed,
            cancelled = cancelled,
            calls = named,
            omitted = (rows.size - named.size).coerceAtLeast(0),
            callers = callers,
            ages = ages(rows, nowMs),
        )
    }

    /** The outstanding set banded by age. See [Age]. */
    private fun ages(
        rows: List<Open>,
        nowMs: Long,
    ): List<Age> {
        val counts = IntArray(AGE_BANDS.size)
        for (row in rows) {
            val sec = ((nowMs - row.issuedMs) / 1000).coerceAtLeast(0)
            var band = 0
            for (i in AGE_BANDS.indices) if (sec >= AGE_BANDS[i]) band = i
            counts[band]++
        }
        return AGE_BANDS.indices.map { Age(AGE_BANDS[it], counts[it]) }
    }

    /**
     * Log lines for every call past [slowAfterMs], repeated every
     * [rewarnAfterMs] while it is still out. The log rather than the page
     * because a wedge starts while nobody is watching; nothing here cuts a
     * call, since cutting an ingest pass discards a batch nothing re-offers.
     * Returns the lines so the caller owns the prefix and a test can assert them.
     */
    fun warnSlow(nowMs: Long = now()): List<String> {
        if (slowAfterMs <= 0) return emptyList()
        val due =
            open
                .filter { nowMs - it.issuedMs >= slowAfterMs && nowMs - it.warnedAtMs >= rewarnAfterMs }
                .sortedBy { it.issuedMs }
        if (due.isEmpty()) return emptyList()
        val out = due.take(MAX_WARN_LINES)
        for (row in out) row.warnedAtMs = nowMs
        val lines =
            out.map { row ->
                "router: store call SLOW — ${row.caller} ${row.op}" +
                    (row.asked?.let { " ($it)" } ?: "") +
                    " has been running ${fmtDuration(nowMs - row.issuedMs)} " +
                    "(${outstanding.get()} store call(s) outstanding, ${row.outstandingAtIssue} when this one was issued). " +
                    "Nothing here cuts it — the remedy is at the store"
            }
        // The tail is disclosed, not dropped; the unnamed keep their clock and
        // are named on a later pass.
        return if (due.size > out.size) {
            lines + "router: store call SLOW — and ${due.size - out.size} more past the same bound, named on a later pass"
        } else {
            lines
        }
    }

    /**
     * The longest-running call in one clause, for the health line's `wedged`
     * branch. Null when nothing is outstanding, which for a wedge means the
     * worker is stuck somewhere other than a store call.
     */
    fun describeOldest(nowMs: Long = now()): String? {
        val oldest = open.minByOrNull { it.issuedMs } ?: return null
        return "${oldest.caller} ${oldest.op}" +
            (oldest.asked?.let { " ($it)" } ?: "") +
            ", ${fmtDuration(nowMs - oldest.issuedMs)} in"
    }

    companion object Key : CoroutineContext.Key<StoreCalls> {
        /**
         * `SYNC_STORE_SLOW_SEC` and `SYNC_STORE_REWARN_SEC`, refused rather than
         * defaulted when they are not numbers. Zero for the first turns the
         * warning off and keeps the report.
         */
        fun fromEnv(env: Map<String, String>): StoreCalls =
            StoreCalls(
                slowAfterMs = seconds(env, "SYNC_STORE_SLOW_SEC", DEFAULT_SLOW_AFTER_MS),
                rewarnAfterMs = seconds(env, "SYNC_STORE_REWARN_SEC", DEFAULT_REWARN_AFTER_MS),
            )

        private fun seconds(
            env: Map<String, String>,
            name: String,
            fallbackMs: Long,
        ): Long =
            env[name]?.trim()?.takeIf { it.isNotEmpty() }?.let {
                (it.toLongOrNull()?.takeIf { n -> n >= 0 } ?: error("$name='$it' is not a whole number of seconds (0 disables the warning).")) * 1_000
            } ?: fallbackMs

        /**
         * A summary of what a filter asks for, never the filter. See [Call.asked].
         * Kinds first, then the sizes that decide cost, then the window; absent
         * members are left out rather than printed as zero.
         */
        fun summarise(filter: Filter): String =
            buildList {
                filter.kinds?.takeIf { it.isNotEmpty() }?.let { add("kinds ${it.joinToString(",")}") }
                filter.authors
                    ?.size
                    ?.takeIf { it > 0 }
                    ?.let { add("$it author(s)") }
                filter.ids
                    ?.size
                    ?.takeIf { it > 0 }
                    ?.let { add("$it id(s)") }
                // Tag keys and their widths; the values are urls and ids by the hundred.
                filter.tags?.takeIf { it.isNotEmpty() }?.let { tags ->
                    add(tags.entries.joinToString(", ") { (key, values) -> "#$key x${values.size}" })
                }
                window(filter.since, filter.until)?.let { add(it) }
                filter.limit?.let { add("limit $it") }
            }.joinToString(", ").ifEmpty { "everything" }

        /** The list form, which is how the negentropy and delete paths ask. */
        fun summarise(filters: List<Filter>): String =
            when (filters.size) {
                0 -> "nothing"
                1 -> summarise(filters.first())
                else -> "${filters.size} filters: ${summarise(filters.first())}, …"
            }

        /** A bare id probe, which carries no filter. */
        fun ids(count: Int): String = "$count id(s)"

        /** The version probe: one kind against a chunk of authors. */
        fun authorsOfKind(
            kind: Int,
            authors: Int,
        ): String = "kind $kind, $authors author(s)"

        /** How many events a write is carrying. */
        fun events(count: Int): String = "$count event(s)"

        /**
         * The time window as a span, since a negentropy window's cost is its
         * width. Open ends are named: "since 2019" and "until now" are different asks.
         */
        private fun window(
            since: Long?,
            until: Long?,
        ): String? =
            when {
                since != null && until != null -> "window ${fmtDuration((until - since).coerceAtLeast(0) * 1_000)}"
                since != null -> "since $since"
                until != null -> "until $until"
                else -> null
            }

        /**
         * Who asked: one constant per subsystem, named for the code that makes
         * the call so the document greps back to it. Ingest is split three ways
         * because a batch pass makes three calls against three engine paths.
         */
        const val CALLER_INGEST_DEDUP = "ingest.dedup"

        const val CALLER_INGEST_VERSIONS = "ingest.versions"

        const val CALLER_INGEST_WRITE = "ingest.write"

        /** The negentropy pager's reads: the count that sizes a window, and the id snapshot it compares with. */
        const val CALLER_VISIT_NEGENTROPY = "visit.negentropy"

        /** The retraction audit: the owned-ask snapshot, and the deletes a clean reconcile licences. */
        const val CALLER_AUDIT_RETRACTION = "audit.retraction"

        /** The healer resolving what to hand back to a relay that is missing it. */
        const val CALLER_HEAL_RESOLVE = "heal.resolve"

        /** The `dir = up` push: one id snapshot per pass, then the events the diff names. */
        const val CALLER_PUSH_UPSTREAM = "push.upstream"

        /** The monitor reading its own standing verdicts back, and writing new ones. */
        const val CALLER_MONITOR_VERDICTS = "monitor.verdicts"

        const val CALLER_MONITOR_PUBLISH = "monitor.publish"

        /** The url round-up: every relay-list source walked out of the store at the head of a sweep. */
        const val CALLER_SOURCE_RELAY_LISTS = "source.relayLists"

        /**
         * What was asked, the store method verbatim rather than a bucket:
         * `count` and `snapshotIdsForNegentropy` are a sizing query and a read
         * of gigabytes of ids, and a bucket cannot be grepped back to its line.
         */
        const val OP_EXISTING_IDS = "existingIds"

        const val OP_NEWEST_VERSIONS = "newestVersions"

        const val OP_BATCH_INSERT = "batchInsert"

        const val OP_QUERY = "query"

        const val OP_COUNT = "count"

        const val OP_SNAPSHOT_IDS = "snapshotIdsForNegentropy"

        const val OP_INSERT = "insert"

        const val OP_DELETE = "delete"

        /**
         * How many outstanding calls the document names. A cap where [InFlight]
         * has none: calls are bounded by a product of ingest workers, store
         * fan-out and concurrent visits, not one configured width. Rows are
         * longest-running first, so a cut only drops the youngest.
         */
        const val MAX_CALL_ROWS = 200

        /** Distinct callers tallied before [tallyFor] folds the rest into one. */
        const val MAX_CALLERS = 64

        /** Where callers past [MAX_CALLERS] land; named, so a folded tally cannot read as a missing subsystem. */
        const val OVERFLOW_CALLER = "other callers"

        /** Slow-call lines per pass; the rest are counted, not printed. */
        const val MAX_WARN_LINES = 10

        const val DEFAULT_SLOW_AFTER_MS = 60_000L

        const val DEFAULT_REWARN_AFTER_MS = 300_000L

        /** The histogram's bands in seconds, logarithmic so a wedge and a healthy call land in different rows. */
        val AGE_BANDS = listOf(0L, 1L, 10L, 60L, 300L, 900L)
    }
}

/**
 * Book one store call, if this coroutine is running under a [StoreCalls].
 * With nothing installed, which is every test and embedded caller, [block]
 * runs untouched and nothing is recorded.
 */
suspend fun <T> storeCall(
    caller: String,
    op: String,
    filter: String? = null,
    block: suspend () -> T,
): T {
    val calls = currentCoroutineContext()[StoreCalls] ?: return block()
    return calls.track(caller, op, filter, block)
}
