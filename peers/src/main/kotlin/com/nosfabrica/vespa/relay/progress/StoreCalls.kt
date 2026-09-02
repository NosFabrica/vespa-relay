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
 * WHICH STORE CALLS THIS PROCESS HAS OUTSTANDING RIGHT NOW, who asked for each
 * one, and for how long — listed on the same terms [InFlight] lists a stream's
 * relays.
 *
 * ## The question this exists to answer
 *
 * `oldestBatchSec` says two ingest workers have been inside a batch pass for
 * 794 seconds. It does not say WHAT CALL they are in, and that single missing
 * fact is why every wedge in this router's history ended in inference. A batch
 * pass makes up to three different store calls — the id probe, the version
 * probe and the write — against three different engine paths with three
 * different remedies, and from outside the process they are one number.
 * `IngestPipeline.wedged` is explicit that nothing here ends such a call: the
 * store's query client sets `readTimeout(0)` on purpose, so a response that
 * never comes holds the worker for the life of the process. The router's whole
 * job is therefore to SAY WHICH ONE, and it could not.
 *
 * The relay side of that question has been answered for a while — `inFlight`
 * names every relay a stream is holding, with clocks — and the store side had
 * no equivalent at all. This is that equivalent.
 *
 * ## What a row is
 *
 * One call from THIS process to the store that has been issued and has not come
 * back. Not a Vespa request: a call here can fan out into a chunked read inside
 * the store (`existingIds` chunks at `VESPA_DEDUP_CHUNK`), and the row is the
 * thing OUR code is suspended in, which is the thing an operator can act on.
 * Never a queued unit of our own — nothing waits for a slot on this side, which
 * is itself a finding and the reason [Call.outstandingAtIssue] is published.
 *
 * ## Attribution, and why it rides the coroutine context
 *
 * Every subsystem — the ingest probes, the negentropy pager, the healer, the
 * retraction audit, the monitor's verdict reads — hits one anonymous
 * `IEventStore`. So when the engine's container queue sits at 599 there is
 * nothing anywhere that can say whose requests those are, and "what is filling
 * the queue" is a question five visible negentropy reconciles cannot answer.
 * [Caller] is that answer.
 *
 * The registry is reached through [currentCoroutineContext] rather than passed
 * down, and the choice is deliberate. Three of the call sites are functions of
 * an `object` (`RelayDiscovery`) taking the store as a parameter, and two more
 * are adapters the store's own interfaces construct, so a constructor argument
 * would have to be threaded through six public signatures and every test that
 * calls them — for a diagnostic. Cross-cutting instrumentation in this codebase
 * is already reached without plumbing (`IngestStats.timed` is a global object
 * from the store, called from both `:peers` and `:sync`); a context element is
 * the same reach with none of a global's costs, since it is scoped to the
 * process's own engine scope, cannot leak between tests, and is absent — rather
 * than silently shared — anywhere it was not installed.
 *
 * **A call outside a scope carrying the element is UNTRACKED, not
 * misattributed**, and [storeCall] says so by running the block untouched. That
 * is the honest failure: the alternative — a global that every test and every
 * embedded caller writes into — produces a report describing calls nobody in
 * this process made.
 *
 * ## What it deliberately does NOT publish
 *
 * The half of the queue-vs-service question that lives on the SERVER. An
 * operator watching a fresh query return in 0.7s while queued ones wait 175s is
 * inferring that the wait is queueing rather than work, and only a service-start
 * timestamp from the store settles it outright. Nothing here can produce one:
 * the store builds its own OkHttp client with no header or interceptor seam, so
 * neither an `X-Caller` on the wire nor a server-side stamp is reachable from
 * this repository — both want a change in `vespa-eventstore`.
 *
 * What this side CAN say, it says: [Call.outstandingAtIssue] is how many calls
 * this process already had out when this one was issued, and the store's own
 * request dispatcher is 1,024 wide (`VespaHttp.MAX_CONCURRENT_REQUESTS`), far
 * above anything this router runs. So a slow call with a small
 * `outstandingAtIssue` did not wait on OUR side of the wire, which puts the
 * queue at the engine — the inference, but made from a measurement rather than
 * from the absence of one.
 */
class StoreCalls(
    /**
     * How long a call runs before [warnSlow] names it — `SYNC_STORE_SLOW_SEC`,
     * in millis, or 0 to warn about nothing.
     *
     * SIXTY SECONDS, and it is deliberately nowhere near
     * [com.nosfabrica.vespa.relay.ingest.IngestPipeline.WEDGE_AFTER_MS]'s ten
     * minutes. That number decides whether to publish the word `wedged`, and a
     * false wedge is worse than a late one — it retires the word. This decides
     * whether to print a line, which costs nothing and can be wrong all day. A
     * healthy `oldestBatchSec` was measured at 43 on production, so a minute
     * clears the ordinary shape while still turning a status-page snapshot into
     * a timeline hours before anyone opens the page.
     */
    private val slowAfterMs: Long = DEFAULT_SLOW_AFTER_MS,
    /**
     * …and how long before a call still running is named AGAIN.
     *
     * A wedge is watched over hours and the log is the only thing awake for it,
     * so one line per stuck call is a timeline of one point. Five minutes is a
     * line every five minutes per held call — enough to read a duration off the
     * log, few enough that ten stuck calls do not bury the health line they sit
     * beside.
     */
    private val rewarnAfterMs: Long = DEFAULT_REWARN_AFTER_MS,
    /**
     * Where a row's issue stamp comes from.
     *
     * A parameter only so a test can put a call at a known age without holding
     * one open for thirteen minutes — the same seam and the same reason
     * `IngestPipeline`'s `wedgeAfterMs` is a parameter. Nothing configures it,
     * and the wall clock is what ships.
     */
    private val now: () -> Long = System::currentTimeMillis,
) : AbstractCoroutineContextElement(StoreCalls) {
    /**
     * One call in flight, and the clocks that say what it is doing.
     *
     * No `stage` member and no equivalent of a leg's `quietForSec`: a store call
     * delivers nothing until it delivers everything, so there is no partial
     * progress to report and manufacturing one would be the mistake
     * [Processors.Holding] refuses when it declines to fill in [InFlight]'s
     * transfer clocks.
     */
    class Call(
        /** WHO asked — see the `CALLER_*` constants, which are this router's own subsystem names. */
        val caller: String,
        /** …and WHAT it asked for, named for the store method rather than a category — see the `OP_*` constants. */
        val op: String,
        /**
         * WHAT IT ASKED FOR — a summary of the filter, never the filter: kinds,
         * how many authors, how many ids, the window.
         *
         * Summarised rather than echoed because the two shapes that matter most
         * here are exactly the two that cannot be published whole — an
         * `existingIds` probe carries two thousand ids and a negentropy window
         * carries the corpus. A hundred rows of those is a document nobody can
         * open, and the id list answers nothing the count does not: what an
         * operator needs from a wedged `ingest.dedup` is "2,048 ids", not which.
         *
         * `asked` and not `filter`, which the coverage report already publishes
         * as a filter OBJECT, echoed verbatim. One word over two shapes is how
         * a reader looks up the wrong one — the overload the `inBatch` entry in
         * `StatusVocabulary` exists to complain about.
         *
         * Null for a call that carries no filter at all.
         */
        val asked: String?,
        /** When it was issued, in epoch seconds — so a line in the log can be lined up against a row here. */
        val issuedAt: Long,
        /** …and how long it has been running. THE NUMBER on this row. */
        val elapsedSec: Long,
        /**
         * How many store calls this process already had outstanding when this
         * one was issued.
         *
         * The client-side half of "is the store slow, or is my request waiting
         * in line" — see the class header for why the other half is not ours to
         * publish. Read it as a measurement of OUR OWN contribution: this
         * router is the only thing writing to that engine, so a slow call
         * issued with two others outstanding did not queue behind us.
         */
        val outstandingAtIssue: Int,
    )

    /**
     * One subsystem's traffic through the store — the counters that answer
     * "whose requests are these".
     *
     * `issued = returned + failed + cancelled + outstanding`, and every member
     * is published including the zeroes, so a reader can check that identity
     * rather than take the row on trust. It is the same rule the candidate
     * partition follows in [Processors.Work]: a member that appears only on
     * damage cannot be told from a router too old to say.
     */
    class Caller(
        val caller: String,
        val issued: Long,
        /** Came back with an answer. */
        val returned: Long,
        /** …threw. A store the schema has drifted under fails here rather than hanging, and the two want opposite next moves. */
        val failed: Long,
        /**
         * …or was cancelled, which is shutdown and not a fault.
         *
         * Counted apart for the reason `IngestPipeline.dropDuplicates` rethrows
         * cancellation rather than swallowing it: a cancelled call folded into
         * `failed` would report a clean stop as a store that is refusing work.
         */
        val cancelled: Long,
        /** …and how many are still out, with the age of the oldest, or null when none are. */
        val outstanding: Int,
        val oldestOutstandingSec: Long?,
    )

    /**
     * One band of the outstanding-age histogram: calls that have been running
     * at least [fromSec] and less than the next band's.
     *
     * The bands PARTITION the outstanding set and sum back to
     * [Snapshot.outstanding], which is what makes them readable as a shape
     * rather than as six numbers — a thousand calls all under a second is a
     * busy router, and eight hundred under a second with two over ten minutes
     * is the finding. The last band is open-ended.
     */
    class Age(
        val fromSec: Long,
        val calls: Int,
    )

    /** Everything above, as of one instant. */
    class Snapshot(
        val outstanding: Int,
        val issued: Long,
        val returned: Long,
        val failed: Long,
        val cancelled: Long,
        /** The outstanding calls, longest-running FIRST. */
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
        /** When [warnSlow] last named it, so a long call is re-named on a clock rather than every pass. */
        @Volatile
        var warnedAtMs: Long = 0
    }

    /** One caller's lifetime tally. */
    private class Tally {
        val issued = AtomicLong()
        val returned = AtomicLong()
        val failed = AtomicLong()
        val cancelled = AtomicLong()
        val outstanding = AtomicInteger()
    }

    private val open = ConcurrentHashMap<Long, Open>()
    private val tallies = ConcurrentHashMap<String, Tally>()
    private val outstanding = AtomicInteger()
    private val nextTicket = AtomicLong()

    /**
     * Run [block] as one store call by [caller], booked from the moment it is
     * issued to the moment it returns HOWEVER it returns.
     *
     * The removal is in a `finally` for the reason every `released` in this
     * package is: a call that threw, was cancelled, or ran out a deadline has
     * stopped being outstanding either way, and a row that outlives its call is
     * a fault report about work that is not happening.
     */
    suspend fun <T> track(
        caller: String,
        op: String,
        filter: String? = null,
        block: suspend () -> T,
    ): T {
        val tally = tallyFor(caller)
        val ticket = nextTicket.incrementAndGet()
        // Read BEFORE this call is counted into it, so the number is what this
        // call found rather than what it made.
        val found = outstanding.get()
        outstanding.incrementAndGet()
        tally.outstanding.incrementAndGet()
        tally.issued.incrementAndGet()
        open[ticket] = Open(tally.name(caller), op, filter, now(), found)
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
            open.remove(ticket)
            outstanding.decrementAndGet()
            tally.outstanding.decrementAndGet()
        }
    }

    /**
     * The tally [caller] reports into, keeping at most [MAX_CALLERS] of them.
     *
     * Bounded on `IngestPipeline.noteRejection`'s reasoning and against the same
     * failure: every caller name in this repository is a constant, so the map
     * cannot grow — until one call site interpolates a url or a stream name into
     * one, at which point a router doing that per event retains a string per
     * event during the one incident where heap is already the thing to protect.
     * Past the ceiling everything folds into one named bucket, so the line says
     * a tally was folded rather than implying the callers vanished.
     */
    private fun tallyFor(caller: String): Tally {
        tallies[caller]?.let { return it }
        // Racy by a caller or two at the boundary: the point is a bound, not an
        // exact size.
        if (tallies.size >= MAX_CALLERS) return tallies.computeIfAbsent(OVERFLOW_CALLER) { Tally() }
        return tallies.computeIfAbsent(caller) { Tally() }
    }

    /** Which name a tally is actually filed under, so a row and its counters cannot disagree. */
    private fun Tally.name(asked: String): String = if (tallies[asked] === this) asked else OVERFLOW_CALLER

    /** Everything outstanding and every caller's tally, as of [nowMs]. */
    fun snapshot(nowMs: Long = now()): Snapshot {
        // Snapshotted before it is sorted: the map moves under a busy router on
        // every call, and a comparator reading a value that changes mid-sort is
        // the one way this could throw into a report.
        val rows = open.values.toList()
        val named =
            rows
                // Longest-running FIRST, which is the opposite of a stream's
                // `inFlight` and the same order [Processors.Holding] uses, for
                // the same reason: a held relay is how the mirror works, and a
                // store call that has not come back is by construction the
                // anomaly. Then by caller and op, so two calls issued in one
                // millisecond do not swap places between two rollups of one
                // state.
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
        val oldestPerCaller = HashMap<String, Long>()
        for (row in rows) oldestPerCaller.merge(row.caller, row.issuedMs, ::minOf)
        val callers =
            tallies
                .map { (name, t) ->
                    Caller(
                        caller = name,
                        issued = t.issued.get(),
                        returned = t.returned.get(),
                        failed = t.failed.get(),
                        cancelled = t.cancelled.get(),
                        outstanding = t.outstanding.get().coerceAtLeast(0),
                        oldestOutstandingSec = oldestPerCaller[name]?.let { ((nowMs - it) / 1000).coerceAtLeast(0) },
                    )
                }
                // Whoever is holding the most, first — the row an operator
                // asking "whose requests are these" is looking for. Ties fall
                // back to lifetime traffic and then to the name, so one state
                // rolls up one way twice.
                .sortedWith(compareByDescending<Caller> { it.outstanding }.thenByDescending { it.issued }.thenBy { it.caller })
        return Snapshot(
            outstanding = rows.size,
            issued = tallies.values.sumOf { it.issued.get() },
            returned = tallies.values.sumOf { it.returned.get() },
            failed = tallies.values.sumOf { it.failed.get() },
            cancelled = tallies.values.sumOf { it.cancelled.get() },
            calls = named,
            // Never silent, for [InFlight.omitted]'s reason: a list that does
            // not disclose its truncation reads as the whole answer.
            omitted = (rows.size - named.size).coerceAtLeast(0),
            callers = callers,
            ages = ages(rows, nowMs),
        )
    }

    /** The outstanding set banded by age — see [Age]. */
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
     * WHICH CALLS HAVE BEEN RUNNING TOO LONG, as lines for the log — one per
     * call past [slowAfterMs], repeated every [rewarnAfterMs] for as long as it
     * is still out.
     *
     * The log rather than the page because a wedge happens while nobody is
     * watching: a status document is a snapshot of the moment somebody looked,
     * and what an investigation needs is when it STARTED. Upstream deliberately
     * refuses deadlines on these reads and this router agrees with the reasoning
     * — cutting an ingest pass discards a batch of good events that nothing
     * re-offers — so the only thing left to do about a call that will not end is
     * to say so, repeatedly, with enough on the line to act on.
     *
     * Returns the lines rather than printing them: the caller owns the log
     * prefix, and a function that prints cannot be asserted.
     */
    fun warnSlow(nowMs: Long = now()): List<String> {
        if (slowAfterMs <= 0) return emptyList()
        val due =
            open.values
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
        // The tail is disclosed rather than dropped, for the reason `omitted`
        // exists: a capped list that does not say it is capped reads as the
        // whole answer, and here it would read as FEWER stuck calls than there
        // are. The unnamed ones keep their clock and are named on a later pass.
        return if (due.size > out.size) {
            lines + "router: store call SLOW — and ${due.size - out.size} more past the same bound, named on a later pass"
        } else {
            lines
        }
    }

    /**
     * THE LONGEST-RUNNING CALL, in one clause for a line that has already
     * decided something is wrong.
     *
     * For the health line's `wedged` branch, which says "the store stopped
     * answering; look there" and until now could not say what it stopped
     * answering. [warnSlow] carries the same fact but on its own re-warn clock,
     * so four wedge lines in five would have had no companion — and the wedge
     * line is the one an operator greps for.
     *
     * Null when nothing is outstanding, which is a real state for a wedge: a
     * worker held inside a batch pass that is NOT in a store call is stuck
     * somewhere else entirely, and that is a finding rather than a gap in this
     * report.
     */
    fun describeOldest(nowMs: Long = now()): String? {
        val oldest = open.values.minByOrNull { it.issuedMs } ?: return null
        return "${oldest.caller} ${oldest.op}" +
            (oldest.asked?.let { " ($it)" } ?: "") +
            ", ${fmtDuration(nowMs - oldest.issuedMs)} in"
    }

    companion object Key : CoroutineContext.Key<StoreCalls> {
        /**
         * `SYNC_STORE_SLOW_SEC` and `SYNC_STORE_REWARN_SEC`, refused rather than
         * silently defaulted when they are not numbers — the rule every other
         * knob in this process follows, because a mistyped value that quietly
         * reverts is a setting an operator believes is in effect.
         *
         * Zero for the first one turns the warning off and keeps the report: the
         * page costs nothing per call and the log is the only part anyone can
         * choose to be tired of.
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
         * A summary of what a filter ASKS FOR, never the filter itself — see
         * [Call.asked].
         *
         * Reads in the order an operator scans it: kinds first (which subsystem
         * this is), then the sizes that decide how expensive it is, then the
         * window. Members the filter does not carry are absent rather than
         * printed as zero, so the line is as short as the ask is narrow.
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
                // The tag KEYS and their widths. The values are urls, ids and
                // d-tags by the hundred — the same reason the ids are counted
                // rather than listed.
                filter.tags?.takeIf { it.isNotEmpty() }?.let { tags ->
                    add(tags.entries.joinToString(", ") { (key, values) -> "#$key x${values.size}" })
                }
                window(filter.since, filter.until)?.let { add(it) }
                filter.limit?.let { add("limit $it") }
            }.joinToString(", ").ifEmpty { "everything" }

        /** …and the same for the list form, which is how the negentropy and delete paths ask. */
        fun summarise(filters: List<Filter>): String =
            when (filters.size) {
                0 -> "nothing"
                1 -> summarise(filters.first())
                else -> "${filters.size} filters: ${summarise(filters.first())}, …"
            }

        /** A bare id probe, which carries no filter at all — see [Call.asked]. */
        fun ids(count: Int): String = "$count id(s)"

        /** …and the version probe, whose ask is one kind against a chunk of authors. */
        fun authorsOfKind(
            kind: Int,
            authors: Int,
        ): String = "kind $kind, $authors author(s)"

        /** How many events a write is carrying. */
        fun events(count: Int): String = "$count event(s)"

        /**
         * The time window, as a SPAN rather than as two epoch seconds.
         *
         * A negentropy window is the one ask here whose cost is mostly its
         * width, and two ten-digit numbers side by side is not a width anybody
         * subtracts at a glance. Both open ends are named, because "since 2019"
         * and "everything before now" are different asks and an absent member
         * cannot say which.
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
         * WHO ASKED. One constant per subsystem that reads or writes the store,
         * named for the code that makes the call rather than for a category —
         * `ingest.dedup` is greppable from the document back to
         * `IngestPipeline.dropDuplicates`, where `probe` would not be.
         *
         * The split inside ingest is the whole point of the vocabulary: a batch
         * pass makes three different calls against three different engine paths,
         * and `oldestBatchSec` reports all three as one number.
         */
        const val CALLER_INGEST_DEDUP = "ingest.dedup"

        const val CALLER_INGEST_VERSIONS = "ingest.versions"

        const val CALLER_INGEST_WRITE = "ingest.write"

        /** The negentropy pager's own reads — the count that sizes a window, and the id snapshot it compares with. */
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

        /** The url round-up — every relay-list source walked out of the store, which is minutes at the head of every sweep. */
        const val CALLER_SOURCE_RELAY_LISTS = "source.relayLists"

        /**
         * WHAT WAS ASKED, named for the store method — `existingIds`,
         * `snapshotIdsForNegentropy` and the rest, verbatim.
         *
         * Verbatim rather than bucketed into probe/search/count/put, because a
         * bucket is a word nobody can grep their way from the document back to
         * the line that made the call, and telling `count` from
         * `snapshotIdsForNegentropy` is exactly the difference between a cheap
         * sizing query and a read of gigabytes of ids.
         */
        const val OP_EXISTING_IDS = "existingIds"

        const val OP_NEWEST_VERSIONS = "newestVersions"

        const val OP_BATCH_INSERT = "batchInsert"

        const val OP_QUERY = "query"

        const val OP_COUNT = "count"

        const val OP_SNAPSHOT_IDS = "snapshotIdsForNegentropy"

        const val OP_INSERT = "insert"

        const val OP_DELETE = "delete"

        const val OP_DISTINCT_TAG_VALUES = "distinctTagValues"

        /**
         * How many outstanding calls the document names.
         *
         * A cap, where [InFlight] has none, and the difference is the bound. A
         * leg there is a worker and ONE configured width bounds the list; a call
         * here is bounded by a PRODUCT of three — ingest workers times the
         * store's query fan-out, plus one per concurrent visit, plus the
         * monitor's chunked reads — and a router with 67 concurrent visits can
         * carry hundreds. Every row also carries a filter summary, where a leg
         * carries a url.
         *
         * Two hundred covers every shape measured here with room over, the rows
         * are longest-running FIRST so a cut can only ever drop the youngest,
         * and `omitted` says how many it dropped.
         */
        const val MAX_CALL_ROWS = 200

        /**
         * Distinct callers tallied before [tallyFor] folds the rest into one.
         * Ten constants exist; sixty-four is the same safety ceiling
         * `IngestPipeline.REASON_LIMIT` is, against the same accident.
         */
        const val MAX_CALLERS = 64

        /** Where callers past [MAX_CALLERS] land — named, so a folded tally cannot read as a missing subsystem. */
        const val OVERFLOW_CALLER = "other callers"

        /**
         * Slow-call lines per pass. Ten stuck calls is already the finding; a
         * hundred would bury the health line they sit beside, and the count of
         * what was left out is printed instead.
         */
        const val MAX_WARN_LINES = 10

        /** See the constructor. */
        const val DEFAULT_SLOW_AFTER_MS = 60_000L

        const val DEFAULT_REWARN_AFTER_MS = 300_000L

        /**
         * The histogram's bands, in seconds — see [Age].
         *
         * Logarithmic rather than even, because the question is which ORDER of
         * magnitude a call is in: everything healthy is under a second, a
         * negentropy snapshot of a wide filter honestly takes tens, and the two
         * bands past five minutes are where a wedge lives. Even bands would put
         * the whole healthy corpus in one row and tell nothing apart.
         */
        val AGE_BANDS = listOf(0L, 1L, 10L, 60L, 300L, 900L)
    }
}

/**
 * Book one store call, if this coroutine is running under a [StoreCalls].
 *
 * The nothing-installed path runs [block] untouched and records nothing, which
 * is every test and every embedded caller — see the class header on why that is
 * the honest failure rather than a global everybody writes into.
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
