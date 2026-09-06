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

/**
 * THE PULSE'S NUMBERS, ON THE LOG — for the deployment that cannot open the
 * pulse.
 *
 * `/pulse.html` is behind an admin gate because the document quotes what
 * people searched for. That gate is right, and it also means the ordinary
 * operational questions — which activity is spending the engine's time, what
 * is holding the write gate, is a background walk moving at all — are
 * unanswerable from a terminal. Diagnosing a store that was matching ~20M
 * documents a second while retiring almost no work took hours of poking at
 * Vespa's own metrics endpoint precisely because of this.
 *
 * NON-SENSITIVE BY CONSTRUCTION. Activity and port counters, stage timings
 * and lock holders only. Never [CostLedger.Snapshot.topTerms],
 * [CostLedger.Snapshot.topObservers] or [CostLedger.Snapshot.slowReads] —
 * those are the members the gate exists for, and a log line is not gated.
 * Adding one here would quietly publish search terms to anything that reads
 * container logs.
 *
 * COUNTERS, NOT RATES: every number is cumulative since the process started,
 * so two lines subtract to give an interval. A rate computed here would be an
 * average over the whole run and would flatten exactly the change an operator
 * is looking for.
 */
object StoreMetricsLog {
    /** How many stages to name — the longest by total time, which is the question being asked. */
    private const val TOP_STAGES = 8

    fun line(
        role: String,
        metrics: CostLedger.Snapshot,
        stages: Map<String, IngestStats.Stage> = IngestStats.snapshot(),
        held: List<IngestStats.Held> = IngestStats.heldAll(),
    ): String {
        val activities =
            metrics.ports
                .groupBy { it.activity }
                .map { (activity, ports) -> Triple(activity.name, ports.sumOf { it.calls }, ports.sumOf { it.docs } to ports.sumOf { it.nanos }) }
                .sortedByDescending { it.third.second }
                .joinToString(" ") { (name, calls, docsNanos) ->
                    "$name(calls=$calls docs=${docsNanos.first} ms=${docsNanos.second / 1_000_000})"
                }
        val slowest =
            stages.entries
                .sortedByDescending { it.value.totalNanos }
                .take(TOP_STAGES)
                .joinToString(" ") { (name, s) -> "$name(ms=${s.totalNanos / 1_000_000} calls=${s.calls} mean=${s.meanNanos / 1_000_000}ms)" }
        // The DETAIL is the useful half — "derive 500 subject(s) in 10
        // chunk(s)" names the work, where the stage label only names the lock.
        val now = System.nanoTime()
        val holding =
            held
                .joinToString(", ") { h ->
                    "${h.stage} ${(now - h.sinceNanos) / 1_000_000}ms" + (h.detail?.let { " \"$it\"" } ?: "")
                }.ifBlank { "-" }
        val gauges =
            metrics.gauges.entries
                .sortedBy { it.key }
                .joinToString(" ") { "${it.key}=${it.value}" }
        return "store-metrics[$role] activities: $activities | stages: $slowest | held: $holding | gauges: $gauges"
    }

    /**
     * Log [line] for [store] every [everySeconds], or nothing at all when that
     * is 0. A DAEMON thread rather than a coroutine: both mains want this and
     * only one of them has an ambient scope, and a metrics log must never be
     * the reason a process refuses to exit.
     */
    fun startLogging(
        role: String,
        store: VespaEventStore,
        everySeconds: Int,
    ): Thread? {
        if (everySeconds <= 0) return null
        val t =
            Thread({
                while (true) {
                    try {
                        Thread.sleep(everySeconds * 1000L)
                        println(line(role, store.metrics()))
                        store.backgroundStatus().takeIf { it.isNotBlank() }?.let { println("store-background[$role] $it") }
                    } catch (e: InterruptedException) {
                        Thread.currentThread().interrupt()
                        return@Thread
                    } catch (t: Throwable) {
                        // A metrics line is never worth taking the process down.
                        System.err.println("store-metrics[$role]: could not read metrics (${t.message?.take(200)})")
                    }
                }
            }, "store-metrics-$role")
        t.isDaemon = true
        t.start()
        return t
    }
}
