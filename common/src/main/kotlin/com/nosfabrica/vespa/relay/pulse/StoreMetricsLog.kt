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
 * The pulse's operational numbers on the log, for a deployment that cannot open the gated page.
 * Counters and gauges only, cumulative since the process started: never the search terms,
 * observer keys or slow reads the gate exists for, because a log line is not gated.
 */
object StoreMetricsLog {
    /** How many stages to name, longest by total time first. */
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
        // The detail names the work; the stage label only names the lock.
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
     * Log [line] for [store] every [everySeconds]; zero logs nothing. A daemon thread, because
     * only one of the two mains has an ambient scope and this must never keep a process alive.
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
