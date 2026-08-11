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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.relay.router.progress.CycleTally

/**
 * The fan-out set one cycle derived, held so the next cycle can start on it.
 *
 * ## What is being avoided, and what is not
 *
 * Deriving this list is the only expensive thing between "the stream wakes up"
 * and "the first byte is downloading". It is a store walk over every relay-list
 * event the sources name (or the tag projection standing in for one), a
 * normalisation pass over every url they carry, an alias `apply` — one `#d`
 * query per 500 urls — and the exclude filter. On a full store that is minutes,
 * and it is paid to produce a list that differs from the previous cycle's by a
 * handful of urls.
 *
 * Nothing about DIALLING is cached. The NIP-66 known-dead set is re-read at the
 * top of every cycle and host strikes are cycle-local, so a relay that died an
 * hour ago is skipped on a cached list exactly as it would be on a fresh one.
 * This is the list of urls to consider, not a decision about any of them.
 *
 * ## Why it expires two ways
 *
 * By AGE, because the store keeps filling and a dynamic stream's fan-out is
 * supposed to widen as it does — that is the reason discovery sits inside the
 * loop at all. The bound is the stream's own `refreshSeconds`, which is what
 * that number always meant: how often the sources are re-read.
 *
 * By the FOLD's version, because the other half of the alias mechanism runs on
 * its own clock. [AliasMonitor] publishes verdicts hours after the cycle that
 * submitted the candidates, and a list built before those verdicts existed goes
 * on dialling urls now known to be one relay — a socket, a band and a slot in
 * the concurrency gate each, for events the survivor in the same list is
 * already delivering. Rebuilding is how those verdicts are read, so a changed
 * [AliasMonitor.generation] has to force it.
 *
 * An EMPTY list is never reusable. It means discovery found nothing — a cold
 * store, or sources that match nothing yet — and the stream is in its retry
 * backoff, where the whole question is whether the store has filled since.
 */
internal class CachedRelayList(
    /** What the cycle fans out over: canonical urls, with their bound narrows. */
    val relays: List<DiscoveredRelay>,
    /** Urls discovery handed over, before the fold or the exclude list. */
    private val discovered: Int,
    private val foldedOntoAnother: Int,
    private val excluded: Int,
    private val hosts: Int,
    private val folded: Map<String, String>,
    private val builtAtMs: Long,
    /** [AliasMonitor.generation] as it was when this list was derived. */
    private val aliasGeneration: Long,
) {
    /** How long ago this list was derived, in seconds. */
    fun ageSec(nowMs: Long): Long = ((nowMs - builtAtMs) / 1000).coerceAtLeast(0)

    /**
     * May the next cycle run on this list rather than deriving its own?
     *
     * [maxAgeSec] is the stream's `refreshSeconds` and [aliasGeneration] the
     * fold's version right now — see the class header for why both.
     */
    fun reusableAt(
        nowMs: Long,
        maxAgeSec: Long,
        aliasGeneration: Long,
    ): Boolean = relays.isNotEmpty() && aliasGeneration == this.aliasGeneration && ageSec(nowMs) < maxAgeSec

    /**
     * A FRESH tally for a cycle about to run on this list.
     *
     * Never the previous cycle's: a [CycleTally]'s outcome counters are
     * mutated by the fan-out as urls settle, so handing the same object to two
     * cycles would publish the second one's dispositions added to the first's,
     * against a `taken` that counts each url once — a partition that cannot
     * close, reported as `balanced: false`.
     *
     * The provenance members ARE the previous cycle's, because they are facts
     * about this list rather than about the cycle: these urls really were
     * discovered, really were folded, really were excluded. [CycleTally.listAgeSec]
     * is what says when.
     */
    fun tally(nowMs: Long): CycleTally =
        CycleTally(
            discovered = discovered,
            foldedOntoAnother = foldedOntoAnother,
            excluded = excluded,
            hosts = hosts,
            folded = folded,
            listAgeSec = ageSec(nowMs),
        )
}
