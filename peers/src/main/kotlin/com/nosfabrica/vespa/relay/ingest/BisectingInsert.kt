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
package com.nosfabrica.vespa.relay.ingest

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException

/**
 * Write [events] through [write]; if that throws, split the batch and write
 * the halves, down to the single event the writer cannot take.
 *
 * A bulk write fails as a unit, so one bad event would otherwise cost the
 * whole batch — 999 good events lost per bad one at the default size, with no
 * retry. Bisecting costs ~2·log2(n) extra writes on a failing batch, nothing
 * on a healthy one, and ends holding the offender by itself for [onPoison] to
 * report. Re-writing the good halves is safe: re-inserting an already-applied
 * event is a duplicate the store rejects.
 *
 * Free-standing and injectable so the isolation can be tested without a store.
 */
internal suspend fun insertBisecting(
    events: List<Event>,
    write: suspend (List<Event>) -> List<IEventStore.InsertOutcome>,
    onOutcomes: (List<Event>, List<IEventStore.InsertOutcome>) -> Unit,
    onPoison: (Event, Throwable) -> Unit,
    onGaveUp: (List<Event>, Throwable) -> Unit = { _, _ -> },
) = bisect(events, write, onOutcomes, onPoison, onGaveUp, intArrayOf(ISOLATION_WRITE_BUDGET))

private suspend fun bisect(
    events: List<Event>,
    write: suspend (List<Event>) -> List<IEventStore.InsertOutcome>,
    onOutcomes: (List<Event>, List<IEventStore.InsertOutcome>) -> Unit,
    onPoison: (Event, Throwable) -> Unit,
    onGaveUp: (List<Event>, Throwable) -> Unit,
    budget: IntArray,
) {
    if (events.isEmpty()) return
    try {
        // Outcomes come back positionally aligned with the batch, which is what
        // lets a caller attribute a rejection to the event that earned it —
        // the whole basis for deciding whether an id is worth remembering.
        onOutcomes(events, write(events))
    } catch (e: CancellationException) {
        throw e
    } catch (e: Throwable) {
        if (events.size == 1) {
            onPoison(events.single(), e)
            return
        }
        // Splitting assumes ONE event is at fault. When the store itself is
        // refusing (a full disk, a dead engine) every half fails all the way
        // down and isolation turns one failed write into ~2n — precisely the
        // wrong moment to multiply the load. So spend a fixed budget and give
        // up on the remainder when it runs out.
        if (budget[0] <= 0) {
            onGaveUp(events, e)
            return
        }
        budget[0] -= 2
        val mid = events.size / 2
        bisect(events.subList(0, mid), write, onOutcomes, onPoison, onGaveUp, budget)
        bisect(events.subList(mid, events.size), write, onOutcomes, onPoison, onGaveUp, budget)
    }
}

/**
 * Writes one batch may spend isolating its bad events before giving up.
 * Isolating k bad events out of n costs about `2·k·log2(n)` writes, so 64
 * covers three in a 1000-event batch — past the rate seen in practice. What
 * it really bounds is the store-wide case, where every write fails.
 */
private const val ISOLATION_WRITE_BUDGET = 64
