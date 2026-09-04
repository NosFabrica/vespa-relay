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
 * the halves, down to the single event the writer cannot take, which goes to
 * [onPoison]. Re-writing a good half is safe: an already-applied event comes
 * back as a duplicate.
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
        // Outcomes are positionally aligned with the batch; that alignment is
        // what attributes a rejection to the event that earned it.
        onOutcomes(events, write(events))
    } catch (e: CancellationException) {
        throw e
    } catch (e: Throwable) {
        if (events.size == 1) {
            onPoison(events.single(), e)
            return
        }
        // Splitting assumes one event is at fault. When the store itself is
        // refusing, every half fails, and the budget bounds the extra writes.
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

/** Writes one batch may spend isolating its bad events before giving up. */
private const val ISOLATION_WRITE_BUDGET = 64
