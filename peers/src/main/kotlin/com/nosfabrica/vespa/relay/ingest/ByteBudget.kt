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
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import java.util.concurrent.atomic.AtomicLong

/**
 * A bound on bytes held, taken by weight. [acquire] admits while what is held is under [limit],
 * so an event larger than the whole budget still goes through, alone, and nothing waits behind a
 * large event that cannot fit: the bound is [limit] plus the one event that crossed it.
 *
 * A waiter suspends, never blocks: it is a connection's read loop, which is how a full budget
 * becomes TCP backpressure on the publishers instead of a heap that keeps growing.
 */
internal class ByteBudget(
    val limit: Long,
) {
    private val held = AtomicLong()

    /** Bumped on every release, so a waiter that read a full budget wakes when it changes. */
    private val released = MutableStateFlow(0L)

    val heldBytes: Long get() = held.get()

    /** [acquire] without waiting: true when taken, false when the budget is spent. */
    fun tryAcquire(bytes: Long): Boolean {
        while (true) {
            val now = held.get()
            if (now >= limit) return false
            if (held.compareAndSet(now, now + bytes)) return true
        }
    }

    suspend fun acquire(bytes: Long) {
        while (true) {
            val seen = released.value
            val now = held.get()
            if (now < limit) {
                if (held.compareAndSet(now, now + bytes)) return
                continue
            }
            // Read before the check above, so a release in between is not missed.
            released.first { it != seen }
        }
    }

    fun release(bytes: Long) {
        if (bytes == 0L) return
        held.addAndGet(-bytes)
        released.update { it + 1 }
    }
}

/**
 * What a parsed [event] holds in the heap, erring high: every string's header and its characters
 * (Latin-1 is one byte a character, counted as such for tags; content counted at two, since it
 * is the field likely to be wider), every tag's array. Measured against the real thing on kind 3s
 * (2026-10-17): 400 `p` tags parse to 53 KB and this says 68 KB; 3,600 tags (a 256 KB frame)
 * parse to 476 KB and this says 608 KB. One pass over the tags, no allocation.
 */
internal fun approxHeapBytes(event: Event): Long {
    var bytes = EVENT_OVERHEAD + 2L * event.content.length
    for (tag in event.tags) {
        bytes += ARRAY_OVERHEAD + 4L * tag.size
        for (value in tag) bytes += STRING_OVERHEAD + value.length
    }
    return bytes
}

/** The event object, its id, pubkey and sig strings. */
private const val EVENT_OVERHEAD = 512L
private const val ARRAY_OVERHEAD = 16L
private const val STRING_OVERHEAD = 40L
