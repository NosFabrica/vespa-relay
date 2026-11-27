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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.EventCmd
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PassThroughPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * A bound on each connection's publishes in flight, submitted and not yet answered. At it, that connection's reader
 * waits in its own submit, so its socket alone stops being read and no one connection fills the ingest queue every
 * connection shares. [policy] marks a connection's publishes and [over] holds them to its slots.
 */
class PublishSlots(
    /** Publishes one connection may have in flight; 0 bounds nothing. */
    val perConnection: Int = DEFAULT_PER_CONNECTION,
    /** How old an unclaimed mark is before a sweep drops it. */
    private val staleNanos: Long = STALE_NANOS,
    /** Marks between sweeps. */
    private val sweepEvery: Long = SWEEP_EVERY,
) {
    /**
     * Each accepted publish's connection slots, by id, from the policy stage to the submit that
     * claims it: quartz hands the backend's submit no connection, and the session submits right
     * after its policy accepts, on the same reader. A publish that never reaches the
     * clients' lane (its connection dropped in between, or the mirror lane took its id) leaves its
     * entry behind, and quartz's policies hear no disconnect, so [Marker] sweeps entries older than
     * [STALE_NANOS] every [SWEEP_EVERY] marks: a bound on what such strays can hold.
     */
    private val marked = ConcurrentHashMap<String, Mark>()

    /** Marks waiting to be claimed (or swept). */
    val pending: Int get() = marked.size

    private class Mark(
        val slots: Semaphore,
        val atNanos: Long,
    )

    private val marks = AtomicLong()

    /** Entries the sweep dropped: publishes marked that never reached the clients' lane. */
    val strays: Long get() = stray.get()
    private val stray = AtomicLong()

    private val waiting = AtomicInteger()
    private val waits = AtomicLong()

    /** Publishes waiting for a slot right now, each one connection's reader. */
    val held: Int get() = waiting.get()

    /** Publishes that had to wait for a slot, since boot. */
    val waited: Long get() = waits.get()

    /**
     * A fresh per-connection policy that refuses nothing, null when nothing is bounded. It must follow every policy
     * that can refuse, so a mark means a submit. [skip] names the publishes another lane takes, left unmarked.
     */
    fun policy(skip: ((Event) -> Boolean)? = null): IRelayPolicy? = if (perConnection > 0) Marker(skip) else null

    /** [clients] with each marked publish held to its connection's slots. */
    fun over(clients: EventIngest): EventIngest = if (perConnection > 0) Held(clients) else clients

    private inner class Marker(
        private val skip: ((Event) -> Boolean)?,
    ) : PassThroughPolicy() {
        private val slots = Semaphore(perConnection)

        override fun accept(cmd: EventCmd): PolicyResult<EventCmd> {
            if (skip?.invoke(cmd.event) != true) {
                val now = System.nanoTime()
                marked[cmd.event.id] = Mark(slots, now)
                if (marks.incrementAndGet() % sweepEvery == 0L) {
                    marked.entries.removeIf { (_, mark) -> (now - mark.atNanos > staleNanos).also { if (it) stray.incrementAndGet() } }
                }
            }
            return PolicyResult.Accepted(cmd)
        }
    }

    private inner class Held(
        private val clients: EventIngest,
    ) : EventIngest {
        override suspend fun submit(
            event: Event,
            skipVerify: Boolean,
            onComplete: (IEventStore.InsertOutcome) -> Unit,
        ) {
            val slots = marked.remove(event.id)?.slots ?: return clients.submit(event, skipVerify, onComplete)
            if (!slots.tryAcquire()) {
                waits.incrementAndGet()
                waiting.incrementAndGet()
                try {
                    slots.acquire()
                } finally {
                    waiting.decrementAndGet()
                }
            }
            val freed = AtomicBoolean()
            try {
                clients.submit(event, skipVerify) { outcome ->
                    if (freed.compareAndSet(false, true)) slots.release()
                    onComplete(outcome)
                }
            } catch (e: Throwable) {
                // A cancelled submit may still be answered later; whichever comes first frees the slot.
                if (freed.compareAndSet(false, true)) slots.release()
                throw e
            }
        }

        override fun hasBacklog(): Boolean = clients.hasBacklog()

        override fun close() = clients.close()
    }

    companion object {
        /** Twice the 64 a bulk client keeps in flight per socket, and an eighth of the default ingest queue. */
        const val DEFAULT_PER_CONNECTION = 128

        /** Marks between sweeps of [marked]. */
        const val SWEEP_EVERY = 1024L

        /** A mark this old was never claimed: the claim follows the mark on the same call path. */
        val STALE_NANOS = TimeUnit.SECONDS.toNanos(60)
    }
}
