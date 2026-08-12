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
package com.nosfabrica.vespa.relay.router

import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * The one local id snapshot a negentropy stream reconciles every relay against,
 * held so that overlapping passes can never leave two of them on the heap.
 *
 * ## The number this is protecting
 *
 * A stream's id set is every id we hold for its filter — measured at 24.8M ids
 * and gigabytes resident for one stream, which is why it is built once per pass
 * and shared across the fan-out rather than per relay (per-relay would multiply
 * the store work by 16,000), and why `SyncEngine`'s stream gate lets only one be
 * BUILT at a time. Two of them resident at once is what previously pushed the
 * heap to its ceiling.
 *
 * ## Why a rotation needs generations at all
 *
 * While the fan-out ended in a join, the set's lifetime was the pass's: the last
 * relay returned, the set went out of scope, the next pass built a new one.
 * Nothing overlapped. A rotation deliberately overlaps — a pass ends when the
 * last url is handed out, and its stragglers run on into the next one — so a
 * straggler is still reconciling against generation N when the next pass wants
 * to install N+1.
 *
 * Three ways to handle that, and only one of them is bounded:
 *
 *  - *Wait for the stragglers.* That is the join again, and the join is the
 *    thing being removed.
 *  - *Install regardless.* Unbounded: a hung leg holding generation N for hours
 *    while passes keep installing N+1, N+2 … is the heap ceiling with extra
 *    steps.
 *  - *Install only when nothing older is still held*, which is what [mayInstall]
 *    answers. At most two generations are ever alive — the current one and the
 *    single retired one still draining — and a pass that arrives while an older
 *    one is held simply reuses what it has.
 *
 * ## What reuse costs, and why it is the cheap side
 *
 * A stale snapshot makes the reconcile ask for events we have since stored, so
 * they arrive and ingest drops them as duplicates. That is bandwidth, and
 * `ProbeGate` already measures a duplicate at 17µs. It is not a correctness
 * problem in either direction: the band records what was WALKED, not what the
 * diff believed, and the store dedups on insert. Reconciling against a snapshot
 * taken at the start of a pass is already the accepted behaviour anyway —
 * ingest is asynchronous, so the set is stale by the second relay.
 */
internal class SharedIdSet {
    /** One build of the set, and how many asks are still reading it. */
    private class Generation(
        val ids: List<IdAndTime>,
    ) {
        val holders = AtomicInteger()
    }

    @Volatile
    private var current: Generation? = null

    /**
     * The generation this one replaced, kept only until its last reader is
     * done. Never more than one: [mayInstall] refuses while it is occupied.
     */
    @Volatile
    private var retired: Generation? = null

    @Volatile
    private var builtAtMs = 0L

    /** The `since` of the window the current set was built for; null is unbounded. */
    @Volatile
    private var builtForSince: Long? = null

    /** What the last build cost, which is what [worthRebuilding] paces itself against. */
    @Volatile
    private var lastBuildMs = 0L

    /**
     * One ask's read of the set. [release] from a `finally` — a lease never
     * returned pins its generation forever, which stops every future rebuild.
     */
    class Lease(
        val ids: List<IdAndTime>,
        private val onRelease: () -> Unit,
    ) {
        private val released = AtomicBoolean()

        /**
         * Idempotent, because the alternative is silent and permanent: a second
         * release decrements a holder count that has already reached zero, and
         * the generation then looks occupied by a reader that does not exist —
         * after which [mayInstall] answers false forever.
         */
        fun release() {
            if (released.compareAndSet(false, true)) onRelease()
        }
    }

    /**
     * Under the same lock as [install] and [lease], because the interleaving it
     * prevents is silent and permanent: `install` deciding the outgoing
     * generation still has readers, the last of them releasing before the
     * retirement slot is written, and the slot then holding a generation nobody
     * will ever release — after which [mayInstall] answers false forever and the
     * stream reconciles against its first snapshot for the life of the process.
     * Nothing about that failure is visible except a diff that slowly stops
     * finding anything.
     */
    @Synchronized
    private fun drop(g: Generation) {
        if (g.holders.decrementAndGet() <= 0 && g === retired) retired = null
    }

    /**
     * May a fresh set be built and installed right now?
     *
     * False exactly when a previous generation is still being read, which is the
     * whole bound: no build happens that would put a third set on the heap. A
     * caller told no keeps running on the set it has — see the class header on
     * why that is the cheap side of the trade.
     */
    fun mayInstall(): Boolean = retired == null

    /**
     * Replace the set. The generation being replaced stays alive for its
     * existing readers and for no one else; new leases get [ids].
     *
     * Only call this having been told [mayInstall] — installing over an occupied
     * retirement slot would drop this class's one guarantee, so it refuses
     * rather than silently exceeding it.
     */
    @Synchronized
    fun install(
        ids: List<IdAndTime>,
        nowMs: Long,
        forSince: Long?,
        buildMs: Long,
    ) {
        check(mayInstall()) { "a previous id set is still being read — install would leave three generations alive" }
        val outgoing = current
        current = Generation(ids)
        builtAtMs = nowMs
        builtForSince = forSince
        lastBuildMs = buildMs
        // Nothing is reading the outgoing one, so there is nothing to retire and
        // it is garbage immediately. This is the ordinary case: a pass whose
        // stragglers all finished before the next build.
        if (outgoing != null && outgoing.holders.get() > 0) retired = outgoing
    }

    /** Whether anything has been installed yet. */
    fun isEmpty(): Boolean = current == null

    /** Take a read of the current set for one ask. */
    @Synchronized
    fun lease(): Lease {
        val g = current ?: return Lease(emptyList()) {}
        g.holders.incrementAndGet()
        return Lease(g.ids) { drop(g) }
    }

    /** Sets alive right now: 0, 1, or — while a retirement drains — 2. */
    fun generationsAlive(): Int = (if (current != null) 1 else 0) + (if (retired != null) 1 else 0)

    /** Ids in the set new asks are reading. */
    fun size(): Int = current?.ids?.size ?: 0

    /** How old the set new asks are reading is, in seconds. */
    fun ageSec(nowMs: Long): Long = if (current == null) 0 else ((nowMs - builtAtMs) / 1000).coerceAtLeast(0)

    /**
     * Is a fresh build worth what it costs, right now?
     *
     * ## The cost this exists to stop paying
     *
     * Building the set is a full store walk — measured at 24.8M ids, gigabytes,
     * and minutes for one stream. While a dynamic fan-out ended in a join, one
     * build per pass meant one build per `refreshSeconds`, six hours by default,
     * and asking the question never came up.
     *
     * A rotation broke that arithmetic in the direction nothing warns about.
     * Passes are as frequent as `recycleSeconds` now, so a negentropy stream
     * with a short list and a five-second gap would rebuild every few seconds:
     * a stream that spends effectively all of its time walking the store and
     * none of it mirroring, with no error anywhere and a progress line that
     * looks busy.
     *
     * ## The rule
     *
     * Rebuild when the set is older than **ten times what the last build cost**,
     * with a one-minute floor. Self-pacing rather than a fixed interval, because
     * the number that matters is the SHARE of the stream's time spent building:
     * a one-second walk is reused for a minute, a ninety-second walk for fifteen,
     * and a five-minute walk for fifty — capping the overhead at roughly a tenth
     * either way, on a corpus whose size this code cannot know in advance.
     *
     * ## …and the one thing that overrides age
     *
     * A WIDER window. [forSince] narrows the walk to what the hungriest relay
     * still needs, so a set built for a narrow window and reused against a wider
     * one is a SUBSET of what the diff needs — the reconcile would then believe
     * we lack events we hold and re-download them, which is the opposite of what
     * the set is for. A widened window means a relay with no band appeared (a
     * new url, or a fold that expired), and that is worth a rebuild whatever the
     * clock says. Narrower is free: a superset only ever means fewer downloads.
     *
     * Staleness in the other direction is safe and deliberate. A set missing
     * events ingested since it was built makes the reconcile ask for them again;
     * they arrive, and ingest drops them as duplicates at a measured 17µs.
     */
    fun worthRebuilding(
        nowMs: Long,
        neededSince: Long?,
    ): Boolean {
        if (current == null) return true
        if (widerThanBuilt(neededSince)) return true
        return nowMs - builtAtMs >= (lastBuildMs * REBUILD_COST_MULTIPLE).coerceAtLeast(MIN_REBUILD_INTERVAL_MS)
    }

    /**
     * Does [neededSince] reach further back than the window the set was built
     * for? `null` is unbounded, so it is wider than every bounded window and
     * narrower than none.
     */
    private fun widerThanBuilt(neededSince: Long?): Boolean {
        val built = builtForSince ?: return false
        return neededSince == null || neededSince < built
    }

    companion object {
        /**
         * The share of a stream's time a rebuild may take: one build per ten
         * builds' worth of elapsed time, i.e. about a tenth.
         */
        const val REBUILD_COST_MULTIPLE = 10L

        /**
         * …and a floor under that, for the store small enough to walk in a
         * second. Ten seconds of reuse for a one-second walk would still be a
         * rebuild every other pass at `recycleSeconds = 5`, which is the shape
         * this exists to prevent rather than a small version of it.
         */
        const val MIN_REBUILD_INTERVAL_MS = 60_000L
    }
}
