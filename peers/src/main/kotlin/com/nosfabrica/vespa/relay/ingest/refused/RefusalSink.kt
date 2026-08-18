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
package com.nosfabrica.vespa.relay.ingest.refused

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason

/**
 * Which store refusals mean "this event is unstorable here, permanently, as a
 * property of the event rather than of our health".
 *
 * The exclusions are the load-bearing part:
 *
 *  - **`InsertOutcome.Failed` is not here and must never be.** The event was
 *    good and the failure was the store's; recording it would turn a transient
 *    fault into permanent silent loss, which is the exact failure mode
 *    `IngestPipeline.lostToStore` exists to make loud.
 *  - **A bad signature is not here.** An id is the hash of the *content*, not
 *    of the signature, so the same id can arrive correctly signed from another
 *    relay. One relay's corruption must not become permanent.
 *  - **`DUPLICATE` is not here.** It is already in our id set, so a reconcile
 *    never asks for it; a row would be pure growth for no saving.
 *  - **Operator sweeps are not here.** They never reach this path at all —
 *    `SWEEP_ORPHAN_SCORES_ON_START` and by-kind reclaim are *designed* to be
 *    re-downloaded.
 */
object PermanentRefusals {
    fun isPermanent(reason: String): Boolean =
        reason.startsWith(RejectionReason.PREFIX_REPLACED) ||
            reason == RejectionReason.DELETED ||
            reason == RejectionReason.VANISHED ||
            reason == RejectionReason.EXPIRED

    /** The subset a repair could plausibly fix by handing the relay something newer. */
    fun isHealable(reason: String): Boolean =
        reason.startsWith(RejectionReason.PREFIX_REPLACED) ||
            reason == RejectionReason.DELETED ||
            reason == RejectionReason.VANISHED
}

/**
 * Where a submitted event came from, and what this stream is allowed to do
 * about it. Carried through the ingest queue because the healer's switches are
 * per-stream while the pipeline is shared — the caller knows its stream, the
 * pipeline never has to.
 */
data class IngestOrigin(
    val url: NormalizedRelayUrl? = null,
    val healContent: Boolean = false,
    val healRetractions: Boolean = false,
) {
    companion object {
        /** A local or unattributed write: nothing to heal, nobody to heal it at. */
        val Local = IngestOrigin()
    }
}

/**
 * Where the ingest pipeline reports a store refusal, and where it asks whether
 * an event is already suppressed.
 *
 * Kept as an interface so [com.nosfabrica.vespa.relay.ingest.IngestPipeline]
 * has no idea that filters, epochs or upstream repairs exist — it reports what
 * the store decided and asks one question, which is all a test needs to drive.
 */
interface RefusalSink {
    /**
     * Whether [onRefused] will ever read the [IngestOrigin] it is handed.
     *
     * Only the heal path uses it — suppression keys on the event's own id and
     * `created_at`. The pipeline builds a per-batch id→origin map to carry it,
     * so a sink that answers `false` here saves every ingest batch a HashMap
     * and one hashed insert per event. That map used to be unconditional,
     * which billed every deployment for a lookup no stream had asked for.
     */
    val tracksOrigins: Boolean

    /** True when this event has been twice refused and should not be stored again. */
    fun isSuppressed(event: Event): Boolean

    /**
     * The store refused [event], which arrived via [origin]. Called off the
     * download path, on an ingest worker, after the batch write returns.
     */
    fun onRefused(
        event: Event,
        origin: IngestOrigin,
        reason: String,
    )

    companion object {
        /** Suppression off: answers no to everything and records nothing. */
        val None: RefusalSink =
            object : RefusalSink {
                override val tracksOrigins = false

                override fun isSuppressed(event: Event) = false

                override fun onRefused(
                    event: Event,
                    origin: IngestOrigin,
                    reason: String,
                ) = Unit
            }
    }
}
