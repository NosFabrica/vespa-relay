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
 * Which store refusals mean the event is unstorable here as a property of the event, not of
 * our health. A bad signature is excluded because the same id can arrive correctly signed
 * from another relay; a duplicate because a reconcile never asks for an id we hold.
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

/** Where a submitted event came from and what its stream may heal; the pipeline is shared. */
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

/** Where the ingest pipeline reports a store refusal and asks whether an event is already suppressed. */
interface RefusalSink {
    /** Whether [onRefused] reads the [IngestOrigin] it is handed; false saves every batch the id map. */
    val tracksOrigins: Boolean

    /** True when this event has been twice refused and should not be stored again. */
    fun isSuppressed(event: Event): Boolean

    /** The store refused [event], which arrived via [origin]. Called on an ingest worker. */
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
