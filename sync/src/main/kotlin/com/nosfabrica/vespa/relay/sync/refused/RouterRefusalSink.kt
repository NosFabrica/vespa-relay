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
package com.nosfabrica.vespa.relay.sync.refused

import com.nosfabrica.vespa.relay.ingest.refused.IngestOrigin
import com.nosfabrica.vespa.relay.ingest.refused.PermanentRefusals
import com.nosfabrica.vespa.relay.ingest.refused.RefusalSink
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.sync.heal.HealKey
import com.nosfabrica.vespa.relay.sync.heal.HealQueue
import com.nosfabrica.vespa.relay.sync.heal.StaleRef
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import com.vitorpamplona.quartz.nip01Core.store.owner
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag

/**
 * One store refusal in, a repair and at most one filter row out. The enqueue happens before
 * the recording: a suppressed id is never downloaded again, so the repair must be queued
 * before the id can reach the filter.
 */
class RouterRefusalSink(
    private val refused: RefusedIds,
    private val queue: HealQueue,
    private val suppressionEnabled: Boolean,
    /** Whether any configured stream may heal; if none, the pipeline need not carry origins. */
    healingPossible: Boolean = true,
) : RefusalSink {
    override val tracksOrigins: Boolean = healingPossible

    override fun isSuppressed(event: Event): Boolean = suppressionEnabled && refused.suppressed(event.id, event.createdAt)

    override fun onRefused(
        event: Event,
        origin: IngestOrigin,
        reason: String,
    ) {
        if (!PermanentRefusals.isPermanent(reason)) return

        // A local, not a smart cast: `origin` is another module's type.
        val from = origin.url
        if (from != null && PermanentRefusals.isHealable(reason)) {
            healKeyFor(event, reason, origin)?.let { key ->
                queue.offer(from, key, StaleRef(event.id, event.createdAt))
            }
        }

        if (suppressionEnabled) refused.record(event.id, event.createdAt)
    }

    /**
     * The repair this refusal asks for, or null when the stream's switches forbid it. Content
     * and retractions are switched separately.
     */
    private fun healKeyFor(
        event: Event,
        reason: String,
        origin: IngestOrigin,
    ): HealKey? =
        when {
            reason.startsWith(RejectionReason.PREFIX_REPLACED) -> {
                if (!origin.healContent) {
                    null
                } else {
                    val d = if (event.kind.isAddressable()) event.dTag() else null
                    if (event.kind.isReplaceable() || event.kind.isAddressable()) {
                        HealKey.content(event.kind, event.pubKey, d)
                    } else {
                        null
                    }
                }
            }

            reason == RejectionReason.DELETED -> {
                if (origin.healRetractions) HealKey.deletion(event.owner(), event.id) else null
            }

            reason == RejectionReason.VANISHED -> {
                if (origin.healRetractions) HealKey.vanish(event.owner()) else null
            }

            else -> {
                null
            }
        }
}
