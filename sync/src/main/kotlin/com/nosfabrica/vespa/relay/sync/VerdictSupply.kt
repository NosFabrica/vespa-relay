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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.config.SyncStream
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.withTimeoutOrNull

/**
 * The streams whose relays come from kind-30166 verdicts, which only a running monitor writes.
 * The mirror knows nothing of the monitor, so a stream whose verdicts never arrive says so here
 * rather than syncing nothing in silence.
 */
internal class VerdictSupply(
    private val store: IEventStore,
    streams: List<SyncStream>,
    /** The wall clock on each check, short against the health line it runs on. */
    private val deadlineMs: Long = DEFAULT_DEADLINE_MS,
) {
    /** Stream name to the verdict filters it rosters or gates on, each asked for one match. */
    private val asked: Map<String, List<Filter>> =
        streams
            .mapNotNull { stream ->
                val discovery = stream.discovery ?: return@mapNotNull null
                val verdicts =
                    (discovery.sources + discovery.gatedBy)
                        .filter { RelayDiscoveryEvent.KIND in it.filter.kinds.orEmpty() }
                        .map { it.filter.copy(limit = 1) }
                verdicts.takeIf { it.isNotEmpty() }?.let { stream.name to it }
            }.toMap()

    /**
     * One line per stream none of whose verdict filters matches a stored record; empty when each has some.
     * A check the store does not answer in time names nothing: an unanswered store is the store-call row's to report.
     */
    suspend fun starved(): List<String> =
        asked.mapNotNull { (stream, filters) ->
            val held = filters.any { holds(it) ?: return@mapNotNull null }
            if (held) {
                null
            } else {
                "router: health stream `$stream` takes its relays from kind-30166 verdicts and the store holds none " +
                    "its relaySource or gatedBy matches, so it syncs nothing. Nothing in this process writes them: " +
                    "is a monitor signing them (the `monitor` service, MONITOR_CONFIG_LOCAL), or does verdictsFrom name its key?"
            }
        }

    /** Does the store hold a match for [filter]; null when it did not answer within [deadlineMs]. */
    private suspend fun holds(filter: Filter): Boolean? =
        withTimeoutOrNull(deadlineMs) {
            storeCall(StoreCalls.CALLER_VERDICT_SUPPLY, StoreCalls.OP_QUERY, StoreCalls.summarise(filter)) {
                store.query<Event>(filter).isNotEmpty()
            }
        }

    companion object {
        const val DEFAULT_DEADLINE_MS = 10_000L
    }
}
