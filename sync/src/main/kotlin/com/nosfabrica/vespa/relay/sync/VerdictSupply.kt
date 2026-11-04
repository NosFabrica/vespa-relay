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
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent

/**
 * The streams whose relays come from kind-30166 verdicts, which only a running monitor writes.
 * The mirror knows nothing of the monitor, so a stream whose verdicts never arrive says so here
 * rather than syncing nothing in silence.
 */
internal class VerdictSupply(
    private val store: IEventStore,
    streams: List<SyncStream>,
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

    /** One line per stream none of whose verdict filters matches a stored record; empty when each has some. */
    suspend fun starved(): List<String> =
        asked.mapNotNull { (stream, filters) ->
            val held = filters.any { store.query<Event>(it).isNotEmpty() }
            if (held) {
                null
            } else {
                "router: health stream `$stream` takes its relays from kind-30166 verdicts and the store holds none " +
                    "its relaySource or gatedBy matches, so it syncs nothing. Nothing in this process writes them: " +
                    "is a monitor signing them (the `monitor` service, MONITOR_CONFIG_LOCAL), or does verdictsFrom name its key?"
            }
        }
}
