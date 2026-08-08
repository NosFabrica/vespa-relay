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
package com.nosfabrica.vespa.relay.router.discovery

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAll
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException

/**
 * One relay's newest window, as the set of ids in it — the measurement
 * [RelayAliases] folds on.
 *
 * A bare `{"limit": n}` REQ on purpose: no kinds, no authors, no `since`. The
 * question is "what is at the end of THIS url", and any narrowing invites the
 * two dials to disagree for a reason that has nothing to do with whether they
 * are the same server. It ends at EOSE, so a relay that holds fewer than [limit]
 * events answers in one round trip.
 *
 * Everything it downloads is handed to [onEvent] before the ids are counted.
 * The probe is a sync that also identifies: on a url that turns out to be
 * distinct, the window was worth having anyway, and on one that turns out to be
 * a duplicate the store drops it as already-held. Nothing is fetched twice to
 * pay for the verdict.
 */
class AliasProbe(
    private val client: NostrClient,
    private val limit: Int,
    private val timeoutMs: Long,
) {
    /**
     * The ids at [url], or null when it could not be asked. Null and empty are
     * different answers and must stay that way: empty is a relay that holds
     * nothing (and is therefore no evidence of anything), null is a relay that
     * never spoke, and neither may be folded.
     */
    suspend fun fingerprint(
        url: NormalizedRelayUrl,
        onEvent: suspend (Event) -> Unit,
    ): Set<String>? =
        try {
            val events = client.fetchAll(url, Filter(limit = limit), timeoutMs)
            for (event in events) onEvent(event)
            events.mapTo(HashSet()) { it.id }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // A dead url in a relay list is the ordinary case here, not an
            // incident — and unlike a sync failure it costs nothing to skip:
            // the url simply keeps whatever verdict it already had, which for
            // an unprobed one is "dial it normally".
            null
        }
}
