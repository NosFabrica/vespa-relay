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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.peers.DialGate
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

/**
 * A per-url pass's dials: [measure] once per url, under [gate] and inside it the url's own
 * [deadlineMs], so the wait for a permit is never charged to the relay. [cut] hears of each url
 * the deadline stopped; nothing about it is a fact about the relay.
 */
internal suspend fun dialEach(
    urls: List<NormalizedRelayUrl>,
    gate: DialGate,
    deadlineMs: (NormalizedRelayUrl) -> Long,
    progress: Processors.Handle?,
    cut: (NormalizedRelayUrl) -> Unit,
    measure: suspend (NormalizedRelayUrl) -> Unit,
) = coroutineScope {
    for (url in urls) {
        launch {
            // Counted in the launch, not from `invokeOnCompletion`: that handler can run after this
            // scope has resumed, and a late tick would land on the next phase's count.
            try {
                gate.withPermit(url) {
                    val ran =
                        withTimeoutOrNull(deadlineMs(url)) {
                            try {
                                measure(url)
                            } finally {
                                progress?.released(url.url)
                            }
                        }
                    if (ran == null) cut(url)
                }
            } finally {
                // The url is behind the pass however it ended, cancellation included.
                progress?.attempted()
            }
        }
    }
}
