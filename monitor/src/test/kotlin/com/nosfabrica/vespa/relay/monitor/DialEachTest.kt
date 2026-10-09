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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** A pass's progress closes when its dials do, whichever way each one ended. */
class DialEachTest {
    @Test
    fun `every url is attempted once and released, measured or cut, before the call returns`() =
        runBlocking {
            val processors = Processors()
            val progress = processors.of("pass")
            val urls = (0 until 9).map { RelayUrlNormalizer.normalize("wss://r$it.example") }
            val stalls = urls.filterIndexed { i, _ -> i % 3 == 0 }.toSet()
            progress.measuring(urls.size, Processors.UNIT_URL)
            val cut = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
            val measured = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

            dialEach(urls, DialGate.over(3, null), { 50L }, progress, cut = { cut += it }) { url ->
                progress.holding(url.url, "dialling")
                if (url in stalls) awaitCancellation()
                measured += url
            }

            val pass = processors.snapshot().single()
            assertEquals(urls.size, pass.measuring!!.attempted, "read the moment the call returns, with no late ticks to wait for")
            assertEquals(stalls, cut, "the deadline's urls, and only those")
            assertEquals(urls.toSet() - stalls, measured)
            assertNull(pass.inFlight, "nothing is left held, the cut urls included")
        }
}
