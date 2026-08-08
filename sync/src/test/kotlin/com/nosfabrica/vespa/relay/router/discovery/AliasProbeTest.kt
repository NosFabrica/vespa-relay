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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The probe's retry policy. Measured against live relays: `max_limit` is 500 on
 * half the hosts that advertise one, and a relay may REFUSE an over-large ask
 * rather than truncate it — purplepag.es answers `{"limit": 1000}` with `CLOSED
 * blocked: limit too high`. A refusal reaches this layer as an empty answer, so
 * the second, smaller ask is what keeps those hosts foldable.
 */
class AliasProbeTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private val signer = NostrSignerSync()

    private fun events(n: Int): List<Event> = (0 until n).map { signer.sign(1_700_000_000L + it, 1, emptyArray(), "e$it") }

    @Test
    fun `a full first answer is not asked twice`() =
        runBlocking {
            val asks = mutableListOf<Int>()
            val probe =
                AliasProbe(fetch = { _, n ->
                    asks += n
                    events(3)
                }, limit = 500)

            assertEquals(3, probe.fingerprint(url) {}?.size)
            assertEquals(listOf(500), asks)
        }

    @Test
    fun `an empty first answer is retried smaller`() =
        runBlocking {
            val asks = mutableListOf<Int>()
            val probe =
                AliasProbe(
                    fetch = { _, n ->
                        asks += n
                        if (n == 500) emptyList() else events(7)
                    },
                    limit = 500,
                )

            assertEquals(7, probe.fingerprint(url) {}?.size)
            assertEquals(listOf(500, RelayAliases.FALLBACK_PROBE_LIMIT), asks)
        }

    @Test
    fun `a url that cannot be asked at all stays null, never empty`() =
        runBlocking {
            val probe = AliasProbe(fetch = { _, _ -> null }, limit = 500)

            // Null is what stops [RelayAliases] folding it. An empty set would
            // be an assertion that the relay holds nothing.
            assertNull(probe.fingerprint(url) {})
        }

    @Test
    fun `a relay that really is empty answers empty, not null`() =
        runBlocking {
            val probe = AliasProbe(fetch = { _, _ -> emptyList() }, limit = 500)

            assertEquals(emptySet(), probe.fingerprint(url) {})
        }

    @Test
    fun `everything downloaded reaches ingest before it is counted`() =
        runBlocking {
            val seen = mutableListOf<String>()
            val probe = AliasProbe(fetch = { _, _ -> events(4) }, limit = 500)

            val print = probe.fingerprint(url) { seen += it.id }

            // The probe is a sync that also identifies: nothing it pulled is
            // thrown away to pay for the verdict.
            assertEquals(4, seen.size)
            assertTrue(seen.toSet() == print)
        }
}
