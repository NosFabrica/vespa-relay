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

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.SyncCoverage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertTrue

/** A future-dated event a relay served must not hide what is written between now and its stamp. */
class FutureEdgeTest {
    private val url = RelayUrlNormalizer.normalize("wss://future.example")
    private val notes = Filter(kinds = listOf(1))

    @Test
    fun `an event stamped ahead of the clock leaves the present outstanding`() {
        val now = System.currentTimeMillis() / 1000
        val ahead = now + 20 * 3_600
        val bands = SyncBands(null)
        bands.record(
            "s",
            url,
            notes,
            observedMin = now - 86_400,
            observedMax = ahead,
            paged = true,
            observedByKind = mapOf(1 to SyncCoverage.Span(now - 86_400, ahead, false)),
        )
        // Written a minute from now, before the clock reaches the future-dated event's stamp.
        val soon = now + 60
        val legs = bands.legs("s", url, notes)
        assertTrue(
            legs.any { (it.since ?: Long.MIN_VALUE) <= soon && soon <= (it.until ?: Long.MAX_VALUE) },
            "legs $legs leave an event written at $soon unasked",
        )
    }

    @Test
    fun `a band file written with an edge ahead of the clock is cut on load`() {
        val now = System.currentTimeMillis() / 1000
        val file =
            kotlin.io.path
                .createTempFile("bands", ".json")
                .toFile()
        try {
            // As an older build wrote it: the band's edge twenty hours past the clock.
            val ahead = now + 20 * 3_600
            val span = "\"min\": ${now - 86_400}, \"max\": $ahead, \"complete\": false"
            file.writeText(
                "{\"s\": {${Json.encodeToString(notes.toJson())}: {\"${url.url}\": " +
                    "{$span, \"fullAt\": 0, \"spans\": {\"1\": {$span}}}}}}",
            )
            val reloaded = SyncBands(file)
            val soon = now + 60
            assertTrue(
                reloaded.legs("s", url, notes).any { (it.since ?: Long.MIN_VALUE) <= soon && soon <= (it.until ?: Long.MAX_VALUE) },
                "a reloaded future edge still hides the present",
            )
        } finally {
            file.delete()
        }
    }
}
