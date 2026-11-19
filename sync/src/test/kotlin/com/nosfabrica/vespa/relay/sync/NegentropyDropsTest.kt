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

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.single.RelayDisconnect
import com.vitorpamplona.quartz.nip01Core.relay.client.single.SentFrames
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Which cause a failed window is counted under, from quartz's typed failure. */
class NegentropyDropsTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.example")
    private val drops = NegentropyDrops()

    private fun failed(
        detail: String,
        failure: NegentropySyncException.Failure?,
        disconnect: RelayDisconnect? = null,
        sent: SentFrames? = null,
    ) = NegentropySyncException(url, Filter(kinds = listOf(1)), NegentropySyncException.Reason.UNAVAILABLE, detail, failure = failure, disconnect = disconnect, sent = sent)

    private fun end(
        side: RelayDisconnect.Side,
        code: Int? = null,
        reason: String? = null,
        errorType: String? = null,
    ) = RelayDisconnect(side, code, reason, errorType, 2_000, 1_000, SentFrames(3, 1_200_000, "NEG-MSG", 900, "NEG-MSG"))

    private fun causes(): Map<String, Long> =
        (drops.document()!!["causes"] as JsonArray).associate {
            val row = it as JsonObject
            row["cause"]!!.jsonPrimitive.content to row["windows"]!!.jsonPrimitive.content.toLong()
        }

    @Test
    fun `a dropped socket is counted under how it ended`() {
        drops.record(url, 10, failed("closed: relay disconnected", NegentropySyncException.Failure.DISCONNECTED, end(RelayDisconnect.Side.RELAY, 1009, "bye")))
        drops.record(url, 10, failed("closed: relay disconnected", NegentropySyncException.Failure.DISCONNECTED, end(RelayDisconnect.Side.CLIENT, reason = "released")))
        drops.record(url, 10, failed("closed: relay disconnected", NegentropySyncException.Failure.DISCONNECTED, end(RelayDisconnect.Side.TRANSPORT, reason = "reset", errorType = "SocketException")))
        assertEquals(mapOf("relay closed 1009 bye" to 1L, "we closed (released)" to 1L, "transport SocketException" to 1L), causes())
    }

    @Test
    fun `a refusal in the relay's own words keeps them`() {
        drops.record(url, 10, failed("blocked: negentropy disabled", NegentropySyncException.Failure.REFUSED))
        assertEquals(mapOf("refused: blocked: negentropy disabled" to 1L), causes())
    }

    @Test
    fun `refusals that differ only in their numbers are one cause`() {
        drops.record(url, 10, failed("rate-limited: 37 requests in 10s", NegentropySyncException.Failure.REFUSED))
        drops.record(url, 10, failed("rate-limited: 52 requests in 10s", NegentropySyncException.Failure.REFUSED))
        assertEquals(mapOf("refused: rate-limited: # requests in #s" to 2L), causes())
    }

    @Test
    fun `the largest frame comes from the ended socket, else from the live one`() {
        drops.record(url, 10, failed("closed: relay disconnected", NegentropySyncException.Failure.DISCONNECTED, end(RelayDisconnect.Side.RELAY, 1009)))
        drops.record(url, 10, failed("relay went silent", NegentropySyncException.Failure.SILENT, sent = SentFrames(1, 700, "NEG-OPEN", 700, "NEG-OPEN")))
        val rows = (drops.document()!!["causes"] as JsonArray).map { it as JsonObject }.associateBy { it["cause"]!!.jsonPrimitive.content }
        assertEquals("1200000", rows["relay closed 1009"]!!["largestSentMax"].toString())
        assertEquals("700", rows["silent"]!!["largestSentMax"].toString())
    }

    @Test
    fun `one socket end is counted once, however many windows it failed`() {
        val end = end(RelayDisconnect.Side.RELAY, 1009, "too big")
        drops.record(url, 10, failed("closed: relay disconnected", NegentropySyncException.Failure.DISCONNECTED, end))
        val again = drops.record(url, 10, failed("closed: relay disconnected", NegentropySyncException.Failure.DISCONNECTED, end))
        assertEquals(mapOf("relay closed 1009 too big" to 1L), causes())
        assertTrue("not counted again" in again, again)

        // The same words on a later socket are a later end.
        drops.record(url, 10, failed("closed: relay disconnected", NegentropySyncException.Failure.DISCONNECTED, end(RelayDisconnect.Side.RELAY, 1009, "too big")))
        assertEquals(mapOf("relay closed 1009 too big" to 2L), causes())
    }

    @Test
    fun `a failure with the socket still up is counted every time`() {
        repeat(2) { drops.record(url, 10, failed("relay went silent", NegentropySyncException.Failure.SILENT)) }
        assertEquals(mapOf("silent" to 2L), causes())
    }

    @Test
    fun `the log sentence carries the end, the rounds and our side of the window`() {
        val line =
            drops.record(
                url,
                4_200,
                NegentropySyncException(
                    url,
                    Filter(kinds = listOf(1)),
                    NegentropySyncException.Reason.UNAVAILABLE,
                    "closed: relay disconnected",
                    failure = NegentropySyncException.Failure.DISCONNECTED,
                    disconnect = end(RelayDisconnect.Side.RELAY, 1009, "too big"),
                    rounds = 3,
                ),
            )
        assertTrue("1009" in line && "3 NEG-MSG round(s)" in line && "we hold 4k" in line, line)
    }
}
