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
package com.nosfabrica.vespa.relay.ingest

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A probe that throws costs time and is counted; it never teaches its gate that the probe stopped paying. */
class IngestProbeFailureTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://here.example")

    private fun ingest(
        offer: List<Event>,
        knownIds: (suspend (List<String>) -> Set<String>)?,
        newestVersions: (suspend (Int, List<String>) -> Map<String, AddressVersion>)?,
        probeTimeoutMs: Long = IngestPipeline.PROBE_TIMEOUT_MS,
    ): IngestPipeline =
        runBlocking {
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)
            val scope = CoroutineScope(Job())
            val pipeline =
                IngestPipeline(
                    store,
                    IngestTuning(concurrency = 1, batch = 1000),
                    audit = null,
                    servingPressure = null,
                    scope = scope,
                    knownIds = knownIds,
                    newestVersions = newestVersions,
                    probeTimeoutMs = probeTimeoutMs,
                )
            offer.forEach { pipeline.submit(it, skipVerify = false) }
            pipeline.start()
            var waitedMs = 0
            while (pipeline.accepted.get() + pipeline.rejected.get() < offer.size && waitedMs < 10_000) {
                delay(5)
                waitedMs += 5
            }
            scope.cancel()
            pipeline.close()
            pipeline
        }

    @Test
    fun `a failed id probe is counted and leaves the gate's learned rate unjudged`() {
        val signer = NostrSignerSync()
        val notes = (0 until 200).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "note $it") }
        val failing: suspend (List<String>) -> Set<String> = { throw IllegalStateException("store down") }
        val pipeline = ingest(notes, knownIds = failing, newestVersions = null)

        assertEquals(200, pipeline.accepted.get(), "a failed probe must cost time, never events")
        val status = pipeline.probeStatus()
        assertTrue("id 1 failed" in status, status)
        // Taught by the failure, the gate would read "id 0% dropped" and drop to sampling.
        assertFalse("dropped" in status, status)
    }

    @Test
    fun `a failed version probe is counted and leaves the gate's learned rate unjudged`() {
        val profiles = (0 until 200).map { NostrSignerSync().sign<Event>(1_700_000_000L, 0, emptyArray(), "{}") }
        val failing: suspend (Int, List<String>) -> Map<String, AddressVersion> = { _, _ -> throw IllegalStateException("store down") }
        val pipeline = ingest(profiles, knownIds = null, newestVersions = failing)

        assertEquals(200, pipeline.accepted.get())
        val status = pipeline.probeStatus()
        assertTrue("version 1 failed" in status, status)
        assertFalse("version 0% dropped" in status, status)
    }

    // NosFabrica/vespa-relay#289: a version query that never answered held its pass for 4 h 40 min.
    @Test
    fun `a version probe that never answers is cut and its batch is stored unprobed`() {
        val profiles = (0 until 200).map { NostrSignerSync().sign<Event>(1_700_000_000L, 0, emptyArray(), "{}") }
        val silent: suspend (Int, List<String>) -> Map<String, AddressVersion> = { _, _ -> awaitCancellation() }
        val pipeline = ingest(profiles, knownIds = null, newestVersions = silent, probeTimeoutMs = 200)

        assertEquals(200, pipeline.accepted.get(), "the cut probe costs its timeout, never the batch")
        val status = pipeline.probeStatus()
        assertTrue("version 1 failed" in status, status)
    }

    @Test
    fun `an id probe that never answers is cut and its batch is stored unprobed`() {
        val signer = NostrSignerSync()
        val notes = (0 until 200).map { signer.sign<Event>(1_700_000_000L + it, 1, emptyArray(), "note $it") }
        val silent: suspend (List<String>) -> Set<String> = { awaitCancellation() }
        val pipeline = ingest(notes, knownIds = silent, newestVersions = null, probeTimeoutMs = 200)

        assertEquals(200, pipeline.accepted.get())
        assertTrue("id 1 failed" in pipeline.probeStatus(), pipeline.probeStatus())
    }
}
