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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * The sweep cursor, mostly the cases where it must not be believed: a claim
 * about a different ask, and a claim old enough that the peer has moved on.
 */
class SweepStateTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example")
    private val other = RelayUrlNormalizer.normalize("wss://other.example")
    private val notes = Filter(kinds = listOf(1))
    private val mirror = "notes"

    private fun tempFile(): File {
        val f = File.createTempFile("sweep-state", ".json")
        f.delete()
        return f
    }

    @Test
    fun `an unknown peer falls back to the caller's size`() {
        assertEquals(50_000, SweepState(null).target(relay, 50_000))
    }

    @Test
    fun `a learned size and cap survive a restart`() {
        val f = tempFile()
        SweepState(f).use {
            it.learnCap(relay, 1_000_000, 800_000)
            it.setTarget(other, 12_500)
            it.flush()
        }
        val reopened = SweepState(f)
        assertEquals(800_000, reopened.target(relay, 1))
        assertEquals(1_000_000, reopened.peer(relay)?.cap)
        assertEquals(12_500, reopened.target(other, 1))
        assertNull(reopened.peer(other)?.cap, "a peer that never stated a cap must not acquire one")
    }

    @Test
    fun `a cursor survives a restart and is keyed on the stream and the peer`() {
        val f = tempFile()
        SweepState(f).use {
            it.advance(SweepState.keyFor(mirror, relay, notes), 1_500, 2_000)
            it.flush()
        }
        val reopened = SweepState(f)
        assertEquals(1_500, reopened.reconciled(SweepState.keyFor(mirror, relay, notes))?.downTo)
        assertEquals(2_000, reopened.reconciled(SweepState.keyFor(mirror, relay, notes))?.upTo)
        assertNull(reopened.reconciled(SweepState.keyFor(mirror, other, notes)), "another relay's sweep is not ours")
        assertNull(reopened.reconciled(SweepState.keyFor("archive", relay, notes)), "another stream's sweep is not ours either")
    }

    @Test
    fun `advancing only ever widens the finished region`() {
        val state = SweepState(null)
        state.advance(SweepState.keyFor(mirror, relay, notes), 1_500, 2_000)
        state.advance(SweepState.keyFor(mirror, relay, notes), 1_000, 1_499)
        val mark = assertNotNull(state.reconciled(SweepState.keyFor(mirror, relay, notes)))
        assertEquals(1_000, mark.downTo)
        assertEquals(2_000, mark.upTo)

        // A window inside the claim cannot narrow it.
        state.advance(SweepState.keyFor(mirror, relay, notes), 1_800, 1_900)
        assertEquals(1_000, state.reconciled(SweepState.keyFor(mirror, relay, notes))?.downTo)
        assertEquals(2_000, state.reconciled(SweepState.keyFor(mirror, relay, notes))?.upTo)
    }

    @Test
    fun `a window disjoint from the claim is not merged across the gap`() {
        // The resumed-sweep shape: the segment above the claim finishes its first window disjoint from it.
        val state = SweepState(null)
        state.advance(SweepState.keyFor(mirror, relay, notes), 1_000, 2_000)
        state.advance(SweepState.keyFor(mirror, relay, notes), 3_000, 4_000)
        val mark = assertNotNull(state.reconciled(SweepState.keyFor(mirror, relay, notes)))
        assertEquals(1_000, mark.downTo)
        assertEquals(2_000, mark.upTo, "a claim may only ever mean 'everything in here was compared'")

        // Forfeited until the segment reaches back down to the claim, then merged.
        state.advance(SweepState.keyFor(mirror, relay, notes), 2_001, 2_999)
        assertEquals(1_000, state.reconciled(SweepState.keyFor(mirror, relay, notes))?.downTo)
        assertEquals(2_999, state.reconciled(SweepState.keyFor(mirror, relay, notes))?.upTo)
    }

    @Test
    fun `the cursor ignores the time bounds and nothing else`() {
        val state = SweepState(null)
        state.advance(SweepState.keyFor(mirror, relay, notes), 1_000, 2_000)

        // Same ask, different window: what a sweep varies.
        assertNotNull(state.reconciled(SweepState.keyFor(mirror, relay, notes.copy(since = 5, until = 9))))
        // Different ask: reconciling kind 1 says nothing about kind 0.
        assertNull(state.reconciled(SweepState.keyFor(mirror, relay, Filter(kinds = listOf(0)))))
        assertNull(state.reconciled(SweepState.keyFor(mirror, relay, notes.copy(authors = listOf("a".repeat(64))))))
    }

    @Test
    fun `a stale cursor is not acted on`() {
        val fresh = SweepState(null, staleAfterSeconds = 3_600)
        fresh.advance(SweepState.keyFor(mirror, relay, notes), 1_000, 2_000)
        assertNotNull(fresh.reconciled(SweepState.keyFor(mirror, relay, notes)))

        // Zero horizon: anything written before this instant is already too old.
        val stale = SweepState(null, staleAfterSeconds = -1)
        stale.advance(SweepState.keyFor(mirror, relay, notes), 1_000, 2_000)
        assertNull(stale.reconciled(SweepState.keyFor(mirror, relay, notes)), "an aged claim must be re-compared, not trusted")
    }

    @Test
    fun `finishing a leg drops its cursor and leaves the peer's size`() {
        val state = SweepState(null)
        state.setTarget(relay, 12_500)
        state.advance(SweepState.keyFor(mirror, relay, notes), 1_000, 2_000)
        state.finish(SweepState.keyFor(mirror, relay, notes))

        assertNull(state.reconciled(SweepState.keyFor(mirror, relay, notes)))
        assertEquals(12_500, state.target(relay, 1), "what the peer will take outlives the sweep that learned it")
    }

    @Test
    fun `the file nests the stream, the filter and the relay`() {
        val f = tempFile()
        SweepState(f).use {
            it.setTarget(relay, 12_500)
            it.advance(SweepState.keyFor(mirror, relay, notes), 1_500, 2_000)
            it.flush()
        }
        val root = Json.parseToJsonElement(f.readText()).jsonObject
        val mark =
            assertNotNull(root["sweeps"])
                .jsonObject[mirror]!!
                .jsonObject[notes.toJson()]!!
                .jsonObject[relay.url]!!
                .jsonObject
        assertEquals(1_500L, mark["downTo"]!!.jsonPrimitive.long)
        assertEquals(2_000L, mark["upTo"]!!.jsonPrimitive.long)
        // `peers` stays flat: a learned window size belongs to the peer, not to an ask.
        assertEquals(
            12_500,
            root["peers"]!!
                .jsonObject[relay.url]!!
                .jsonObject["target"]!!
                .jsonPrimitive.int,
        )
    }

    // ---- a file written before the format nested ---------------------------

    /** The pre-stream flat key: relay, a pipe, then the shape. */
    private fun writeFlat(
        f: File,
        key: String,
        downTo: Long,
        upTo: Long,
    ) = f.writeText(
        Json.encodeToString(
            JsonObject.serializer(),
            buildJsonObject {
                put(
                    "sweeps",
                    buildJsonObject {
                        put(
                            key,
                            buildJsonObject {
                                put("downTo", downTo)
                                put("upTo", upTo)
                                put("at", System.currentTimeMillis() / 1000)
                            },
                        )
                    },
                )
            },
        ),
    )

    @Test
    fun `a pre-stream cursor is claimed by the stream that asks, and only that one`() {
        val f = tempFile()
        writeFlat(f, "${relay.url}|${notes.toJson()}", 1_500, 2_000)

        val reopened = SweepState(f)
        assertEquals(1_500, reopened.reconciled(SweepState.keyFor(mirror, relay, notes))?.downTo)
        assertNull(
            reopened.reconciled(SweepState.keyFor("archive", relay, notes)),
            "a second stream must not inherit a claim about a walk it never made",
        )

        // Claimed means moved: from here on it is written under the stream.
        reopened.flush()
        val sweeps = Json.parseToJsonElement(f.readText()).jsonObject["sweeps"]!!.jsonObject
        assertNull(sweeps["${relay.url}|${notes.toJson()}"], "the flat key must not survive the claim")
        assertEquals(
            1_500L,
            sweeps[mirror]!!
                .jsonObject[notes.toJson()]!!
                .jsonObject[relay.url]!!
                .jsonObject["downTo"]!!
                .jsonPrimitive.long,
        )
    }

    @Test
    fun `a stale pre-stream cursor is dropped, not filed under whoever asked`() {
        val f = tempFile()
        writeFlat(f, "${relay.url}|${notes.toJson()}", 1_500, 2_000)

        val state = SweepState(f, staleAfterSeconds = -1)
        assertNull(state.reconciled(SweepState.keyFor(mirror, relay, notes)), "an aged claim must be re-compared")
        state.flush()

        val sweeps = Json.parseToJsonElement(f.readText()).jsonObject["sweeps"]!!.jsonObject
        assertEquals(0, sweeps.size, "neither flat nor filed under a stream — it is worth nothing to anyone")
    }

    @Test
    fun `claiming widens what a sweep has already recorded, never replaces it`() {
        // `advance` claims too, so the window just finished may already be in the map at adoption.
        val f = tempFile()
        writeFlat(f, "${relay.url}|${notes.toJson()}", 1_500, 2_000)

        val reopened = SweepState(f)
        reopened.advance(SweepState.keyFor(mirror, relay, notes), 900, 1_499)
        val mark = assertNotNull(reopened.reconciled(SweepState.keyFor(mirror, relay, notes)))
        assertEquals(900, mark.downTo, "the window just finished")
        assertEquals(2_000, mark.upTo, "and the ground the pre-stream cursor already held")
    }

    @Test
    fun `an unclaimed pre-stream cursor is written back, not dropped`() {
        // The flusher runs long before a slow stream reaches this relay.
        val f = tempFile()
        val key = "${other.url}|${notes.toJson()}"
        writeFlat(f, key, 1_500, 2_000)

        val state = SweepState(f)
        // A different pair, so nothing claims the one on disk.
        state.advance(SweepState.keyFor(mirror, relay, notes), 3_000, 4_000)
        state.flush()

        val sweeps = Json.parseToJsonElement(f.readText()).jsonObject["sweeps"]!!.jsonObject
        assertEquals(1_500L, sweeps[key]!!.jsonObject["downTo"]!!.jsonPrimitive.long, "still there, still flat")
        assertNotNull(sweeps[mirror], "beside the stream that has claimed its own")
        // And still claimable after that round trip.
        assertEquals(1_500, SweepState(f).reconciled(SweepState.keyFor("archive", other, notes))?.downTo)
    }

    @Test
    fun `a corrupt file starts fresh instead of failing the boot`() {
        val f = tempFile()
        f.writeText("{not json")
        val state = SweepState(f)
        assertEquals(0, state.size())
        assertEquals(7, state.target(relay, 7))
    }

    @Test
    fun `no file configured keeps everything in memory`() {
        val state = SweepState(null)
        state.advance(SweepState.keyFor(mirror, relay, notes), 1_000, 2_000)
        state.flush()
        assertNotNull(state.reconciled(SweepState.keyFor(mirror, relay, notes)))
    }
}
