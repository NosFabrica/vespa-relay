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

import com.nosfabrica.vespa.relay.status.SyncCoverageReport
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The status page and the file read one snapshot; whatever is reused between reads, none may be stale. */
class SnapshotFreshnessTest {
    private val a = RelayUrlNormalizer.normalize("wss://a.example")
    private val b = RelayUrlNormalizer.normalize("wss://b.example")
    private val notes = Filter(kinds = listOf(1))

    private fun now() = System.currentTimeMillis() / 1000

    private fun JsonObject.relays(
        stream: String,
        filter: Filter,
    ) = (
        this[stream]
            ?.jsonObject
            ?.get(filter.toJson())
            ?.jsonObject
            ?.keys
    ).orEmpty()

    @Test
    fun `every change to the bands is in the next snapshot`() {
        val bands = SyncBands(null)
        assertTrue(bands.snapshot().isEmpty())

        bands.record("s", a, notes, now() - 600, now() - 60, paged = true)
        assertEquals(setOf(a.url), bands.snapshot().relays("s", notes))
        assertEquals(setOf(a.url), bands.snapshot().relays("s", notes), "an unchanged read")

        bands.record("s", b, notes, now() - 600, now() - 60, paged = true)
        assertEquals(setOf(a.url, b.url), bands.snapshot().relays("s", notes))

        bands.noteCannotReconcile("s", a, notes, "tier:all", "no NIP-77")
        assertNotNull(bands.snapshot()["#cannotReconcile"])

        bands.dropFolded("s", listOf(b))
        assertEquals(setOf(a.url), bands.snapshot().relays("s", notes), "a fold hides the relay at once")
    }

    @Test
    fun `the file holds what the last change made`() {
        val f = File.createTempFile("bands", ".json").also { it.delete() }
        try {
            val bands = SyncBands(f)
            bands.record("s", a, notes, now() - 600, now() - 60, paged = true)
            bands.snapshot()
            bands.record("s", b, notes, now() - 600, now() - 60, paged = true)
            bands.flush()
            assertEquals(setOf(a.url, b.url), SyncBands(f).snapshot().relays("s", notes))
        } finally {
            f.delete()
        }
    }

    private fun downTo(doc: JsonObject) =
        doc["sweeps"]!!
            .jsonObject["s"]!!
            .jsonObject.values
            .single()
            .jsonObject[a.url]!!
            .jsonObject["downTo"]!!
            .jsonPrimitive.content

    @Test
    fun `every change to the sweep cursors is in the next snapshot`() {
        val sweeps = SweepState(null)
        val cursor = SweepState.keyFor("s", a, notes)
        assertTrue(sweeps.snapshot()["sweeps"]!!.jsonObject.isEmpty())

        sweeps.advance(cursor, 100, 200)
        assertEquals("100", downTo(sweeps.snapshot()))

        sweeps.advance(cursor, 50, 99)
        assertEquals("50", downTo(sweeps.snapshot()))

        sweeps.finish(cursor)
        assertNull(sweeps.snapshot()["sweeps"]!!.jsonObject["s"])
    }

    @Test
    fun `a report built after another reads its own document`() {
        val now = now()
        val first = SyncBands(null).apply { record("s", a, notes, now - 600, now - 60, paged = true) }.snapshot()
        val photos = Filter(kinds = listOf(20))
        val second = SyncBands(null).apply { record("s", a, photos, now - 600, now - 60, paged = true) }.snapshot()

        SyncCoverageReport.build(first, null, now)
        val report = SyncCoverageReport.build(second, null, now)!!
        val filter =
            report["streams"]!!
                .jsonArray
                .single()
                .jsonObject["filter"]!!
                .jsonObject
        assertEquals("[20]", filter["kinds"].toString(), "the second document's filter, not a remembered one")
    }

    /** Runs [mutate] while another thread flushes [flush] in a tight loop, then closes. */
    private fun racingFlusher(
        flush: () -> Unit,
        mutate: () -> Unit,
    ) {
        val done = AtomicBoolean()
        val flusher = Thread { while (!done.get()) flush() }.apply { start() }
        try {
            mutate()
        } finally {
            done.set(true)
            flusher.join()
        }
    }

    @Test
    fun `close persists every band change a racing flush did not`() {
        val f = File.createTempFile("bands", ".json").also { it.delete() }
        try {
            repeat(ROUNDS) { round ->
                f.delete()
                val bands = SyncBands(f)
                racingFlusher(bands::flush) {
                    repeat(CHANGES) {
                        val relay = RelayUrlNormalizer.normalize("wss://r$it.example")
                        bands.record("s", relay, notes, now() - 600, now() - 60, paged = true)
                    }
                }
                bands.close()
                assertEquals(bands.snapshot(), SyncBands(f).snapshot(), "round $round: the file holds the last change")
            }
        } finally {
            f.delete()
        }
    }

    @Test
    fun `close persists every sweep change a racing flush did not`() {
        val f = File.createTempFile("sweeps", ".json").also { it.delete() }
        try {
            repeat(ROUNDS) { round ->
                f.delete()
                val sweeps = SweepState(f)
                racingFlusher(sweeps::flush) {
                    repeat(CHANGES) { sweeps.advance(SweepState.keyFor("s", RelayUrlNormalizer.normalize("wss://r$it.example"), notes), 100L + it, 200L + it) }
                }
                sweeps.close()
                assertEquals(sweeps.snapshot(), SweepState(f).snapshot(), "round $round: the file holds the last change")
            }
        } finally {
            f.delete()
        }
    }

    @Test
    fun `a write that failed is retried by the next flush`() {
        val dir = Files.createTempDirectory("bands").toFile()
        val f = File(dir, "bands.json")
        try {
            val bands = SyncBands(f)
            bands.record("s", a, notes, now() - 600, now() - 60, paged = true)
            // A non-empty directory where the file goes makes the move fail.
            File(f, "blocker").apply { parentFile.mkdirs() }.writeText("")
            bands.flush()
            f.deleteRecursively()
            bands.close()
            assertEquals(setOf(a.url), SyncBands(f).snapshot().relays("s", notes))
        } finally {
            dir.deleteRecursively()
        }
    }

    private companion object {
        const val ROUNDS = 100
        const val CHANGES = 20
    }
}
