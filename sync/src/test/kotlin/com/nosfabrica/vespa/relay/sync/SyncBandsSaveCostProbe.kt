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
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.io.File
import java.io.Writer
import java.lang.management.ManagementFactory
import java.util.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * What one save and one load of a `SYNC_STATE_FILE` shaped like staging's cost: a 197-kind content
 * stream on three bands plus its bare key, about a million per-kind spans. Prints time, bytes
 * allocated and file size. Selected by `-DprodScaleProbe=true`; it needs `-PtestHeap=8g`.
 */
class SyncBandsSaveCostProbe {
    private val outDir = File(System.getProperty("prodScaleDir") ?: "build/prod-scale")

    private val threads = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean

    private val contentKinds = (0 until 197).map { if (it < 60) it else 1000 + it * 37 }

    @Test
    fun `a staging-sized state file, saved and loaded`() {
        if (System.getProperty("prodScaleProbe") != "true") {
            println("[skip] SyncBandsSaveCostProbe — set -DprodScaleProbe=true (and -PtestHeap=8g) to build a ~75MB corpus")
            return
        }
        outDir.mkdirs()
        val compact = File(outDir, "staging.json").also(::generate)
        val pretty = File(outDir, "staging-pretty.json")
        pretty.writeText(Json { prettyPrint = true }.encodeToString(JsonObject.serializer(), Json.parseToJsonElement(compact.readText()).jsonObject))
        println("  corpus               ${compact.length() / 1_000_000} MB compact, ${pretty.length() / 1_000_000} MB pretty-printed")

        val bands = measure("load (pretty)") { SyncBands(pretty) }
        measure("snapshot (status)") { bands.snapshot() }
        // Any change dirties the map, and the flush rewrites all of it.
        bands.record("contentViaOutbox", RelayUrlNormalizer.normalize("wss://one-more.example"), Filter(kinds = listOf(1)), 1_700_000_000L, 1_700_000_100L, paged = true)
        measure("save") { bands.flush() }
        println("  file after save      ${pretty.length() / 1_000_000} MB")
        val reloaded = measure("load (as saved)") { SyncBands(pretty) }
        assertEquals(bands.snapshot(), reloaded.snapshot(), "what was saved is what loads")
    }

    private fun <T> measure(
        what: String,
        block: () -> T,
    ): T {
        System.gc()
        val bytes = threads.currentThreadAllocatedBytes
        val start = System.nanoTime()
        val out = block()
        val ms = (System.nanoTime() - start) / 1_000_000
        println("  ${what.padEnd(20)} $ms ms, ${(threads.currentThreadAllocatedBytes - bytes) / 1_000_000} MB allocated")
        return out
    }

    /** Streamed by hand: the corpus as one tree would need the heap this probe is measuring. */
    private fun generate(f: File) {
        val rnd = Random(20261007)
        val content = Filter(kinds = contentKinds).toJson()
        val profiles = Filter(kinds = listOf(0, 3, 10002)).toJson()

        /** Some kinds seeded to one shared span, as `--seed-covered` leaves them; the rest walked apart. */
        fun band(
            kinds: List<Int>,
            spans: Int,
            seeded: Int,
        ): JsonObject {
            val min = 1_690_000_000L + rnd.nextInt(20_000_000)
            val max = min + 60_000_000 + rnd.nextInt(10_000_000)
            val byKind =
                kinds.shuffled(rnd).take(spans).withIndex().associate { (i, kind) ->
                    kind to if (i < seeded) Triple(min, max, true) else Triple(min + rnd.nextInt(30_000_000), max - rnd.nextInt(1_000_000), rnd.nextBoolean())
                }
            return buildJsonObject {
                put("min", byKind.values.minOf { it.first })
                put("max", byKind.values.maxOf { it.second })
                put("complete", byKind.values.all { it.third })
                put("fullAt", if (rnd.nextBoolean()) 1_759_000_000L + rnd.nextInt(800_000) else 0L)
                put(
                    "spans",
                    buildJsonObject {
                        byKind.forEach { (kind, s) ->
                            put(
                                kind.toString(),
                                buildJsonObject {
                                    put("min", s.first)
                                    put("max", s.second)
                                    put("complete", s.third)
                                },
                            )
                        }
                    },
                )
            }
        }

        fun Writer.str(s: String) = write(JsonPrimitive(s).toString())

        fun Writer.stream(
            name: String,
            filter: String,
            relays: Int,
            kinds: List<Int>,
            spans: () -> Int,
            seeded: Int,
        ) {
            str(name)
            write(":{")
            str(filter)
            write(":{")
            repeat(relays) { i ->
                if (i > 0) write(",")
                write("\"wss://relay-$i.example.com/\":")
                write(band(kinds, spans(), seeded).toString())
            }
            write("}},")
        }

        f.bufferedWriter().use { w ->
            w.write("{")
            for (tier in listOf("#t0", "#t1", "#t2", "")) w.stream("contentViaOutbox$tier", content, 3_700, contentKinds, { 39 + rnd.nextInt(80) }, 39)
            for (tier in listOf("#t0", "#t1", "")) w.stream("profileViaOutbox$tier", profiles, 5_367, listOf(0, 3, 10002), { 3 }, 0)
            w.write("\"#bandClocks\":{")
            listOf("contentViaOutbox" to content, "profileViaOutbox" to profiles).forEachIndexed { s, (stream, filter) ->
                if (s > 0) w.write(",")
                w.str(stream)
                w.write(":{")
                listOf("t0", "t1", "t2").forEachIndexed { b, band ->
                    if (b > 0) w.write(",")
                    w.write("\"$band\":{")
                    w.str(filter)
                    w.write(":{")
                    repeat(5_600) { i ->
                        if (i > 0) w.write(",")
                        w.write("\"wss://relay-$i.example.com/\":${1_759_000_000 + rnd.nextInt(800_000)}")
                    }
                    w.write("}}")
                }
                w.write("}")
            }
            w.write("}}")
        }
    }
}
