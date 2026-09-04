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
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.io.File
import java.util.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Builds a `SYNC_STATE_FILE` the size and shape of the production one that
 * motivated the flat-key prune, loads it, and asserts every flat key is gone
 * and every named stream's band survived byte for byte. Writes `before.json`
 * and `after.json` to `-DprodScaleDir` for `SyncCoverageReportProdScaleProbe`.
 * Selected by `-DprodScaleProbe=true`; pass `--rerun` or Gradle skips it silently.
 */
class SyncBandsProdScaleProbe {
    companion object {
        // Spans per band is the one free knob, tuned so the corpus lands on the real file's size.
        // A band carries a span per kind the walk observed, and flat bands stopped being written early.
        private const val NESTED_SPANS = 9
        private const val FLAT_SPANS = 6
    }

    /** Where the two files land for the relay-side probe to pick up. */
    private val outDir = File(System.getProperty("prodScaleDir") ?: "build/prod-scale")

    private val profiles = Filter(kinds = listOf(0, 10002, 10040))

    // 128 kinds, so each flat key carrying this filter is about 700 bytes before its band.
    private val content = Filter(kinds = contentKinds())

    private val assertionAuthors = (0 until 24).map { "%064x".format(it * 7919) }
    private val assertions = Filter(kinds = listOf(30382), authors = assertionAuthors)
    private val indexers = Filter(kinds = listOf(0, 3, 10002))

    private fun contentKinds(): List<Int> {
        // The members do not matter; the serialised width does.
        val kinds = sortedSetOf(0, 1, 9, 48106)
        var k = 3
        while (kinds.size < 128) {
            kinds += k
            k += if (k < 100) 3 else 397
        }
        return kinds.toList().take(128)
    }

    @Test
    fun `a production-scale state file loads, prunes and rewrites`() {
        if (System.getProperty("prodScaleProbe") != "true") {
            println("[skip] SyncBandsProdScaleProbe — set -DprodScaleProbe=true to build a ~14MB corpus")
            return
        }
        outDir.mkdirs()
        val before = File(outDir, "before.json")
        val after = File(outDir, "after.json")

        val built = generate(before)
        println("── corpus ──────────────────────────────────────────────")
        println("  file                 ${before.length() / 1_000_000.0} MB")
        println("  top-level keys       ${built.topLevel}   (${built.streams} streams, ${built.flat} flat)")
        println("  band entries         ${built.nestedBands + built.flat}   = ${built.nestedBands} nested + ${built.flat} flat")
        println("  flat share of bytes  ${(built.flatBytes * 100 / before.length())}%")

        // The constructor is where the prune happens.
        System.gc()
        val heapBefore = usedHeap()
        val loadStart = System.nanoTime()
        val bands = SyncBands(before.also { it.copyTo(after, overwrite = true) }.let { after })
        val loadMs = (System.nanoTime() - loadStart) / 1_000_000
        System.gc()
        val heapAfter = usedHeap()

        val flushStart = System.nanoTime()
        bands.flush()
        val flushMs = (System.nanoTime() - flushStart) / 1_000_000

        println("── prune ───────────────────────────────────────────────")
        println("  load + prune         $loadMs ms")
        println("  flush (rewrite)      $flushMs ms")
        println("  retained heap        ${(heapAfter - heapBefore) / 1_000_000} MB")
        println("  file after           ${after.length() / 1_000_000.0} MB  (was ${before.length() / 1_000_000.0} MB)")
        println("  bytes reclaimed      ${(before.length() - after.length()) / 1_000_000.0} MB  (${(before.length() - after.length()) * 100 / before.length()}%)")

        // ---- what the prune must have done --------------------------------
        val written = Json.parseToJsonElement(after.readText()).jsonObject
        val flatLeft = written.count { (_, v) -> v.jsonObject["min"] != null }
        assertEquals(0, flatLeft, "every flat key is gone")
        assertEquals(built.streams, written.size, "and only the streams are left")

        // Band by band, not by count: a prune that dropped one relay in thousands still counts right.
        val source = Json.parseToJsonElement(before.readText()).jsonObject
        var checked = 0
        for ((stream, byFilter) in source) {
            if (byFilter.jsonObject["min"] != null) continue
            for ((filter, byRelay) in byFilter.jsonObject) {
                for ((relay, band) in byRelay.jsonObject) {
                    val out =
                        written[stream]
                            ?.jsonObject
                            ?.get(filter)
                            ?.jsonObject
                            ?.get(relay)
                    assertEquals(band, out, "$stream / $relay must survive the prune byte for byte")
                    checked++
                }
            }
        }
        assertEquals(built.nestedBands, checked, "and every one of them was actually compared")
        println("  nested bands intact  $checked / ${built.nestedBands}")

        // Nothing left to prune means nothing dirty, so an idle router stops rewriting.
        val stamp = after.lastModified()
        SyncBands(after).flush()
        assertEquals(stamp, after.lastModified(), "the boot after the prune writes nothing")
        println("  second boot          wrote nothing")
        assertTrue(after.length() < before.length(), "the file got smaller")
    }

    private fun usedHeap(): Long = Runtime.getRuntime().let { it.totalMemory() - it.freeMemory() }

    private class Built(
        val topLevel: Int,
        val streams: Int,
        val flat: Int,
        val nestedBands: Int,
        val flatBytes: Long,
    )

    /** The corpus: relay counts per stream and the flat split are the production ones. */
    private fun generate(f: File): Built {
        val rnd = Random(20260811)
        var nested = 0

        /**
         * One band. The band-level `min`/`max`/`complete` must be the outer edges of
         * the spans: [bandOf] restores only `spans` and `fullAt` and quartz recomputes
         * the rest, so edges written independently are corrected on load and fail the
         * byte-for-byte comparison.
         */
        fun band(
            kinds: List<Int>,
            spanCount: Int,
            // Per band, not per span: a reconcile completes the whole filter at once.
            settled: Boolean,
        ): JsonObject {
            val base = 1_690_000_000L + rnd.nextInt(20_000_000)
            val spans =
                kinds.shuffled(rnd).take(spanCount).associateWith {
                    val lo = base + rnd.nextInt(1_000_000)
                    Triple(lo, lo + rnd.nextInt(40_000_000), settled)
                }
            return buildJsonObject {
                put("min", spans.values.minOf { it.first })
                put("max", spans.values.maxOf { it.second })
                put("complete", spans.values.all { it.third })
                put("fullAt", if (rnd.nextBoolean()) 1_754_600_000L + rnd.nextInt(400_000) else 0L)
                put(
                    "spans",
                    buildJsonObject {
                        spans.forEach { (kind, s) ->
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

        fun stream(
            filter: Filter,
            relays: Int,
            host: String,
            reconciled: Int,
            host2: String = host,
            extraLegs: Int = 0,
        ) = buildJsonObject {
            put(
                filter.toJson(),
                buildJsonObject {
                    repeat(relays) { i ->
                        put("wss://$host-$i.example/", band(filter.kinds!!, NESTED_SPANS, settled = i < reconciled))
                        nested++
                    }
                },
            )
            // A second, narrower leg on the same relays adds bands without adding rows.
            if (extraLegs > 0) {
                put(
                    filter.copy(authors = listOf("%064x".format(1))).toJson(),
                    buildJsonObject {
                        repeat(extraLegs) { i ->
                            // `complete` ANDs across a relay's legs, so the second leg carries the verdict of the leg it joins.
                            put("wss://$host2-$i.example/", band(filter.kinds!!, NESTED_SPANS, settled = i < reconciled))
                            nested++
                        }
                    },
                )
            }
        }

        // The folded subpath aliases the flat keys name: many paths on few hosts.
        val paths = listOf("dynamo-yankee", "lantern", "bravo", "tango-lima", "kilo", "sierra-echo")
        var flatCount = 0
        var flatBytes = 0L

        val root =
            buildJsonObject {
                put("indexers", stream(indexers, 5, "indexer", reconciled = 4))
                put("profileViaOutbox", stream(profiles, 3469, "profile", reconciled = 2709))
                put("contentViaOutbox", stream(content, 3563, "content", reconciled = 2033, extraLegs = 23))
                put("assertions", stream(assertions, 5, "assertion", reconciled = 2))

                listOf(profiles to 1578, content to 1043, assertions to 3).forEach { (filter, count) ->
                    val json = filter.toJson()
                    repeat(count) { i ->
                        val key = "wss://alias-${i % 400}.example/${paths[i % paths.size]}-$i $json"
                        val value = band(filter.kinds!!, FLAT_SPANS, settled = false)
                        put(key, value)
                        flatCount++
                        flatBytes += key.length + Json.encodeToString(JsonObject.serializer(), value).length
                    }
                }
            }

        f.parentFile?.mkdirs()
        f.writeText(Json { prettyPrint = true }.encodeToString(JsonObject.serializer(), root))
        return Built(topLevel = root.size, streams = 4, flat = flatCount, nestedBands = nested, flatBytes = flatBytes)
    }
}
