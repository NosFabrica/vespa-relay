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
package com.nosfabrica.vespa.relay.router

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
 * The prune, run over a `SYNC_STATE_FILE` the size and shape of the one that
 * motivated it — 2,628 top-level keys of which 2,624 are flat, ~9.7k bands,
 * ~14MB — rather than over the four-band fixtures the unit tests use.
 *
 * ```bash
 * ./gradlew :sync:test --tests '*SyncBandsProdScaleProbe*' -DprodScaleProbe=true --rerun -i
 * ```
 *
 * `--rerun` is load-bearing for the same reason it is on `RealRelayDrainProbe`:
 * the task is up-to-date-checked, so a second identical run is SKIPPED and
 * prints nothing, which reads as a silent pass.
 *
 * **The corpus is RECONSTRUCTED, not the staging file.** Every number it is
 * built to is measured and quoted in the issue — the four stream names and
 * their relay/reconciled/paged counts, the 1,578 + 1,043 + 3 split of the flat
 * keys across three filters, the folded subpath aliases those keys name, and
 * the 18% of bytes they account for. What that buys is the thing unit tests
 * cannot show: that the prune holds at a scale where the file does not fit in
 * a comfortable heap, and that it leaves every named stream's bands untouched
 * while it does. It writes `before.json` and `after.json` for
 * `SyncCoverageReportProdScaleProbe` to chart, because the report lives in the
 * other module and the two only meet on disk — which is also how the relay and
 * the router meet in production.
 */
class SyncBandsProdScaleProbe {
    companion object {
        // The one free knob in the corpus, tuned so the whole lands on the
        // ~13.8MB the real file measured. A live band carries a span per kind
        // the walk actually OBSERVED, so it is a property of the traffic rather
        // than of the filter — a 128-kind filter does not write 128 spans. The
        // flat bands are narrower because they stopped being written the day
        // the format nested and never saw another walk.
        private const val NESTED_SPANS = 9
        private const val FLAT_SPANS = 6
    }

    /** Where the two files land for the relay-side probe to pick up. */
    private val outDir = File(System.getProperty("prodScaleDir") ?: "build/prod-scale")

    // The four streams the deployment runs, with the row counts /stats.json
    // reported for each. `reconciled` is the band-level `complete`.
    private val profiles = Filter(kinds = listOf(0, 10002, 10040))

    // 128 kinds, which is what makes this stream's filter — and so its every
    // flat KEY — about 700 bytes before a band is written at all.
    private val content = Filter(kinds = contentKinds())

    private val assertionAuthors = (0 until 24).map { "%064x".format(it * 7919) }
    private val assertions = Filter(kinds = listOf(30382), authors = assertionAuthors)
    private val indexers = Filter(kinds = listOf(0, 3, 10002))

    private fun contentKinds(): List<Int> {
        // A spread that ends at 48106, as the issue's excerpt does. The exact
        // members do not matter; the SERIALISED WIDTH does, because it is what
        // a flat key carrying this filter costs 1,043 times over.
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

        // Load: the constructor is where the prune happens.
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

        // The whole point: the streams are UNTOUCHED. Compared band by band
        // against the input, not by count — a prune that dropped one relay of
        // 3,563 would still count right on every summary the card prints.
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

        // A second boot on the pruned file must be a no-op: nothing left to
        // prune means nothing to mark dirty, so an idle router stops rewriting.
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

    /**
     * The corpus. Relay counts per stream and the flat split are the issue's
     * measured ones; the per-band `spans` width is the one free knob, tuned so
     * the whole comes out at the ~13.8MB the file actually was.
     */
    private fun generate(f: File): Built {
        val rnd = Random(20260811)
        var nested = 0

        /**
         * One band, with the invariant the real writer keeps: the band-level
         * `min`/`max`/`complete` are the OUTER EDGES of the per-kind spans, not
         * values of their own.
         *
         * Getting this wrong is what the probe caught on its first run, and it
         * is worth stating because it is not obvious from the file: [bandOf]
         * restores only `spans` and `fullAt`, and quartz recomputes the outer
         * three from them. So a band whose outer edges disagree with its spans
         * is silently corrected on load — a fixture that writes them
         * independently fails a byte-for-byte comparison against its own input
         * while the code under test is behaving perfectly.
         */
        fun band(
            kinds: List<Int>,
            spanCount: Int,
            // Decided per BAND, not per span, because that is how a band
            // becomes `complete`: a reconcile finishes the whole filter at
            // once. Deriving it per span instead made a 9-span band complete
            // one time in 512, and the card came out at 431 of 3,469
            // reconciled where the deployment reported 2,709.
            settled: Boolean,
        ): JsonObject {
            val base = 1_690_000_000L + rnd.nextInt(20_000_000)
            val spans =
                // Only the kinds a walk actually observed carry a span, which
                // is why a 128-kind filter does not write 128 of them.
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
            // How many of them are RECONCILED, straight off the card the issue
            // quotes. The rest are mid-page, which is the state most of the
            // corpus is in at any moment.
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
            // A handful of relays reached by a SECOND, narrower leg — which is
            // why the file holds 7,065 bands across 7,042 relays.
            if (extraLegs > 0) {
                put(
                    filter.copy(authors = listOf("%064x".format(1))).toJson(),
                    buildJsonObject {
                        repeat(extraLegs) { i ->
                            // The SAME relays as the main leg, so these add
                            // bands without adding rows — which is why the file
                            // holds 7,065 bands across 7,042 relays. `complete`
                            // ANDs across a relay's legs, so a second leg that
                            // is still paging must not flip a reconciled row:
                            // it carries the same verdict as the leg it joins.
                            put("wss://$host2-$i.example/", band(filter.kinds!!, NESTED_SPANS, settled = i < reconciled))
                            nested++
                        }
                    },
                )
            }
        }

        // The folded subpath aliases the flat keys name: many paths on few
        // hosts, which is what made them aliases in the first place.
        val paths = listOf("dynamo-yankee", "lantern", "bravo", "tango-lima", "kilo", "sierra-echo")
        var flatCount = 0
        var flatBytes = 0L

        val root =
            buildJsonObject {
                put("indexers", stream(indexers, 5, "indexer", reconciled = 4))
                put("profileViaOutbox", stream(profiles, 3469, "profile", reconciled = 2709))
                put("contentViaOutbox", stream(content, 3563, "content", reconciled = 2033, extraLegs = 23))
                put("assertions", stream(assertions, 5, "assertion", reconciled = 2))

                // 1,578 + 1,043 + 3 = 2,624, the split the issue measured.
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
