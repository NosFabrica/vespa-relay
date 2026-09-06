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
package com.nosfabrica.vespa.relay.config

import org.junit.jupiter.api.Assumptions.assumeTrue
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The shipped examples must parse to the same streams as the operator's gitignored `sync.conf`,
 * and to the same monitor sources as their `monitor.conf`. Skipped, never passed, where there is
 * no local pair.
 */
class ExampleMatchesLiveConfTest {
    /** The fields that decide what a stream syncs, as text: `Filter` has no `equals`, so its json stands in. */
    private fun shape(s: SyncStream) =
        listOf(
            s.name,
            s.dir,
            s.filter.toJson(),
            s.urls.map { it.url },
            s.trusted,
            s.deleteMissing,
            s.discovery?.refreshSeconds,
            s.discovery
                ?.exclude
                ?.let { ex -> ex.urls.map { it.url }.sorted() + ex.patterns.map { it.pattern }.sorted() },
            s.discovery?.sources?.map { src -> src.filter.toJson() to src.selects },
        ).joinToString("\n")

    /** Tests run from the module directory, so the repo root is one level up. */
    private fun find(name: String) = listOf(File("../$name"), File(name)).firstOrNull { it.isFile }

    private fun load(file: File) = RouterConfigLoader.parse(file.readText())

    @Test
    fun `the shipped example parses to the same streams a live config runs`() {
        val liveFile = find("sync.conf")
        assumeTrue(liveFile != null, "no sync.conf here — it is gitignored, so this check is local only")
        val exampleFile = checkNotNull(find("sync.conf.example")) { "sync.conf.example is tracked and must exist" }

        val live = load(liveFile!!).streams
        val example = load(exampleFile).streams

        assertEquals(live.map { it.name }, example.map { it.name }, "the two configs define different streams")
        for ((l, e) in live.zip(example)) {
            assertEquals(shape(l), shape(e), "stream '${l.name}' differs between sync.conf and sync.conf.example")
        }
    }

    /** What the monitor measures and on which clocks, as text, for the same reason [shape] is text. */
    private fun monitorShape(m: MonitorConfig) =
        listOf(
            m.sources.map { src -> src.filter.toJson() to src.selects },
            m.exclude.urls
                .map { it.url }
                .sorted() +
                m.exclude.patterns
                    .map { it.pattern }
                    .sorted(),
            m.sweepSeconds,
            m.fastLaneSeconds,
            m.dialConcurrency,
        ).joinToString("\n")

    @Test
    fun `the shipped monitor example parses to the same sources a live monitor runs`() {
        // Its own check, because the two files drift apart independently now — that is the
        // point of splitting them, and it is also the way a monitor quietly stops measuring.
        val liveFile = find("monitor.conf")
        assumeTrue(liveFile != null, "no monitor.conf here — it is gitignored, so this check is local only")
        val exampleFile = checkNotNull(find("monitor.conf.example")) { "monitor.conf.example is tracked and must exist" }

        // Both parse to a MonitorConfig whatever they hold, so the emptiness check is on `sources`.
        val live = loadMonitor(liveFile!!)
        val example = loadMonitor(exampleFile)
        assertTrue(example.sources.isNotEmpty(), "monitor.conf.example is the template and must name what it measures")

        assertEquals(monitorShape(example), monitorShape(live), "monitor.conf and monitor.conf.example measure different sets")
    }

    /** A monitor file is the block's contents, so it needs a stream config to be parsed beside. */
    private fun loadMonitor(file: File): MonitorConfig =
        RouterConfigLoader
            .parse(
                """streams { none { dir = "down", filter = { "kinds": [1] }, urls = [] } }""",
                monitorHocon = file.readText(),
            ).monitor!!
}
