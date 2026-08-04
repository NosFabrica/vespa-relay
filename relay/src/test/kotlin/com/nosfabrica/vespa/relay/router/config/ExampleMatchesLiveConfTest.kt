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
package com.nosfabrica.vespa.relay.router.config

import org.junit.jupiter.api.Assumptions.assumeTrue
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * `router.conf.example` is what a new operator copies, and it is only worth
 * shipping if it produces the sync a real deployment runs. Two files describing
 * the same thing drift, and the drift is invisible: the example still parses,
 * still looks right, and quietly mirrors something else.
 *
 * So this asserts they parse to the same streams. Comments, key order and
 * whitespace are free to differ — the parsed result is not.
 *
 * A LOCAL check, and it cannot be anything else. `router.conf` is gitignored on
 * purpose — it is machine-local, holding one operator's relay list and filters —
 * so a clean checkout has nothing to compare against and CI never will. Written
 * without noticing that, it failed CI on the file's absence, which is a test
 * reporting the repository's design as a defect.
 *
 * Where there is no `router.conf` this is SKIPPED rather than passed. A test
 * that returns green having compared nothing is the more expensive mistake: it
 * says the example was verified when the run never looked at it.
 */
class ExampleMatchesLiveConfTest {
    /**
     * Everything about a stream that decides what it syncs.
     *
     * Spelled out rather than comparing [SyncStream] directly: `Filter` has no
     * `equals`, so a data-class comparison falls back to identity and fails on
     * two filters that are the same filter. Its json is what [SyncBands]
     * already keys bands on, so it is this codebase's own definition of "the
     * same ask".
     */
    private fun shape(s: SyncStream) =
        listOf(
            s.name,
            s.dir,
            s.filter.toJson(),
            s.urls.map { it.url },
            s.trusted,
            s.sync,
            s.deleteMissing,
            s.dynamic?.refreshSeconds,
            s.dynamic?.concurrency,
            s.dynamic?.authorsPerLeg,
            s.dynamic
                ?.exclude
                ?.map { it.url }
                ?.sorted(),
            s.dynamic?.sources?.map { src -> src.filter.toJson() to src.selects },
        ).joinToString("\n")

    /** Tests run from the module directory, so the repo root is one level up. */
    private fun find(name: String) = listOf(File("../$name"), File(name)).firstOrNull { it.isFile }

    private fun load(file: File) = RouterConfigLoader.parse(file.readText())

    @Test
    fun `the shipped example parses to the same streams a live config runs`() {
        val liveFile = find("router.conf")
        assumeTrue(liveFile != null, "no router.conf here — it is gitignored, so this check is local only")
        val exampleFile = checkNotNull(find("router.conf.example")) { "router.conf.example is tracked and must exist" }

        val live = load(liveFile!!).streams
        val example = load(exampleFile).streams

        assertEquals(live.map { it.name }, example.map { it.name }, "the two configs define different streams")
        for ((l, e) in live.zip(example)) {
            assertEquals(shape(l), shape(e), "stream '${l.name}' differs between router.conf and router.conf.example")
        }
    }
}
