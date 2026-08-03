/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * `router.conf.example` is what a new operator copies, and it is only worth
 * shipping if it produces the sync this deployment actually runs. Two files
 * describing the same thing drift, and the drift is invisible: the example still
 * parses, still looks right, and quietly mirrors something else.
 *
 * So this asserts they parse to the same streams. Comments, key order and
 * whitespace are free to differ — the parsed result is not.
 */
class ExampleMatchesLiveConfTest {
    /**
     * Everything about a stream that decides what it syncs.
     *
     * Spelled out rather than comparing [MirrorStream] directly: `Filter` has no
     * `equals`, so a data-class comparison falls back to identity and fails on
     * two filters that are the same filter. Its json is what [SyncCursors]
     * already keys bands on, so it is this codebase's own definition of "the
     * same ask".
     */
    private fun shape(s: MirrorStream) =
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

    private fun load(name: String) =
        RouterConfigLoader.parse(
            checkNotNull(listOf(File("../$name"), File(name)).firstOrNull { it.isFile }) { "missing $name" }.readText(),
        )

    @Test
    fun `the shipped example parses to the same streams this deployment runs`() {
        val live = load("router.conf").streams
        val example = load("router.conf.example").streams

        assertEquals(live.map { it.name }, example.map { it.name }, "the two configs define different streams")
        for ((l, e) in live.zip(example)) {
            assertEquals(shape(l), shape(e), "stream '${l.name}' differs between router.conf and router.conf.example")
        }
    }
}
