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

import java.io.File
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The two files a new deployment copies, held to what EVERY deployment needs. Their values track
 * a real one, so the router's design rules are checked elsewhere, against a fixture written to
 * follow them ([RouterConfExamplesTest]). What stays here is the part no deployment may trade
 * away: an example that crash-loops, oversubscribes the dispatcher, or scans for relays in a kind
 * nothing mirrors is a broken install.
 */
class ShippedExamplesTest {
    /** Tests run from the module dir; the examples sit at the repo root. */
    private fun at(name: String): File = requireNotNull(listOf(File("../$name"), File(name)).firstOrNull { it.isFile }) { "missing $name" }

    /** Both shipped files, parsed together the way a deployment reads them. */
    private val example: RouterConfig =
        RouterConfigLoader.parse(
            syncOrigin = at("sync.conf.example"),
            monitorOrigin = at("monitor.conf.example"),
        )

    @Test
    fun `the shipped pair boots`() {
        // sync.conf.example's header copies BOTH files, and its streams discover their relays, so
        // parsed together they have to survive the undeclared-monitor refusal.
        RouterConfigLoader.refuseUndeclaredMonitor(example)
        assertTrue(example.discoveryStreams().isNotEmpty(), "the shape: without a discovery stream the refusal cannot fire")
        assertNotNull(example.monitorSources(), "…so the monitor half has to declare what it measures")
    }

    @Test
    fun `a static stream seeds the store the discovery scans read from`() {
        // On an empty store a discovery scan finds nothing: something has to be named outright.
        assertTrue(example.streams.any { it.discovery == null && it.urls.isNotEmpty() }, "the example names no relay to start from")
    }

    @Test
    fun `the shipped budgets fit under the socket ceiling`() {
        // Duplicated rather than imported from `VisitPool`: `:peers` does not see `:sync`.
        val dispatcherHeadroom = 900
        val dials = example.streams.sumOf { it.visitConcurrency ?: RouterConfig.DEFAULT_VISIT_CONCURRENCY }
        val tails = example.streams.sumOf { it.maxLiveConcurrency ?: RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY }
        assertTrue(
            dials + tails <= dispatcherHeadroom,
            "the example asks for up to $dials dial(s) and $tails tail(s) = ${dials + tails} sockets, over the " +
                "$dispatcherHeadroom this router leaves for the static upstreams, the monitor and the healer",
        )
    }

    @Test
    fun `every discovery scan reads a kind some stream actually mirrors`() {
        // A scan for a kind nothing mirrors fails silently: no error, just no relays.
        val mirrored = example.streams.flatMap { it.filter.kinds.orEmpty() }.toSet()
        val scanned =
            example.discoveryStreams().flatMap { it.discovery!!.sources } + example.monitorSources()?.sources.orEmpty()
        // Kind 30166 excepted: the monitor writes those into this store itself.
        scanned.filterNot { it.filter.kinds == listOf(30166) }.forEach { source ->
            source.filter.kinds.orEmpty().forEach { kind ->
                assertTrue(kind in mirrored, "a relay scan reads kind $kind, which no stream mirrors")
            }
        }
    }

    @Test
    fun `every dynamic stream states how often its list is re-derived`() {
        example.discoveryStreams().forEach { stream ->
            assertTrue(stream.discovery!!.refreshSeconds > 0, "'${stream.name}' needs a refresh period")
        }
    }
}
