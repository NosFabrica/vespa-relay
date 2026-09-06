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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.config.MonitorConfig
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Whether the monitor runs, and over what, asked of the whole config through the real loader. */
class MonitorGateTest {
    private fun config(text: String) = RouterConfigLoader.parse(text.trimIndent())

    /** What the mirror would hand the plane for this config. */
    private fun sourcesOf(text: String) = config(text).monitorSources()

    /** The same, with the monitor declared in its own file — `monitor.conf`, contents at top level. */
    private fun withMonitorFile(
        streams: String,
        monitor: String,
    ) = RouterConfigLoader.parse(streams.trimIndent(), monitorHocon = monitor.trimIndent())

    /** The monitor block's contents, bare, as its own file carries them. */
    private val bareMonitorFile =
        """
        sources = [
            {
                select = [ { kind = 10002, tag = "r", marker = "write" } ]
                filter = { "kinds": [10002] }
            }
        ]
        """

    /** Streams on static urls, every candidate arriving through `monitor { sources }`. */
    private val pureMonitor =
        """
        streams {
            pinned {
                dir = "down"
                filter = { "kinds": [1] }
                urls = [ "wss://upstream.example" ]
            }
        }
        monitor {
            sources = [
                {
                    select = [ { kind = 10002, tag = "r", marker = "write" } ]
                    filter = { "kinds": [10002] }
                }
            ]
        }
        """

    /** The same streams and no monitor block at all. */
    private val staticOnly =
        """
        streams {
            pinned {
                dir = "down"
                filter = { "kinds": [1] }
                urls = [ "wss://upstream.example" ]
            }
        }
        """

    /** A stream that parses relay lists itself. */
    private val streamDiscovers =
        """
        streams {
            content {
                dir = "down"
                filter = { "kinds": [1] }
                relaySource = [
                    {
                        select = [ { kind = 10002, tag = "r", marker = "write" } ]
                        filter = { "kinds": [10002] }
                    }
                ]
            }
        }
        """

    @Test
    fun `a monitor block is a source, even when no stream carries one`() {
        val cfg = config(pureMonitor)
        assertTrue(cfg.discoveryStreams().isEmpty(), "the shape: nothing here is a discovery stream")
        assertNotNull(cfg.monitorSources(), "urls enter through the monitor's own declaration, so the passes run")
    }

    @Test
    fun `a static config still runs no monitor`() {
        assertNull(sourcesOf(staticOnly))
    }

    @Test
    fun `a stream that parses relay lists lends the monitor nothing`() {
        // The url set this deployment signs claims about is its own declaration. A stream never
        // contributes to it, so editing one cannot widen what we say about somebody else's relay.
        assertNull(
            sourcesOf(streamDiscovers),
            "a discovery stream used to be a monitor source by existing, which made an unrelated edit publish",
        )
    }

    @Test
    fun `the monitor declares its relay lists in its own file`() {
        val cfg = withMonitorFile(streamDiscovers, bareMonitorFile)
        val sources = assertNotNull(cfg.monitorSources(), "monitor.conf is the whole declaration; nothing else is needed")
        assertEquals(1, sources.sources.size)
        assertEquals(
            listOf(10002),
            sources.sources
                .single()
                .filter.kinds,
            "and it names the relay lists to scan itself",
        )
    }

    @Test
    fun `the monitor file carries its own clocks and excludes`() {
        val cfg =
            withMonitorFile(
                staticOnly,
                """
                sweepSeconds = 900
                fastLaneSeconds = 60
                dialConcurrency = 4
                exclude = [ "wss://noisy.example" ]
                sources = [ { select = [ { kind = 10002, tag = "r", marker = "write" } ], filter = { "kinds": [10002] } } ]
                """,
            )
        val monitor = assertNotNull(cfg.monitor)
        assertEquals(900L, monitor.sweepSeconds)
        assertEquals(60L, monitor.fastLaneSeconds)
        assertEquals(4, monitor.dialConcurrency)
        assertEquals(900L, cfg.monitorSources()!!.refreshSeconds, "the sweep is what re-reads the lists")
    }

    @Test
    fun `declaring the monitor twice is refused rather than resolved`() {
        // Two declarations cannot both be the truth, and picking one silently is how a deployment
        // measures a set nobody is looking at.
        val e =
            assertFailsWith<IllegalArgumentException> {
                withMonitorFile(pureMonitor, bareMonitorFile)
            }
        assertTrue(e.message!!.contains("Keep one"), "the message says which to delete: ${e.message}")
    }

    @Test
    fun `a monitor file wrapped in its own block name is refused`() {
        // The copy-paste out of a one-file config. Read past, it would parse to a monitor with no
        // sources — measuring nothing, quietly, which is the failure this whole rule exists for.
        val e =
            assertFailsWith<IllegalArgumentException> {
                withMonitorFile(staticOnly, "monitor {\n${bareMonitorFile.trimIndent()}\n}")
            }
        assertTrue(e.message!!.contains("unwrap"), "the message says what to do: ${e.message}")
    }

    @Test
    fun `a block that tunes the clocks and names no sources is not a declaration`() {
        // The hole the guard was written for, wearing a monitor block: `sources` is what says
        // what to measure, and one that only sets a clock never said. It would have run the
        // plane over nothing, with every row `off` and not a word on the way past.
        val cfg = withMonitorFile(streamDiscovers, "fastLaneSeconds = 60")
        assertNotNull(cfg.monitor, "the block parses — it is the declaration that is missing")
        assertNull(cfg.monitorSources())
        val e = assertFailsWith<IllegalArgumentException> { RouterConfigLoader.refuseUndeclaredMonitor(cfg) }
        assertTrue(e.message!!.contains("MONITOR_CONFIG_FILE"), "and the boot says where the declaration goes: ${e.message}")
    }

    @Test
    fun `an empty sources list is the deployment that measures nothing on purpose`() {
        val cfg = withMonitorFile(streamDiscovers, "sources = []")
        assertNotNull(cfg.monitor, "the declaration exists, so the boot has its answer")
        assertNull(cfg.monitorSources(), "and the answer is that nothing is measured")
        RouterConfigLoader.refuseUndeclaredMonitor(cfg)
    }

    @Test
    fun `a deployment that discovers and declares no monitor is refused at boot`() {
        // Parsing is fine — the config is well formed; it is the process that would run inert.
        val cfg = config(streamDiscovers)
        val e = assertFailsWith<IllegalArgumentException> { RouterConfigLoader.refuseUndeclaredMonitor(cfg) }
        assertTrue(e.message!!.contains("MONITOR_CONFIG_FILE"), "the message says where the declaration goes: ${e.message}")
        RouterConfigLoader.refuseUndeclaredMonitor(config(staticOnly))
        RouterConfigLoader.refuseUndeclaredMonitor(config(pureMonitor))
    }

    // `config.monitor?.fastLaneSeconds` is null both for no block and for the `= 0` off switch.
    @Test
    fun `a config with no monitor block still gets the default fast lane`() {
        assertEquals(
            MonitorConfig.DEFAULT_FAST_LANE_SECONDS,
            MonitorEngine.fastLaneSecondsFor(config(streamDiscovers).monitor),
            "a stream-discovering deployment has no monitor block, and lost its fast lane to that",
        )
        assertEquals(MonitorConfig.DEFAULT_FAST_LANE_SECONDS, MonitorEngine.fastLaneSecondsFor(config(staticOnly).monitor))
    }

    @Test
    fun `an operator who turned the lane off keeps it off`() {
        val off =
            config(
                """
                streams { pinned { dir = "down", filter = { "kinds": [1] }, urls = [ "wss://upstream.example" ] } }
                monitor {
                    fastLaneSeconds = 0
                    sources = [ { select = [ { kind = 10002, tag = "r", marker = "write" } ], filter = { "kinds": [10002] } } ]
                }
                """,
            )
        assertNull(
            MonitorEngine.fastLaneSecondsFor(off.monitor),
            "0 is the documented off switch; a default that overrode it would restart a lane by hand-tuning",
        )
        // The same block without the off switch, so the assertion above is about the 0 and not the block.
        val on =
            config(
                """
                streams { pinned { dir = "down", filter = { "kinds": [1] }, urls = [ "wss://upstream.example" ] } }
                monitor {
                    sources = [ { select = [ { kind = 10002, tag = "r", marker = "write" } ], filter = { "kinds": [10002] } } ]
                }
                """,
            )
        assertEquals(MonitorConfig.DEFAULT_FAST_LANE_SECONDS, MonitorEngine.fastLaneSecondsFor(on.monitor))
    }
}
