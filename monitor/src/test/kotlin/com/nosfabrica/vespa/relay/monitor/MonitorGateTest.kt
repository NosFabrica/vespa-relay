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
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Whether the monitor runs, and over what, asked of the whole config through the real loader. */
class MonitorGateTest {
    private fun config(text: String) = RouterConfigLoader.parse(text.trimIndent())

    /** What the mirror would hand the plane for this config. */
    private fun derivationsOf(text: String) = config(text).monitorDerivations()

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
        assertTrue(cfg.monitor?.sources?.isNotEmpty() == true, "and yet urls enter through the monitor block")
        assertTrue(
            MonitorEngine.hasMonitorSources(cfg.monitorDerivations()),
            "the passes have a corpus to work on, so the monitor has to run",
        )
    }

    @Test
    fun `a static config still runs no monitor`() {
        assertEquals(false, MonitorEngine.hasMonitorSources(derivationsOf(staticOnly)))
    }

    @Test
    fun `a stream that parses relay lists lends the monitor nothing on its own`() {
        // The url set the monitor signs claims about is declared; editing a stream never widens it.
        assertEquals(
            emptyList(),
            derivationsOf(streamDiscovers),
            "a discovery stream used to be a monitor source by existing, which made an unrelated edit publish",
        )
        assertEquals(false, MonitorEngine.hasMonitorSources(derivationsOf(streamDiscovers)))
    }

    @Test
    fun `inheritStreams names the streams the monitor measures`() {
        val named = derivationsOf(streamDiscovers.trimEnd() + "\nmonitor { inheritStreams = [\"content\"] }")
        assertEquals(listOf("stream content"), named.map { it.first })
        assertTrue(MonitorEngine.hasMonitorSources(named))
    }

    @Test
    fun `inheritStreams true takes every discovery stream, and false takes none`() {
        assertEquals(
            listOf("stream content"),
            derivationsOf(streamDiscovers.trimEnd() + "\nmonitor { inheritStreams = true }").map { it.first },
        )
        assertEquals(
            emptyList(),
            derivationsOf(streamDiscovers.trimEnd() + "\nmonitor { inheritStreams = false }"),
            "the deployment that mirrors these streams and deliberately measures nothing",
        )
    }

    @Test
    fun `an inherited stream and the block's own sources are both derivations`() {
        val both =
            derivationsOf(
                streamDiscovers.trimEnd() +
                    """

                    monitor {
                        inheritStreams = true
                        sources = [ { select = [ { kind = 10002, tag = "r", marker = "write" } ], filter = { "kinds": [10002] } } ]
                    }
                    """,
            )
        assertEquals(listOf("stream content", "monitor sources"), both.map { it.first })
    }

    @Test
    fun `inheritStreams naming a stream that does not discover is refused`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                config(streamDiscovers.trimEnd() + "\nmonitor { inheritStreams = [\"typo\"] }")
            }
        assertTrue(e.message!!.contains("typo"), "the message names what was not found: ${e.message}")
    }

    @Test
    fun `a deployment that discovers and declares no monitor is refused at boot`() {
        // Parsing is fine — the config is well formed; it is the process that would run inert.
        val cfg = config(streamDiscovers)
        val e = assertFailsWith<IllegalArgumentException> { RouterConfigLoader.refuseUndeclaredMonitor(cfg) }
        assertTrue(e.message!!.contains("inheritStreams"), "the message says how to keep the old behaviour: ${e.message}")
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
