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
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * **DOES THE MONITOR RUN?** — one question, asked of the config, and for a
 * while it was asked of the wrong half of it.
 *
 * `SyncEngine.start` gated `aliasMonitor.start()` on `discoveryStreams`, which
 * is the streams carrying a `relaySource` of their own. That was the whole
 * story when a stream was the only way a url could enter the system. The
 * `monitor { sources }` block is the other way, and it is not a corner: the
 * block's own KDoc offers moving every ounce of relay-list parsing off the
 * streams, and `StreamWorld` unions both sides precisely so a deployment can.
 *
 * Take that offer all the way — static `urls` on the streams, every url
 * arriving through the monitor block — and `discoveryStreams` is empty while
 * the block names three sources. The urls were derived correctly and then
 * nothing ran over them: no fold, no stability gate, no fitness, not one
 * `prime` ever signed, and four rows on the monitor card reading `off` for the
 * life of the process. Nothing said so, because `off` is also what those rows
 * correctly say on the static config this gate was written for.
 *
 * The rule is over the config so the deployment that broke can be handed to it
 * whole, through the REAL loader: what counts as a source is the loader's
 * decision, and a test that builds a `RouterConfig` by hand cannot fail when
 * that decision changes.
 */
class MonitorGateTest {
    private fun config(text: String) = RouterConfigLoader.parse(text.trimIndent())

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

    /** The same streams, and no monitor block at all — the static config the old gate was for. */
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

    /** A stream that parses relay lists itself, which is what the old gate looked for. */
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
            MonitorEngine.hasMonitorSources(cfg),
            "the passes have a corpus to work on, so the monitor has to run",
        )
    }

    @Test
    fun `a static config still runs no monitor`() {
        // The other direction, and the reason the gate exists at all: a config
        // naming its upstreams by hand has no duplicate urls to find, and the
        // rows saying `off` there is the honest report rather than a fault.
        assertEquals(false, MonitorEngine.hasMonitorSources(config(staticOnly)))
    }

    @Test
    fun `a stream that parses relay lists is a source on its own`() {
        // Unchanged by the fix, and asserted so the widening cannot be mistaken
        // for a replacement: this is the deployment the old rule was right for.
        assertTrue(MonitorEngine.hasMonitorSources(config(streamDiscovers)))
    }

    /**
     * **A LANE THAT IS OFF AND A LANE NOBODY CONFIGURED READ THE SAME**, and
     * they are opposite intentions. `config.monitor?.fastLaneSeconds` is null
     * for both, and the engine used to carry that null straight out — so the
     * deployment above, which is a supported shape and has no `monitor` block,
     * ran no fast lane at all and made a new relay wait a whole `sweepSeconds`
     * for its first `prime`. Its two neighbours, `sweepSeconds` and
     * `dialConcurrency`, both defaulted on that path; only this one did not.
     *
     * The other direction is the reason this cannot simply be `?: DEFAULT`:
     * `fastLaneSeconds = 0` is the documented off switch and the loader maps it
     * to null, so a blanket fallback would restart a lane an operator stopped.
     */
    @Test
    fun `a config with no monitor block still gets the default fast lane`() {
        assertEquals(
            MonitorConfig.DEFAULT_FAST_LANE_SECONDS,
            MonitorEngine.fastLaneSecondsFor(config(streamDiscovers)),
            "a stream-discovering deployment has no monitor block, and lost its fast lane to that",
        )
        assertEquals(MonitorConfig.DEFAULT_FAST_LANE_SECONDS, MonitorEngine.fastLaneSecondsFor(config(staticOnly)))
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
            MonitorEngine.fastLaneSecondsFor(off),
            "0 is the documented off switch; a default that overrode it would restart a lane by hand-tuning",
        )
        // …and the same block WITHOUT the off switch takes the default, so the
        // assertion above is about the 0 rather than about the block.
        val on =
            config(
                """
                streams { pinned { dir = "down", filter = { "kinds": [1] }, urls = [ "wss://upstream.example" ] } }
                monitor {
                    sources = [ { select = [ { kind = 10002, tag = "r", marker = "write" } ], filter = { "kinds": [10002] } } ]
                }
                """,
            )
        assertEquals(MonitorConfig.DEFAULT_FAST_LANE_SECONDS, MonitorEngine.fastLaneSecondsFor(on))
    }
}
