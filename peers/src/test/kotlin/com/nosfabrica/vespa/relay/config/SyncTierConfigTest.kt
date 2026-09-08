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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Age-banded schedules as they are written in the config. */
class SyncTierConfigTest {
    private fun conf(body: String) =
        """
        streams {
          profiles {
            dir = "down"
            filter = { "kinds": [0] }
            urls = [ "wss://relay.example" ]
            $body
          }
        }
        """.trimIndent()

    private val banded =
        """
        negentropy = [
          { maxAge = 2592000,  every = 604800   }
          { maxAge = 31536000, every = 2592000  }
          { every = 31536000 }
        ]
        """.trimIndent()

    @Test
    fun `the schedule the structure was asked for`() {
        val stream = RouterConfigLoader.parse(conf(banded)).streams.single()

        assertEquals(3, stream.negentropyTiers.size)
        assertEquals(listOf(2592000L, 31536000L, null), stream.negentropyTiers.map { it.maxAgeSeconds })
        assertEquals(listOf(604800L, 2592000L, 31536000L), stream.negentropyTiers.map { it.everySeconds })
        assertEquals(stream.negentropyTiers, stream.negentropySchedule)
    }

    @Test
    fun `fetching takes the same shape`() {
        val stream =
            RouterConfigLoader
                .parse(
                    conf(
                        """
                        refetch = [
                          { maxAge = 2592000, every = 2592000 }
                          { every = 31536000 }
                        ]
                        """.trimIndent(),
                    ),
                ).streams
                .single()

        assertEquals(listOf(2592000L, null), stream.refetchTiers.map { it.maxAgeSeconds })
        assertEquals(listOf(2592000L, 31536000L), stream.refetchTiers.map { it.everySeconds })
    }

    @Test
    fun `a bare period still resolves to one band over the whole past`() {
        val stream = RouterConfigLoader.parse(conf("negentropySyncThePastSeconds = 604800")).streams.single()

        assertEquals(listOf(SyncTier(maxAgeSeconds = null, everySeconds = 604800L)), stream.negentropySchedule)
        assertTrue(SyncTier.isUnbanded(stream.negentropySchedule), "so its keys stay the ones it has always used")
        assertEquals("", SyncTier.bandIdOf(stream.negentropySchedule, stream.negentropySchedule[0]))
    }

    @Test
    fun `a zero cadence is a band re-read as of today`() {
        // What `assertions` needs: the upstream is the source of truth, so its past is never
        // taken on trust. `attemptSpacingSeconds` still floors the retry at 15 minutes.
        val stream = RouterConfigLoader.parse(conf("negentropy = [ { every = 0 } ]")).streams.single()

        assertEquals(0L, stream.negentropySchedule.single().everySeconds)
    }

    // ---- the refusals ------------------------------------------------------

    @Test
    fun `a list beside its scalar is refused, not merged`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                RouterConfigLoader.parse(conf("$banded\nnegentropySyncThePastSeconds = 604800"))
            }
        assertTrue(e.message!!.contains("same schedule at different resolutions"), e.message!!)
    }

    @Test
    fun `bands out of order are refused`() {
        // Reversed, the bands would overlap and leave a hole, and a hole in the middle of the
        // past is invisible: every band still reports itself verified.
        val e =
            assertFailsWith<IllegalArgumentException> {
                RouterConfigLoader.parse(
                    conf(
                        """
                        negentropy = [
                          { maxAge = 31536000, every = 2592000 }
                          { maxAge = 2592000,  every = 604800  }
                        ]
                        """.trimIndent(),
                    ),
                )
            }
        assertTrue(e.message!!.contains("youngest-first"), e.message!!)
    }

    @Test
    fun `only the last band may reach the corpus floor`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                RouterConfigLoader.parse(
                    conf(
                        """
                        negentropy = [
                          { every = 604800 }
                          { maxAge = 31536000, every = 2592000 }
                        ]
                        """.trimIndent(),
                    ),
                )
            }
        assertTrue(e.message!!.contains("only the"), e.message!!)
    }

    @Test
    fun `a band with no cadence is refused`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                RouterConfigLoader.parse(conf("negentropy = [ { maxAge = 2592000 } ]"))
            }
        assertTrue(e.message!!.contains("no `every`"), e.message!!)
    }

    @Test
    fun `an empty list is refused rather than read as nothing scheduled`() {
        val e = assertFailsWith<IllegalArgumentException> { RouterConfigLoader.parse(conf("negentropy = []")) }
        assertTrue(e.message!!.contains("empty"), e.message!!)
    }
}
