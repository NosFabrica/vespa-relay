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
package com.nosfabrica.vespa.relay.fold

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The probe does not verify event ids, so a relay chooses every id it serves. FingerprintIndex
 * chains on the id's FIRST 64-bit word only, so ids that agree on it all land in one chain and
 * every lookup walks every entry of every head: quadratic in the host's whole id count, where the
 * pairwise merge it replaced was linear per pair. learn() never suspends, so this freezes the
 * group's coroutine, the pass's awaitAll, and with it AliasMonitor's one scheduler.
 *
 * Fails (times out the bound) if the finding is real; passes once the index keys on the whole id.
 */
class FingerprintIndexCollisionTest {
    private fun hex(
        random: Random,
        words: Int,
    ) = (1..words).joinToString("") { "%016x".format(random.nextLong()) }

    private fun timeLearn(firstWord: (Random) -> String): Long {
        val random = Random(7)
        val paths = 200
        val group = (0 until paths).map { RelayUrlNormalizer.normalize("wss://filter.example/p$it") }
        // Each path its own window (no two share an id), so none folds and every one becomes a head.
        val prints = group.associateWith { Fingerprint.of((0 until RelayAliases.DEFAULT_PROBE_TARGET).map { firstWord(random) + hex(random, 3) }) }
        val view = RelayAliases()
        val started = System.nanoTime()
        val learned = view.learn(group, view.toProbe(group).first(), prints)
        val tookMs = (System.nanoTime() - started) / 1_000_000
        assertEquals(group.toSet(), learned.distinct)
        return tookMs
    }

    @Test
    fun `ids sharing a first word cost what random ids cost`() {
        val random = timeLearn { hex(it, 1) }
        // A relay serving made-up ids that all start with the same 16 hex digits.
        val colliding = timeLearn { "0000000000000000" }
        assertTrue(colliding < 2_000 && colliding < 20 * maxOf(random, 50), "colliding first words took ${colliding}ms, random ids ${random}ms")
    }
}
