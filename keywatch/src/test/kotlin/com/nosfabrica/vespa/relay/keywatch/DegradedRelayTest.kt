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
package com.nosfabrica.vespa.relay.keywatch

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * search.brainstorm.world while part of its index is unreachable (measured 2026-11-18): COUNT is
 * refused, a REQ answers from what it can reach with a plain EOSE. An absence read then is not one.
 */
class DegradedRelayTest {
    private val lens = Lens()

    /** Goes partial on the first fetch [trigger] matches, and heals after [healAfter] health checks (never, if null). */
    private class Degrading(
        private val inner: Corpus,
        private val trigger: (Filter) -> Boolean,
        private var healAfter: Int?,
    ) : Corpus {
        var degraded = false
        private var triggered = false

        override suspend fun fetch(filters: List<Filter>): List<Event> {
            if (!triggered && filters.any(trigger)) {
                triggered = true
                degraded = true
            }
            // The production relay's REQ: whatever the reachable part holds, here nothing, and no error.
            return if (degraded) emptyList() else inner.fetch(filters)
        }

        override suspend fun count(filter: Filter): Int? = if (degraded) throw RelayDegraded("COUNT refused") else inner.count(filter)

        override suspend fun healthy(): Boolean {
            if (!degraded) return true
            val left = healAfter ?: return false
            healAfter = left - 1
            if (left - 1 <= 0) degraded = false
            return false
        }
    }

    private fun config() = KeyWatchConfig(lens.observer.hex, pageCap = 4, degradedPauseMs = 1, maxDegradedWaitMs = 50, phaseAttempts = 3)

    /** An unranked victim, so its history is asked for: one note, then its secret in a stranger's profile. */
    private fun corpusWithVictim(victim: TestKey): InMemoryCorpus = InMemoryCorpus(listOf(lens.list(1), event(victim, 1, "gm", 1_000), profile(TestKey(), 2_000, victim.nsec)))

    private val historyCheck: (Filter) -> Boolean = { it.until != null && it.authors?.size == 1 && it.limit == 1 }

    @Test
    fun `a history read from a partial index is redone, and the victim stays compromised`() {
        val victim = TestKey()
        val corpus = Degrading(corpusWithVictim(victim), historyCheck, healAfter = 2)
        val result = runBlocking { KeyWatch(corpus, config()) {}.run(2_000_000_000) }

        // Read during the outage, the victim had "no history" and would have been public from birth.
        assertEquals(KeyClass.COMPROMISED_PUBLIC, result.findings.single { it.pubkey == victim.hex }.keyClass)
    }

    @Test
    fun `a relay that never heals ends the pass without a result`() {
        val victim = TestKey()
        val corpus = Degrading(corpusWithVictim(victim), historyCheck, healAfter = null)
        val failure = assertFailsWith<IllegalStateException> { runBlocking { KeyWatch(corpus, config()) {}.run(2_000_000_000) } }
        assertTrue("no report written" in failure.message.orEmpty())
    }
}
