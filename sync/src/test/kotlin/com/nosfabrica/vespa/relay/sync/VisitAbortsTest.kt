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
package com.nosfabrica.vespa.relay.sync

import com.nosfabrica.vespa.relay.progress.StatusVocabulary
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The abort instrument: that the partition closes, that the line names what an
 * operator needs, and that every counter it publishes has a definition.
 */
class VisitAbortsTest {
    private val url = RelayUrlNormalizer.normalize("wss://relay.nostr.build")

    private fun aborts(
        resayAfterMs: Long = VisitAborts.DEFAULT_RESAY_AFTER_MS,
        clock: () -> Long,
    ) = VisitAborts(resayAfterMs, clock)

    @Test
    fun `the reasons partition the total, exactly`() {
        // THE PROPERTY THE ROW IS READ BY. `abortedVisits` is the number that
        // says whether the resync converges, and the split is the only thing
        // that says what would fix it — so a reason that did not sum back into
        // the total would be a card reporting an arithmetic error against a
        // pool that is working, which is the failure the fold's `undecided`
        // rows already taught this repository to avoid.
        val a = VisitAborts()
        a.record("content", url, VisitAborts.Reason.AUTH_REQUIRED, "139 kinds", null)
        a.record("content", url, VisitAborts.Reason.AUTH_REQUIRED, "139 kinds", null)
        a.record("indexers", url, VisitAborts.Reason.CLOSED, "kinds 30166", null)

        val counts = a.counts().associate { it.name to it.value }
        assertEquals(3L, counts["abortedVisits"])
        assertEquals(2L, counts["abortedAuthRequired"])
        assertEquals(1L, counts["abortedClosed"])
        assertEquals(
            counts["abortedVisits"],
            VisitAborts.Reason.entries.sumOf { counts.getValue(it.count) },
            "the reasons must add up to the total they are a split of",
        )
        // Every reason is published even at zero: a wall this deployment has
        // never met is a fact about it, and an absent row reads as a counter
        // somebody forgot to wire.
        assertEquals(VisitAborts.Reason.entries.size + 1, counts.size)
    }

    @Test
    fun `every abort counter the row publishes has a definition`() {
        // The same rule StatusVocabularyTest holds for the document as a whole,
        // held here at the source: these names are chosen in an enum, so the
        // glossary can rot without the document changing shape.
        for (reason in VisitAborts.Reason.entries) {
            assertTrue(
                StatusVocabulary.TERMS.containsKey(reason.count),
                "${reason.count} is published with no term, so a reader needs this source to read it",
            )
        }
    }

    @Test
    fun `the line names the relay, the stream, the ask and what the relay said`() {
        // The whole complaint in one assertion: the `!clean` path used to
        // return naming NONE of these, and it is ~90% of every abort counted.
        val said =
            VisitAborts().record(
                "contentViaOutbox",
                url,
                VisitAborts.Reason.CLOSED,
                asked = VisitAborts.asked(Filter(kinds = (1..139).toList())),
                said = "error: too many kinds in filter",
            )
        assertNotNull(said)
        assertTrue("contentViaOutbox" in said)
        assertTrue(url.url in said)
        assertTrue("139 kinds" in said, "the WIDTH, not 139 numbers: $said")
        assertTrue("too many kinds" in said, "what the relay said for itself")
    }

    @Test
    fun `one line per pair per re-say window, and the counting never stops`() {
        // A relay that is refusing is refusing on every visit, so at one line
        // per abort this deployment would write four a second and bury the
        // health line beside it. The counters are the continuous record; the
        // lines are the index into them.
        var nowMs = 1_000L
        val a = aborts(resayAfterMs = 30_000) { nowMs }
        assertNotNull(a.record("content", url, VisitAborts.Reason.CLOSED, "139 kinds", null), "the first sighting is always said")
        assertNull(a.record("content", url, VisitAborts.Reason.CLOSED, "139 kinds", null))
        // A DIFFERENT reason on the same relay is a different finding and is
        // said at once — the window is per (stream, relay, reason), not per
        // relay, because a relay that starts refusing on auth after refusing on
        // width is news.
        assertNotNull(a.record("content", url, VisitAborts.Reason.AUTH_REQUIRED, "139 kinds", null))
        assertNotNull(a.record("indexers", url, VisitAborts.Reason.CLOSED, "kinds 30166", null))

        nowMs += 30_000
        assertNotNull(a.record("content", url, VisitAborts.Reason.CLOSED, "139 kinds", null), "and again once the window is up")

        // Silence is never lost accounting: everything above is counted
        // whether or not it was spoken.
        assertEquals(5L, a.counts().first { it.name == "abortedVisits" }.value)
    }

    @Test
    fun `a clean visit forgets the wall the last one met`() {
        // The row is about where a pair stands NOW. Without this it never
        // stopped being about where it once stood: a pair that met a transient
        // refusal at boot and has written no band since — a relay that is
        // simply empty for this filter is the ordinary case — read `refused`
        // with a stale sentence at the top of a worst-first table for the life
        // of the process.
        val a = VisitAborts()
        a.record("content", url, VisitAborts.Reason.UNREACHABLE, "139 kinds", null)
        assertNotNull(a.last("content", url))

        a.cleared("content", url)
        assertNull(a.last("content", url), "the visit came back clean, so there is no wall to report")
        // THE COUNTERS ARE THE LIFETIME RECORD and are untouched: the abort
        // happened, and a row going quiet must not un-count it.
        assertEquals(1L, a.counts().first { it.name == "abortedVisits" }.value)
        assertEquals(1L, a.counts().first { it.name == "abortedUnreachable" }.value)
        // …and one stream clearing does not speak for another's.
        a.record("indexers", url, VisitAborts.Reason.CLOSED, "kinds 30166", null)
        a.cleared("content", url)
        assertNotNull(a.last("indexers", url), "the unit is the pair; clearing one is not clearing the relay")
    }

    @Test
    fun `each walk ending quartz can refuse with maps to a reason of its own`() {
        // Distinct, because a lump is what `abortedVisits` already was: an auth
        // wall wants a key the relay accepts, a CLOSED wants its own sentence
        // read, and silence wants neither. `DRAINED` and `LIMIT_REACHED` are
        // not refusals and never reach here — see VisitPool.refusedOutright,
        // whose own test pins that.
        val refusals =
            listOf(
                PagedFetchResult.End.AUTH_REQUIRED,
                PagedFetchResult.End.CLOSED,
                PagedFetchResult.End.IDLE,
                PagedFetchResult.End.CANNOT_CONNECT,
                PagedFetchResult.End.UNPAGEABLE,
            )
        assertEquals(refusals.size, refusals.map { VisitAborts.of(it) }.toSet().size, "one reason each, none folded together")
        assertEquals(VisitAborts.Reason.AUTH_REQUIRED, VisitAborts.of(PagedFetchResult.End.AUTH_REQUIRED))
    }

    @Test
    fun `a narrow ask is spelled out, a wide one is measured`() {
        assertEquals("kinds 0,3", VisitAborts.asked(Filter(kinds = listOf(0, 3))))
        assertEquals("139 kinds", VisitAborts.asked(Filter(kinds = (1..139).toList())))
        assertEquals(
            "kinds 30382, 1 author(s), since 1700000000",
            VisitAborts.asked(Filter(kinds = listOf(30382), authors = listOf("a".repeat(64)), since = 1_700_000_000)),
        )
        // An ask that names nothing at all still reads as something.
        assertEquals("everything", VisitAborts.asked(Filter()))
    }
}
