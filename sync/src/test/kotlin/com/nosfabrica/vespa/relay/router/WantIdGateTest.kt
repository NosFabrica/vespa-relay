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
package com.nosfabrica.vespa.relay.router

import com.nosfabrica.vespa.relay.router.refused.RefusedIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.utils.Hex
import java.nio.file.Files
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The predicate the router hands quartz to decline an id before the download
 * REQ — the one hook on the negentropy path that saves the bytes rather than
 * the CPU.
 *
 * Two properties, and the first is the one that would rot quietly. Quartz's
 * `NeedGate` hands a batch straight back when the predicate is ABSENT — same
 * list instance, no copy — so passing a lambda that always answers true is not
 * equivalent to passing none. It is the same work plus an allocation per batch,
 * forever, for every deployment that never opted in.
 */
class WantIdGateTest {
    private fun dir() = Files.createTempDirectory("wantid").toFile().also { it.deleteOnExit() }

    /**
     * Real SHA-256 hex, not `"%064x".format(n)`.
     *
     * The filter takes its bucket from the id's first 16 hex characters and its
     * fingerprint from the next 8, with no hashing of its own — an event id is
     * already a uniform hash. Sequential ids are all zeros across both of those
     * slices, so `...0001` and `...0002` land in the same bucket with the same
     * fingerprint and every one of them is a "hit". That is a property of the
     * test data, not of the filter, and it is why `CuckooFilterTest` feeds it
     * real digests too.
     */
    private fun id(n: Int): String = Hex.encode(MessageDigest.getInstance("SHA-256").digest("want-$n".toByteArray()))

    private val window = Filter(kinds = listOf(1), since = 1_779_000_000L, until = 1_781_000_000L)

    /**
     * The same expression the three call sites use. Kept here rather than
     * reached into: they are private by design, and what is being pinned is the
     * SHAPE — null when off, a real predicate when on.
     */
    private fun wantIdFor(
        refused: RefusedIds,
        window: Filter,
    ): ((String) -> Boolean)? =
        if (!refused.enabled) {
            null
        } else {
            { candidate -> !refused.suppressedInWindow(candidate, window.since, window.until) }
        }

    @Test
    fun `suppression off hands quartz no predicate at all`() {
        // NOT a lambda returning true. `NeedGate.keep` returns the batch
        // unchanged and uncopied only when wantId is null, so a
        // trivially-permissive predicate would cost every un-opted-in
        // deployment a filter pass and an ArrayList per reconcile batch and
        // save nothing.
        assertNull(wantIdFor(RefusedIds.disabled(), window))
    }

    @Test
    fun `suppression on hands quartz a predicate that declines twice-refused ids`() {
        val refused = RefusedIds(dir(), 86_400L, 10_000)
        val stale = id(1)
        val wanted = id(2)
        val at = 1_780_000_000L
        refused.record(stale, at)
        refused.record(stale, at)

        val gate = assertNotNull(wantIdFor(refused, window), "an enabled filter must produce a predicate")
        assertFalse(gate(stale), "a twice-refused id must never reach a REQ")
        assertTrue(gate(wanted), "everything else still has to be fetched")
        refused.close()
    }

    @Test
    fun `one refusal is not enough to decline an id`() {
        // The two-refusal gate, seen from the wire side: a single sighting must
        // still be downloadable, or one false positive upstream of here would
        // cost an event permanently.
        val refused = RefusedIds(dir(), 86_400L, 10_000)
        val once = id(3)
        refused.record(once, 1_780_000_000L)

        val gate = assertNotNull(wantIdFor(refused, window))
        assertTrue(gate(once), "a first refusal is a candidate, not a verdict")
        refused.close()
    }

    @Test
    fun `the predicate is keyed on the window, because an id is all the reconcile gives us`() {
        // A reconcile names ids; no body has arrived, so there is no
        // `created_at` to key on. A window that does not cover the id's epoch
        // must therefore not decline it — being wrong in that direction skips
        // an event we wanted.
        val refused = RefusedIds(dir(), 86_400L, 10_000)
        val stale = id(4)
        val at = 1_780_000_000L
        refused.record(stale, at)
        refused.record(stale, at)

        val covering = assertNotNull(wantIdFor(refused, window))
        assertFalse(covering(stale))

        val elsewhere = Filter(kinds = listOf(1), since = 1_700_000_000L, until = 1_700_086_400L)
        val gate = assertNotNull(wantIdFor(refused, elsewhere))
        assertTrue(gate(stale), "an id suppressed in another epoch is not suppressed in this window")
        refused.close()
    }
}
