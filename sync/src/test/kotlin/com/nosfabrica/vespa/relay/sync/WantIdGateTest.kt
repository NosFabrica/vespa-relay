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

import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.utils.Hex
import java.nio.file.Files
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The predicate the router hands quartz to decline an id before the download REQ. */
class WantIdGateTest {
    private fun dir() = Files.createTempDirectory("wantid").toFile().also { it.deleteOnExit() }

    /** Real SHA-256 hex: the filter buckets on the id's leading hex, so sequential ids collide. */
    private fun id(n: Int): String = Hex.encode(MessageDigest.getInstance("SHA-256").digest("want-$n".toByteArray()))

    private val window = Filter(kinds = listOf(1), since = 1_779_000_000L, until = 1_781_000_000L)

    /** The expression the call sites use, private there; what is pinned is null when off. */
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
        // Not a lambda returning true: `NeedGate.keep` copies the batch unless wantId is null.
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
        val refused = RefusedIds(dir(), 86_400L, 10_000)
        val once = id(3)
        refused.record(once, 1_780_000_000L)

        val gate = assertNotNull(wantIdFor(refused, window))
        assertTrue(gate(once), "a first refusal is a candidate, not a verdict")
        refused.close()
    }

    @Test
    fun `the predicate is keyed on the window, because an id is all the reconcile gives us`() {
        // A reconcile names ids with no `created_at`; a window outside the id's epoch must not decline it.
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
