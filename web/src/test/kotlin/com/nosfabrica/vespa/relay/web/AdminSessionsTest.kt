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
package com.nosfabrica.vespa.relay.web

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

/**
 * The sessions one signature opens. A bug here hands somebody a document that names what this
 * relay's users searched for, so each case is written in the direction that fails open.
 */
class AdminSessionsTest {
    private var clock = 1_000_000L
    private val admin = "a".repeat(64)

    private fun sessions(
        ttl: Long = 60_000,
        max: Int = 4,
    ) = AdminSessions(ttlMillis = ttl, max = max, now = { clock })

    @Test
    fun `a session is whoever opened it, until it is not`() {
        val s = sessions(ttl = 60_000)
        val token = s.open(admin)

        assertEquals(admin, s.holder(token))

        // Fixed expiry, not sliding: reading does not renew.
        clock += 59_000
        assertEquals(admin, s.holder(token))
        clock += 2_000
        assertNull(s.holder(token), "an expired session must not still answer")
    }

    @Test
    fun `an unknown token is nobody`() {
        val s = sessions()
        s.open(admin)

        // Anything that is not a token we minted must resolve to no one, including the shapes a
        // probe would try.
        assertNull(s.holder(null))
        assertNull(s.holder(""))
        assertNull(s.holder("not-a-token"))
        assertNull(s.holder(" "))
    }

    @Test
    fun `two sessions are two different tokens`() {
        val s = sessions()

        // Reusing a token across sign-ins would make logout meaningless.
        assertNotEquals(s.open(admin), s.open(admin))
    }

    @Test
    fun `closing one session leaves the others alone`() {
        val s = sessions()
        val first = s.open(admin)
        val second = s.open(admin)

        s.close(first)

        assertNull(s.holder(first))
        assertEquals(admin, s.holder(second), "one browser signing out must not sign out the others")
        // A logout for a token that was never live is a no-op, never a crash.
        s.close("never-existed")
        s.close(null)
    }

    @Test
    fun `the live set is bounded, oldest first`() {
        val s = sessions(max = 3)
        val a = s.open(admin)
        val b = s.open(admin)
        s.open(admin)
        // Touch `b` so the eviction has a chance to pick the wrong one.
        s.holder(b)

        val d = s.open(admin)

        assertEquals(3, s.size(), "a signer opening sessions in a loop must not grow this without bound")
        assertNull(s.holder(a), "the least recently used session is the one that goes")
        assertEquals(admin, s.holder(b))
        assertEquals(admin, s.holder(d))
    }

    @Test
    fun `expired sessions stop counting against the bound`() {
        val s = sessions(ttl = 10_000, max = 4)
        repeat(4) { s.open(admin) }

        clock += 11_000

        // Otherwise a burst of sign-ins an hour ago would evict the session somebody is using now.
        assertEquals(0, s.size())
        assertEquals(admin, s.holder(s.open(admin)))
    }
}
