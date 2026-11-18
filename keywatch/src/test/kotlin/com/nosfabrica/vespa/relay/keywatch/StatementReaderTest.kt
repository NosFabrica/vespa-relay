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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class StatementReaderTest {
    private val me = TestKey()
    private val other = TestKey()

    @Test
    fun `the old key naming its successor is an outgoing claim`() {
        val claim = assertIs<TransitionClaim>(StatementReader.read(event(me, 1, "My nsec leaked. Moving to my new npub: nostr:${other.npub}", 1)).single())

        assertEquals(me.hex, claim.oldKey)
        assertEquals(other.hex, claim.newKey)
        assertEquals(Signer.OLD_KEY, claim.signedBy)
        assertEquals(Reason.COMPROMISE, claim.reason)
    }

    @Test
    fun `a new key naming the old one is an incoming claim`() {
        val claim = assertIs<TransitionClaim>(StatementReader.read(profile(me, 1, "jordan", "my old account was ${other.npub}")).single())

        assertEquals(other.hex, claim.oldKey)
        assertEquals(me.hex, claim.newKey)
        assertEquals(Signer.NEW_KEY, claim.signedBy)
        assertEquals(Reason.ROTATION, claim.reason)
    }

    @Test
    fun `reasons are told apart`() {
        assertEquals(Reason.PRECAUTION, StatementReader.reasonOf("my nsec may have been leaked, switching just in case"))
        assertEquals(Reason.LOST, StatementReader.reasonOf("I lost my nsec"))
        assertEquals(Reason.COMPROMISE, StatementReader.reasonOf("account was hacked"))
    }

    @Test
    fun `an alt names its primary`() {
        val claim = assertIs<SecondaryClaim>(StatementReader.read(profile(me, 1, "me (alt)", "alt account of ${other.npub}")).single())
        assertEquals(me.hex, claim.secondary)
        assertEquals(other.hex, claim.primary)
    }

    @Test
    fun `a note recommending someone else is no claim`() {
        assertTrue(StatementReader.read(event(me, 1, "Go follow nostr:${other.npub}, great posts", 1)).isEmpty())
        // Migration wording, but not about the speaker: no first person near the key.
        assertTrue(StatementReader.read(event(me, 1, "Bitcoin is moving to new highs. nostr:${other.npub}", 1)).isEmpty())
    }

    @Test
    fun `a third party naming two keys is a statement, a warning one is against`() {
        val a = TestKey()
        val b = TestKey()
        val vouch = assertIs<ThirdPartyStatement>(StatementReader.read(event(me, 1, "${a.npub}'s old npub is compromised, follow ${b.npub}", 1)).single())
        assertEquals(setOf(a.hex, b.hex), vouch.keys)
        assertTrue(!vouch.against)

        val warning = assertIs<ThirdPartyStatement>(StatementReader.read(event(me, 1, "${b.npub} is a scam impersonating ${a.npub}, their old key is fine", 1)).single())
        assertTrue(warning.against)
    }
}
