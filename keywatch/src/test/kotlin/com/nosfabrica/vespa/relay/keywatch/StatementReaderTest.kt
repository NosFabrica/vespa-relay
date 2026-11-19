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

    // The cases below are real phrasings from the first production report's 1,809 claims.

    @Test
    fun `migration words that do not lead into the key are no claim`() {
        for (text in listOf(
            "Most of my signal conversations have moved to nostr:${other.npub}",
            "now I switched to using Amber to sign in and Fountain is just spinning. Apparently, nostr:${other.npub}",
            "new account i am excited about on nostr, go follow ${other.npub}",
            "I moved to WordPress/Woocommerce. BTW: static HTML is cool, editable in browser, no superskills needed. check out nostr:${other.npub}",
            "nostr:${other.npub} thanks! I moved to my new account yesterday",
        )) {
            assertTrue(StatementReader.read(event(me, 1, text, 1)).isEmpty(), text)
        }
    }

    @Test
    fun `a newly created account is a secondary, not a successor`() {
        val text = "I just created a new npub / brand called, nostr:${other.npub}"
        assertIs<SecondaryClaim>(StatementReader.read(event(me, 1, text, 1)).single())
    }

    @Test
    fun `a profile calling itself the old account names its successor`() {
        for (about in listOf("Old account. New one at nostr:${other.npub}", "OLD ACCOUNT go to nostr:${other.npub}", "Old account of nostr:${other.npub}", "This is my old account. I'm now on mstdn.social: nostr:${other.npub}")) {
            val claim = assertIs<TransitionClaim>(StatementReader.read(profile(me, 1, "me", about)).single(), about)
            assertEquals(me.hex, claim.oldKey, about)
            assertEquals(Signer.OLD_KEY, claim.signedBy, about)
        }
        val newKey = assertIs<TransitionClaim>(StatementReader.read(profile(me, 1, "me", "i fixed my handle\n\nold account at nostr:${other.npub}")).single())
        assertEquals(Signer.NEW_KEY, newKey.signedBy)
    }

    @Test
    fun `the claims that were real still read`() {
        val compromised = assertIs<TransitionClaim>(StatementReader.read(event(me, 1, "THIS ACCOUNT HAS BEEN COMPROMISED Please unfollow this account and follow our new account nostr:${other.npub}", 1)).single())
        assertEquals(Reason.COMPROMISE, compromised.reason)
        assertEquals(Signer.OLD_KEY, compromised.signedBy)
        assertIs<TransitionClaim>(StatementReader.read(event(me, 1, "I'm migrating to new keys. Here's my new shiny npub: nostr:${other.npub}", 1)).single())
        assertIs<TransitionClaim>(StatementReader.read(profile(me, 1, "me", "this account will be deleted in 2 weeks my new account can be found at nostr:${other.npub}")).single())
    }

    @Test
    fun `a footer advertising an account is no claim, and a far-off word does not set the reason`() {
        val bulletin = "Use the recent chaos as a chance to review all your security protocols. Follow us on NOSTR: ${other.npub}"
        assertTrue(StatementReader.read(event(me, 1, bulletin, 1)).isEmpty())
        assertIs<TransitionClaim>(StatementReader.read(profile(me, 1, "me", "follow me on nostr: ${other.npub}")).single())

        val far = "A compromised DevOps laptop drained an exchange. " + "Market notes. ".repeat(40) + "I have moved to nostr:${other.npub}"
        assertEquals(Reason.ROTATION, assertIs<TransitionClaim>(StatementReader.read(event(me, 1, far, 1)).single()).reason)
    }
}
