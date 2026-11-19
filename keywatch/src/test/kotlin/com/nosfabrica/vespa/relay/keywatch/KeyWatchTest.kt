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
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The whole pass over an in-memory relay: one scenario per class, and the vouching rule. */
class KeyWatchTest {
    private val lens = Lens()
    private val policy = VouchPolicy(minVoucherRank = 50, minVouchers = 2)

    // Two well-ranked third parties, one weak one.
    private val alice = TestKey()
    private val carol = TestKey()
    private val weak = TestKey()

    private fun repost(
        who: TestKey,
        of: Event,
        at: Long,
    ) = event(who, 6, "", at, arrayOf(arrayOf("e", of.id), arrayOf("p", of.pubKey)))

    private fun run(vararg events: Event): KeyWatchResult {
        val base =
            listOf(lens.list(1), lens.card(alice, 90, 1), lens.card(carol, 70, 1), lens.card(weak, 10, 1))
        val corpus = InMemoryCorpus(base + events)
        return runBlocking { KeyWatch(corpus, KeyWatchConfig(lens.observer.hex, pageCap = 3, policy = policy)) {}.run(2_000_000_000) }
    }

    private fun KeyWatchResult.of(key: TestKey) = findings.filter { it.pubkey == key.hex }

    @Test
    fun `a published secret on a key with a past is compromised, and its later activity is reported`() {
        val victim = TestKey()
        val stranger = TestKey()
        val result =
            run(
                lens.card(victim, 95, 1),
                event(victim, 1, "gm", 1_000),
                profile(stranger, 2_000, victim.nsec),
                event(victim, 1, "still here", 3_000),
            )

        val f = result.of(victim).single()
        assertEquals(KeyClass.COMPROMISED_PUBLIC, f.keyClass)
        assertEquals(Status.PROVEN, f.status)
        assertEquals(2_000, f.since)
        assertEquals(3_000, f.activeAfter)
    }

    @Test
    fun `an unranked key needs a day of history before its leak to have been anyone's`() {
        val burner = TestKey()
        val person = TestKey()
        val stranger = TestKey()
        val result =
            run(
                // Measured shape of the bot farms: one profile, then 83 seconds later its own nsec.
                profile(burner, 1_000, "gm"),
                profile(burner, 1_083, "Mr.${burner.nsec}"),
                event(person, 1, "gm", 1_000),
                profile(stranger, 1_000 + 2 * 86_400, person.nsec),
            )

        assertEquals(KeyClass.PUBLIC_FROM_BIRTH, result.of(burner).single().keyClass)
        assertEquals(KeyClass.COMPROMISED_PUBLIC, result.of(person).single().keyClass)
    }

    @Test
    fun `a self-published key with no past, or a shared invite key, was public from birth`() {
        val bot = TestKey()
        val group = TestKey()
        val poster = TestKey()
        val result = run(profile(bot, 1_000, "Mr.${bot.nsec}"), event(poster, 1, "https://iris.to/chat#${group.nsec}", 1_000))

        assertEquals(KeyClass.PUBLIC_FROM_BIRTH, result.of(bot).single().keyClass)
        assertEquals(KeyClass.PUBLIC_FROM_BIRTH, result.of(group).single().keyClass)
    }

    @Test
    fun `a transition only two well-ranked third parties cited is validated, and names its successor`() {
        val old = TestKey()
        val new = TestKey()
        val claim = event(old, 1, "My key was compromised. My new npub is nostr:${new.npub}", 1_000)
        val result = run(claim, repost(alice, claim, 1_100), repost(carol, claim, 1_200), repost(new, claim, 1_300), repost(weak, claim, 1_400))

        val f = result.of(old).single()
        assertEquals(KeyClass.COMPROMISED_ACCESS, f.keyClass)
        assertEquals(Status.VALIDATED, f.status)
        assertEquals(new.hex, f.successor)
        val reasons =
            result.transitions
                .single()
                .vouches
                .associate { it.vouch.voucher to it.ineligible }
        assertNull(reasons[alice.hex])
        assertEquals("is one of the two keys", reasons[new.hex])
        assertEquals("rank 10 < 50", reasons[weak.hex])
    }

    @Test
    fun `the keys' own word is never enough`() {
        val old = TestKey()
        val new = TestKey()
        val claim = event(old, 1, "I moved to my new account nostr:${new.npub}", 1_000)
        // The old key, the new key and a weak account all push it; one strong voucher is not two.
        val result = run(claim, profile(new, 1_050, "me", "my old account was ${old.npub}"), repost(new, claim, 1_100), repost(weak, claim, 1_200), repost(alice, claim, 1_300))

        val f = result.of(old).single()
        assertEquals(KeyClass.RETIRED, f.keyClass)
        assertEquals(Status.CLAIMED, f.status)
        assertNull(f.successor)
        assertEquals(new.hex, f.relatedKey)
    }

    @Test
    fun `a voucher whose own secret is public does not count`() {
        val old = TestKey()
        val new = TestKey()
        val leaker = TestKey()
        val claim = event(old, 1, "I moved to my new account nostr:${new.npub}", 1_000)
        val result =
            run(
                event(alice, 1, "gm", 10),
                profile(leaker, 20, alice.nsec),
                claim,
                repost(alice, claim, 1_100),
                repost(carol, claim, 1_200),
            )

        assertEquals(Status.CLAIMED, result.of(old).single { it.keyClass == KeyClass.RETIRED }.status)
        assertEquals(
            "its own key is public",
            result.transitions
                .single()
                .vouches
                .single { it.vouch.voucher == alice.hex }
                .ineligible,
        )
    }

    @Test
    fun `an eligible warning contests the transition`() {
        val old = TestKey()
        val new = TestKey()
        val claim = event(old, 1, "I moved to my new account nostr:${new.npub}", 1_000)
        val warning = event(carol, 1, "Beware, this is a scam", 1_200, arrayOf(arrayOf("q", claim.id)))

        assertEquals(Status.CONTESTED, run(claim, repost(alice, claim, 1_100), warning).of(old).single().status)
    }

    @Test
    fun `two successors for one old key are contested unless exactly one is validated`() {
        val old = TestKey()
        val x = TestKey()
        val y = TestKey()
        val toX = event(old, 1, "I moved to my new account nostr:${x.npub}", 1_000)
        val toY = event(old, 1, "I moved to my new account nostr:${y.npub}", 1_001)

        assertTrue(run(toX, toY).transitions.all { it.status == Status.CONTESTED })

        val settled = run(toX, toY, repost(alice, toX, 1_100), repost(carol, toX, 1_200)).transitions.associate { it.transition.newKey to it.status }
        assertEquals(Status.VALIDATED, settled[x.hex])
        assertEquals(Status.CONTESTED, settled[y.hex])
    }

    @Test
    fun `a secondary is validated only when its primary names it back`() {
        val primary = TestKey()
        val alt = TestKey()
        val stray = TestKey()
        val result =
            run(
                profile(alt, 1_000, "alt", "alt account of ${primary.npub}"),
                profile(primary, 1_000, "me", "my bot: ${alt.npub}"),
                profile(stray, 1_000, "stray", "alt account of ${alice.npub}"),
            )

        assertEquals(Status.VALIDATED, result.of(alt).single().status)
        assertEquals(Status.CLAIMED, result.of(stray).single().status)
    }

    @Test
    fun `a NIP-62 request is proven vanishing`() {
        val gone = TestKey()
        val f = run(event(gone, 62, "bye", 1_000, arrayOf(arrayOf("relay", "ALL_RELAYS")))).of(gone).single()
        assertEquals(KeyClass.VANISHED, f.keyClass)
        assertEquals(Status.PROVEN, f.status)
    }

    @Test
    fun `the report carries no secret`() {
        val victim = TestKey()
        val bot = TestKey()
        val result = run(lens.card(victim, 95, 1), event(victim, 1, "gm", 10), profile(TestKey(), 20, victim.nsec), profile(bot, 30, bot.nsec))
        val json = Report.toJson(result, "wss://test", lens.observer.hex, policy).toString()

        assertTrue(victim.hex in json)
        for (key in listOf(victim, bot)) {
            assertFalse(key.nsec in json, "an nsec leaked into the report")
            assertFalse(key.secretHex in json, "a secret leaked into the report")
        }
        assertFalse("nsec1" in json)
    }
}
