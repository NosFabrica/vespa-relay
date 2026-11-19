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
import kotlinx.serialization.json.JsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A pass that builds on the report of the one before: reads only what arrived since, keeps what
 * cannot change, decides again what can, and says what changed. Every prior goes through the
 * report's JSON, the way production hands one pass to the next.
 */
class IncrementalPassTest {
    private val lens = Lens()
    private val policy = VouchPolicy(minVoucherRank = 50, minVouchers = 2)
    private val alice = TestKey()
    private val carol = TestKey()
    private val relay = "wss://test"

    private val day = 86_400L
    private val firstPass = 100 * day
    private val secondPass = firstPass + 10 * day

    private val victim = TestKey()
    private val old = TestKey()
    private val new = TestKey()
    private val claim = event(old, 1, "I moved to my new account nostr:${new.npub}", 20 * day)

    /** What the relay holds at the first pass. */
    private val before: List<Event> =
        listOf(
            lens.list(1),
            lens.card(alice, 90, 1),
            lens.card(carol, 70, 1),
            lens.card(victim, 95, 1),
            event(victim, 1, "gm", 1 * day),
            profile(TestKey(), 10 * day, victim.nsec),
            event(victim, 1, "still here", 50 * day),
            claim,
            repost(alice, claim, 30 * day),
        )

    private fun repost(
        who: TestKey,
        of: Event,
        at: Long,
    ) = event(who, 6, "", at, arrayOf(arrayOf("e", of.id), arrayOf("p", of.pubKey)))

    private fun pass(
        events: List<Event>,
        now: Long,
        prior: PriorReport? = null,
        policy: VouchPolicy = this.policy,
    ) = runBlocking { KeyWatch(InMemoryCorpus(events), KeyWatchConfig(lens.observer.hex, pageCap = 3, policy = policy)) {}.run(now, prior) }

    private fun handOver(
        result: KeyWatchResult,
        now: Long,
        policy: VouchPolicy = this.policy,
    ) = assertNotNull(Report.read(Report.toJson(result, relay, lens.observer.hex, policy), relay, lens.observer.hex, now, 30 * day) {})

    private fun KeyWatchResult.status(of: TestKey) = findings.single { it.pubkey == of.hex && it.basis == Basis.TRANSITION }.status

    @Test
    fun `the next pass reads only what arrived, and reports what changed`() {
        val first = pass(before, firstPass)
        assertEquals(Status.CLAIMED, first.status(old))

        val newcomer = TestKey()
        val arrived =
            listOf(
                repost(carol, claim, firstPass + 3 * day),
                event(newcomer, 1, "gm", 1 * day),
                profile(TestKey(), firstPass + 4 * day, newcomer.nsec),
                event(victim, 1, "anyone there?", firstPass + 5 * day),
            )
        val second = pass(before + arrived, secondPass, handOver(first, secondPass))

        assertFalse(second.full)
        assertTrue(second.eventsRead < first.eventsRead, "read ${second.eventsRead} events, the full pass read ${first.eventsRead}")
        // The claim was cited again since: decided again, now with two vouchers.
        assertEquals(Status.VALIDATED, second.status(old))
        assertEquals(new.hex, second.findings.single { it.pubkey == old.hex }.successor)
        // The old leak is carried over, not found again.
        assertEquals(KeyClass.COMPROMISED_PUBLIC, second.findings.single { it.pubkey == victim.hex }.keyClass)

        val changes = second.changes.associate { (it.pubkey to it.kind) to it }
        assertEquals(Status.CLAIMED, changes.getValue(old.hex to Change.Kind.STATUS).from)
        assertEquals(KeyClass.COMPROMISED_PUBLIC, changes.getValue(newcomer.hex to Change.Kind.NEW).keyClass)
        assertEquals(firstPass + 5 * day, changes.getValue(victim.hex to Change.Kind.SIGNING_AGAIN).activeAfter)
        assertEquals(3, second.changes.size, second.changes.toString())
    }

    @Test
    fun `an event that arrives backdated past the cursor waits for the next full pass`() {
        val first = pass(before, firstPass)
        val late = TestKey()
        // Stored after the first pass, but dated 30 days before it: older than the cursor's overlap.
        val backdated = listOf(event(late, 1, "gm", 1 * day), profile(TestKey(), firstPass - 30 * day, late.nsec))

        val incremental = pass(before + backdated, secondPass, handOver(first, secondPass))
        assertTrue(incremental.findings.none { it.pubkey == late.hex })

        val full = pass(before + backdated, secondPass)
        assertEquals(KeyClass.COMPROMISED_PUBLIC, full.findings.single { it.pubkey == late.hex }.keyClass)
    }

    @Test
    fun `a validation stands when a voucher's rank drifts, and is decided again under a new policy`() {
        val validated = pass(before + repost(carol, claim, 40 * day), firstPass)
        assertEquals(Status.VALIDATED, validated.status(old))

        // Carol's card drops below the bar: the provider replaced it (the relay keeps the newest).
        val drifted = before.filterNot { it.pubKey == lens.provider.hex && it.tags.any { t -> t[0] == "d" && t[1] == carol.hex } } + lens.card(carol, 10, 2)
        val kept = pass(drifted + repost(carol, claim, 40 * day), secondPass, handOver(validated, secondPass))
        assertEquals(Status.VALIDATED, kept.status(old))
        assertTrue(kept.changes.none { it.pubkey == old.hex })

        val stricter = VouchPolicy(minVoucherRank = 50, minVouchers = 3)
        val redecided = pass(before + repost(carol, claim, 40 * day), secondPass, handOver(validated, secondPass), stricter)
        assertEquals(Status.CLAIMED, redecided.status(old))
    }

    @Test
    fun `an eligible warning after a validation contests it`() {
        val validated = pass(before + repost(carol, claim, 40 * day), firstPass)
        val dave = TestKey()
        val warning = event(dave, 1, "Beware, this is a scam", firstPass + day, arrayOf(arrayOf("q", claim.id)))
        val next = pass(before + repost(carol, claim, 40 * day) + lens.card(dave, 80, 1) + warning, secondPass, handOver(validated, secondPass))

        assertEquals(Status.CONTESTED, next.status(old))
        assertEquals(Status.VALIDATED, next.changes.single { it.pubkey == old.hex }.from)
    }

    @Test
    fun `a report from another observer, format or month is not built on`() {
        val json = Report.toJson(pass(before, firstPass), relay, lens.observer.hex, policy)
        val other = TestKey().hex
        assertNull(Report.read(json, relay, other, secondPass, 30 * day) {})
        assertNull(Report.read(json, "wss://elsewhere", lens.observer.hex, secondPass, 30 * day) {})
        assertNull(Report.read(json, relay, lens.observer.hex, firstPass + 31 * day, 30 * day) {})
        val v1 = JsonObject(json - "formatVersion")
        assertNull(Report.read(v1, relay, lens.observer.hex, secondPass, 30 * day) {})
    }

    @Test
    fun `a report read back holds what was written`() {
        val result = pass(before + repost(carol, claim, 40 * day), firstPass)
        val prior = handOver(result, secondPass)

        assertEquals(result.findings.toSet(), prior.findings.toSet())
        val t = prior.transitions.single()
        assertEquals(listOf(claim.id), t.claims.map { it.event.id })
        assertEquals(setOf(alice.hex, carol.hex), t.vouches.map { it.voucher }.toSet())
        assertEquals(firstPass, prior.sweptUntil)
    }
}
