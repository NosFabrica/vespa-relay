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
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip19Bech32.Nip19Parser
import com.vitorpamplona.quartz.nip19Bech32.entities.NEvent
import com.vitorpamplona.quartz.nip19Bech32.entities.NNote

enum class Stance { ENDORSE, AGAINST }

/** How a third party cited a transition. A plain reply only ever counts against. */
enum class VouchForm { REPOST, QUOTE, REPLY, STATEMENT }

class Vouch(
    val voucher: HexKey,
    val stance: Stance,
    val form: VouchForm,
    val event: Event,
)

/**
 * What a transition's evidence has to clear. The thresholds are the operator's call, read in the
 * lens of one observer: the same evidence can validate under one lens and not another.
 *
 * - [minVoucherRank]: a voucher's rank card must reach this.
 * - [minVouchers]: this many eligible endorsements, and no eligible voucher against.
 * - [aboveNewKey]: a voucher must outrank the key it is admitting, so a well-ranked new key
 *   cannot be admitted by weaker accounts.
 * - [requireKnewOldKey]: only vouchers whose follow list holds the old key count.
 */
data class VouchPolicy(
    val minVoucherRank: Int = 50,
    val minVouchers: Int = 2,
    val aboveNewKey: Boolean = true,
    val requireKnewOldKey: Boolean = false,
)

enum class Status {
    /** A cryptographic or structural fact: a key derived from a published secret, a signed NIP-62. */
    PROVEN,

    /** A claim enough independent, well-ranked accounts endorsed. */
    VALIDATED,

    /** Only the keys involved say so. */
    CLAIMED,

    /** Eligible vouchers disagree, or the old key has more than one claimed successor. */
    CONTESTED,
}

/** Every claim between the same two keys, in either direction. */
class Transition(
    val oldKey: HexKey,
    val newKey: HexKey,
    val claims: List<TransitionClaim>,
) {
    val reason: Reason = claims.minOf { it.reason }
    val firstClaimAt: Long = claims.minOf { it.event.createdAt }
    val signers: Set<Signer> = claims.map { it.signedBy }.toSet()
}

/** One voucher's standing for one transition, and why it does or does not count. */
class VouchVerdict(
    val vouch: Vouch,
    val rank: Int?,
    val knewOldKey: Boolean,
    val ineligible: String?,
) {
    val eligible get() = ineligible == null
}

class TransitionVerdict(
    val transition: Transition,
    val status: Status,
    val vouches: List<VouchVerdict>,
)

/**
 * Decides whether a transition is admitted. Neither key's word counts: the old key may be in an
 * attacker's hands, and anyone can mint a new key that claims an old one. Only third parties do,
 * and only when they stand on their own:
 *
 * - not one of the two keys, and not a key whose secret is public or was never anyone's ([excluded]);
 * - cited the claim after it was made;
 * - ranked at [VouchPolicy.minVoucherRank] or more, and above the new key when [VouchPolicy.aboveNewKey].
 *
 * One voucher counts once, by their latest citation.
 */
class TransitionValidator(
    private val policy: VouchPolicy,
    private val rankOf: (HexKey) -> Int?,
    private val follows: (HexKey) -> Set<HexKey>?,
    private val excluded: Set<HexKey>,
) {
    fun validate(
        transition: Transition,
        vouches: List<Vouch>,
    ): TransitionVerdict {
        val latest = vouches.groupBy { it.voucher }.values.map { all -> all.maxBy { it.event.createdAt } }
        val newKeyRank = rankOf(transition.newKey) ?: 0
        val verdicts =
            latest.map { vouch ->
                val rank = rankOf(vouch.voucher)
                val knew = follows(vouch.voucher)?.contains(transition.oldKey) == true
                val why =
                    when {
                        vouch.voucher == transition.oldKey || vouch.voucher == transition.newKey -> "is one of the two keys"
                        vouch.voucher in excluded -> "its own key is public"
                        vouch.event.createdAt < transition.firstClaimAt -> "predates the claim"
                        vouch.form == VouchForm.REPLY && vouch.stance == Stance.ENDORSE -> "a reply is not an endorsement"
                        rank == null || rank < policy.minVoucherRank -> "rank ${rank ?: "none"} < ${policy.minVoucherRank}"
                        policy.aboveNewKey && rank <= newKeyRank -> "rank $rank does not outrank the new key ($newKeyRank)"
                        policy.requireKnewOldKey && !knew -> "does not follow the old key"
                        else -> null
                    }
                VouchVerdict(vouch, rank, knew, why)
            }
        val eligible = verdicts.filter { it.eligible }
        val status =
            when {
                eligible.any { it.vouch.stance == Stance.AGAINST } -> Status.CONTESTED
                eligible.count { it.vouch.stance == Stance.ENDORSE } >= policy.minVouchers -> Status.VALIDATED
                else -> Status.CLAIMED
            }
        return TransitionVerdict(transition, status, verdicts.sortedByDescending { it.rank ?: -1 })
    }

    companion object {
        /**
         * An old key may have one successor. When several are claimed, only a lone validated one
         * survives; every other claim for that old key, validated or not, is contested.
         */
        fun settleSuccessors(verdicts: List<TransitionVerdict>): List<TransitionVerdict> =
            verdicts.groupBy { it.transition.oldKey }.values.flatMap { forOldKey ->
                if (forOldKey.size == 1) return@flatMap forOldKey
                val validated = forOldKey.filter { it.status == Status.VALIDATED }
                forOldKey.map { v ->
                    if (validated.size == 1 && v === validated.single()) v else TransitionVerdict(v.transition, Status.CONTESTED, v.vouches)
                }
            }
    }
}

/** Turns the citations of claim events into vouches, each tied to the claim it cites. */
object VouchReader {
    /**
     * [cited] maps a claim event id to its transition key. A repost endorses; a quote endorses
     * unless it warns; a reply only ever counts as a warning.
     */
    fun read(
        event: Event,
        cited: Set<String>,
    ): List<Pair<String, Vouch>> {
        val warns = StatementReader.WARNING.containsMatchIn(event.content)
        val quoted = event.tags.filter { it.size > 1 && it[0] == "q" && it[1] in cited }.map { it[1] }
        val referenced = event.tags.filter { it.size > 1 && it[0] == "e" && it[1] in cited }.map { it[1] }
        return when (event.kind) {
            6, 16 -> {
                referenced.map { it to Vouch(event.pubKey, Stance.ENDORSE, VouchForm.REPOST, event) }
            }

            1 -> {
                val stance = if (warns) Stance.AGAINST else Stance.ENDORSE
                val asQuote = (quoted + referenced.filter { it in mentionsIn(event.content) }).toSet()
                asQuote.map { it to Vouch(event.pubKey, stance, VouchForm.QUOTE, event) } +
                    (referenced.toSet() - asQuote).map { it to Vouch(event.pubKey, stance, VouchForm.REPLY, event) }
            }

            else -> {
                emptyList()
            }
        }
    }

    private val EVENT_REF = Regex("(?:nostr:)?(note1[02-9ac-hj-np-z]{58}|nevent1[02-9ac-hj-np-z]+)", RegexOption.IGNORE_CASE)

    /** The event ids a note mentions inline (`nostr:nevent1…`), which is how a quote without a `q` tag cites. */
    private fun mentionsIn(content: String): Set<String> =
        EVENT_REF
            .findAll(content)
            .mapNotNull { m ->
                when (
                    val e =
                        runCatching {
                            Nip19Parser.uriToRoute(m.groupValues[1].lowercase())?.entity
                        }.getOrNull()
                ) {
                    is NNote -> e.hex
                    is NEvent -> e.hex
                    else -> null
                }
            }.toSet()
}
