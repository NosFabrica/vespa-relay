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

import com.vitorpamplona.quartz.nip01Core.core.HexKey

/** What keywatch can say about a key. A key can hold several at once. */
enum class KeyClass {
    /** Its secret is public, and the key had a life before it was: the person's identity is exposed. */
    COMPROMISED_PUBLIC,

    /** The owner says someone else got the key, but the secret is nowhere to be found. */
    COMPROMISED_ACCESS,

    /** Moved away "just in case": no evidence the key was taken. */
    SUSPECTED,

    /** The owner says the key is lost; whoever claims to be its successor cannot prove it. */
    LOST,

    /** Moved away with no compromise claimed. */
    RETIRED,

    /** An alt, bot or backup of another key. */
    SECONDARY,

    /** Its secret was published from the start (bot farms, shared invite keys): it never authenticated anyone. */
    PUBLIC_FROM_BIRTH,

    /** It asked relays to erase it (NIP-62). */
    VANISHED,
}

/**
 * What a finding rests on, which decides what a later run does with it: a published secret
 * ([SECRET], [ENCRYPTED_SECRET]) and a NIP-62 request ([VANISH]) are facts and are only added to;
 * a [TRANSITION] is recomputed from its stored claims and vouches; a [SECONDARY] is asked again
 * until its primary names it back.
 */
enum class Basis { SECRET, ENCRYPTED_SECRET, TRANSITION, SECONDARY, VANISH }

/**
 * One fact about one key. [since] bounds when it became true: for a leaked secret, the earliest
 * event that published it, so the key's events before it stay trustworthy. [evidence] holds
 * event ids only; a report never carries a secret.
 */
data class Finding(
    val pubkey: HexKey,
    val keyClass: KeyClass,
    val status: Status,
    val since: Long?,
    val evidence: List<String>,
    val detail: String,
    val basis: Basis,
    /** Set only once a transition is [Status.VALIDATED]: the key that speaks for this one now. */
    val successor: HexKey? = null,
    /** The other key a claim names (claimed successor, or a secondary's primary), whatever its status. */
    val relatedKey: HexKey? = null,
    val activeAfter: Long? = null,
)

/** What is known of a key whose secret was found, besides the finding itself. */
class LeakFacts(
    /** An event it signed a day or more before its secret first appeared, or a rank card: it was somebody's. */
    val hadHistory: Boolean,
    /** Its newest event after the leak; whoever signed it, owner or not, holds the key. */
    val activeAfter: Long?,
)

object LeakClassifier {
    /**
     * A key whose secret was published is [KeyClass.COMPROMISED_PUBLIC] when it had a life before
     * the leak, and [KeyClass.PUBLIC_FROM_BIRTH] when it did not or when every exposure is a
     * shared-invite link. Both are [Status.PROVEN]: the derivation is the proof.
     */
    fun classify(
        pubkey: HexKey,
        exposures: List<Exposure>,
        facts: LeakFacts,
    ): Finding {
        val leakAt = exposures.minOf { it.event.createdAt }
        val evidence =
            exposures
                .sortedBy { it.event.createdAt }
                .map { it.event.id }
                .distinct()
                .take(EVIDENCE_CAP)
        val forms = exposures.map { it.form }.toSet().joinToString(",") { it.name.lowercase() }
        val by =
            if (exposures.all { it.selfPublished }) {
                "by the key itself"
            } else if (exposures.none { it.selfPublished }) {
                "by other keys"
            } else {
                "by itself and others"
            }
        return if (exposures.all { it.sharedLink } || !facts.hadHistory) {
            val why = if (exposures.all { it.sharedLink }) "handed out in an invite link" else "published $by before the key signed anything"
            Finding(pubkey, KeyClass.PUBLIC_FROM_BIRTH, Status.PROVEN, leakAt, evidence, "$forms $why", Basis.SECRET)
        } else {
            // Whether the key still signs goes in [Finding.activeAfter], which every run refreshes.
            Finding(pubkey, KeyClass.COMPROMISED_PUBLIC, Status.PROVEN, leakAt, evidence, "$forms published $by", Basis.SECRET, activeAfter = facts.activeAfter)
        }
    }

    /** The same leak seen again in a later run: more evidence, and an earlier bound if a backdated copy turned up. */
    fun merge(
        prior: Finding,
        exposures: List<Exposure>,
    ): Finding =
        prior.copy(
            since = listOfNotNull(prior.since, exposures.minOfOrNull { it.event.createdAt }).min(),
            evidence = (prior.evidence + exposures.sortedBy { it.event.createdAt }.map { it.event.id }).distinct().take(EVIDENCE_CAP),
        )

    const val EVIDENCE_CAP = 10
}

/** The class a transition gives its old key. */
fun Reason.oldKeyClass(): KeyClass =
    when (this) {
        Reason.COMPROMISE -> KeyClass.COMPROMISED_ACCESS
        Reason.LOST -> KeyClass.LOST
        Reason.PRECAUTION -> KeyClass.SUSPECTED
        Reason.ROTATION -> KeyClass.RETIRED
    }
