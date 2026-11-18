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
import com.vitorpamplona.quartz.nip19Bech32.entities.NProfile
import com.vitorpamplona.quartz.nip19Bech32.entities.NPub
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.math.abs

/** Why a key says it changed hands, strongest first. */
enum class Reason { COMPROMISE, LOST, PRECAUTION, ROTATION }

/** Which side of a transition signed its claim. Neither one proves the new key is the person's. */
enum class Signer { OLD_KEY, NEW_KEY }

/** Something an event says about keys. */
sealed interface Statement {
    val event: Event
}

/** "This key is now that key", signed by one of the two. A claim and nothing more. */
class TransitionClaim(
    val oldKey: HexKey,
    val newKey: HexKey,
    val reason: Reason,
    val signedBy: Signer,
    override val event: Event,
) : Statement

/** "This key is an alt / bot / backup of that one", signed by the secondary. */
class SecondaryClaim(
    val secondary: HexKey,
    val primary: HexKey,
    override val event: Event,
) : Statement

/**
 * A third party naming exactly two keys alongside migration or compromise wording ("X's old
 * npub is compromised, follow Y"). It cannot say which is old on its own; it counts as a vouch
 * (or, with warning words, a vouch against) for a transition between the same two keys.
 */
class ThirdPartyStatement(
    val keys: Set<HexKey>,
    val against: Boolean,
    override val event: Event,
) : Statement

/**
 * Reads transition, secondary-key and third-party statements out of profiles (kind 0) and notes
 * (kind 1). Built for precision over recall: the claims are only candidates, and vouching is
 * what admits them, so a sentence it cannot attribute cleanly is dropped rather than guessed.
 */
object StatementReader {
    private fun words(pattern: String) = Regex(pattern, RegexOption.IGNORE_CASE)

    /** The speaker is the OLD key, pointing at its successor. */
    private val OUTGOING =
        words(
            "\\bnew (npub|account|key|profile|pubkey|identity|nostr account)|\\bmov(ed|ing) (over )?to\\b|\\bmigrat(e|ed|ing)\\b[^.\\n]{0,24}\\bto\\b|" +
                "\\bswitch(ed|ing) to\\b|\\bnow (at|on|using)\\b|\\bfollow me (at|on|here)\\b|\\bdead account\\b|\\bno longer (use|using|active)\\b|" +
                "\\bthis (account|npub|key) is (dead|retired|abandoned|deprecated|inactive|compromised|hacked)|" +
                "\\bnova conta\\b|\\bnueva cuenta\\b|\\bneue[rn]? (account|npub|schlüssel)",
        )

    /** The speaker is the NEW key, naming the one it replaces. */
    private val INCOMING =
        words(
            "\\bold (npub|account|key|profile|pubkey)|\\bprevious (npub|account|key)|\\bformer(ly)?\\b|\\bused to be\\b|" +
                "\\bconta antiga\\b|\\bcuenta (antigua|vieja)\\b|\\balte[rn]? (account|npub|schlüssel)",
        )

    /** The speaker is a secondary key of the one it names. */
    private val SECONDARY =
        words("\\balt( account)?\\b|\\bsecondary (account|key)\\b|\\bbackup account\\b|\\bbot (run|operated|maintained|managed|made) by\\b|\\b(main|primary) (account|npub|key)\\b")

    private val COMPROMISE = words("compromis|\\bhack|\\bleak|\\bstolen\\b|\\bstole\\b|\\bexposed\\b|\\bbreach|gehackt|hackead|comprometid|vazad|filtrad")
    private val PRECAUTION =
        words(
            "\\b(may|might) (have )?(been )?(leak|compromis)|\\bpossibl[ey] (leak|compromis)|\\bjust in case\\b|\\bto be safe\\b|\\bprecaution|\\bout of caution\\b",
        )
    private val LOST = words("\\blost (my |the )?(key|nsec|private key|access|keys|seed)|\\bcan'?t (access|log ?in)|\\bcannot (access|log ?in)|\\bforgot (my )?(nsec|key|password)")
    private val FIRST_PERSON = words("\\b(my|i|i'm|i am|me|this account|this npub|this key|minha|meu|eu|mi|yo|mein|meine|ich)\\b")

    /** A citation that warns rather than endorses. */
    val WARNING = words("\\bscam|\\bfake\\b|impersonat|\\bnot (him|her|them|me|real)\\b|\\bdon'?t follow|\\bdo not follow|\\bbeware\\b|phishing|\\bimpost[eo]r|\\bfraud")

    private val KEY_REF = Regex("(?:nostr:)?(npub1[02-9ac-hj-np-z]{58}|nprofile1[02-9ac-hj-np-z]+)", RegexOption.IGNORE_CASE)

    /** How close the wording must sit to the key it is about, in a note. A profile is short enough to read whole. */
    private const val NOTE_WINDOW = 120

    fun read(event: Event): List<Statement> {
        val text = textOf(event) ?: return emptyList()
        val refs =
            KEY_REF
                .findAll(text)
                .mapNotNull { m -> keyOf(m.groupValues[1])?.let { it to m.range.first } }
                .filter { it.first != event.pubKey }
                .toList()
        val keys = refs.map { it.first }.toSet()
        return when (keys.size) {
            1 -> listOfNotNull(selfStatement(event, text, keys.single(), refs.map { it.second }))
            2 -> listOfNotNull(thirdParty(event, text, keys))
            else -> emptyList()
        }
    }

    private fun selfStatement(
        event: Event,
        text: String,
        other: HexKey,
        positions: List<Int>,
    ): Statement? {
        val near = if (event.kind == 0) text else positions.joinToString(" … ") { around(text, it) }
        if (event.kind == 1 && !FIRST_PERSON.containsMatchIn(near)) return null
        val outgoing = nearestMatch(OUTGOING, near, positions, text)
        val incoming = nearestMatch(INCOMING, near, positions, text)
        val reason = reasonOf(near)
        return when {
            outgoing != null && (incoming == null || outgoing <= incoming) -> TransitionClaim(event.pubKey, other, reason, Signer.OLD_KEY, event)
            incoming != null -> TransitionClaim(other, event.pubKey, reason, Signer.NEW_KEY, event)
            SECONDARY.containsMatchIn(near) -> SecondaryClaim(event.pubKey, other, event)
            else -> null
        }
    }

    private fun thirdParty(
        event: Event,
        text: String,
        keys: Set<HexKey>,
    ): Statement? {
        if (event.kind != 1) return null
        val talksTransition = OUTGOING.containsMatchIn(text) || INCOMING.containsMatchIn(text) || COMPROMISE.containsMatchIn(text)
        if (!talksTransition) return null
        return ThirdPartyStatement(keys, WARNING.containsMatchIn(text), event)
    }

    fun reasonOf(text: String): Reason =
        when {
            PRECAUTION.containsMatchIn(text) -> Reason.PRECAUTION
            COMPROMISE.containsMatchIn(text) -> Reason.COMPROMISE
            LOST.containsMatchIn(text) -> Reason.LOST
            else -> Reason.ROTATION
        }

    /** Distance from the wording to the nearest key mention; in a profile every match is "near". */
    private fun nearestMatch(
        pattern: Regex,
        near: String,
        positions: List<Int>,
        text: String,
    ): Int? {
        if (!pattern.containsMatchIn(near)) return null
        val hits = pattern.findAll(text).map { it.range.first }.toList()
        if (hits.isEmpty()) return 0
        return positions.minOf { p -> hits.minOf { abs(it - p) } }
    }

    private fun around(
        text: String,
        at: Int,
    ) = text.substring((at - NOTE_WINDOW).coerceAtLeast(0), (at + NOTE_WINDOW).coerceAtMost(text.length))

    /** A note's content; a profile's name, display name and about, the fields a person writes in. */
    private fun textOf(event: Event): String? =
        when (event.kind) {
            1 -> {
                event.content
            }

            0 -> {
                val profile = runCatching { Json.parseToJsonElement(event.content) as? JsonObject }.getOrNull()
                profile?.let { p -> PROFILE_FIELDS.mapNotNull { (p[it] as? JsonPrimitive)?.content }.joinToString("\n") }
            }

            else -> {
                null
            }
        }

    private val PROFILE_FIELDS = listOf("name", "display_name", "displayName", "about")

    private fun keyOf(ref: String): HexKey? =
        when (val entity = runCatching { Nip19Parser.uriToRoute(ref.lowercase())?.entity }.getOrNull()) {
            is NPub -> entity.hex
            is NProfile -> entity.hex
            else -> null
        }
}
