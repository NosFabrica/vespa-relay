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

/** Why a key says it changed hands, strongest first. */
enum class Reason { COMPROMISE, LOST, PRECAUTION, ROTATION }

/** Which side of a transition signed its claim. Neither one proves the new key is the person's. */
enum class Signer { OLD_KEY, NEW_KEY }

/**
 * The three things a claim or a vouch needs of its event, and all a report keeps of it: a later run
 * rebuilds its transitions from these without fetching anything.
 */
data class EventRef(
    val id: String,
    val pubKey: HexKey,
    val createdAt: Long,
)

fun Event.ref() = EventRef(id, pubKey, createdAt)

/** Something an event says about keys. */
sealed interface Statement {
    val event: EventRef
}

/** "This key is now that key", signed by one of the two. A claim and nothing more. */
class TransitionClaim(
    val oldKey: HexKey,
    val newKey: HexKey,
    val reason: Reason,
    val signedBy: Signer,
    override val event: EventRef,
) : Statement

/** "This key is an alt / bot / backup of that one", signed by the secondary. */
class SecondaryClaim(
    val secondary: HexKey,
    val primary: HexKey,
    override val event: EventRef,
) : Statement

/**
 * A third party naming exactly two keys alongside migration or compromise wording ("X's old
 * npub is compromised, follow Y"). It cannot say which is old on its own; it counts as a vouch
 * (or, with warning words, a vouch against) for a transition between the same two keys.
 */
class ThirdPartyStatement(
    val keys: Set<HexKey>,
    val against: Boolean,
    override val event: EventRef,
) : Statement

/**
 * Reads transition, secondary-key and third-party statements out of profiles (kind 0) and notes
 * (kind 1). Built for precision over recall: the claims are only candidates, and vouching is
 * what admits them, so a sentence it cannot attribute cleanly is dropped rather than guessed.
 */
object StatementReader {
    private fun words(pattern: String) = Regex(pattern, RegexOption.IGNORE_CASE)

    /**
     * The speaker is the OLD key, and the phrase leads into its successor ("my new npub is …",
     * "follow me at …", "I have moved to …"). Measured on 1,809 claims from the first production
     * report: half of the "validated" ones were popular notes with migration words somewhere and
     * an unrelated npub elsewhere ("a new beginning", "conversations have moved to", "switched to
     * Amber"), and their reposts endorsed the note, not a key change. So the phrase must come
     * right before the key it names, and a bare "moved to" needs a speaker who is the account.
     */
    private val OUTGOING =
        words(
            "\\bnew (npub|account|key|keys|profile|pubkey|identity|nostr account|home)\\b|" +
                "\\b(i|i'?ve|i have|i'?m|i am|we|we'?ve|we have|we'?re|this account|account)( (have|has|had|just|now|finally))? " +
                "(moved|moving|migrated|migrating)( (over|my account|this account|accounts))? to\\b|" +
                "\\bmigrat(e|ed|ing) (my account |this account )?to\\b|\\bswitch(ed|ing) to (a |my )?new\\b|" +
                "\\bfollow (me|us) (at|here|over at|instead|now)\\b|\\bdead account\\b|" +
                "\\bthis (account|npub|key) (is|was|has been) (dead|retired|abandoned|deprecated|inactive|compromised|hacked|moved)\\b|" +
                "\\bnova conta\\b|\\bnovo npub\\b|\\bnueva cuenta\\b|\\bnuevo npub\\b|\\bneue[rn]? (account|npub|schlüssel)",
        )

    /**
     * In a profile the speaker is the account itself, so a bare "moved to", or "follow me on …",
     * is enough there. In a note "follow us on NOSTR: npub…" is a footer advertising an account
     * (measured: a daily bulletin carried it in every one of ~100 issues), not a successor.
     */
    private val OUTGOING_IN_PROFILE = words("\\bmov(ed|ing)( over)? to\\b|\\bnow at\\b|\\bfollow (me|us) on\\b")

    /** The speaker is the NEW key, and the phrase leads into the one it replaces. */
    private val INCOMING =
        words(
            "\\bold (npub|account|key|keys|profile|pubkey)\\b|\\b(previous|original|former) (npub|account|key|profile)\\b|" +
                "\\bconta antiga\\b|\\bcuenta (antigua|vieja)\\b|\\balte[rn]? (account|npub|schlüssel)",
        )

    /** The speaker is a secondary key, and the phrase leads into its primary. */
    private val SECONDARY =
        words(
            "\\balts?( account)?\\b|\\bsecondary (account|key)\\b|\\bbackup account\\b|" +
                "\\b(bot )?(run|operated|maintained|managed|made|owned) by\\b|\\b(main|primary) (account|npub|key)\\b",
        )

    private val COMPROMISE = words("compromis|\\bhack|\\bleak|\\bstolen\\b|\\bstole\\b|\\bexposed\\b|\\bbreach|gehackt|hackead|comprometid|vazad|filtrad")
    private val PRECAUTION =
        words(
            "\\b(may|might) (have )?(been )?(leak|compromis)|\\bpossibl[ey] (leak|compromis)|\\bjust in case\\b|\\bto be safe\\b|\\bprecaution|\\bout of caution\\b",
        )
    private val LOST = words("\\blost (my |the )?(key|nsec|private key|access|keys|seed)|\\bcan'?t (access|log ?in)|\\bcannot (access|log ?in)|\\bforgot (my )?(nsec|key|password)")
    private val FIRST_PERSON = words("\\b(my|i|i'm|i am|i've|me|we|our|us|this account|this npub|this key|minha|meu|eu|mi|yo|mein|meine|ich)\\b")

    /** Words between the phrase and the key that make it someone else's account being recommended. */
    private val RECOMMENDING = words("\\bgo follow|\\bcheck (out|them)|\\bshout ?out|\\bthanks|\\bexcited about|\\bgreat\\b")

    /** Before "a new account": the speaker made another one, not a successor. */
    private val CREATED = words("\\b(created|made|started|opened|set up|setting up|launched|launching)( (a|another|my|an))?( (brand|second|separate|dedicated))? ?$")

    /** Right after an "old account" phrase: it points away from the speaker, so the speaker is the old one. */
    private val REDIRECTS = words("^\\s*(of\\b|[.!)\\]]|-+|>|go to\\b|follow\\b|new\\b|now\\b|moved\\b|see\\b|please\\b)")

    /** Right before an "old account" phrase: "this is (my) old account". */
    private val SELF_IS_OLD = words("\\b(this is|this was|i am|i'm|it's|its)( (my|an|the))? ?$|\\($")

    /** A citation that warns rather than endorses. */
    val WARNING = words("\\bscam|\\bfake\\b|impersonat|\\bnot (him|her|them|me|real)\\b|\\bdon'?t follow|\\bdo not follow|\\bbeware\\b|phishing|\\bimpost[eo]r|\\bfraud")

    private val KEY_REF = Regex("(?:nostr:)?(npub1[02-9ac-hj-np-z]{58}|nprofile1[02-9ac-hj-np-z]+)", RegexOption.IGNORE_CASE)

    /** At most this much between the end of the phrase and the key: "… is here: ", "… can be found at ". */
    private const val MAX_GAP = 30

    /** How much text before a phrase is read for what qualifies it ("I just created …", "this is my …"). */
    private const val SPEAKER_LOOKBACK = 24

    /** How much of a note before the key says why it moved. */
    private const val REASON_WINDOW = 300

    /** How far before the key a note's first-person word may sit. */
    private const val SPEAKER_WINDOW = 160

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

    private enum class Lead { OUTGOING, INCOMING, SECONDARY }

    /**
     * What the phrase right before a key mention says, if anything: the match that ends closest
     * to the key, within [MAX_GAP], with nothing recommending someone else in between.
     */
    private fun leadInto(
        text: String,
        at: Int,
        profile: Boolean,
    ): Lead? {
        val from = (at - MAX_GAP - LONGEST_PHRASE).coerceAtLeast(0)
        val before = text.substring(from, at)
        val patterns =
            listOfNotNull(
                Lead.OUTGOING to OUTGOING,
                (Lead.OUTGOING to OUTGOING_IN_PROFILE).takeIf { profile },
                Lead.INCOMING to INCOMING,
                Lead.SECONDARY to SECONDARY,
            )
        val (lead, match) =
            patterns
                .flatMap { (lead, re) -> re.findAll(before).map { lead to it } }
                .maxByOrNull { it.second.range.last } ?: return null
        val gap = before.substring(match.range.last + 1)
        if (gap.length > MAX_GAP || RECOMMENDING.containsMatchIn(gap)) return null
        val ahead = before.substring((match.range.first - SPEAKER_LOOKBACK).coerceAtLeast(0), match.range.first)
        return when {
            // "I just created a new npub / brand called …": an extra account, not a replacement.
            lead == Lead.OUTGOING && CREATED.containsMatchIn(ahead) && match.value.startsWith("new", ignoreCase = true) -> Lead.SECONDARY

            // "Old account. New one at …", "OLD ACCOUNT go to …", "Old account of …", "This is my
            // old account. I'm now on …": the speaker IS the old key, and the key named is its successor.
            lead == Lead.INCOMING && (REDIRECTS.containsMatchIn(gap) || SELF_IS_OLD.containsMatchIn(ahead)) -> Lead.OUTGOING

            else -> lead
        }
    }

    private fun selfStatement(
        event: Event,
        text: String,
        other: HexKey,
        positions: List<Int>,
    ): Statement? {
        val profile = event.kind == 0
        val (lead, at) = positions.firstNotNullOfOrNull { p -> leadInto(text, p, profile)?.let { it to p } } ?: return null
        if (!profile && !FIRST_PERSON.containsMatchIn(text.substring((at - SPEAKER_WINDOW).coerceAtLeast(0), at))) return null
        // A profile is about its key throughout; a note only around the claim (a bulletin said
        // "compromised" 5,000 characters from its footer and read as a compromise).
        val reason = reasonOf(if (profile) text else text.substring((at - REASON_WINDOW).coerceAtLeast(0), at))
        return when (lead) {
            Lead.OUTGOING -> TransitionClaim(event.pubKey, other, reason, Signer.OLD_KEY, event.ref())
            Lead.INCOMING -> TransitionClaim(other, event.pubKey, reason, Signer.NEW_KEY, event.ref())
            Lead.SECONDARY -> SecondaryClaim(event.pubKey, other, event.ref())
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
        return ThirdPartyStatement(keys, WARNING.containsMatchIn(text), event.ref())
    }

    fun reasonOf(text: String): Reason =
        when {
            PRECAUTION.containsMatchIn(text) -> Reason.PRECAUTION
            COMPROMISE.containsMatchIn(text) -> Reason.COMPROMISE
            LOST.containsMatchIn(text) -> Reason.LOST
            else -> Reason.ROTATION
        }

    /** Room for the longest phrase above before [MAX_GAP] starts counting. */
    private const val LONGEST_PHRASE = 80

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
