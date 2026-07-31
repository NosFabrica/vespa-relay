/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.relay

import com.vitorpamplona.quartz.nip19Bech32.decodePublicKeyAsHexOrNull

/**
 * Public keys as an operator writes them: `npub1…`.
 *
 * Configuration is read by people, and hex is where people make mistakes —
 * sixty-four characters with no checksum, so a truncated or mistyped one is
 * still a perfectly valid-looking key that simply is not anybody. An npub
 * carries a checksum and says what it is, which is why every Nostr client shows
 * one. Raw hex is still accepted, because that is the form a pubkey takes inside
 * an event and operators do copy it out of one, but the examples are all npub.
 *
 * Everything here converts to lowercase hex, which is what the store, the
 * filters and NIP-11 all speak.
 *
 * ## Why a bad value stops the process
 *
 * The parser this replaces silently dropped anything it could not read. That is
 * the wrong failure for every one of these settings: a dropped entry in
 * `RELAY_ADMIN_PUBKEYS` is an admin who cannot administer, in `DENY_PUBKEYS` it
 * is a ban that is not enforced, and in `DEFAULT_OBSERVER` it is a relay whose
 * ranked searches quietly return nothing. None of those announce themselves —
 * they look exactly like the feature not working. A typo is a typo and it must
 * be loud.
 */
object PubKeys {
    private val HEX64 = Regex("^[0-9a-f]{64}$")

    /**
     * One key, as 64 lowercase hex. Throws if [raw] is neither npub nor hex.
     *
     * quartz's decoder does the work — it already takes npub, nprofile and bare
     * hex — with one thing taken away from it. It also accepts an `nsec` and
     * helpfully hands back the matching PUBLIC key, which is the right call in a
     * client where someone pastes whichever they have, and exactly the wrong one
     * here: a private key must never be sitting in a public-key setting, and
     * quietly deriving from it would leave it there for good.
     */
    fun decode(
        raw: String,
        varName: String,
    ): String {
        val trimmed = raw.trim().removeSurrounding("\"")
        val hex =
            when {
                // Rejected outright: quartz would accept it and hand back the
                // matching public key.
                trimmed.startsWith("nsec1") -> null

                // Bech32 goes to quartz, which knows npub, nprofile and the rest.
                trimmed.startsWith("n") -> decodePublicKeyAsHexOrNull(trimmed)

                // Bare hex is validated HERE rather than delegated. Hex.decode maps
                // characters outside [0-9a-f] instead of refusing them, so one
                // mistyped digit comes back as a perfectly valid — and completely
                // different — key, which nothing downstream could ever notice.
                else -> trimmed.lowercase().takeIf { it.matches(HEX64) }
            }
        return hex?.takeIf { it.length == 64 }
            ?: throw IllegalArgumentException("$varName must be an npub1… or 64 hex characters, got ${describe(trimmed)}")
    }

    /** Null or blank stays null; anything else must parse. */
    fun decodeOrNull(
        raw: String?,
        varName: String,
    ): String? = raw?.trim()?.ifEmpty { null }?.let { decode(it, varName) }

    /** A comma/space/newline separated list. Every entry must parse. */
    fun decodeSet(
        raw: String?,
        varName: String,
    ): Set<String> =
        raw
            ?.split(',', ' ', '\n')
            ?.map { it.trim() }
            ?.filter { it.isNotEmpty() }
            ?.map { decode(it, varName) }
            ?.toSet()
            .orEmpty()

    /** Enough to spot the mistake without echoing a whole mistyped key back. */
    private fun describe(value: String): String =
        when {
            value.startsWith("nsec1") -> {
                "an nsec — that is a PRIVATE key, and it does not belong in this setting"
            }

            value.startsWith("nprofile") || value.startsWith("nevent") || value.startsWith("note1") -> {
                "a ${value.takeWhile { it.isLetter() }}, which is not a bare public key"
            }

            value.length == 64 -> {
                "64 characters that are not all hex"
            }

            else -> {
                "${value.length} characters"
            }
        }
}
