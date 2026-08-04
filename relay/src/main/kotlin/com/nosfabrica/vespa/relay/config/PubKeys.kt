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
package com.nosfabrica.vespa.relay.config

import com.vitorpamplona.quartz.nip19Bech32.decodePublicKeyAsHexOrNull

/**
 * Parses every pubkey setting: `npub1…` (or another bech32 public-key form),
 * converted to the lowercase hex the store and filters speak.
 *
 * Bare hex is refused on purpose: hex has no checksum, so one mistyped
 * character is a valid-looking key that simply is not anybody, and nothing
 * downstream could ever notice. An npub cannot be corrupted silently.
 *
 * A bad value throws instead of being dropped, because a silently dropped
 * entry looks exactly like the feature not working — an admin who cannot
 * administer, a ban that is not enforced. A typo must be loud.
 */
object PubKeys {
    private val HEX64 = Regex("^[0-9a-f]{64}$")

    /** One key, as 64 lowercase hex. Throws unless [raw] is a bech32 public key. */
    fun decode(
        raw: String,
        varName: String,
    ): String {
        // BIP-173 allows the ALL-UPPERCASE spelling (QR exports produce it);
        // normalizing case first keeps `NPUB1…` valid and — just as important
        // — keeps the nsec detection below from being sidestepped by case.
        val trimmed =
            raw
                .trim()
                .removeSurrounding("\"")
                .let { if (it.none(Char::isLowerCase)) it.lowercase() else it }
        val hex =
            when {
                // Rejected outright: quartz would accept an nsec and hand back
                // the matching public key, leaving a private key in a public
                // setting for good.
                trimmed.startsWith("nsec1") -> null

                // Bech32 goes to quartz, which knows npub, nprofile and the rest.
                trimmed.startsWith("n") -> decodePublicKeyAsHexOrNull(trimmed)

                // Anything else — including bare hex — is refused; [describe]
                // says how to fix it.
                else -> null
            }
        return hex?.takeIf { it.length == 64 }
            ?: throw IllegalArgumentException("$varName must be an npub1…, got ${describe(trimmed)}")
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
                "a ${value.takeWhile { it.isLetter() }} that does not decode to a public key"
            }

            value.startsWith("npub1") -> {
                // It IS an npub in shape; the checksum failed. Saying "must be
                // an npub, got 63 characters" would tell the operator the one
                // thing they already believe.
                "an npub that would not decode — recopy it"
            }

            value.lowercase().matches(HEX64) -> {
                // NOT echoed back in any form: 64 hex chars might be the
                // public key the operator meant — or their hex-encoded SECRET
                // pasted into the wrong slot, byte-identical in shape. An
                // error log that re-encodes it would publish it.
                "bare hex, which has no checksum — convert it to its npub form (any nostr key tool does this) and use that"
            }

            value.length == 64 -> {
                "64 characters that are not all hex"
            }

            else -> {
                "${value.length} characters"
            }
        }
}
