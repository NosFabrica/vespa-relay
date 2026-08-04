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
 * Parses every pubkey setting: `npub1…` or 64-hex, converted to lowercase hex.
 *
 * A bad value throws instead of being dropped, because a silently dropped
 * entry looks exactly like the feature not working — an admin who cannot
 * administer, a ban that is not enforced. A typo must be loud.
 */
object PubKeys {
    private val HEX64 = Regex("^[0-9a-f]{64}$")

    /** One key, as 64 lowercase hex. Throws if [raw] is neither npub nor hex. */
    fun decode(
        raw: String,
        varName: String,
    ): String {
        val trimmed = raw.trim().removeSurrounding("\"")
        val hex =
            when {
                // Rejected outright: quartz would accept an nsec and hand back
                // the matching public key, leaving a private key in a public
                // setting for good.
                trimmed.startsWith("nsec1") -> null

                // Bech32 goes to quartz, which knows npub, nprofile and the rest.
                trimmed.startsWith("n") -> decodePublicKeyAsHexOrNull(trimmed)

                // Bare hex is validated here: Hex.decode maps characters outside
                // [0-9a-f] instead of refusing them, so one mistyped digit would
                // come back as a valid — and completely different — key.
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
