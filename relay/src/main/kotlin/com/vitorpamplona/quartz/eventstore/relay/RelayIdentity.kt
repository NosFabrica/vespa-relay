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

import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip19Bech32.decodePrivateKeyAsHexOrNull
import com.vitorpamplona.quartz.utils.Hex

/**
 * This relay's own identity — one keypair for every role in which the relay
 * acts as itself.
 *
 * Three things needed a key and each of them is the same claim, so they share
 * one:
 *
 *  - **NIP-11 `self`.** What the relay advertises as its own pubkey. Derived
 *    here rather than declared, which is the point: a declared pubkey is an
 *    assertion nobody can check, while a derived one is provable against
 *    everything below.
 *  - **NIP-42.** Paid relays, allowlisted relays and a growing share of the ones
 *    an outbox fan-out discovers serve nothing until the client proves who it
 *    is. An unanswered challenge is indistinguishable from an empty relay.
 *  - **NIP-66.** A monitor publishing relay-liveness records is its own pubkey
 *    with its own profile — and for a relay, that profile is simply the relay's.
 *
 * Splitting them buys separation nobody asked for and costs the ability to
 * verify: a client seeing a 30166 or an AUTH response could not tell whether it
 * came from the relay whose NIP-11 it just read.
 *
 * ## A real identity, not a transport detail
 *
 * This makes the relay's activity attributable: every relay it authenticates to
 * learns this pubkey and can log, rate-limit or bill against it, and the signed
 * 22242s and 30166s are ordinary events others may keep. That is the point — it
 * is how a paid relay recognises a subscriber — but it makes the key a
 * deliberate choice rather than a default. There is no generated fallback: an
 * ephemeral key would be a stranger every restart, and would mint a fresh author
 * for every monitor record it ever wrote.
 *
 * Unset, the relay simply acts anonymously: it answers no challenges, publishes
 * no liveness records, and advertises no `self`. Everything else works.
 */
object RelayIdentity {
    private val HEX64 = Regex("^[0-9a-f]{64}$")

    const val ENV_VAR = "RELAY_NSEC"

    /**
     * The signer built from [ENV_VAR], or null when it is unset or blank.
     *
     * Accepts `nsec1…` or 64 hex characters. A value that is neither is a
     * configuration error and throws rather than starting anonymous: a relay
     * silently serving nothing because it never answered a challenge is exactly
     * the failure this is meant to fix, so a typo must be loud.
     */
    fun fromEnv(env: (String) -> String? = System::getenv): NostrSignerInternal? {
        val raw = env(ENV_VAR)?.trim()?.ifEmpty { null } ?: return null
        return signerFor(raw)
    }

    fun signerFor(secret: String): NostrSignerInternal {
        val trimmed = secret.trim().removeSurrounding("\"")
        val hex =
            when {
                // quartz owns the bech32 side, and returns null for an npub rather
                // than pretending a public key could sign.
                trimmed.startsWith("n") -> decodePrivateKeyAsHexOrNull(trimmed)

                // Bare hex is checked HERE, not delegated: Hex.decode maps
                // characters outside [0-9a-f] instead of refusing them, so one
                // mistyped digit yields a valid key that is not the one anybody
                // meant — and the relay would authenticate, and sign, as a
                // stranger from then on, with nothing to indicate it.
                else -> trimmed.lowercase().takeIf { it.matches(HEX64) }
            }?.takeIf { it.length == 64 }
                ?: throw IllegalArgumentException(
                    "$ENV_VAR must be an nsec1… or 64 hex characters, got ${describe(secret)}",
                )
        return NostrSignerInternal(KeyPair(privKey = Hex.decode(hex)))
    }

    /**
     * Enough to identify the mistake, never enough to leak the key — this string
     * goes into an exception message that will be logged.
     */
    private fun describe(secret: String): String =
        when {
            secret.startsWith("npub1") -> "an npub (that is the public key — this needs the private one)"
            secret.startsWith("nsec") -> "something starting with nsec but not nsec1"
            else -> "${secret.length} characters"
        }
}
