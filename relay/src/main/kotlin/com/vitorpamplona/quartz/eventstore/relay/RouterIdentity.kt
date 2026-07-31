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
 * The keypair the router authenticates with, when it has one.
 *
 * Some relays serve nothing until the client proves who it is (NIP-42): paid
 * relays, allowlisted relays, and an increasing number of the ones an outbox
 * fan-out discovers from users' 10002s. Without a key we simply skip the
 * challenge, which costs only whatever those relays would have served.
 *
 * ## This is a real identity, not a transport detail
 *
 * Authenticating makes the mirroring attributable: every relay that challenges
 * us learns this pubkey and can log, rate-limit or bill against it, and the
 * signed 22242s are ordinary events those relays may keep. That is the point —
 * it is how a paid relay recognises a subscriber — but it means the key is a
 * deliberate choice rather than a default. There is no generated fallback here
 * for that reason: an ephemeral key would authenticate as a stranger every
 * restart, which is worse than not authenticating at all.
 *
 * Prefer a key dedicated to this relay over the operator's personal one. The
 * signer can sign anything it is handed; only the challenge is ever handed to it
 * here, but a leaked key is a leaked key.
 */
object RouterIdentity {
    const val ENV_VAR = "ROUTER_AUTH_NSEC"

    /**
     * The signer built from [ENV_VAR], or null when it is unset or blank.
     *
     * Accepts `nsec1…` or 64 hex characters. A value that is neither is a
     * configuration error and throws rather than starting unauthenticated: a
     * relay silently serving nothing is exactly the failure this is meant to
     * fix, so a typo must be loud.
     */
    fun fromEnv(env: (String) -> String? = System::getenv): NostrSignerInternal? {
        val raw = env(ENV_VAR)?.trim()?.ifEmpty { null } ?: return null
        return signerFor(raw)
    }

    fun signerFor(secret: String): NostrSignerInternal {
        val hex =
            when {
                // Not `NSec(secret).hex` — that constructor takes the hex, so it
                // would happily wrap the bech32 string itself and hand back a
                // keypair derived from nonsense.
                secret.startsWith("nsec1") -> {
                    decodePrivateKeyAsHexOrNull(secret)
                        ?: throw IllegalArgumentException("$ENV_VAR is not a valid nsec")
                }

                secret.length == 64 && secret.all { it.isDigit() || it in 'a'..'f' || it in 'A'..'F' } -> {
                    secret.lowercase()
                }

                else -> {
                    throw IllegalArgumentException(
                        "$ENV_VAR must be an nsec1… or 64 hex characters, got ${describe(secret)}",
                    )
                }
            }
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
