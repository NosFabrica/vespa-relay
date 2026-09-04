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

import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip19Bech32.decodePrivateKeyAsHexOrNull
import com.vitorpamplona.quartz.utils.Hex

/**
 * The relay's own keypair (`RELAY_NSEC`), one key for every role it acts in
 * as itself: the NIP-11 `self`, the NIP-42 challenges, the NIP-66 records.
 * No generated fallback, since an ephemeral key is a stranger every restart;
 * unset, the relay acts anonymously.
 */
object RelayIdentity {
    const val ENV_VAR = "RELAY_NSEC"

    /**
     * The signer built from [ENV_VAR], or null when it is unset or blank.
     * Anything else that does not decode throws rather than starting anonymous.
     */
    fun fromEnv(env: (String) -> String? = System::getenv): NostrSignerInternal? {
        val raw = env(ENV_VAR)?.trim()?.ifEmpty { null } ?: return null
        return signerFor(raw)
    }

    fun signerFor(secret: String): NostrSignerInternal {
        // BIP-173 allows the all-uppercase spelling (QR exports produce it).
        val trimmed =
            secret
                .trim()
                .removeSurrounding("\"")
                .let { if (it.none(Char::isLowerCase)) it.lowercase() else it }
        val hex =
            // Only nsec1: bare hex has no checksum, and an npub decodes to null here.
            (if (trimmed.startsWith("n")) decodePrivateKeyAsHexOrNull(trimmed) else null)
                ?.takeIf { it.length == 64 }
                ?: throw IllegalArgumentException(
                    "$ENV_VAR must be an nsec1…, got ${describe(trimmed)}",
                )
        return NostrSignerInternal(KeyPair(privKey = Hex.decode(hex)))
    }

    /** Enough to identify the mistake, never enough to leak the key. */
    private fun describe(secret: String): String =
        when {
            secret.startsWith("npub1") -> "an npub (that is the public key — this needs the private one)"
            secret.startsWith("nsec") -> "an nsec that would not decode — recopy it"
            secret.length == 64 -> "a bare hex key — this setting takes only nsec1…, which carries the checksum hex lacks"
            else -> "${secret.length} characters"
        }
}
