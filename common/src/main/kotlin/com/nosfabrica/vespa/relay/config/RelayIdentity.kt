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
 * The relay's own keypair (`RELAY_NSEC`), shared by every role in which it acts
 * as itself: the NIP-11 `self` it advertises, the NIP-42 challenges it answers,
 * and the NIP-66 liveness records it signs. One key on purpose — a reader can
 * then verify that a 30166 or an AUTH response came from the relay whose NIP-11
 * they just read.
 *
 * There is no generated fallback: an ephemeral key would be a stranger every
 * restart. Unset, the relay simply acts anonymously — it answers no challenges,
 * publishes no liveness records, and advertises no `self`.
 */
object RelayIdentity {
    const val ENV_VAR = "RELAY_NSEC"

    /**
     * The signer built from [ENV_VAR], or null when it is unset or blank.
     * Only `nsec1…` is accepted — bare hex has no checksum, and a mistyped
     * hex key would have the relay sign as a stranger with nothing to
     * indicate it. Anything else throws rather than starting anonymous: a
     * relay silently serving nothing because it never answered a challenge is
     * exactly the failure this key exists to fix.
     */
    fun fromEnv(env: (String) -> String? = System::getenv): NostrSignerInternal? {
        val raw = env(ENV_VAR)?.trim()?.ifEmpty { null } ?: return null
        return signerFor(raw)
    }

    fun signerFor(secret: String): NostrSignerInternal {
        // BIP-173 allows the ALL-UPPERCASE spelling (QR exports produce it);
        // without normalizing, `NSEC1…` would die in [describe] as "63
        // characters" — the one hint that never mentions case.
        val trimmed =
            secret
                .trim()
                .removeSurrounding("\"")
                .let { if (it.none(Char::isLowerCase)) it.lowercase() else it }
        val hex =
            // quartz owns the bech32 side, and returns null for an npub rather
            // than pretending a public key could sign. Bare hex never reaches
            // it: this setting takes only nsec1 — see [describe].
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
