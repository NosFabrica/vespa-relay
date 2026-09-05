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
import com.vitorpamplona.quartz.nip19Bech32.toNsec
import com.vitorpamplona.quartz.utils.Hex
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * A misconfigured identity must not start: an upstream refusing an unauthenticated client looks
 * exactly like one with nothing to say.
 */
class RelayIdentityTest {
    // A throwaway key, generated for this test and used nowhere.
    private val hex = "5c0c523f2b9b1b0ac0a3f11e9dfd2ff1e1a3ab5a3ec1b8bb0b6dd08b2b0b1d6f"

    private fun env(vararg pairs: Pair<String, String>): (String) -> String? = pairs.toMap()::get

    @Test
    fun `no key configured is not an error — it just does not authenticate`() {
        assertNull(RelayIdentity.fromEnv(env()))
        assertNull(RelayIdentity.fromEnv(env(RelayIdentity.ENV_VAR to "")))
        assertNull(RelayIdentity.fromEnv(env(RelayIdentity.ENV_VAR to "   ")))
    }

    @Test
    fun `a bare hex key is refused — this setting takes only nsec`() {
        // Hex has no checksum: one mistyped character is a different valid key.
        val e = assertFailsWith<IllegalArgumentException> { RelayIdentity.signerFor(hex) }
        assertEquals(true, e.message!!.contains("nsec1"), "got: ${e.message}")
        assertFailsWith<IllegalArgumentException> { RelayIdentity.signerFor(hex.uppercase()) }
    }

    @Test
    fun `surrounding whitespace is forgiven`() {
        // A paste out of a password manager brings a newline along.
        val nsec = KeyPair(privKey = Hex.decode(hex)).privKey!!.toNsec()
        val signer = RelayIdentity.fromEnv(env(RelayIdentity.ENV_VAR to "  $nsec\n"))!!
        assertEquals(RelayIdentity.signerFor(nsec).pubKey, signer.pubKey)
    }

    @Test
    fun `an nsec decodes to exactly the key it encodes`() {
        val nsec = KeyPair(privKey = Hex.decode(hex)).privKey!!.toNsec()
        val expected = NostrSignerInternal(KeyPair(privKey = Hex.decode(hex))).pubKey
        assertEquals(expected, RelayIdentity.signerFor(nsec).pubKey)
    }

    @Test
    fun `an all-uppercase nsec is valid bech32 and decodes`() {
        // BIP-173 allows the all-caps spelling; QR exports produce it.
        val nsec = KeyPair(privKey = Hex.decode(hex)).privKey!!.toNsec()
        assertEquals(RelayIdentity.signerFor(nsec).pubKey, RelayIdentity.signerFor(nsec.uppercase()).pubKey)
    }

    @Test
    fun `an nsec-shaped string that is not valid bech32 is rejected`() {
        // Treating the bech32 text as hex would derive a keypair from nonsense.
        assertFailsWith<IllegalArgumentException> {
            RelayIdentity.signerFor("nsec1" + "q".repeat(58))
        }
    }

    @Test
    fun `an npub is rejected with a message that says why`() {
        val e =
            assertFailsWith<IllegalArgumentException> {
                RelayIdentity.signerFor("npub1${"q".repeat(58)}")
            }
        assertEquals(true, e.message!!.contains("public key"), "got: ${e.message}")
    }

    @Test
    fun `a typo is rejected loudly rather than starting unauthenticated`() {
        assertFailsWith<IllegalArgumentException> { RelayIdentity.signerFor("not-a-key") }
        // Hex in any shape is refused outright.
        assertFailsWith<IllegalArgumentException> { RelayIdentity.signerFor(hex.dropLast(1)) }
        assertFailsWith<IllegalArgumentException> { RelayIdentity.signerFor(hex + "ab") }
        assertFailsWith<IllegalArgumentException> { RelayIdentity.signerFor(hex.dropLast(1) + "z") }
    }

    @Test
    fun `the failure message never contains the key`() {
        // It goes into a log line.
        val secret = "sk-this-should-never-appear-in-any-log-output-abcdef"
        val e = assertFailsWith<IllegalArgumentException> { RelayIdentity.signerFor(secret) }
        assertEquals(false, e.message!!.contains(secret), "got: ${e.message}")
    }
}
