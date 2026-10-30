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
package com.nosfabrica.vespa.relay.identity

import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip19Bech32.entities.NProfile
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.nip19Bech32.toNsec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A pubkey setting holds one public key and nothing else; anything more refuses the boot, a private key by name. */
class PubKeysTest {
    private val key = KeyPair()
    private val hex = key.pubKey.toHexKey()
    private val npub = key.pubKey.toNpub()
    private val nsec = KeyPair().privKey!!.toNsec()

    private fun decode(raw: String) = PubKeys.decode(raw, "RELAY_ADMIN_PUBKEYS")

    private fun refused(raw: String) = assertFailsWith<IllegalArgumentException>(raw) { decode(raw) }.message.orEmpty()

    @Test
    fun `an npub in any of its spellings is its key`() {
        assertEquals(hex, decode(npub))
        assertEquals(hex, decode("nostr:$npub"))
        assertEquals(hex, decode(npub.uppercase()))
        assertEquals(hex, decode("  \"$npub\"  "))
        assertEquals(hex, decode(NProfile.create(hex, emptyList())))
    }

    @Test
    fun `a private key is refused by name and never turned into its public key`() {
        assertTrue("PRIVATE key" in refused(nsec))
        assertTrue("PRIVATE key" in refused("$npub$nsec"), "an npub with an nsec glued on still carries the secret")
        assertFalse(nsec in refused(nsec), "the secret is not echoed back")
    }

    @Test
    fun `a key inside other text is refused rather than fished out`() {
        refused("$npub trailing words")
        refused("my admin is $npub")
        refused("${npub}x")
        refused(NProfile.create(hex, emptyList()) + "q")
        refused("note: $npub")
    }

    @Test
    fun `bare hex is refused, since it has no checksum`() {
        assertTrue("bare hex" in refused(hex))
    }
}
