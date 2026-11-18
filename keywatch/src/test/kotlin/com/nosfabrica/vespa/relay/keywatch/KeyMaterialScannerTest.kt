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

import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip01Core.crypto.Nip01Crypto
import com.vitorpamplona.quartz.nip06KeyDerivation.Bip39Mnemonics
import com.vitorpamplona.quartz.nip06KeyDerivation.Nip06
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KeyMaterialScannerTest {
    private val poster = TestKey()

    @Test
    fun `an nsec in a note derives the key it controls`() {
        val victim = TestKey()
        val found = KeyMaterialScanner.scan(event(poster, 1, "oops: ${victim.nsec}", 100))

        assertEquals(listOf(victim.hex), found.map { it.pubkey })
        assertEquals(SecretForm.NSEC, found.single().form)
        assertFalse(found.single().selfPublished)
        assertFalse(found.single().sharedLink)
    }

    @Test
    fun `an nsec as a profile name is found, and a self-named one is self-published`() {
        val bot = TestKey()
        val found = KeyMaterialScanner.scan(profile(bot, 100, "Mr.${bot.nsec}", "Just your average nostr enjoyer"))

        assertEquals(bot.hex, found.single().pubkey)
        assertTrue(found.single().selfPublished)
    }

    @Test
    fun `an nsec in a URL fragment is a shared invite link, a hashtag is not`() {
        val group = TestKey()
        assertTrue(KeyMaterialScanner.scan(event(poster, 1, "join https://iris.to/chat#${group.nsec}", 1)).single().sharedLink)
        assertFalse(KeyMaterialScanner.scan(event(poster, 1, "test #${group.nsec} the nsec", 1)).single().sharedLink)
    }

    @Test
    fun `hex is a secret only right after a word naming one`() {
        val victim = TestKey()
        assertEquals(victim.hex, KeyMaterialScanner.scan(event(poster, 1, "my private key: ${victim.secretHex}", 1)).single().pubkey)
        // The same 64 hex characters alone read as an id or a pubkey, which they almost always are.
        assertTrue(KeyMaterialScanner.scan(event(poster, 1, "look at ${victim.secretHex}", 1)).isEmpty())
    }

    @Test
    fun `a valid mnemonic derives its NIP-06 key, numbered or not`() {
        val words = Bip39Mnemonics.toMnemonics(Random(7).nextBytes(16))
        val expected = Nip01Crypto.pubKeyCreate(Nip06().privateKeyFromMnemonic(words.joinToString(" "))).toHexKey()
        val numbered = words.mapIndexed { i, w -> "${i + 1}. $w" }.joinToString("\n")

        assertEquals(expected, KeyMaterialScanner.scan(event(poster, 1, "my seed phrase:\n$numbered", 1)).single { it.form == SecretForm.MNEMONIC }.pubkey)
    }

    @Test
    fun `prose made of wordlist words is not a mnemonic`() {
        // Twelve BIP-39 words whose checksum fails, between words off the list. (Not "abandon" x11 +
        // "about": that is the standard test vector, and the scanner rightly finds it.)
        val words = Bip39Mnemonics.toMnemonics(Random(11).nextBytes(16)).toMutableList()
        words[11] = Bip39Mnemonics.englishWordlist.first { !Bip39Mnemonics.isValid((words.dropLast(1) + it).joinToString(" ")) }
        val text = "nostr ${words.joinToString(" ")} nostr"
        assertTrue(KeyMaterialScanner.scan(event(poster, 1, text, 1)).none { it.form == SecretForm.MNEMONIC })
    }

    @Test
    fun `an ncryptsec is noted without a key`() {
        val found = KeyMaterialScanner.scan(event(poster, 1, "backup ncryptsec1qgg9947rlpvqu76pj5ecreduf9jxhselq2nae2kghhvd5g7dgjflf8", 1))
        assertEquals(SecretForm.NCRYPTSEC, found.single().form)
        assertNull(found.single().pubkey)
    }
}
