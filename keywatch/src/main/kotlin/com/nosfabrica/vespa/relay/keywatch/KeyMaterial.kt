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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.hexToByteArray
import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip01Core.crypto.Nip01Crypto
import com.vitorpamplona.quartz.nip06KeyDerivation.Bip39Mnemonics
import com.vitorpamplona.quartz.nip06KeyDerivation.Nip06
import com.vitorpamplona.quartz.nip19Bech32.Nip19Parser
import com.vitorpamplona.quartz.nip19Bech32.entities.NSec

/** How a secret appeared. Only the first three name a key; an `ncryptsec` needs its password. */
enum class SecretForm { NSEC, HEX, MNEMONIC, NCRYPTSEC }

/**
 * One secret seen in one event, reduced to the public key it controls. The secret itself is
 * never kept: this class has no field that could carry it into a report.
 *
 * [pubkey] is null only for [SecretForm.NCRYPTSEC], which names no key until decrypted.
 * [sharedLink] marks a key handed out in a URL fragment (`iris.to/chat#nsec1…`), the
 * group-invite pattern, where publishing the key is the point.
 */
class Exposure(
    val pubkey: HexKey?,
    val form: SecretForm,
    val sharedLink: Boolean,
    val event: Event,
) {
    val selfPublished get() = pubkey == event.pubKey
}

/** Finds secret keys in an event's content and tag values, and derives the key each controls. */
object KeyMaterialScanner {
    private val NSEC = Regex("nsec1[02-9ac-hj-np-z]{58}", RegexOption.IGNORE_CASE)
    private val NCRYPTSEC = Regex("ncryptsec1[02-9ac-hj-np-z]{20,}", RegexOption.IGNORE_CASE)

    /**
     * 64 hex characters right after a word naming a secret key. Bare 64-hex strings are event
     * ids and public keys far more often than secrets, so they are never tried on their own.
     */
    private val HEX_AFTER_WORD =
        Regex(
            "(?:nsec|private[ _-]?key|priv[ _-]?key|privkey|secret[ _-]?key|\\bsk\\b)[^0-9a-f]{0,24}\\b([0-9a-f]{64})\\b",
            RegexOption.IGNORE_CASE,
        )

    private val WORD = Regex("[a-z]+")
    private val MNEMONIC_LENGTHS = setOf(12, 15, 18, 21, 24)
    private val wordlist: Set<String> by lazy { Bip39Mnemonics.englishWordlist.toSet() }

    fun scan(event: Event): List<Exposure> {
        val text =
            buildString {
                append(event.content)
                event.tags.forEach { tag -> tag.forEach { append('\n').append(it) } }
            }
        val found = mutableListOf<Exposure>()
        val seen = mutableSetOf<HexKey>()

        fun add(
            pubkey: HexKey?,
            form: SecretForm,
            sharedLink: Boolean = false,
        ) {
            if (pubkey == null || seen.add(pubkey)) found += Exposure(pubkey, form, sharedLink, event)
        }

        NSEC.findAll(text).forEach { m ->
            val pubkey = (Nip19Parser.uriToRoute(m.value.lowercase())?.entity as? NSec)?.let { runCatching { it.toPubKeyHex() }.getOrNull() }
            if (pubkey != null) add(pubkey, SecretForm.NSEC, isSharedLink(text, m.range.first))
        }
        HEX_AFTER_WORD.findAll(text).forEach { m -> pubkeyOf(m.groupValues[1])?.let { add(it, SecretForm.HEX) } }
        mnemonics(text).forEach { add(it, SecretForm.MNEMONIC) }
        if (NCRYPTSEC.containsMatchIn(text)) add(null, SecretForm.NCRYPTSEC)
        return found
    }

    /** The token holding the key is a URL and the key is its fragment: `https://…#nsec1…`. */
    private fun isSharedLink(
        text: String,
        keyStart: Int,
    ): Boolean {
        if (keyStart == 0 || text[keyStart - 1] != '#') return false
        val tokenStart = text.lastIndexOfAny(charArrayOf(' ', '\n', '\t', '"', '\''), keyStart - 1) + 1
        return "://" in text.substring(tokenStart, keyStart)
    }

    private fun pubkeyOf(secretHex: String): HexKey? = runCatching { Nip01Crypto.pubKeyCreate(secretHex.lowercase().hexToByteArray()).toHexKey() }.getOrNull()

    /**
     * Windows of a valid mnemonic length, inside runs of BIP-39 words, whose checksum holds.
     * Numbers and punctuation between the words (`1. abandon 2. ability …`) do not break a run;
     * any other word does, which is what keeps ordinary prose from being read as a seed. A run may
     * start or end with a word that belongs to the sentence around it ("my *seed* phrase: …"), so
     * every window is tried, longest first, and a window inside one already accepted is not.
     */
    private fun mnemonics(text: String): List<HexKey> {
        val runs = mutableListOf<MutableList<String>>()
        var run = mutableListOf<String>()
        WORD.findAll(text.lowercase()).forEach { w ->
            if (w.value in wordlist) {
                run += w.value
            } else {
                if (run.isNotEmpty()) runs += run
                run = mutableListOf()
            }
        }
        if (run.isNotEmpty()) runs += run
        return runs
            .filter { it.size >= MNEMONIC_LENGTHS.min() && it.size <= MAX_RUN }
            .flatMap(::validWindows)
            .mapNotNull { runCatching { Nip01Crypto.pubKeyCreate(Nip06().privateKeyFromMnemonic(it)).toHexKey() }.getOrNull() }
    }

    private fun validWindows(run: List<String>): List<String> {
        val taken = BooleanArray(run.size)
        val found = mutableListOf<String>()
        for (length in MNEMONIC_LENGTHS.sortedDescending()) {
            for (start in 0..run.size - length) {
                if ((start until start + length).any { taken[it] }) continue
                val phrase = run.subList(start, start + length).joinToString(" ")
                if (Bip39Mnemonics.isValid(phrase)) {
                    found += phrase
                    (start until start + length).forEach { taken[it] = true }
                }
            }
        }
        return found
    }

    /** Longer runs are word lists, not someone's seed; trying every window of them is wasted work. */
    private const val MAX_RUN = 48
}
