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
import com.vitorpamplona.quartz.nip01Core.core.toHexKey
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.nip19Bech32.toNsec
import java.util.concurrent.atomic.AtomicLong

/** A key with its bech32 forms. Tests only: production code never holds a secret. */
class TestKey {
    private val pair = KeyPair()
    val hex: HexKey = pair.pubKey.toHexKey()
    val secretHex: String = pair.privKey!!.toHexKey()
    val npub: String = pair.pubKey.toNpub()
    val nsec: String = pair.privKey!!.toNsec()
}

private val ids = AtomicLong()

/** An event as a relay serves it. Ids are unique; signatures are never checked by keywatch. */
fun event(
    author: TestKey,
    kind: Int,
    content: String,
    createdAt: Long,
    tags: Array<Array<String>> = emptyArray(),
): Event = Event("%064x".format(ids.incrementAndGet()), author.hex, createdAt, kind, tags, content, "00".repeat(64))

fun profile(
    author: TestKey,
    createdAt: Long,
    name: String,
    about: String = "",
) = event(author, 0, """{"name":${quote(name)},"about":${quote(about)}}""", createdAt)

private fun quote(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

/**
 * A relay in memory that answers the way search.brainstorm.world does: filters per NIP-01, and a
 * NIP-50 search where every term must appear (a quoted phrase as a whole). Lens tokens such as
 * `include:spam` are declarations, not terms. It also records whether every filter carried one.
 */
class InMemoryCorpus(
    events: List<Event>,
) : Corpus {
    private val events = events.sortedByDescending { it.createdAt }

    override suspend fun fetch(filters: List<Filter>): List<Event> =
        filters
            .flatMap { f -> matching(f).let { all -> f.limit?.let { all.take(it) } ?: all } }
            .distinctBy { it.id }

    override suspend fun count(filter: Filter): Int = matching(filter).size

    private fun matching(f: Filter): List<Event> = events.filter { f.match(it) && searchMatches(f.search, it) }

    private fun searchMatches(
        search: String?,
        event: Event,
    ): Boolean {
        if (search == null) return true
        val terms = TERM.findAll(search).map { it.groupValues[1].ifEmpty { it.groupValues[2] } }.filterNot { ':' in it }
        return terms.all { event.content.contains(it, ignoreCase = true) }
    }

    private companion object {
        val TERM = Regex("\"([^\"]+)\"|(\\S+)")
    }
}

/** An observer whose kind 10040 names [provider] for `30382:rank`, and the provider's cards. */
class Lens(
    val observer: TestKey = TestKey(),
    val provider: TestKey = TestKey(),
) {
    fun list(createdAt: Long) = event(observer, 10040, "", createdAt, arrayOf(arrayOf("30382:rank", provider.hex, "wss://scores.example.com")))

    fun card(
        subject: TestKey,
        rank: Int,
        createdAt: Long,
    ) = event(provider, 30382, "", createdAt, arrayOf(arrayOf("d", subject.hex), arrayOf("rank", rank.toString())))
}
