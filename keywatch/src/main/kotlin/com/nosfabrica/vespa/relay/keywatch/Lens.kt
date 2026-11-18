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

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ServiceProviderTag
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.followerCount
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.rank

/** One NIP-85 contact card's two numbers. */
class Card(
    val rank: Int?,
    val followers: Int?,
)

/**
 * The observer whose trust ranks the vouchers: its kind 10040 names the provider of `30382:rank`,
 * and that provider's contact cards are the ranks. Cards are fetched lazily and cached, so every
 * key the run meets is asked for once.
 */
class RankLens private constructor(
    private val corpus: Corpus,
    val provider: HexKey,
) {
    private val cards = mutableMapOf<HexKey, Card?>()

    fun rankOf(pubkey: HexKey): Int? = cards[pubkey]?.rank

    fun cardOf(pubkey: HexKey): Card? = cards[pubkey]

    /** Fetches the cards of every key in [pubkeys] not asked for yet. */
    suspend fun load(pubkeys: Collection<HexKey>) {
        val missing = pubkeys.filter { it !in cards }.distinct()
        missing.forEach { cards[it] = null }
        val filters =
            missing.chunked(CARDS_PER_FILTER).map {
                Filter(kinds = listOf(UserAssertionEvent.KIND), authors = listOf(provider), tags = mapOf("d" to it), limit = it.size)
            }
        corpus.fetch(filters).forEach { card ->
            val subject = card.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: return@forEach
            if (subject in cards) cards[subject] = Card(card.tags.rank(), card.tags.followerCount())
        }
    }

    companion object {
        private const val CARDS_PER_FILTER = 500

        /** Resolves the observer's newest kind 10040 to its rank provider; null when it names none. */
        suspend fun resolve(
            corpus: Corpus,
            observer: HexKey,
        ): RankLens? {
            val lists = corpus.fetch(listOf(Filter(kinds = listOf(TrustProviderListEvent.KIND), authors = listOf(observer), limit = 5)))
            val newest = lists.maxByOrNull { it.createdAt } ?: return null
            val provider =
                newest.tags
                    .mapNotNull { ServiceProviderTag.parse(it) }
                    .firstOrNull { it.service == ProviderTypes.rank }
                    ?.pubkey ?: return null
            return RankLens(corpus, provider)
        }
    }
}
