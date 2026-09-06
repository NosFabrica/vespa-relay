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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix.RESTRICTED
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.serviceProviders
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

/**
 * Fires on each successful NIP-42 AUTH with the pubkey and the connection's channel. Non-suspend:
 * it runs before the `OK` frame, so anything that touches the store goes behind a launch.
 */
typealias AuthNotifier = (HexKey, (Message) -> Unit) -> Unit

/**
 * What this relay knows about a reader's trust chain, told as a NOTICE when they sign in: their
 * kind 10040 naming a `30382:rank` service, then a kind 30382 signed by that service. Only the
 * first unmet link is said, and a failed store read says nothing.
 */
class TrustNotice(
    private val store: IEventStore,
    /** Owned by the composition root and cancelled at shutdown, so a check cannot outlive the process. */
    private val scope: CoroutineScope,
) {
    /** The [AuthNotifier] shape: start the walk and return. [send] stays valid after the connection closes. */
    fun check(
        pubkey: HexKey,
        send: (Message) -> Unit,
    ) {
        scope.launch {
            notices(pubkey).forEach { send(NoticeMessage(it)) }
        }
    }

    /** At most one notice, the first unmet link. */
    internal suspend fun notices(pubkey: HexKey): List<String> {
        // Null is a store that could not say; empty is a store saying they published no list.
        val lists = read(providerListFilter(pubkey)) ?: return emptyList()
        // 10040 is replaceable, so `limit = 1` cannot hand back a superseded list.
        val list = lists.firstOrNull() ?: return listOf(NO_PROVIDER)
        val services = list.rankServices().ifEmpty { return listOf(NO_RANK_SERVICE) }
        val cards = read(scoreCardFilter(services)) ?: return emptyList()
        return if (cards.isEmpty()) listOf(noScores(services)) else emptyList()
    }

    /** Every service this list names for ranking, matching what the provider map resolves. */
    private fun Event.rankServices(): List<HexKey> =
        tags
            .serviceProviders()
            .filter { it.service == ProviderTypes.rank }
            .map { it.pubkey }
            .distinct()

    /** What the store holds for [filter], or null when it could not say. Only a store answer may claim absence. */
    private suspend fun read(filter: Filter): List<Event>? =
        try {
            store.query(filter)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // One line: a store that stopped answering produces one of these per AUTH.
            println("trust-notice: check failed for kind ${filter.kinds?.firstOrNull()}: ${e.message}")
            null
        }

    companion object {
        /** The whole list, because its `30382:rank` tag is what the next ask is addressed to. */
        internal fun providerListFilter(pubkey: HexKey) =
            Filter(
                kinds = listOf(TrustProviderListEvent.KIND),
                authors = listOf(pubkey),
                limit = 1,
            )

        /**
         * Existence only, keyed on the services (the cards' signers), not on the reader. One filter
         * for all of [services], since any one having landed here is enough.
         */
        internal fun scoreCardFilter(services: List<HexKey>) =
            Filter(
                kinds = listOf(ContactCardEvent.KIND),
                authors = services,
                limit = 1,
            )

        /** Each notice names the kind and stops; the `restricted:` prefix comes from quartz's table. */
        internal val NO_PROVIDER = RESTRICTED.format("no kind 10040 for you here — ranked search will be empty")

        internal val NO_RANK_SERVICE = RESTRICTED.format("your kind 10040 names no usable 30382:rank service")

        /** Named, because a provider never mirrored is the operator's fix, not the reader's. */
        internal fun noScores(services: List<HexKey>) = RESTRICTED.format("no kind 30382 from ${services.joinToString(", ")} here yet")
    }
}
