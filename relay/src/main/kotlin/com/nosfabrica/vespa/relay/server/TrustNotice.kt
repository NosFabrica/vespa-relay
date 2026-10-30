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

import com.nosfabrica.vespa.eventstore.LensedEventStore
import com.nosfabrica.vespa.eventstore.TrustChain
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix.RESTRICTED
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore

/**
 * Fires on each successful NIP-42 AUTH with the pubkey and the connection's channel. Non-suspend:
 * it runs before the `OK` frame, so anything that touches the store goes behind a launch.
 */
typealias AuthNotifier = (HexKey, (Message) -> Unit) -> Unit

/**
 * What this relay knows about a reader's trust chain, told as a NOTICE when they sign in: the first
 * link the store's own [LensedEventStore.lensOf] cannot follow, so the notice and the ranked reads
 * agree on which provider counts. A failed store read says nothing.
 */
class TrustNotice(
    private val store: LensedEventStore,
    /** Owned by the composition root and cancelled at shutdown, so a check cannot outlive the process. */
    private val scope: CoroutineScope,
    maxInFlight: Int = MAX_IN_FLIGHT,
) {
    /** A burst of fresh keys signing in costs at most this many store reads; the rest get no notice. */
    private val inFlight = Semaphore(maxInFlight)

    /** The [AuthNotifier] shape: start the check and return. [send] stays valid after the connection closes. */
    fun check(
        pubkey: HexKey,
        send: (Message) -> Unit,
    ) {
        // Skipped rather than queued: the notice is a courtesy, and a queue would grow with the burst.
        if (!inFlight.tryAcquire()) return
        // On completion, not in the body: a launch into a cancelled scope never runs its body.
        scope
            .launch { notices(pubkey).forEach { send(NoticeMessage(it)) } }
            .invokeOnCompletion { inFlight.release() }
    }

    /** At most one notice, the first unmet link. */
    internal suspend fun notices(pubkey: HexKey): List<String> {
        val chain =
            try {
                store.lensOf(pubkey)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // One line: a store that stopped answering produces one of these per AUTH.
                println("trust-notice: check failed: ${e.message}")
                return emptyList()
            }
        return when (chain.gap) {
            null -> emptyList()
            TrustChain.Gap.NO_PROVIDER_LIST -> listOf(NO_PROVIDER)
            TrustChain.Gap.NO_RANK_SERVICE -> listOf(NO_RANK_SERVICE)
            TrustChain.Gap.NO_RANK_CARDS -> listOfNotNull((chain.storedRankService ?: chain.rankService)?.let(::noScores))
        }
    }

    companion object {
        /** Sign-in checks one relay runs at once. */
        const val MAX_IN_FLIGHT = 16

        /** Each notice names the kind and stops; the `restricted:` prefix comes from quartz's table. */
        internal val NO_PROVIDER = RESTRICTED.format("no kind 10040 for you here — ranked search will be empty")

        internal val NO_RANK_SERVICE = RESTRICTED.format("your kind 10040 names no usable 30382:rank service")

        /** Named, because a provider never mirrored is the operator's fix, not the reader's. */
        internal fun noScores(service: HexKey): String = RESTRICTED.format("no kind 30382 from $service here yet")
    }
}
