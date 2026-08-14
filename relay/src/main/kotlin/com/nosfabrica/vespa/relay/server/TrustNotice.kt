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
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

/**
 * Fires on each successful NIP-42 AUTH with the pubkey that authenticated and
 * this connection's channel to the client. Non-suspend on purpose: it is
 * invoked from quartz's `authorize` hook, which runs BEFORE the `OK` frame —
 * anything that touches the store belongs behind a [CoroutineScope.launch],
 * not in front of a login.
 */
typealias AuthNotifier = (HexKey, (Message) -> Unit) -> Unit

/**
 * What this relay knows about a reader's trust chain, told to them the moment
 * they sign in.
 *
 * The store treats the reader's web-of-trust lens as a FILTER rather than an
 * ordering, so a signed-in reader whose chain has not been mirrored here gets
 * an EMPTY ranked search instead of a degraded one — which from the outside is
 * indistinguishable from a broken relay. `web/readiness.js` answers that for
 * OUR page, at the cost of seven round trips; every other client on the
 * protocol had no way to ask at all. NOTICE is the one channel NIP-01 gives a
 * relay for saying something about a connection that is not an answer to a
 * command, and a login is exactly the moment the answer changes.
 *
 * Two facts, asked TOGETHER because neither reads the other:
 *
 *  - **kind 10040, authored by them** — their NIP-85 trust provider list. It
 *    names the service whose scores rank for them, and without it the trust
 *    projection has no observer cells to key on their pubkey at all: ranked
 *    search comes back empty and nothing downstream can explain why.
 *  - **kind 30382 with `d` = them** — a score card ABOUT them, from whichever
 *    service signed it. Held or not, the reader's own reads are unaffected;
 *    what it decides is the other direction — an author no provider has scored
 *    sits below the default `min_rank` floor, so their events are the ones
 *    filtered out of everybody else's ranked search here.
 *
 * That second check is deliberately NOT "does this relay hold the reader's
 * provider's whole card set" (`readiness.js`'s third link, the one whose bar
 * says *importing — 62%*). That question cannot be asked until the 10040 has
 * been read and parsed for its `30382:rank` tag, which makes it a second round
 * trip that depends on the first; these two are one round trip each and run
 * concurrently.
 *
 * **Silence is a state.** A reader holding both hears nothing — the failure
 * mode of a status channel is nagging people who are fine, the same rule
 * `worthShowing` holds on the page — and so is a reader whose check FAILED: a
 * store that throws is never read as a store that holds nothing. Publishing
 * "we do not have your provider list" because Vespa was briefly unreachable
 * would be a claim about the reader's own publishing that this relay cannot
 * support, and it would send them off to fix something that is not broken.
 */
class TrustNotice(
    private val store: IEventStore,
    /**
     * Where the two reads run. Owned by the composition root and cancelled at
     * shutdown, so a check outliving its connection dies with the process
     * rather than holding a store call open past it.
     */
    private val scope: CoroutineScope,
) {
    /**
     * The [AuthNotifier] shape: start the checks and return immediately.
     *
     * [send] is the connection's own channel and stays valid after it closes —
     * quartz's session drops what it cannot deliver — so a reader who
     * authenticated and left costs one wasted pair of reads and nothing else.
     */
    fun check(
        pubkey: HexKey,
        send: (Message) -> Unit,
    ) {
        scope.launch {
            notices(pubkey).forEach { send(NoticeMessage(it)) }
        }
    }

    /**
     * The words this reader is owed, in chain order — empty when there is
     * nothing to say. Both reads are issued before either is awaited: a
     * signed-in reader missing both links should wait for one round trip, not
     * two.
     */
    internal suspend fun notices(pubkey: HexKey): List<String> =
        coroutineScope {
            val provider = async { holds(providerListFilter(pubkey)) }
            val scored = async { holds(scoreCardFilter(pubkey)) }
            buildList {
                if (provider.await() == false) add(NO_PROVIDER)
                if (scored.await() == false) add(NO_SCORES)
            }
        }

    /**
     * Whether the store holds anything matching [filter], or null when it could
     * not say. Null is what keeps a failed read out of the notices: `false`
     * claims the reader has not published something, and only an answer from
     * the store may make that claim.
     */
    private suspend fun holds(filter: Filter): Boolean? =
        try {
            store.query<Event>(filter).isNotEmpty()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Not a failed login and not the reader's problem — the check is
            // the only thing that did not happen. One line, because a store
            // that has stopped answering will produce one of these per AUTH.
            println("trust-notice: check failed for ${filter.kinds?.firstOrNull()}: ${e.message}")
            null
        }

    companion object {
        /**
         * `limit = 1` because the question is existence. The store's own count
         * would answer it too, but a count over a kind-and-tag match set is the
         * shape `StatsRollup` keeps off the per-minute cadence — one row back
         * is strictly less work than a total nobody reads.
         */
        internal fun providerListFilter(pubkey: HexKey) =
            Filter(
                kinds = listOf(TrustProviderListEvent.KIND),
                authors = listOf(pubkey),
                limit = 1,
            )

        /**
         * NIP-85 puts the scored pubkey in the card's `d` tag (quartz's
         * `ContactCardEvent.aboutUser()`), so "scored anywhere by anyone" is a
         * single-tag ask that needs no provider resolved first. The card's
         * AUTHOR is the service; asking by author is the other question, and
         * this one deliberately does not ask it.
         */
        internal fun scoreCardFilter(pubkey: HexKey) =
            Filter(
                kinds = listOf(ContactCardEvent.KIND),
                tags = mapOf("d" to listOf(pubkey)),
                limit = 1,
            )

        /**
         * Both notices name the kind, because the reader who can act on this is
         * running a client that speaks in kinds, and "trust provider list" is
         * not a searchable string in anyone's codebase.
         */
        internal const val NO_PROVIDER =
            "trust: this relay holds no kind 10040 for you — it does not know which service scores your " +
                "web of trust, so ranked search will come back empty. Publish a NIP-85 trust provider list " +
                "to a relay this one mirrors."

        internal const val NO_SCORES =
            "trust: this relay holds no kind 30382 about you yet — no trust provider has scored your pubkey " +
                "here, so your own events sit below the default rank floor of everyone else's ranked search."
    }
}
