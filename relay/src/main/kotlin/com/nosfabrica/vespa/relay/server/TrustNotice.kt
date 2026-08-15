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
 * Fires on each successful NIP-42 AUTH with the pubkey that authenticated, the
 * id of the connection it authenticated on, and that connection's channel to
 * the client. Non-suspend on purpose: it is invoked from quartz's `authorize`
 * hook, which runs BEFORE the `OK` frame — anything that touches the store
 * belongs behind a [CoroutineScope.launch], not in front of a login.
 *
 * ONE hook with three arguments rather than two hooks, because there is exactly
 * one seam here — quartz's `authorize` is the only place a verified AUTH, its
 * connection and its `send` are all in scope at once — and a second hook on it
 * would be a second thing to remember to wire. The two listeners want different
 * halves: [TrustNotice] answers the reader over `send` and has no use for the
 * connection, while [AuthedReaders] records presence per connection and has no
 * use for `send`. `RelayMain` is where they are put together, which is what a
 * composition root is for.
 *
 * The connection id is quartz's own (`RequestContext.connectionId`), minted from
 * one process-wide counter, and the same value the engine hands
 * `RelayServerListener.onDisconnect` — which is the only reason presence can be
 * ended when the socket closes rather than guessed at from a timeout.
 */
typealias AuthNotifier = (HexKey, Long, (Message) -> Unit) -> Unit

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
 * Two links, and they are a CHAIN — the second ask is addressed to whatever the
 * first one names, so they cannot be fired together and the first unmet link is
 * the only thing said:
 *
 *  - **kind 10040, authored by them** — their NIP-85 trust provider list, and
 *    specifically its `30382:rank` entry. That entry is what
 *    `TrustProjection`'s provider map resolves signer → observer through, so
 *    without one this relay has no cells keyed on their pubkey at all: every
 *    ranked search comes back empty, and nothing downstream can explain why.
 *  - **kind 30382 signed by that service** — the cards themselves. The service
 *    key comes out of the tag above rather than out of the reader's own
 *    pubkey: a card is ABOUT its `d` tag but ranks for whoever NAMED its
 *    signer, so "has anyone scored this reader" and "can this relay rank for
 *    this reader" are different questions with different answers.
 *
 * This is `readiness.js`'s second and third links, in the existence form a
 * relay can answer for itself: the page draws the same chain as a bar because
 * it can also ask the provider's own relay for a denominator, which a NOTICE
 * has no room for and no business doing on a login.
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
     * Where the reads run. Owned by the composition root and cancelled at
     * shutdown, so a check outliving its connection dies with the process
     * rather than holding a store call open past it.
     */
    private val scope: CoroutineScope,
) {
    /**
     * The [AuthNotifier] shape: start the walk and return immediately.
     *
     * [send] is the connection's own channel and stays valid after it closes —
     * quartz's session drops what it cannot deliver — so a reader who
     * authenticated and left costs a wasted read or two and nothing else.
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
     * The words this reader is owed — at most one, and empty when there is
     * nothing to say.
     *
     * ONE notice because the links are a chain: with no 10040 there is no
     * service to ask about, so "we hold none of your provider's scores" is not
     * a second finding but the same one restated as a guess. That is
     * `readiness.js`'s ordering rule — the first unmet link wins and the rest
     * report `waiting` — for the reason a column of red crosses says four
     * things are wrong when one is.
     */
    internal suspend fun notices(pubkey: HexKey): List<String> {
        // Null and empty are the two answers that must not be collapsed here:
        // one is a store that could not say, the other is a store saying they
        // have published no list.
        val lists = read(providerListFilter(pubkey)) ?: return emptyList()
        // 10040 is replaceable, so the store holds at most one per author and
        // `limit = 1` cannot hand back a superseded list.
        val list = lists.firstOrNull() ?: return listOf(NO_PROVIDER)
        val services = list.rankServices().ifEmpty { return listOf(NO_RANK_SERVICE) }
        val cards = read(scoreCardFilter(services)) ?: return emptyList()
        return if (cards.isEmpty()) listOf(noScores(services)) else emptyList()
    }

    /**
     * EVERY service this list names for ranking, empty when it names none this
     * relay could act on.
     *
     * All of them, not the first: `ProviderMap.providersOf` maps every
     * `30382:rank` entry a list carries, so a reader naming two services ranks
     * off either one's cards. Reading only the first told a reader whose
     * SECOND provider is fully mirrored that their scores were missing, on
     * every login, forever.
     *
     * Empty covers three different lists on purpose, because the store cannot
     * tell them apart either. A list carrying only `30382:followers` can order
     * a set and cannot rank one; a `30382:rank` entry missing its relay hint
     * does not parse (quartz's `ServiceProviderTag.parse` requires all three
     * fields); a NIP-44 private list cannot be read here at all. In each case
     * the provider map — which reads exactly this, off the public tag array —
     * resolves nothing, so the notice states what this relay can use rather
     * than guessing at what the reader meant.
     */
    private fun Event.rankServices(): List<HexKey> =
        tags
            .serviceProviders()
            .filter { it.service == ProviderTypes.rank }
            .map { it.pubkey }
            .distinct()

    /**
     * What the store holds for [filter], or null when it could not say. Null is
     * what keeps a failed read out of the notices: an empty list claims the
     * reader has not published something, and only an answer from the store may
     * make that claim.
     */
    private suspend fun read(filter: Filter): List<Event>? =
        try {
            store.query(filter)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Not a failed login and not the reader's problem — the check is
            // the only thing that did not happen. One line, because a store
            // that has stopped answering will produce one of these per AUTH.
            println("trust-notice: check failed for kind ${filter.kinds?.firstOrNull()}: ${e.message}")
            null
        }

    companion object {
        /**
         * The whole list, not its existence: the `30382:rank` tag inside it is
         * what the next ask is addressed to. `limit = 1` because a replaceable
         * kind has one current version and nothing here reads history.
         */
        internal fun providerListFilter(pubkey: HexKey) =
            Filter(
                kinds = listOf(TrustProviderListEvent.KIND),
                authors = listOf(pubkey),
                limit = 1,
            )

        /**
         * Existence only, and keyed on the SERVICES — the cards' signers —
         * rather than on the reader. A relay mirroring a provider holds
         * millions of that provider's cards and typically none about the
         * reader personally, and it is the signer that ranks: the reader's own
         * `d`-tag card decides how THEIR events rank for other people, which
         * is a different question this notice does not ask.
         *
         * One filter for all of [services] — a NIP-01 `authors` list is an OR,
         * and any one of them having landed here means ranking has something
         * to work with.
         *
         * What it does NOT prove is that a mirrored card carries a parseable
         * `rank` tag: `TrustProjection` writes no influence cell for one that
         * does not, so a provider whose cards are all rank-less leaves this
         * quiet while ranked search is still empty. Deliberate — `limit = 1`
         * can only show one card, and reading a rank-less first card as "no
         * scores here" would be a false claim about the whole set, which is
         * the direction this notice never goes.
         */
        internal fun scoreCardFilter(services: List<HexKey>) =
            Filter(
                kinds = listOf(ContactCardEvent.KIND),
                authors = services,
                limit = 1,
            )

        /**
         * A NOTICE is a line in somebody's console, so each says the kind and
         * stops. The kind IS the explanation to the only readers who can act
         * on one, and prose around it is prose nobody scrolls to.
         *
         * The `restricted:` prefix is NIP-42's, taken from quartz's table
         * rather than typed here — NIP-01's convention is a single word and a
         * colon so a client can react programmatically, and a hand-written one
         * is a prefix that drifts out of that vocabulary without failing.
         */
        internal val NO_PROVIDER = RESTRICTED.format("no kind 10040 for you here — ranked search will be empty")

        internal val NO_RANK_SERVICE = RESTRICTED.format("your kind 10040 names no usable 30382:rank service")

        /** Named, because a provider we have never mirrored is the operator's fix, not the reader's. */
        internal fun noScores(services: List<HexKey>) = RESTRICTED.format("no kind 30382 from ${services.joinToString(", ")} here yet")
    }
}
