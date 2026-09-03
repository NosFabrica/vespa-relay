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
package com.nosfabrica.vespa.relay.sync

import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.listeners.RelayConnectionListener
import com.vitorpamplona.quartz.nip01Core.relay.client.single.IRelayClient
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.ClosedMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * WHAT THE RELAY SAID WHEN IT WOULD NOT ANSWER — the sentence
 * [com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult]
 * has no room for.
 *
 * quartz reports how a walk ENDED, which is exactly the right shape for the one
 * decision it exists for: `CLOSED` licenses no coverage claim whether the relay
 * refused on policy, on a rate limit, or on a filter it thinks is too wide.
 * Those are the same fact to a band and three different facts to an operator,
 * and the difference is carried in the `CLOSED`'s own message — which is
 * dropped on the floor between the listener and the result.
 *
 * So this keeps the last thing each relay SAID. Two uses, and both were
 * impossible without it:
 *
 *  - **The abort line can name a cause.** `abortedVisits` said 92.5% of visits
 *    ended early and nothing anywhere said why; the relay had usually been
 *    explicit (`too many kinds in filter: 139`, `restricted: not on the
 *    allowlist`) and nobody was listening. See [VisitAborts].
 *  - **A width refusal can be acted on.** [FilterWidths] reads these to learn
 *    what a relay will accept, which is the only way a cap can be the RELAY's
 *    rather than a constant of ours.
 *
 * A NOTICE counts as much as a CLOSED, and that is not a nicety: the relays
 * that refuse this router's 139-kind filter answer with a bare `NOTICE` and
 * then never EOSE, so the walk ends `IDLE` and the only trace the refusal ever
 * leaves is the sentence.
 */
internal interface RelayComplaints {
    /**
     * The last thing [url] said, if it said anything at or after [sinceMs] —
     * else null.
     *
     * **The instant is the whole contract.** One entry per relay is kept and
     * overwritten, so without a floor a leg that ended IDLE would be handed
     * yesterday's `restricted:` and report it as this walk's cause. Callers
     * pass the instant their own ask went out; anything older is another ask's
     * answer and is not theirs to read.
     */
    fun since(
        url: NormalizedRelayUrl,
        sinceMs: Long,
    ): String?

    companion object {
        /** Heard nothing, ever — the probes, and any pool built without a client to listen on. */
        val DEAF: RelayComplaints =
            object : RelayComplaints {
                override fun since(
                    url: NormalizedRelayUrl,
                    sinceMs: Long,
                ): String? = null
            }
    }
}

/**
 * The real one: a connection listener on the shared client, kept exactly as
 * [com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator] and
 * `RelayLogger` are — registered on construction, unregistered on [close].
 *
 * ONE ENTRY PER RELAY, overwritten. This is not a log and must never become
 * one: it is read by a caller that already knows which relay and which instant
 * it cares about, and a per-relay history would be a growing buffer of strings
 * a nobody reads. The bound is therefore the roster's size, and each entry is a
 * timestamp and at most [MAX_SAID] characters.
 */
internal class ClientRelayComplaints(
    private val client: INostrClient,
    private val now: () -> Long = System::currentTimeMillis,
) : RelayComplaints,
    AutoCloseable {
    private class Said(
        val text: String,
        val atMs: Long,
    )

    // Written on the per-relay socket dispatcher thread, read by the workers.
    private val said = ConcurrentHashMap<NormalizedRelayUrl, Said>()

    override fun since(
        url: NormalizedRelayUrl,
        sinceMs: Long,
    ): String? = said[url]?.takeIf { it.atMs >= sinceMs }?.text

    private val listener =
        object : RelayConnectionListener {
            override suspend fun onIncomingMessage(
                relay: IRelayClient,
                msgStr: String,
                msg: Message,
            ) {
                // NOT `onDisconnected`-scoped, unlike quartz's limits tracker.
                // A relay that refuses a REQ and then drops the connection is
                // the case this exists for, and clearing on disconnect would
                // take the sentence away exactly when the caller comes to read
                // it. The [since] floor is what keeps a stale one from being
                // read as fresh, and it is a better guard than a lifecycle
                // because it is the CALLER's own clock.
                when (msg) {
                    is ClosedMessage -> remember(relay.url, msg.message)
                    is NoticeMessage -> remember(relay.url, msg.message)
                    else -> Unit
                }
            }
        }

    private fun remember(
        url: NormalizedRelayUrl,
        text: String,
    ) {
        val trimmed = text.trim()
        if (trimmed.isEmpty()) return
        said[url] = Said(trimmed.take(MAX_SAID), now())
    }

    init {
        client.addConnectionListener(listener)
    }

    override fun close() {
        client.removeConnectionListener(listener)
        said.clear()
    }

    companion object {
        /**
         * How much of a relay's sentence is kept. Long enough for every refusal
         * measured on this deployment — the longest is
         * `bad req: filter validation failed: too many kinds in filter: 139` at
         * 62 characters — and short enough that a relay answering with a page
         * of prose cannot put it in the log line or in memory.
         */
        const val MAX_SAID = 200
    }
}
