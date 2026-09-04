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
import kotlinx.coroutines.delay
import java.util.concurrent.ConcurrentHashMap

/**
 * The last thing each relay said when it would not answer: the `CLOSED` or
 * `NOTICE` sentence `PagedFetchResult` has no room for. Read by [VisitAborts]
 * to name a cause and by [FilterWidths] to learn a width. A NOTICE counts as
 * much as a CLOSED: a relay refusing a wide filter often answers with a bare
 * NOTICE and never EOSEs, so the sentence is the refusal's only trace.
 */
internal interface RelayComplaints {
    /**
     * The last thing [url] said at or after [sinceMs], else null. One entry
     * per relay is kept, so callers pass the instant their own ask went out;
     * anything older is another ask's answer.
     */
    fun since(
        url: NormalizedRelayUrl,
        sinceMs: Long,
    ): String?

    /**
     * [since], with a moment for the sentence to arrive. Quartz dispatches a
     * `CLOSED` to subscription listeners before connection listeners, so a
     * plain [since] right after `fetchAllPages` returns can be a hop too
     * early. Paid only on a refusal; the fast path is one map read.
     */
    suspend fun awaitSince(
        url: NormalizedRelayUrl,
        sinceMs: Long,
        graceMs: Long = GRACE_MS,
    ): String? {
        val deadline = System.currentTimeMillis() + graceMs
        while (true) {
            since(url, sinceMs)?.let { return it }
            if (System.currentTimeMillis() >= deadline) return null
            delay(POLL_MS)
        }
    }

    companion object {
        /** How long [awaitSince] waits for a sentence that has not landed: a scheduling hop, not a user-facing wait. */
        const val GRACE_MS = 250L

        /** How often [awaitSince] checks inside that grace. */
        const val POLL_MS = 10L

        /** Hears nothing: the probes, and any pool built without a client to listen on. */
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
 * A connection listener on the shared client, registered on construction and
 * unregistered on [close]. One entry per relay, overwritten: this is not a
 * log, and its reader already knows which relay and which instant it wants.
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
                // Not cleared on disconnect: a relay that refuses a REQ and then drops the
                // connection is the case this exists for. The [since] floor guards staleness.
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
        // Bounded, because this listens on the client both planes share and a probe pass
        // dials every url discovery ever named. Past the bound a known relay stays fresh
        // and a new one is dropped: the repeat complainers are the ones anything reads.
        if (said.size >= MAX_RELAYS && !said.containsKey(url)) return
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
        /** How much of a relay's sentence is kept: every refusal seen fits, and a page of prose does not. */
        const val MAX_SAID = 200

        /** How many relays are remembered at all: above any roster, below a discovery sweep. See [remember]. */
        const val MAX_RELAYS = 4_096
    }
}
