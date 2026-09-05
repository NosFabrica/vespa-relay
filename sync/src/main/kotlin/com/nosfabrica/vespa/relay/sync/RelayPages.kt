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
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.EventMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * What the relay sent when the walk says it sent nothing: a sample of the events that crossed
 * the socket during one ask, taken on the connection listener, upstream of the filter match.
 * Armed per walk, one sampler per relay at a time, because several streams share one socket.
 */
internal interface RelayPages {
    /** Starts sampling [url], or returns null when another walk already is. */
    fun arm(
        url: NormalizedRelayUrl,
        /** The filter the walk is about to send; each event is classified against it as it arrives. */
        asked: Filter,
    ): Sample?

    /** Gives the slot back, always from a `finally`, so a throwing walk cannot leave its relay unsampleable. */
    fun free(sample: Sample?)

    /**
     * The sentence, against the filter that was asked. Null for a walk that never held the slot
     * or saw nothing: "sent 0 events" would read as a finding where there is none.
     */
    fun render(
        sample: Sample?,
        /** What the walk itself counted. */
        downloaded: Int,
    ): String?

    /**
     * One walk's worth of what crossed the socket. Written on the relay's dispatcher thread
     * and read by the worker after [free], hence the lock.
     */
    class Sample(
        val url: NormalizedRelayUrl,
        private val asked: Filter,
    ) {
        private val kinds = asked.kinds?.toSet()
        private val rows = ArrayList<Row>(MAX_ROWS)
        private val subs = LinkedHashSet<String>()
        private var seen = 0
        private var offKind = 0
        private var above = 0
        private var below = 0

        private class Row(
            val subId: String,
            val kind: Int,
            val createdAt: Long,
        )

        /** Counted on arrival; only the first [MAX_ROWS] are kept for display. */
        @Synchronized
        fun add(
            subId: String,
            kind: Int,
            createdAt: Long,
        ) {
            seen++
            subs += subId
            if (kinds != null && kind !in kinds) offKind++
            asked.until?.let { if (createdAt > it) above++ }
            asked.since?.let { if (createdAt < it) below++ }
            if (rows.size < MAX_ROWS) rows += Row(subId, kind, createdAt)
        }

        /** The sentence, or null when nothing arrived. Everything in it is a comparison against [asked]. */
        @Synchronized
        fun render(downloaded: Int): String? {
            if (seen == 0) return null
            val faults =
                buildList {
                    if (offKind > 0) add("$offKind off-kind")
                    if (above > 0) add("$above above the `until`")
                    if (below > 0) add("$below below the `since`")
                    if (isEmpty()) {
                        add(
                            if (downloaded == 0) {
                                "all of them MATCHING the ask, which quartz still counted as none — this one is OUR side of the walk"
                            } else {
                                "all of them matching the ask"
                            },
                        )
                    }
                }
            val shown = rows.joinToString(" ") { "k${it.kind}@${it.createdAt}" }
            return "the socket carried $seen event(s) on ${subs.size} subscription(s) [${subs.joinToString()}]: " +
                "${faults.joinToString(", ")} — of the first ${rows.size}: $shown"
        }
    }

    companion object {
        /** Events kept per walk: enough to see a page's shape. */
        const val MAX_ROWS = 5

        /** Samples nothing: the probes, and any pool built without a client to listen on. */
        val DEAF: RelayPages =
            object : RelayPages {
                override fun arm(
                    url: NormalizedRelayUrl,
                    asked: Filter,
                ): Sample? = null

                override fun free(sample: Sample?) = Unit

                override fun render(
                    sample: Sample?,
                    downloaded: Int,
                ): String? = null
            }
    }
}

/** A connection listener on the shared client, registered on construction and unregistered on [close]. */
internal class ClientRelayPages(
    private val client: INostrClient,
) : RelayPages,
    AutoCloseable {
    /** The armed walks; empty is the ordinary state. */
    private val armed = ConcurrentHashMap<NormalizedRelayUrl, RelayPages.Sample>()

    override fun arm(
        url: NormalizedRelayUrl,
        asked: Filter,
    ): RelayPages.Sample? {
        val mine = RelayPages.Sample(url, asked)
        return if (armed.putIfAbsent(url, mine) == null) mine else null
    }

    // Identity, not equality: only the walk that took the slot may free it.
    override fun free(sample: RelayPages.Sample?) {
        if (sample != null) armed.remove(sample.url, sample)
    }

    override fun render(
        sample: RelayPages.Sample?,
        downloaded: Int,
    ): String? = sample?.render(downloaded)

    private val listener =
        object : RelayConnectionListener {
            override suspend fun onIncomingMessage(
                relay: IRelayClient,
                msgStr: String,
                msg: Message,
            ) {
                // Both guards before any work: this runs for every event of every socket.
                if (armed.isEmpty()) return
                if (msg !is EventMessage) return
                armed[relay.url]?.add(msg.subId, msg.event.kind, msg.event.createdAt)
            }
        }

    init {
        client.addConnectionListener(listener)
    }

    override fun close() {
        client.removeConnectionListener(listener)
        armed.clear()
    }
}
