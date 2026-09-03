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
 * WHAT THE RELAY SENT when the walk says it sent nothing — the page
 * [VisitAborts] could name every part of except the one that would settle it.
 *
 * ## The hole this fills (#187)
 *
 * `VisitPool.refusedOutright` aborts on `downloaded == 0`, so every
 * `abortedUnpageable` is a walk in which NOT ONE EVENT MATCHED THE ASK. The
 * pool's own `onEvent` cannot see what did arrive: quartz calls it for events
 * that matched the filter it was given, which on an aborting walk is none of
 * them. So the events exist, they crossed the socket, and every instrument in
 * this process is downstream of the match that discarded them.
 *
 * That is why the issue could not be diagnosed from the outside. Eight of the
 * relays it names were dialled directly in six ask shapes — including
 * `contentViaOutbox`'s real 141 kinds bound to one author, through quartz's own
 * `fetchAllPages` — and every one advanced its cursor or drained honestly. The
 * fault is not visible in a clean dial, so the page has to be caught where it
 * actually happens.
 *
 * ## Why a listener, and what it costs
 *
 * The same seam [RelayComplaints] uses, for the same reason: quartz dispatches
 * every incoming message to connection listeners before anything downstream
 * filters it. This is the only place the events are still all there.
 *
 * **It is on the hottest path in the process** — every event of every socket of
 * both planes — so it is ARMED rather than always-on. Disarmed, the work per
 * message is one `isEmpty` on a map; armed, one map lookup and, for at most
 * [MAX_ROWS] events per walk, a small append. Nothing is retained after
 * [disarm]: the sample belongs to the walk that took it.
 *
 * ## One sampler per relay at a time
 *
 * Several streams visit one relay at once over one socket, so [arm] takes the
 * slot with `putIfAbsent` and hands the loser nothing. A second walk's sample
 * would be the first walk's events anyway — they share the socket — and a line
 * that attributed them would be worse than the line that says nothing.
 *
 * The subscription ids are REPORTED rather than filtered on, and that is
 * deliberate: this walk's own id is not knowable from here (`fetchAllPages`
 * mints its own), and a page that turns out to be carrying another
 * subscription's events is itself the answer. One id in the sample is the
 * walk's; several means the socket's other traffic arrived inside it, which is
 * a different fault with a different fix.
 */
internal interface RelayPages {
    /**
     * Start sampling [url], or return null when another walk already is.
     *
     * The token is what [disarm] needs to prove the slot is still this walk's —
     * see the class header.
     */
    fun arm(url: NormalizedRelayUrl): Sample?

    /**
     * Give the slot back. Called from a `finally`, always: a walk that throws
     * must not leave its relay unsampleable for the life of the process.
     *
     * Separate from [render] because they answer to different things — the slot
     * belongs to the ASK and has to be freed the moment it ends, while the
     * sentence is wanted only on the one path in a hundred that refuses. Folded
     * together, the common path would render a string nobody reads and the
     * error path would have to remember to free.
     */
    fun free(sample: Sample?)

    /**
     * The sentence, against the filter that was asked.
     *
     * Null for a walk that never held the slot, and for one that saw nothing at
     * all — a socket that carried no event is not evidence, and a line saying
     * "sent 0 events" would read as a finding where there is none.
     */
    fun render(
        sample: Sample?,
        asked: Filter,
        /**
         * What the walk itself counted, so the sentence cannot assert something
         * the caller knows to be false.
         *
         * The "everything matched and quartz still counted none" reading is the
         * sharpest thing this instrument can say — it is not a relay
         * misbehaving, it is our side of the walk — and it is only true at
         * zero. `VisitPool` renders on a refusal, where zero is the definition,
         * so it was tempting to bake in; the live probe renders on a walk that
         * downloaded 158 and got told quartz had counted none of them. A
         * sentence that is right only where it happens to be called is a
         * sentence that will be wrong the first time somebody calls it
         * elsewhere.
         */
        downloaded: Int,
    ): String?

    /**
     * One walk's worth of what crossed the socket. Written on the relay's own
     * dispatcher thread and read by the worker after [disarm], hence the lock:
     * at most [MAX_ROWS] appends, so it is never contended for long.
     */
    class Sample(
        val url: NormalizedRelayUrl,
    ) {
        private val rows = ArrayList<Row>(MAX_ROWS)
        private var seen = 0

        private class Row(
            val subId: String,
            val kind: Int,
            val createdAt: Long,
        )

        @Synchronized
        fun add(
            subId: String,
            kind: Int,
            createdAt: Long,
        ) {
            seen++
            if (rows.size < MAX_ROWS) rows += Row(subId, kind, createdAt)
        }

        /**
         * The sentence, or null when nothing arrived.
         *
         * Everything in it is a COMPARISON against [asked], because the raw
         * numbers answer nothing on their own: the question is not what the
         * relay sent, it is which part of the ask it failed to honour. A page
         * that is off-kind and a page that is above the cursor are two
         * different faults wearing one `UNPAGEABLE`.
         */
        @Synchronized
        fun render(
            asked: Filter,
            downloaded: Int,
        ): String? {
            if (seen == 0) return null
            val kinds = asked.kinds?.toSet()
            val offKind = rows.count { kinds != null && it.kind !in kinds }
            val above = rows.count { asked.until != null && it.createdAt > asked.until!! }
            val below = rows.count { asked.since != null && it.createdAt < asked.since!! }
            val subs = rows.map { it.subId }.distinct()
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
        /**
         * Events kept per walk. Enough to see the shape of a page and few
         * enough that an armed sampler on a firehose costs a counter increment
         * after the fifth one.
         */
        const val MAX_ROWS = 5

        /** Samples nothing — the probes, and any pool built without a client to listen on. */
        val DEAF: RelayPages =
            object : RelayPages {
                override fun arm(url: NormalizedRelayUrl): Sample? = null

                override fun free(sample: Sample?) = Unit

                override fun render(
                    sample: Sample?,
                    asked: Filter,
                    downloaded: Int,
                ): String? = null
            }
    }
}

/**
 * The real one: a connection listener on the shared client, registered on
 * construction and unregistered on [close] — the lifecycle
 * [ClientRelayComplaints] already establishes for this seam.
 */
internal class ClientRelayPages(
    private val client: INostrClient,
) : RelayPages,
    AutoCloseable {
    /**
     * The armed walks, and the reason the hot path is cheap: empty is the
     * ordinary state of this map for every relay nobody is walking right now,
     * and an empty map is one volatile read per message.
     */
    private val armed = ConcurrentHashMap<NormalizedRelayUrl, RelayPages.Sample>()

    override fun arm(url: NormalizedRelayUrl): RelayPages.Sample? {
        val mine = RelayPages.Sample(url)
        return if (armed.putIfAbsent(url, mine) == null) mine else null
    }

    // Identity, not equality: only the walk that took the slot may free it, or
    // a slow return would drop a slot a later walk has already armed.
    override fun free(sample: RelayPages.Sample?) {
        if (sample != null) armed.remove(sample.url, sample)
    }

    override fun render(
        sample: RelayPages.Sample?,
        asked: Filter,
        downloaded: Int,
    ): String? = sample?.render(asked, downloaded)

    private val listener =
        object : RelayConnectionListener {
            override suspend fun onIncomingMessage(
                relay: IRelayClient,
                msgStr: String,
                msg: Message,
            ) {
                // BOTH GUARDS BEFORE ANY WORK. This runs for every event of
                // every socket of both planes — the mirror's ingest and the
                // monitor's probe passes alike — so the disarmed case has to
                // cost a size check and nothing else.
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
