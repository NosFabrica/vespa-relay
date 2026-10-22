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
package com.nosfabrica.vespa.relay.ingest

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PublishResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.publishStream
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthSnapshot
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.RejectionReason
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.launch
import okhttp3.Dns
import okhttp3.OkHttpClient
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 * The sync's writes, forwarded to the serving relay as plain NIP-01 EVENTs on one socket signed in
 * (NIP-42) as the relay's own key, so the relay is the only process that writes the index. The
 * relay puts what that connection publishes on its mirror lane (`MirrorLane`): the same policies,
 * the same verification and the same live fan-out as any client's publish, through an ingest
 * pipeline of its own. The OKs come back as the store's own outcomes ([outcomeOf]), so the
 * pipeline, the refusal filter and the band gate read them exactly as they read a local write.
 *
 * [dial] is where the socket goes (on compose, `ws://relay:7777`); [relay] is the address the
 * relay knows itself by (`RELAY_URL`), the one its NIP-42 check reads in the AUTH's `relay` tag.
 * The AUTH is signed for [relay] whatever [dial] says, so an internal address needs no change on
 * the relay side.
 *
 * The socket is held with `keepConnected`: Quartz drops a relay from its pool as soon as nothing
 * wants it, and every redial is a fresh challenge whose first events could land before the AUTH
 * does. Those still get written, through the publishers' lane under the same rules, so a redial
 * costs lane accounting and not correctness; [unsignedBatches] counts it. On the open socket each
 * batch's window opens at once (`publishStream` sends no lone first event there).
 *
 * While the relay is down the socket is redialled every [redialMs], past the client's back-off
 * ([redial]): that back-off is written for relays on the internet, and this one is ours.
 */
class RelayWriter(
    private val dial: NormalizedRelayUrl,
    private val relay: NormalizedRelayUrl,
    private val signer: NostrSigner,
    scope: CoroutineScope,
    /** How long a batch waits for the sign-in before it goes out on the publishers' lane anyway. */
    private val authWaitMs: Long = AUTH_WAIT_MS,
    /** How long the relay may stay silent with events outstanding before they are failed. */
    private val idleTimeoutMs: Long = IDLE_TIMEOUT_MS,
    /** EVENTs a batch keeps unanswered at once; see [IN_FLIGHT]. */
    private val maxInFlight: Int = IN_FLIGHT,
    /** How often a relay that is down is dialled again; see [redial]. */
    private val redialMs: Long = REDIAL_MS,
    /** How [dial]'s host is resolved; a test stands in for compose's, which forgets a stopped container. */
    dns: Dns = Dns.SYSTEM,
) : StoreWriter {
    // Local and long-lived: no Tor, no dispatcher budget to share with the upstream sockets.
    private val okhttp =
        OkHttpClient
            .Builder()
            .pingInterval(Duration.ofSeconds(30))
            .connectTimeout(Duration.ofSeconds(10))
            .dns(dns)
            .build()

    private val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)

    private val authenticator =
        RelayAuthenticator(client, scope) { _, template, _ -> listOf(signer.sign(template.signedFor(relay))) }

    /** Batches written before the socket was signed in, and so through the relay's publishers' lane. */
    val unsignedBatches = AtomicLong()

    override val description = "to the relay at ${dial.url}, signed in as ${signer.pubKey.take(12)}…"

    // The relay's ingest verifies every EVENT a connection sends, on either lane, and answers a
    // bad one `invalid: bad signature or id`: a second check here would only repeat it.
    override val checksSignatures = true

    init {
        client.keepConnected(dial)
        client.connect()
    }

    private val redialing = scope.launch { redial() }

    /**
     * Dials the relay again every [redialMs] while it is down, whatever the client's back-off says.
     * That back-off doubles to five minutes, and a host name that does not resolve goes there at
     * once, as a lapsed domain would. On compose the relay's name stops resolving whenever its
     * container is down, so a relay that came back after a few minutes was left unreached for up
     * to five more: measured with the relay down 3.5 minutes, the last failed dial was an
     * `UnknownHostException` and the next came 301 s later, more than a minute after the relay was
     * listening, while every batch waited. A dial while it is down costs a name lookup or a
     * refused connect; one already in flight is left to finish.
     */
    private suspend fun redial() {
        while (true) {
            delay(redialMs)
            // A dial that throws must not end the loop, nor reach the scope it runs in.
            try {
                val socket = client.getOrCreateRelay(dial)
                if (!socket.isConnected()) {
                    // So the client's own redial after the next failure starts from a second, too.
                    socket.resetBackoff()
                    socket.connectAndSyncFiltersIfDisconnected(ignoreRetryDelays = true)
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                System.err.println("sync: redialling the relay at ${dial.url} failed: ${e.message}")
            }
        }
    }

    /**
     * Events still waiting on an answer from the relay. Between batches it should be zero: an OK,
     * true or false, takes an event out (Quartz 98089642eb), so only what a dropped socket lost
     * stays, to be re-sent on reconnect.
     */
    fun pendingInOutbox(): Int = client.activeOutboxCache(dial).size

    /** Whether the socket is signed in right now. */
    fun signedIn(): Boolean = authenticator.authSnapshot(dial).phase == RelayAuthSnapshot.Phase.AUTHENTICATED

    /** Polled rather than collected: the state flow's map type is a dependency this module does not carry. */
    private suspend fun awaitSignedIn(): Boolean {
        val deadline = System.nanoTime() + authWaitMs * 1_000_000
        while (!signedIn()) {
            if (System.nanoTime() - deadline >= 0) return false
            delay(AUTH_POLL_MS)
        }
        return true
    }

    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> {
        if (events.isEmpty()) return emptyList()
        // Unsigned only on an open socket: with the relay down nothing goes out on either lane, and
        // the batch fails (and withholds its band) on its own.
        if (!awaitSignedIn() && client.getOrCreateRelay(dial).isConnected()) unsigned()
        // Copies of one id share the relay's one verdict, so each id keeps the slots it fills, in order.
        val slots = HashMap<String, ArrayDeque<Int>>(events.size)
        events.forEachIndexed { i, e -> slots.getOrPut(e.id) { ArrayDeque(1) }.addLast(i) }
        val outcomes = arrayOfNulls<IEventStore.InsertOutcome>(events.size)
        client.publishStream(dial, events.asFlow(), maxInFlight = maxInFlight, idleTimeoutMs = idleTimeoutMs) { event, result ->
            synchronized(outcomes) {
                slots[event.id]?.removeFirstOrNull()?.let { outcomes[it] = outcomeOf(result) }
            }
        }
        // A stalled stream stops reading its input, so the events it never sent have no verdict.
        return outcomes.map { it ?: NOT_SENT }
    }

    /** When the last unsigned batch was logged; a stuck sign-in would otherwise log every batch. */
    private val unsignedLoggedAt = AtomicLong()

    private fun unsigned() {
        val count = unsignedBatches.incrementAndGet()
        val now = System.currentTimeMillis()
        val last = unsignedLoggedAt.get()
        if (now - last < UNSIGNED_LOG_MS || !unsignedLoggedAt.compareAndSet(last, now)) return
        System.err.println(
            "sync: not signed in to the relay at ${dial.url} after ${authWaitMs}ms (NIP-42 ${authenticator.authSnapshot(dial).phase}); " +
                "$count batch(es) so far went through its publishers' lane instead of the mirror lane. Stored all the same, " +
                "but check that this process's RELAY_NSEC is the relay's own",
        )
    }

    override suspend fun insert(event: Event) {
        when (val outcome = batchInsert(listOf(event)).single()) {
            IEventStore.InsertOutcome.Accepted -> Unit
            is IEventStore.InsertOutcome.Rejected -> throw IllegalStateException("the relay refused ${event.id}: ${outcome.reason}")
            is IEventStore.InsertOutcome.Failed -> throw IllegalStateException("the relay could not store ${event.id}: ${outcome.reason}")
        }
    }

    /**
     * Sent as signed [MirrorDelete] requests, at most [MirrorDelete.MAX_IDS] ids each; throws unless
     * every one is answered with the hook's own `OK` ([MirrorDelete.ANSWER_PREFIX]).
     */
    override suspend fun delete(filter: Filter) {
        val ids = filter.ids
        require(!ids.isNullOrEmpty()) { "the relay deletes only by id" }
        for (chunk in ids.chunked(MirrorDelete.MAX_IDS)) {
            val request = signer.sign(MirrorDelete.template(filter.copy(ids = chunk)))
            var answer: PublishResult? = null
            client.publishStream(dial, listOf(request).asFlow(), idleTimeoutMs = idleTimeoutMs) { _, result -> answer = result }
            val result = answer
            check(result != null && result.accepted) { "the relay did not delete ${chunk.size} id(s): ${result?.message ?: "no answer"}" }
            // An OK true without the hook's answer is a relay that stored nothing and deleted nothing.
            check(result.message.startsWith(MirrorDelete.ANSWER_PREFIX)) {
                "the relay at ${dial.url} took the delete request for an ordinary event and deleted nothing: it runs no " +
                    "MirrorDelete hook (RELAY_NSEC unset there, or a relay build that predates it)"
            }
        }
    }

    override fun close() {
        redialing.cancel()
        runCatching { client.releaseConnection(dial) }
        runCatching { authenticator.destroy() }
        runCatching { client.close() }
        runCatching {
            okhttp.dispatcher.executorService.shutdown()
            okhttp.connectionPool.evictAll()
        }
    }

    companion object {
        const val AUTH_WAIT_MS = 10_000L

        private const val AUTH_POLL_MS = 50L

        private const val UNSIGNED_LOG_MS = 60_000L

        /** A dial every two seconds while the relay is down; see [redial]. */
        const val REDIAL_MS = 2_000L

        /** Past a pipeline batch's own write time on a loaded relay, so a slow commit is not a lost one. */
        const val IDLE_TIMEOUT_MS = 120_000L

        /**
         * EVENTs one [batchInsert] keeps unanswered, which at the sync's default batch (1,000) is the
         * whole batch at once. The window is per call, and each sync worker writes one batch per call,
         * so Quartz's default 512 held two workers to about 1,000 in flight between them, and the
         * relay's two mirror-lane workers wrote batches of about 290 (where a local write commits
         * about 1,000); every batch pays the store's admission reads once. At 2,000 they average about
         * 550, 95% of events in batches of 512 or more, and a 200k production backfill ran 11% faster
         * (177 s to 157 s, two alternated runs each on a fresh Vespa) on 5% less CPU. The lane's queue
         * holds 4,096, so the relay keeps reading the socket.
         */
        const val IN_FLIGHT = 2_000

        private val NOT_SENT = IEventStore.InsertOutcome.Failed("error: the relay went silent before this event was sent")

        /** OK false prefixes that say "not now" rather than "never": the write is retried, not suppressed. */
        private val TRANSIENT = listOf("error:", "rate-limited:", "auth-required:")

        /**
         * The relay's OK read back as the store outcome it was made from. The relay answers a
         * store rejection in the store's own words, and NIP-01 has it answer a duplicate `OK
         * true` with the `duplicate:` prefix, which is the store's rejection again. A transport
         * failure, `error:`, or a back-off the stream already waited out is [Failed][IEventStore.InsertOutcome.Failed]:
         * the event may well be storable, so the band gate withholds and the refusal filter
         * does not learn the id. Every other refusal is final, as it would be from the store.
         */
        fun outcomeOf(result: PublishResult): IEventStore.InsertOutcome =
            when {
                result.accepted && result.message.startsWith(RejectionReason.PREFIX_DUPLICATE) -> IEventStore.InsertOutcome.Rejected(result.message)
                result.accepted -> IEventStore.InsertOutcome.Accepted
                result.isTransportFailure -> IEventStore.InsertOutcome.Failed("error: ${result.message}")
                TRANSIENT.any { result.message.startsWith(it) } -> IEventStore.InsertOutcome.Failed(result.message)
                else -> IEventStore.InsertOutcome.Rejected(result.message)
            }

        /** [this] AUTH template with its `relay` tag naming [relay], the address the relay checks against. */
        internal fun EventTemplate<RelayAuthEvent>.signedFor(relay: NormalizedRelayUrl): EventTemplate<RelayAuthEvent> = EventTemplate(createdAt, kind, tags.map { if (it.firstOrNull() == "relay") arrayOf("relay", relay.url) else it }.toTypedArray(), content)
    }
}
