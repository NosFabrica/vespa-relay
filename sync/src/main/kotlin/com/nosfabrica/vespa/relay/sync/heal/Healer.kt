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
package com.nosfabrica.vespa.relay.sync.heal

import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.publishAndCollectResults
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import java.util.concurrent.atomic.AtomicLong

/**
 * Which of an author's vanish requests a relay may be handed: `ALL_RELAYS` and the relay's
 * own url, nothing else. The scope filter runs before the newest-wins pick, or a newer
 * relay-scoped request would mask an older `ALL_RELAYS` one.
 */
internal object VanishTargets {
    fun pushableTo(
        candidates: List<RequestToVanishEvent>,
        url: NormalizedRelayUrl,
    ): RequestToVanishEvent? = candidates.filter { it.shouldVanishFrom(url) }.maxByOrNull { it.createdAt }
}

/** How hard the healer is allowed to push. */
data class HealSettings(
    val pacePerPushMs: Long = 40,
    val okTimeoutSeconds: Long = 15,
    /** Repairs one drain pass may attempt before leaving the rest for next time. */
    val maxPerPass: Int = 500,
)

/**
 * Hands upstreams the thing that supersedes the stale copy they served us. A repair can only
 * be queued because the relay served its own stale copy, so the push changes the version a
 * relay serves and never the distribution set.
 */
class Healer(
    private val client: INostrClient,
    private val store: IEventStore,
    private val queue: HealQueue,
    private val caps: WriteCapability,
    private val refused: RefusedIds,
    private val servingPressure: ServingPressure?,
    private val settings: HealSettings = HealSettings(),
) {
    val pushed = AtomicLong()
    val accepted = AtomicLong()
    val refusedByPolicy = AtomicLong()
    val skippedVanished = AtomicLong()
    private val passes = AtomicLong()

    /**
     * Pushes what is queued for [url], at the end of that relay's visit while its connection
     * is live. Usually the previous visit's discoveries: this visit's refusals land in the
     * queue after the drain has run, and the queue coalesces, so nothing is lost.
     */
    suspend fun drain(url: NormalizedRelayUrl) {
        if (caps.isClosed(url)) {
            // It has already told us: nothing to learn and nothing to gain.
            queue.discard(url)
            return
        }
        // Take only what this pass will attempt: an entry out of the queue and never pushed
        // is re-queued only on another refusal, which stops once the id is suppressed.
        val work = queue.drain(url, settings.maxPerPass)
        if (work.isEmpty()) return

        val pass = passes.incrementAndGet()
        val vanishCache = HashMap<String, Boolean>()
        var attempted = 0
        var accepts = 0

        for ((key, stale) in work) {
            if (caps.isClosed(url)) break
            servingPressure?.backoffMs()?.takeIf { it > 0 }?.let { delay(it) }

            try {
                // Not consent: the author asked to leave this relay and it kept serving them.
                // The repair for that pair is the vanish request itself.
                if (key.mode == HealMode.CONTENT && vanishedFrom(key.pubkey, url, vanishCache)) {
                    skippedVanished.incrementAndGet()
                    continue
                }

                val event = resolve(key, url) ?: continue
                attempted++
                val result = client.publishAndCollectResults(event, setOf(url), settings.okTimeoutSeconds)[url] ?: continue
                pushed.incrementAndGet()

                val verdict = OkClassifier.classify(result.accepted, result.message, result.isTransportFailure)

                // Any answer refutes the doubt `strike` accumulates, which is about silence.
                // CLOSED is excluded because it is a verdict, not a sign of life.
                if (verdict != PushVerdict.SILENT && verdict != PushVerdict.CLOSED) caps.succeeded(url)

                when (verdict) {
                    PushVerdict.ACCEPTED -> {
                        accepted.incrementAndGet()
                        accepts++
                    }

                    PushVerdict.CLOSED -> {
                        caps.close(url, result.message)
                        refusedByPolicy.incrementAndGet()
                        // It will refuse the next repair identically, so the served id is
                        // suppressed without waiting for a second refusal.
                        refused.suppressNow(stale.id, stale.createdAt)
                        break
                    }

                    PushVerdict.SILENT -> {
                        caps.strike(url, pass)
                    }

                    PushVerdict.RETRY, PushVerdict.IGNORE -> {
                        Unit
                    }
                }
                if (settings.pacePerPushMs > 0) delay(settings.pacePerPushMs)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                System.err.println("router: heal ${url.url} failed on ${key.mode} ${key.pubkey.take(8)}: ${e.message}")
            }
        }

        if (attempted > 0) {
            System.err.println("router: heal ${url.url} pushed $attempted repair(s), $accepts accepted")
        }
    }

    /**
     * What to hand over, resolved now rather than when queued: a version superseded since
     * pushes the newest, and an event retracted since pushes the retraction instead.
     */
    private suspend fun resolve(
        key: HealKey,
        url: NormalizedRelayUrl,
    ): Event? =
        when (key.mode) {
            HealMode.CONTENT -> {
                read(
                    Filter(
                        kinds = listOf(key.kind),
                        authors = listOf(key.pubkey),
                        tags = key.dTag?.takeIf { it.isNotEmpty() }?.let { mapOf("d" to listOf(it)) },
                    ),
                ).maxByOrNull { it.createdAt }
            }

            HealMode.DELETION -> {
                read(
                    Filter(
                        kinds = listOf(5),
                        authors = listOf(key.pubkey),
                        tags = key.victimId?.let { mapOf("e" to listOf(it)) },
                    ),
                ).maxByOrNull { it.createdAt }
            }

            HealMode.VANISH -> {
                VanishTargets.pushableTo(
                    read(Filter(kinds = listOf(RequestToVanishEvent.KIND), authors = listOf(key.pubkey)))
                        .filterIsInstance<RequestToVanishEvent>(),
                    url,
                )
            }
        }

    /** One store read, booked as this subsystem's under [StoreCalls]. */
    private suspend fun read(filter: Filter): List<Event> =
        storeCall(StoreCalls.CALLER_HEAL_RESOLVE, StoreCalls.OP_QUERY, StoreCalls.summarise(filter)) {
            store.query<Event>(filter)
        }

    private suspend fun vanishedFrom(
        pubkey: String,
        url: NormalizedRelayUrl,
        cache: MutableMap<String, Boolean>,
    ): Boolean =
        cache.getOrPut(pubkey) {
            read(Filter(kinds = listOf(RequestToVanishEvent.KIND), authors = listOf(pubkey)))
                .filterIsInstance<RequestToVanishEvent>()
                .any { it.shouldVanishFrom(url) }
        }

    fun summary(): String =
        "heal ${pushed.get()} pushed, ${accepted.get()} accepted, ${refusedByPolicy.get()} refused, " +
            "${caps.closedCount()}/${caps.probedCount()} relay(s) write-closed, ${queue.size()} queued" +
            (if (queue.dropped.get() > 0) ", ${queue.dropped.get()} dropped" else "")
}
