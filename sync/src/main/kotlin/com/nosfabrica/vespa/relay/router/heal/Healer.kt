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
package com.nosfabrica.vespa.relay.router.heal

import com.nosfabrica.vespa.relay.router.refused.RefusedIds
import com.nosfabrica.vespa.relay.server.ServingPressure
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
 * Which of an author's vanish requests a given relay is allowed to be handed.
 *
 * **SAFETY, and the reason this is its own function rather than a line inside
 * `resolve`.** A kind 62 scoped to one relay is an instruction the author
 * addressed to that relay alone. Handing it to a third party leaks where they
 * are leaving and invites a lax relay to act on someone else's retraction —
 * irreversible if it does. `shouldVanishFrom` is the exact predicate: it
 * passes `ALL_RELAYS` and the relay's own url, and nothing else.
 *
 * The filter runs BEFORE the newest-wins pick, never after. Taking the newest
 * kind 62 first and checking its scope second lets a newer relay-scoped
 * request mask an older `ALL_RELAYS` one that this relay genuinely should
 * receive — the push would then be skipped rather than misdirected, which is
 * safe but wrong, and silently so.
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
 * Hands upstreams the thing that supersedes the stale copy they served us.
 *
 * **The repair, as opposed to the cache.** A tombstone is private memory,
 * exactly as durable as one disk — rotate an epoch, wipe the volume, redeploy,
 * and every id it was suppressing comes back. A healed source stays healed,
 * and it stays healed for every other mirror too.
 *
 * **Consent comes from the trigger, not from a setting.** A repair can only be
 * queued because the relay served us its own stale copy of that author's event,
 * so the target already hosts the author's data and the push changes the
 * *version* it serves, never the *distribution set*. This is why there is no
 * path here that reaches a relay which has never seen the author — the queue is
 * fed by store refusals, and a relay that never sent us anything cannot produce
 * one. Introducing an author to a new relay remains `dir = up`'s job.
 *
 * **Off the hot path by construction.** The sweep only ever put an address in a
 * map. Everything expensive — resolving the winner, the publish, waiting for
 * the `OK` — happens here, per relay, at the end of that relay's own sync while
 * its socket is still open, yielding to [ServingPressure] exactly as ingest
 * does.
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
     * Push everything queued for [url]. Called at the end of that relay's sync,
     * before the socket is released — a `relaySource` stream keeps no live
     * tail, so a fully detached healer would have to re-dial thousands of
     * relays just to publish.
     */
    suspend fun drain(url: NormalizedRelayUrl) {
        if (caps.isClosed(url)) {
            // Nothing to learn and nothing to gain: it has already told us.
            queue.discard(url)
            return
        }
        // Take only what this pass will attempt. Draining the whole queue and
        // then breaking out at the cap threw the remainder away: the entries
        // were already out of the queue, and an id whose repair was dropped
        // that way is re-queued only if it is refused again — which stops
        // happening as soon as it is suppressed. The relay then stayed stale
        // forever with nothing left to say so.
        val work = queue.drain(url, settings.maxPerPass)
        if (work.isEmpty()) return

        val pass = passes.incrementAndGet()
        // One lookup per author per pass, not per push: the guard below is a
        // store query and the same author routinely appears many times.
        val vanishCache = HashMap<String, Boolean>()
        var attempted = 0
        var accepts = 0

        for ((key, stale) in work) {
            // No cap check here: `drain` already took at most maxPerPass, so
            // the bound is applied where the entries are still recoverable
            // rather than after they have left the queue.
            if (caps.isClosed(url)) break
            servingPressure?.backoffMs()?.takeIf { it > 0 }?.let { delay(it) }

            try {
                // SAFETY: the one case where "the relay already holds their
                // data" is NOT consent. The author asked to leave this relay
                // and it kept serving them anyway; pushing their content would
                // re-establish someone who requested removal. The repair for
                // that pair is the vanish request itself, which the retraction
                // path handles.
                if (key.mode == HealMode.CONTENT && vanishedFrom(key.pubkey, url, vanishCache)) {
                    skippedVanished.incrementAndGet()
                    continue
                }

                val event = resolve(key, url) ?: continue
                attempted++
                val result = client.publishAndCollectResults(event, setOf(url), settings.okTimeoutSeconds)[url] ?: continue
                pushed.incrementAndGet()

                when (OkClassifier.classify(result.accepted, result.message, result.isTransportFailure)) {
                    PushVerdict.ACCEPTED -> {
                        caps.succeeded(url)
                        accepted.incrementAndGet()
                        accepts++
                    }

                    PushVerdict.CLOSED -> {
                        caps.close(url, result.message)
                        refusedByPolicy.incrementAndGet()
                        // Certain on its own: it will refuse the next repair
                        // identically, so the id it is serving is suppressed
                        // without waiting for a second refusal. Everything else
                        // still queued falls to the ordinary two-refusal gate.
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
     * What to hand over, resolved NOW rather than when the address was queued.
     * That is what makes the deferral safe against its own races: a version
     * superseded again since the enqueue pushes the newest, and an event
     * retracted since the enqueue pushes the retraction instead.
     */
    private suspend fun resolve(
        key: HealKey,
        url: NormalizedRelayUrl,
    ): Event? =
        when (key.mode) {
            HealMode.CONTENT -> {
                store
                    .query<Event>(
                        Filter(
                            kinds = listOf(key.kind),
                            authors = listOf(key.pubkey),
                            tags = key.dTag?.takeIf { it.isNotEmpty() }?.let { mapOf("d" to listOf(it)) },
                        ),
                    ).maxByOrNull { it.createdAt }
            }

            HealMode.DELETION -> {
                store
                    .query<Event>(
                        Filter(
                            kinds = listOf(5),
                            authors = listOf(key.pubkey),
                            tags = key.victimId?.let { mapOf("e" to listOf(it)) },
                        ),
                    ).maxByOrNull { it.createdAt }
            }

            HealMode.VANISH -> {
                VanishTargets.pushableTo(
                    store
                        .query<Event>(Filter(kinds = listOf(RequestToVanishEvent.KIND), authors = listOf(key.pubkey)))
                        .filterIsInstance<RequestToVanishEvent>(),
                    url,
                )
            }
        }

    private suspend fun vanishedFrom(
        pubkey: String,
        url: NormalizedRelayUrl,
        cache: MutableMap<String, Boolean>,
    ): Boolean =
        cache.getOrPut(pubkey) {
            store
                .query<Event>(Filter(kinds = listOf(RequestToVanishEvent.KIND), authors = listOf(pubkey)))
                .filterIsInstance<RequestToVanishEvent>()
                .any { it.shouldVanishFrom(url) }
        }

    fun summary(): String =
        "heal ${pushed.get()} pushed, ${accepted.get()} accepted, ${refusedByPolicy.get()} refused, " +
            "${caps.closedCount()}/${caps.probedCount()} relay(s) write-closed, ${queue.size()} queued" +
            (if (queue.dropped.get() > 0) ", ${queue.dropped.get()} dropped" else "")
}
