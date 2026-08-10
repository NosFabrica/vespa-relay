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
package com.nosfabrica.vespa.relay.router

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAll
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.time.Duration
import kotlin.test.Test

/**
 * DIAGNOSTIC for issue #91, off by default: asks the provider relay the same
 * question `deleteMissing` asks it, using only what the relay itself hands
 * back — no store involved.
 *
 * Three questions, in order:
 *
 *  1. **How big is the relay's negentropy answer?** Reconcile with an EMPTY
 *     local set: every id it holds for the ask comes back as `need`. That is
 *     the set `deleteMissing` measures our records against.
 *  2. **Is the relay self-consistent?** Reconcile again with the set from (1)
 *     as ours. Two identical sets must diff to nothing; anything else is the
 *     relay disagreeing with itself between two NEG-OPENs.
 *  3. **Does it serve what it reconciles?** Page a sample of what it SERVES
 *     over REQ and check each id for membership in (1). An id the relay
 *     serves but leaves out of its negentropy answer is a false "have" — the
 *     exact record `deleteMissing = true` would delete.
 *  4. With `-Dissue91Page=true`, page the WHOLE served set and reconcile it
 *     back. A completed reconcile against records we watched it serve must
 *     propose deleting nothing.
 *
 * `./gradlew :sync:test --tests '*Issue91LiveProbe*' -Dissue91Probe=true --rerun`
 *
 * **What it answered, 2026-08-10.** For the provider the issue names, that
 * relay holds `118,807` kind-30382 — by negentropy and by REQ, the same ids
 * both ways (`servedButNotOffered = 0`), stable across two sessions, spanning
 * `2026-04-25 12:07` to `2026-08-03 17:27` UTC, and reconciling back to
 * `need = 0, have = 0`. The dry run reports `150784/269591`, and
 * `269591 - 150784 = 118807` exactly: the diff is our set minus theirs, to the
 * record. Nothing here is reporting absent what the relay serves — it serves
 * 118,807 of the 269,591 we hold, and negentropy says so precisely.
 */
class Issue91LiveProbe {
    private val relayUrl = System.getProperty("issue91Relay") ?: "wss://nip85.nosfabrica.com"
    private val author =
        System.getProperty("issue91Author")
            ?: "a1420e44905bf64c4f73b2f072a47e28ff3a0b27a8713da8d9ac32c49e549309"

    @Test
    fun whatDoesTheProviderRelayActuallyAnswer() {
        if (System.getProperty("issue91Probe") != "true") {
            println("[skip] Issue91LiveProbe — set -Dissue91Probe=true to dial the public internet")
            return
        }
        val url = RelayUrlNormalizer.normalize(relayUrl)
        val ask = Filter(kinds = listOf(30382), authors = listOf(author))
        val okhttp =
            OkHttpClient
                .Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .readTimeout(Duration.ofMinutes(10))
                .pingInterval(Duration.ofSeconds(120))
                .build()
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, CoroutineScope(SupervisorJob()))

        runBlocking {
            try {
                // (1) their whole set, as ids
                var started = System.currentTimeMillis()
                val theirs = client.negentropyReconcileIds(url, ask, emptyList(), idleTimeoutMs = 300_000L)
                println(
                    "probe: EMPTY-local reconcile — they offered ${theirs.needIds.size} id(s)," +
                        " have=${theirs.haveIds.size}, windows=${theirs.windows}," +
                        " ${System.currentTimeMillis() - started}ms",
                )
                val theirIds = theirs.needIds.toHashSet()
                println("probe: distinct ids offered = ${theirIds.size}")

                // (2) self-consistency: reconcile their own set back at them.
                //     We do not know their created_at, so ask them again the same
                //     way a second time and compare the two answers as SETS.
                started = System.currentTimeMillis()
                val second = client.negentropyReconcileIds(url, ask, emptyList(), idleTimeoutMs = 300_000L)
                val secondIds = second.needIds.toHashSet()
                println(
                    "probe: second EMPTY-local reconcile — ${second.needIds.size} id(s)," +
                        " windows=${second.windows}, ${System.currentTimeMillis() - started}ms;" +
                        " onlyInFirst=${(theirIds - secondIds).size} onlyInSecond=${(secondIds - theirIds).size}",
                )

                // (3) what they SERVE, sampled from both ends of the timeline,
                //     checked for membership in the negentropy answer.
                for (
                (label, sample) in
                listOf(
                    "newest" to ask.copy(limit = 500),
                    "oldest" to ask.copy(limit = 500, until = 1_700_000_000L),
                )
                ) {
                    val served = client.fetchAll(url, listOf(sample), 120_000L)
                    val missing = served.filter { it.id !in theirIds }
                    println(
                        "probe: $label served=${served.size}" +
                            " notInNegentropyAnswer=${missing.size}" +
                            (missing.take(3).joinToString(prefix = " e.g. ") { "${it.id.take(12)}@${it.createdAt}" }),
                    )
                }

                // (4) the decisive one: page everything the relay SERVES for
                //     the ask, then reconcile that exact set back at it. A
                //     completed reconcile against records we watched it serve
                //     must propose deleting nothing.
                if (System.getProperty("issue91Page") == "true") {
                    started = System.currentTimeMillis()
                    val mine = ArrayList<IdAndTime>()
                    client.fetchAllPages(url, listOf(ask), 300_000L) { e -> mine += IdAndTime(e.createdAt, e.id) }
                    val distinct = mine.distinctBy { it.id }
                    println(
                        "probe: served created_at span ${distinct.minOf { it.createdAt }}" +
                            " .. ${distinct.maxOf { it.createdAt }}",
                    )
                    println(
                        "probe: paged ${mine.size} served event(s) (${distinct.size} distinct id(s))" +
                            " in ${System.currentTimeMillis() - started}ms;" +
                            " servedButNotOffered=${distinct.count { it.id !in theirIds }}",
                    )
                    started = System.currentTimeMillis()
                    val back = client.negentropyReconcileIds(url, ask, distinct, idleTimeoutMs = 300_000L)
                    println(
                        "probe: RECONCILE-BACK mine=${distinct.size} need=${back.needIds.size}" +
                            " have=${back.haveIds.size} (${back.haveIds.size * 100.0 / distinct.size}%)" +
                            " windows=${back.windows} in ${System.currentTimeMillis() - started}ms",
                    )
                }
            } catch (e: Exception) {
                println("probe: FAILED — ${e.javaClass.simpleName}: ${e.message}")
            } finally {
                runCatching { client.disconnect() }
            }
        }
    }
}
