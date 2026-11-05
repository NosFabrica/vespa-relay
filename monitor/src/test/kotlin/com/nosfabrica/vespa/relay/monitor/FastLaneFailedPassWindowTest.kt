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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The lane's window moves past a tick only when its read and every one of its passes completed: a pass
 * that threw, or whose standing read failed, leaves its urls to the next tick rather than the next sweep.
 */
class FastLaneFailedPassWindowTest {
    private val fresh = RelayUrlNormalizer.normalize("wss://new.example.com")

    @Test
    fun `a lane tick whose passes failed looks at its window again`(): Unit =
        runBlocking {
            val asked = CopyOnWriteArrayList<Long>()
            val source =
                object : AliasMonitor.CandidateSource {
                    override suspend fun candidates() = emptyList<NormalizedRelayUrl>()

                    override suspend fun candidatesSince(since: Long): List<NormalizedRelayUrl> {
                        asked += since
                        return listOf(fresh)
                    }
                }
            val handed = CopyOnWriteArrayList<List<NormalizedRelayUrl>>()
            // Anything the pass's guard swallows.
            val failing =
                AliasMonitor.Pass { _, urls ->
                    handed += urls
                    if (handed.size == 1) error("the pass failed") else 0
                }
            val scope = CoroutineScope(Dispatchers.Default)
            try {
                // Over a second apart, so a window that moved on would start at a later second.
                AliasMonitor(
                    emptyList(),
                    scope,
                    startupDelayMs = 0,
                    emptyRetryMs = 60_000,
                    source = source,
                    fastLaneEveryMs = 1_100,
                    fastLanePasses = listOf(failing),
                    retryEveryMs = null,
                ).start()
                withTimeout(10_000) { while (asked.size < 2) delay(20) }
            } finally {
                scope.cancel()
            }
            assertEquals(asked[0], asked[1], "the window of a tick whose only pass failed was skipped; its urls wait for the next sweep")
            assertEquals(listOf(fresh), handed[1], "the retry did not hand the pass the url the failed tick named")
        }

    /** Reads fail, writes land. */
    private class Unreadable(
        private val inner: IEventStore,
    ) : IEventStore by inner {
        override suspend fun <T : Event> query(filter: Filter): List<T> = throw IllegalStateException("the store cannot answer")
    }

    @Test
    fun `a lane tick whose standing read failed looks at its window again`(): Unit =
        runBlocking {
            val asked = CopyOnWriteArrayList<Long>()
            val source =
                object : AliasMonitor.CandidateSource {
                    override suspend fun candidates() = emptyList<NormalizedRelayUrl>()

                    override suspend fun candidatesSince(since: Long): List<NormalizedRelayUrl> {
                        asked += since
                        return listOf(fresh)
                    }
                }
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
            val probe = AliasProbe(fetch = { _, _, _, _ -> error("a pass without its standing verdicts dialled") }, target = 40, page = 40, fallbackPage = 40)
            val stability = ConsistencyPass(consistency = RelayConsistency(), record = RelayVerdictRecord(Unreadable(store), NostrSignerInternal(KeyPair())), probe = probe)
            val scope = CoroutineScope(Dispatchers.Default)
            try {
                AliasMonitor(
                    emptyList(),
                    scope,
                    startupDelayMs = 0,
                    emptyRetryMs = 60_000,
                    source = source,
                    fastLaneEveryMs = 1_100,
                    fastLanePasses = listOf(stability),
                    retryEveryMs = null,
                ).start()
                withTimeout(10_000) { while (asked.size < 2) delay(20) }
            } finally {
                scope.cancel()
            }
            assertEquals(asked[0], asked[1], "the window of a tick whose pass could not read its standing verdicts was skipped")
        }
}
