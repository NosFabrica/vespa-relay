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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.ingest.refused.RefusedIds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PublishResult
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A heal pass runs inside a visit, so however the relay answers, the pass ends soon and gives back what it did not try. */
class HealerTest {
    private val url = RelayUrlNormalizer.normalize("wss://heal.example")

    private class Setup(
        val healer: Healer,
        val queue: HealQueue,
        val caps: WriteCapability,
        val refused: RefusedIds,
        val pushes: AtomicInteger,
        val stale: List<StaleRef>,
    )

    /** [n] authors with a profile in the store, each queued as a repair for [url]. */
    private suspend fun setup(
        scope: CoroutineScope,
        n: Int,
        settings: HealSettings,
        answer: suspend (Event) -> PublishResult?,
    ): Setup {
        val store = NostrSemanticsStore(InMemoryEventIndex())
        val queue = HealQueue()
        val stale = ArrayList<StaleRef>()
        repeat(n) {
            val profile: Event = NostrSignerSync().sign(1_700_000_000L + it, 0, emptyArray(), "{}")
            store.insert(profile)
            val ref = StaleRef("%064x".format(it), 1_699_000_000L)
            stale += ref
            queue.offer(url, HealKey.content(0, profile.pubKey, null), ref)
        }
        val caps = WriteCapability()
        val refused = RefusedIds(Files.createTempDirectory("heal-refused").toFile())
        val pushes = AtomicInteger()
        val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp3.OkHttpClient() }, scope)
        val healer =
            Healer(client, store, queue, caps, refused, null, settings) { event, _: NormalizedRelayUrl, _ ->
                pushes.incrementAndGet()
                answer(event)
            }
        return Setup(healer, queue, caps, refused, pushes, stale)
    }

    private val quick = HealSettings(pacePerPushMs = 0)

    @Test
    fun `a relay that never answers ends the pass after a few silences and keeps the rest queued`(): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val s = setup(scope, 20, quick) { PublishResult(false, PublishResult.NO_RESPONSE) }
                s.healer.drain(url)
                assertEquals(quick.maxSilentInARow, s.pushes.get(), "silence in a row ends the pass")
                assertEquals(quick.maxSilentInARow, s.caps.state(url).strikes, "and every timeout is a strike")
                assertEquals(20 - quick.maxSilentInARow, s.queue.sizeFor(url), "the repairs never tried wait for the next pass")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a push that comes back with no answer at all is a strike`(): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val s = setup(scope, 1, quick) { null }
                s.healer.drain(url)
                assertEquals(1, s.caps.state(url).strikes)
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `a slow but answering relay is held to the pass's budget`(): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                val budgeted = quick.copy(passBudgetMs = 150)
                val s =
                    setup(scope, 40, budgeted) {
                        delay(50)
                        PublishResult(false, "rate-limited: slow down")
                    }
                val startedMs = System.currentTimeMillis()
                s.healer.drain(url)
                assertTrue(System.currentTimeMillis() - startedMs < 40 * 50, "the pass stopped long before its work ran out")
                assertTrue(s.pushes.get() < 40)
                assertEquals(40 - s.pushes.get(), s.queue.sizeFor(url), "and handed the rest back")
            } finally {
                scope.cancel()
            }
        }

    @Test
    fun `auth-required neither closes the relay nor suppresses the stale id`(): Unit =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            try {
                var authed = false
                val s = setup(scope, 5, quick) { if (authed) PublishResult(true, "") else PublishResult(false, "auth-required: sign in") }
                s.healer.drain(url)
                assertFalse(s.caps.isClosed(url), "a login prompt is not a policy")
                assertTrue(s.stale.none { s.refused.suppressed(it.id, it.createdAt) }, "and the served copies stay repairable")
                assertEquals(4, s.queue.sizeFor(url), "the pass ends at the prompt and keeps what it did not try")

                authed = true
                s.healer.drain(url)
                assertEquals(4L, s.healer.accepted.get(), "once authenticated, the next pass delivers")
            } finally {
                scope.cancel()
            }
        }
}
