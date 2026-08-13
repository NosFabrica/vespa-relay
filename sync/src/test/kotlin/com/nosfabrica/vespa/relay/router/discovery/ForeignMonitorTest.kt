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
package com.nosfabrica.vespa.relay.router.discovery

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip66RelayMonitor.reachability.RelayMonitor
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * **WHOSE NIP-66 RECORDS DECIDE WHICH RELAYS THIS ROUTER SKIPS?**
 *
 * The fan-out asks `RelayMonitor.deadSet()` for relays not worth dialling
 * ([DynamicSync] builds `HostStrikes(knownDead = …)` from it), and kind 30166
 * is a PUBLIC kind that this router deliberately mirrors: the documented outbox
 * source reads `kinds = [10002, 10050, 30002, 30166]`, because other monitors'
 * reports are one of the ways relays are discovered at all. So the store holds
 * strangers' reachability claims by design.
 *
 * If the dead set is not scoped to our own monitor, then anyone whose 30166
 * records we mirror can take relays out of our mirror by declaring them dead —
 * and the router would report it as `N relay(s) skipped on earlier NIP-66
 * records`, indistinguishable from its own measurement.
 *
 * This pins the answer against quartz's real code rather than against a reading
 * of it: two monitors over ONE store, and the question is whether one can see
 * the other's verdict.
 */
class ForeignMonitorTest {
    private val okhttp = OkHttpClient.Builder().build()

    @Test
    fun `the dead set is not scoped to our own monitor`() =
        runBlocking {
            val scope = CoroutineScope(SupervisorJob())
            val store = NostrSemanticsStore(InMemoryEventIndex(), relay = null)
            val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
            val dead = RelayUrlNormalizer.normalize("wss://declared-dead.example")
            try {
                // A STRANGER's monitor, writing into the same store — which is
                // exactly what mirroring somebody else's 30166 amounts to.
                val theirs =
                    RelayMonitor(
                        client = client,
                        store = store,
                        scope = scope,
                        signer = NostrSignerInternal(KeyPair()),
                        onError = {},
                    )
                theirs.observer.record(dead, reachable = false, error = "declared dead by someone else")
                theirs.flush()

                // OURS, with a different key, reading the same store.
                val ours =
                    RelayMonitor(
                        client = client,
                        store = store,
                        scope = scope,
                        signer = NostrSignerInternal(KeyPair()),
                        onError = {},
                    )
                ours.refresh()

                // Documenting what quartz DOES, not what it should: the
                // reachability snapshot is queried without an `authors` filter,
                // so a record we never signed lands in the set the fan-out
                // skips on. If this assertion ever flips, quartz has started
                // scoping it and the local guard in SyncEngine can go.
                assertEquals(
                    setOf(dead),
                    ours.deadSet(),
                    "quartz began scoping the reachability snapshot by author — re-check whether the router still needs its own filter",
                )
            } finally {
                runCatching { client.disconnect() }
                scope.cancel()
            }
        }
}
