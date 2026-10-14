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

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.config.RouterConfigLoader
import com.nosfabrica.vespa.relay.monitor.AliasFolding
import com.nosfabrica.vespa.relay.monitor.AliasProbe
import com.nosfabrica.vespa.relay.monitor.RelayAliases
import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/** The roster holds one url per server: a fold, or a server the fold has not decided, is one unit. */
class RosterFoldTest {
    private val self = RelayUrlNormalizer.normalize("ws://localhost:7777")
    private val ours = NostrSignerInternal(KeyPair())

    private fun url(raw: String): NormalizedRelayUrl = RelayUrlNormalizer.normalize(raw)

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = self)

    /** The fold's read half, as the engine wires it; nothing here may dial. */
    private fun folding(store: NostrSemanticsStore) =
        AliasFolding(
            aliases = RelayAliases(),
            record = RelayVerdictRecord(store, ours),
            probe = AliasProbe(fetch = { _, _, _, _ -> error("the roster never dials") }),
        )

    private fun roster(
        store: NostrSemanticsStore,
        config: String,
    ) = RosterBuilder(
        store = store,
        streams = RouterConfigLoader.parse(config.trimIndent()).streams,
        bands = SyncBands(null),
        foldedAway = { urls -> folding(store).applyVerdicts(urls).let { it.aliases + it.standIns + it.waiting } },
    )

    private val graded =
        """
        streams {
            graded { dir = "down", filter = { "kinds": [1] }
                relaySource = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ] }
        }
        """

    private suspend fun prime(
        store: NostrSemanticsStore,
        vararg urls: NormalizedRelayUrl,
    ) {
        for (u in urls) RelayVerdictRecord(store, ours).publishFitness(u, "prime", "answered at a settled anchor", pageable = null, nip77 = null)
    }

    @Test
    fun `an alias and its canonical are one unit whatever their grades say`() =
        runBlocking {
            val store = newStore()
            val alias = url("wss://indexer.coracle.social/alpha-charlie")
            val canonical = url("wss://indexer.coracle.social/india")
            prime(store, alias, canonical)
            RelayVerdictRecord(store, ours).publish(alias, canonical, 500, 500)

            assertEquals(setOf(canonical), roster(store, graded).rebuild().asks.keys)
        }

    @Test
    fun `paths the fold has not decided are one unit, the server's stand-in`() =
        runBlocking {
            val store = newStore()
            val root = url("wss://rrr.pupupu.monster/")
            val paths = listOf("mike", "cipher-prism", "beacon-ivory", "whiskey-oscar-hotel", "karma-sierra", "hotel").map { url("wss://rrr.pupupu.monster/$it") }
            prime(store, root, *paths.toTypedArray())

            assertEquals(setOf(root), roster(store, graded).rebuild().asks.keys)
        }

    @Test
    fun `an author bound to a folded url is asked at the url standing in for it`() =
        runBlocking {
            val store = newStore()
            val root = url("wss://relay.nostr.net/")
            val path = url("wss://relay.nostr.net/beacon")
            prime(store, root, path)
            val writers = listOf(NostrSignerSync(KeyPair()), NostrSignerSync(KeyPair()))
            for ((writer, listed) in writers.zip(listOf(root, path))) {
                store.insert(writer.sign<Event>(1_700_000_000L, 10002, arrayOf(arrayOf("r", listed.url)), ""))
            }
            val outbox =
                """
                streams {
                    outbox { dir = "down", filter = { "kinds": [1] }
                        relaySource = [ { select = [ { tag = "r", relay = 1, authors = "pubkey" } ], filter = { "kinds": [10002] } } ]
                        gatedBy = [ { filter = { "kinds": [30166], "#l": ["prime"] } } ] }
                }
                """

            val asks = roster(store, outbox).rebuild().asks

            assertEquals(setOf(root), asks.keys)
            assertEquals(
                writers.map { it.pubKey }.toSet(),
                asks
                    .getValue(root)
                    .values
                    .flatMap { unit -> unit.asks.flatMap { it.filter.authors.orEmpty() } }
                    .toSet(),
            )
        }
}
