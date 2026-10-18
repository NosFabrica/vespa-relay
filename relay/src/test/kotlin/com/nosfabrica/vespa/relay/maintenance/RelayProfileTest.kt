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
package com.nosfabrica.vespa.relay.maintenance

import com.nosfabrica.vespa.eventstore.NostrSemanticsStore
import com.nosfabrica.vespa.eventstore.engine.memory.InMemoryEventIndex
import com.nosfabrica.vespa.relay.server.Nip11Info
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerInternal
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayInfo
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayType
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The relay's own kind 0 and kind 10002, against a real store so replaceable semantics are its own. */
class RelayProfileTest {
    private val relayUrl = RelayUrlNormalizer.normalize("wss://relay.example")
    private val signer = NostrSignerInternal(KeyPair())
    private val info =
        Nip11Info(
            name = "Example Relay",
            description = "A trust-ranked search relay",
            icon = "https://relay.example/icon.png",
            banner = "https://relay.example/banner.png",
        )

    private fun newStore() = NostrSemanticsStore(InMemoryEventIndex(), relay = relayUrl)

    private fun profile(
        store: IEventStore,
        at: Long = 1_800_000_000L,
    ) = RelayProfile(store, signer, relayUrl, nowSeconds = { at })

    private suspend fun stored(
        store: IEventStore,
        kind: Int,
    ): Event? = store.query<Event>(Filter(kinds = listOf(kind), authors = listOf(signer.pubKey))).maxByOrNull { it.createdAt }

    private fun contentOf(event: Event): Map<String, String> = (Json.parseToJsonElement(event.content) as JsonObject).mapValues { it.value.jsonPrimitive.content }

    /** The `r` tags as (url, marker); an absent marker means both directions, per NIP-65. */
    private fun relaysOf(event: Event): List<Pair<String, String?>> = event.tags.filter { it.size > 1 && it[0] == "r" }.map { it[1] to it.getOrNull(2) }

    @Test
    fun `publishes both kinds against an empty store`() =
        runBlocking {
            val store = newStore()
            val report = profile(store).publish(info)
            assertEquals(listOf(0, 10002), report.published())

            val metadata = stored(store, MetadataEvent.KIND) ?: error("no kind 0 was published")
            assertEquals(signer.pubKey, metadata.pubKey)
            assertTrue(metadata.verify(), "the relay signs its own profile")
            val content = contentOf(metadata)
            assertEquals("Example Relay", content["name"])
            assertEquals("Example Relay", content["display_name"])
            assertEquals("A trust-ranked search relay", content["about"])
            assertEquals("https://relay.example/icon.png", content["picture"])
            assertEquals("https://relay.example/banner.png", content["banner"])

            val list = stored(store, AdvertisedRelayListEvent.KIND) ?: error("no kind 10002 was published")
            assertTrue(list.verify(), "the relay signs its own relay list")
            // No marker: inbox and outbox both.
            assertEquals(listOf("wss://relay.example/" to null), relaysOf(list))
        }

    @Test
    fun `a second pass publishes nothing`() =
        runBlocking {
            val store = newStore()
            profile(store).publish(info)
            val first = stored(store, MetadataEvent.KIND)?.id to stored(store, AdvertisedRelayListEvent.KIND)?.id

            // A later boot with the clock moved on writes nothing, so neither `created_at` creeps.
            val report = profile(store, at = 1_800_009_999L).publish(info)
            assertEquals(emptyList(), report.published())
            assertEquals(first, stored(store, MetadataEvent.KIND)?.id to stored(store, AdvertisedRelayListEvent.KIND)?.id)
        }

    @Test
    fun `a changed description republishes the kind 0 and leaves the relay list alone`() =
        runBlocking {
            val store = newStore()
            profile(store).publish(info)
            val listBefore = stored(store, AdvertisedRelayListEvent.KIND)?.id

            val renamed = profile(store, at = 1_800_000_100L).publish(info.copy(description = "Now with NIP-50 search"))
            assertEquals(listOf(0), renamed.published())
            assertEquals("Now with NIP-50 search", contentOf(stored(store, MetadataEvent.KIND)!!)["about"])
            assertEquals(listBefore, stored(store, AdvertisedRelayListEvent.KIND)?.id, "the relay list said nothing about the description")
        }

    @Test
    fun `keeps profile fields it does not own, and clears the ones it does`() =
        runBlocking {
            val store = newStore()
            // Published by hand for this key: fields this writer does not own, plus an `about` it does.
            store.insert(
                signer.sign<MetadataEvent>(
                    MetadataEvent.createNew(
                        name = "typed in by hand",
                        about = "typed in by hand",
                        lnAddress = "relay@example.com",
                        nip05 = "_@relay.example",
                        createdAt = 1_700_000_000L,
                    ),
                ),
            )

            profile(store).publish(Nip11Info(name = "Example Relay"))

            val content = contentOf(stored(store, MetadataEvent.KIND)!!)
            assertEquals("relay@example.com", content["lud16"], "a field this writer does not own survives the edit")
            assertEquals("_@relay.example", content["nip05"])
            assertEquals("Example Relay", content["name"], "the NIP-11 name replaces what was there")
            // An `about` left behind by a removed description is the drift this mirrors away.
            assertNull(content["about"])
            assertNull(content["picture"])
        }

    /**
     * quartz's `updateFromPast` rebuilds `i` tags through `IdentityClaimTag.parse`,
     * which drops a claim with no proof and truncates an identity at its second colon.
     */
    @Test
    fun `carries identity claims across the edit exactly as it found them`() =
        runBlocking {
            val store = newStore()
            val claims =
                arrayOf(
                    arrayOf("i", "github:alice", "https://gist.github.com/alice/1"),
                    // No proof: quartz's rewrite drops it.
                    arrayOf("i", "telegram:alice"),
                    // A second colon: quartz's rewrite truncates it.
                    arrayOf("i", "matrix:@alice:example.org", "https://example.org/proof"),
                )
            store.insert(
                signer.sign<MetadataEvent>(
                    MetadataEvent.createNew(name = "before", createdAt = 1_700_000_000L, initializer = { claims.forEach { add(it) } }),
                ),
            )

            profile(store).publish(info)

            val after = stored(store, MetadataEvent.KIND)!!.tags.filter { it.firstOrNull() == "i" }
            assertEquals(claims.map { it.toList() }, after.map { it.toList() })
            // Settles: restoring the tags must not itself read as a change on the next pass.
            assertEquals(emptyList(), profile(store, at = 1_800_000_500L).publish(info).published())
        }

    @Test
    fun `keeps other relays in an existing list and upgrades our own entry to read and write`() =
        runBlocking {
            val store = newStore()
            val mirror = RelayUrlNormalizer.normalize("wss://mirror.example")
            store.insert(
                AdvertisedRelayListEvent.create(
                    listOf(
                        AdvertisedRelayInfo(mirror, AdvertisedRelayType.WRITE),
                        AdvertisedRelayInfo(relayUrl, AdvertisedRelayType.READ),
                    ),
                    signer,
                    1_700_000_000L,
                ),
            )

            // The kind 0 rides along because this store holds none.
            assertEquals(listOf(0, 10002), profile(store).publish(info).published())

            val relays = relaysOf(stored(store, AdvertisedRelayListEvent.KIND)!!)
            assertEquals(
                listOf("wss://mirror.example/" to "write", "wss://relay.example/" to null),
                relays,
                "somebody else's entry is untouched; ours becomes both directions, once",
            )
        }

    @Test
    fun `a store that cannot answer publishes nothing`() =
        runBlocking {
            // A throwing read must not read as "no profile stored", which would overwrite a richer one.
            val inner = newStore()
            val store =
                object : IEventStore by inner {
                    override suspend fun <T : Event> query(filter: Filter): List<T> = throw IllegalStateException("Backend communication error")
                }

            assertFailsWith<IllegalStateException> { profile(store).publish(info) }
            assertNull(stored(inner, MetadataEvent.KIND))
            assertNull(stored(inner, AdvertisedRelayListEvent.KIND))
        }

    private fun nameOf(event: Event?): String? = event?.let { contentOf(it)["name"] }

    /** Polls [store] until the newest kind 0 is named [name]; the publisher runs on its own job. */
    private suspend fun awaitName(
        store: IEventStore,
        name: String,
    ) {
        val deadline = System.currentTimeMillis() + 10_000
        while (nameOf(stored(store, MetadataEvent.KIND)) != name) {
            if (System.currentTimeMillis() > deadline) error("the newest kind 0 never said \"$name\": ${nameOf(stored(store, MetadataEvent.KIND))}")
            delay(10)
        }
    }

    /** Renames that land while a publish is in flight end with the last one signed as the newest kind 0. */
    @Test
    fun `the newest kind 0 is the last document asked for, however the publishes overlap`() =
        runBlocking {
            val inner = newStore()
            val held = CompletableDeferred<Unit>()
            val kind0Writes = AtomicInteger(0)
            val store =
                object : IEventStore by inner {
                    override suspend fun insert(event: Event) {
                        if (event.kind == MetadataEvent.KIND && kind0Writes.incrementAndGet() == 1) held.await()
                        inner.insert(event)
                    }
                }
            val current = AtomicReference(info.copy(name = "A"))
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                val publisher = RelayProfilePublisher(scope, profile(store), current::get, log = {})
                publisher.request()
                while (kind0Writes.get() == 0) delay(5)
                for (name in listOf("B", "C", "D")) {
                    current.set(info.copy(name = name))
                    publisher.request()
                }
                held.complete(Unit)
                awaitName(inner, "D")
                delay(200)
                assertEquals("D", nameOf(stored(inner, MetadataEvent.KIND)), "an older document was signed after the newest")
                assertEquals(2, kind0Writes.get(), "three renames during one publish fold into one more")
            } finally {
                scope.cancel()
            }
        }

    /** A publish waiting out a cold engine signs the document as it stands when the engine answers. */
    @Test
    fun `a retry publishes the document current when it runs, not the one it was asked with`() =
        runBlocking {
            val inner = newStore()
            val cold = AtomicBoolean(true)
            val signed = java.util.Collections.synchronizedList(mutableListOf<String?>())
            val store =
                object : IEventStore by inner {
                    override suspend fun <T : Event> query(filter: Filter): List<T> {
                        if (cold.get()) throw IllegalStateException("Backend communication error")
                        return inner.query(filter)
                    }

                    override suspend fun insert(event: Event) {
                        if (event.kind == MetadataEvent.KIND) signed += nameOf(event)
                        inner.insert(event)
                    }
                }
            val current = AtomicReference(info.copy(name = "old"))
            // The engine warms on the first attempt to read the new document, so no attempt that read the old one can succeed.
            val source = { current.get().also { if (it.name == "new") cold.set(false) } }
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                RelayProfilePublisher(scope, profile(store), source, retryMs = 10, log = {}).request()
                delay(50)
                current.set(info.copy(name = "new"))
                awaitName(inner, "new")
                assertEquals(listOf<String?>("new"), signed.toList(), "the stale document was never signed")
            } finally {
                scope.cancel()
            }
        }
}
