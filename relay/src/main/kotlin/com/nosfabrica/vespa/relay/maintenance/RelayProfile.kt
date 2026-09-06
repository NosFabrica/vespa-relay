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

import com.nosfabrica.vespa.relay.server.Nip11Info
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip39ExtIdentities.IdentityClaimTag
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayInfo
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * The relay's own kind 0 and kind 10002, signed with `RELAY_NSEC` and inserted into the store.
 * The NIP-11 doc owns name, description, icon and banner outright; every other field is carried
 * forward. [publish] takes the doc as an argument because NIP-86 RPCs rewrite it at runtime.
 */
class RelayProfile(
    private val store: IEventStore,
    private val signer: NostrSigner,
    val relayUrl: NormalizedRelayUrl,
    private val nowSeconds: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    /** The boot publish and a NIP-86 republish may overlap. */
    private val writing = Mutex()

    val pubKey: String get() = signer.pubKey

    /** [PUBLISHED] wrote an event; [UNCHANGED] found the store already saying it. */
    enum class Outcome {
        PUBLISHED,
        UNCHANGED,
    }

    data class Report(
        val metadata: Outcome,
        val relayList: Outcome,
    ) {
        /** The kinds written, for the boot line. */
        fun published(): List<Int> =
            listOfNotNull(
                MetadataEvent.KIND.takeIf { metadata == Outcome.PUBLISHED },
                AdvertisedRelayListEvent.KIND.takeIf { relayList == Outcome.PUBLISHED },
            )
    }

    /**
     * Bring both events up to date against [info], skipping what the store already says. Throws
     * whatever the store throws: a failed read must never be taken as "nothing stored".
     */
    suspend fun publish(info: Nip11Info): Report =
        writing.withLock {
            Report(metadata = publishMetadata(info), relayList = publishRelayList())
        }

    private suspend fun publishMetadata(info: Nip11Info): Outcome {
        val held = newest(MetadataEvent.KIND)?.let { it as? MetadataEvent ?: MetadataEvent(it.id, it.pubKey, it.createdAt, it.tags, it.content, it.sig) }
        val at = nextCreatedAt(held)
        // Empty rather than null for the owned fields: quartz reads null as "leave it" and blank as "delete it".
        val template =
            if (held == null) {
                MetadataEvent.createNew(
                    name = info.name,
                    displayName = info.name,
                    picture = info.icon.orEmpty(),
                    banner = info.banner.orEmpty(),
                    about = info.description.orEmpty(),
                    createdAt = at,
                )
            } else {
                MetadataEvent.updateFromPast(
                    latest = held,
                    name = info.name,
                    displayName = info.name,
                    picture = info.icon.orEmpty(),
                    banner = info.banner.orEmpty(),
                    about = info.description.orEmpty(),
                    createdAt = at,
                    // updateFromPast re-parses the `i` tags lossily; the initializer runs last, so the originals win.
                    initializer = {
                        remove(IdentityClaimTag.TAG_NAME)
                        held.tags.filter { it.firstOrNull() == IdentityClaimTag.TAG_NAME }.forEach { add(it) }
                    },
                )
            }
        if (held != null && held.content == template.content && held.tags.contentDeepEquals(template.tags)) return Outcome.UNCHANGED
        store.insert(signer.sign<MetadataEvent>(template))
        return Outcome.PUBLISHED
    }

    private suspend fun publishRelayList(): Outcome {
        val held =
            newest(AdvertisedRelayListEvent.KIND)?.let {
                it as? AdvertisedRelayListEvent ?: AdvertisedRelayListEvent(it.id, it.pubKey, it.createdAt, it.tags, it.content, it.sig)
            }
        val listed = held?.relays().orEmpty()
        // Our url is the only entry checked; the rest are somebody else's statements.
        if (listed.any { it.relayUrl == relayUrl && it.type == AdvertisedRelayType.BOTH }) return Outcome.UNCHANGED

        // Ours last, so an entry that named us read-only is upgraded rather than doubled.
        val next = listed.filterNot { it.relayUrl == relayUrl } + AdvertisedRelayInfo(relayUrl, AdvertisedRelayType.BOTH)
        val at = nextCreatedAt(held)
        val event =
            if (held == null) {
                AdvertisedRelayListEvent.create(next, signer, at)
            } else {
                AdvertisedRelayListEvent.replaceRelayListWith(held, next, signer, at)
            }
        store.insert(event)
        return Outcome.PUBLISHED
    }

    /** The newest event of [kind] signed by this relay; a store mid-supersede may hold two. */
    private suspend fun newest(kind: Int): Event? =
        store
            .query<Event>(Filter(kinds = listOf(kind), authors = listOf(signer.pubKey)))
            .maxByOrNull { it.createdAt }

    /** `max(now, held + 1)`: a replaceable edit that is not strictly newer is silently refused. */
    private fun nextCreatedAt(held: Event?): Long = maxOf(nowSeconds(), (held?.createdAt ?: 0L) + 1)
}

/**
 * Publish the relay's own kind 0 and kind 10002 in the background, waiting out a cold engine. A
 * failed read is retried rather than read as an empty store, within a bounded wait. Called at
 * boot and again when a NIP-86 RPC rewrites the document.
 */
fun launchRelayProfile(
    scope: CoroutineScope,
    profile: RelayProfile,
    info: Nip11Info,
) {
    scope.launch {
        var waited = 0L
        var printedFirstFailure = false
        while (true) {
            val result = runCatching { profile.publish(info) }
            result.onSuccess { report ->
                val published = report.published()
                if (published.isEmpty()) {
                    println("profile: kind 0 and kind 10002 already say this — nothing published")
                } else {
                    println(
                        "profile: published ${published.joinToString { "kind $it" }} as ${profile.pubKey.take(12)}… " +
                            "(\"${info.name}\", ${profile.relayUrl.url} read+write)",
                    )
                }
                return@launch
            }
            val cause = result.exceptionOrNull()
            // Shutdown is not a failed publish.
            if (cause is CancellationException) throw cause
            if (waited >= PROFILE_MAX_WAIT_MS) {
                System.err.println(
                    "profile: could not publish the relay's own kind 0 / 10002 after ${waited / 1000}s " +
                        "(${cause?.message?.take(120)}) — serving without them; clients meeting this key elsewhere " +
                        "will see no name and no relay list",
                )
                return@launch
            }
            if (!printedFirstFailure) {
                printedFirstFailure = true
                println("profile: engine not answering yet (${cause?.message?.take(80)}); waiting before publishing the relay's own kind 0 / 10002")
            }
            delay(PROFILE_RETRY_MS)
            waited += PROFILE_RETRY_MS
        }
    }
}

private const val PROFILE_RETRY_MS = 5_000L

private const val PROFILE_MAX_WAIT_MS = 10 * 60 * 1000L
