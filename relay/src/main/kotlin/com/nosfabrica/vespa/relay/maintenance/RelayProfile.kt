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
 * The relay's own kind 0 and kind 10002, signed with `RELAY_NSEC` and stored
 * here — the two events every Nostr client already knows how to read, saying
 * what the NIP-11 document says and where to reach the thing that said it.
 *
 * NIP-11 is only served to whoever asks this host directly. A reader who meets
 * the relay's pubkey anywhere else — in a NIP-42 AUTH, on one of the kind-30166
 * records the monitor signs, as `self` in someone else's copy of our doc — has
 * no name, no description and no way back. A kind 0 and a kind 10002 close
 * both gaps with events, which travel.
 *
 * **The NIP-11 doc is the source, and these two fields ARE its fields.**
 * `RELAY_NAME` becomes `name` and `display_name`; `RELAY_DESCRIPTION` becomes
 * `about`; `RELAY_ICON` and `RELAY_BANNER` become `picture` and `banner`. The
 * relay therefore describes itself once, in the settings it already had, and
 * cannot drift into saying two different things about itself in two places.
 * Everything else in a stored kind 0 — a `nip05`, a `lud16`, anything an
 * operator published for this key by hand — is carried forward untouched
 * (quartz's [MetadataEvent.updateFromPast]), for the same reason the NIP-66
 * monitor edits rather than rewrites: a replaceable event has one address, and
 * a writer that rebuilds it from its own fields alone silently deletes
 * everybody else's.
 *
 * The five fields above are the exception to that rule, and deliberately so:
 * this writer OWNS them, so unsetting `RELAY_DESCRIPTION` clears `about`
 * rather than leaving a description the doc no longer carries. `RELAY_NAME` is
 * the one with a default: unset, the doc still serves `vespa-relay`, so that is
 * what the profile says too — mirroring the doc includes mirroring its default.
 *
 * **The 10002 names this relay read AND write** — `["r", "<RELAY_URL>"]` with
 * no marker, which NIP-65 reads as both — because a relay is its own inbox and
 * its own outbox: what it publishes is here, and reaching it is how you reply.
 * Other entries in an existing list are kept: an operator who added a mirror,
 * or the relay's own `.onion`, has said something we have no basis to
 * contradict. Only OUR url is authored here, and only up to both directions if
 * it was listed as one.
 *
 * **Written when it would change something, and never otherwise.** Both kinds
 * are compared against what the store already holds and skipped when they
 * match, so a restart loop cannot walk the relay's own profile forward one
 * `created_at` at a time.
 *
 * That is also why the doc is passed to [publish] rather than held here: the
 * NIP-11 document is not fixed for the life of the process. `changerelayname`,
 * `changerelaydescription` and `changerelayicon` are NIP-86 RPCs this relay
 * answers, and they rewrite the served document at runtime — so the boot
 * publish and the republish an admin RPC triggers are the same call with
 * different fields, and the profile follows the doc instead of freezing the
 * environment it started with. A [Mutex] serializes them: two RPCs seconds
 * apart would otherwise both read the same held event and both write from it.
 *
 * The events are inserted straight into the store rather than published over
 * the relay's own websocket: this process IS the relay, the store is what it
 * serves, and going out through the front door would only add a NIP-42 dance
 * with ourselves.
 */
class RelayProfile(
    private val store: IEventStore,
    private val signer: NostrSigner,
    val relayUrl: NormalizedRelayUrl,
    private val nowSeconds: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    /** One writer at a time — see the class doc: the boot publish and a NIP-86 republish can overlap. */
    private val writing = Mutex()

    /** The key everything here is signed with — the relay's own. */
    val pubKey: String get() = signer.pubKey

    /** What one kind needed. [PUBLISHED] wrote an event; [UNCHANGED] found the store already saying it. */
    enum class Outcome {
        PUBLISHED,
        UNCHANGED,
    }

    data class Report(
        val metadata: Outcome,
        val relayList: Outcome,
    ) {
        /** For the boot line: which of the two were written, if either. */
        fun published(): List<Int> =
            listOfNotNull(
                MetadataEvent.KIND.takeIf { metadata == Outcome.PUBLISHED },
                AdvertisedRelayListEvent.KIND.takeIf { relayList == Outcome.PUBLISHED },
            )
    }

    /**
     * Bring both events up to date against [info] — whatever the NIP-11
     * document says right now. Throws whatever the store throws — see
     * [launchRelayProfile], which retries: a read that FAILED must never be
     * read as "nothing is stored", because that is the one way this could
     * replace a richer profile with a freshly built one.
     */
    suspend fun publish(info: Nip11Info): Report =
        writing.withLock {
            Report(metadata = publishMetadata(info), relayList = publishRelayList())
        }

    private suspend fun publishMetadata(info: Nip11Info): Outcome {
        val held = newest(MetadataEvent.KIND)?.let { it as? MetadataEvent ?: MetadataEvent(it.id, it.pubKey, it.createdAt, it.tags, it.content, it.sig) }
        val at = nextCreatedAt(held)
        // Empty rather than null for the fields this writer owns: quartz reads
        // null as "leave whatever is there" and blank as "delete it", and a
        // description the operator has REMOVED from the NIP-11 doc must not
        // survive in the profile the doc is supposed to be the source of.
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
                    // NIP-39 claims, put back exactly as they were found.
                    //
                    // [MetadataEvent.updateFromPast] rebuilds the `i` tags: it
                    // drops every one and re-adds what [IdentityClaimTag.parse]
                    // gives back, which is LOSSY in two ways this writer has no
                    // business inflicting on somebody else's tag. A claim with
                    // no proof (`["i", "github:alice"]`) does not parse and
                    // disappears; an identity carrying a second colon
                    // (`matrix:@alice:example.org`) is split on the first one
                    // and reassembled as `matrix:@alice`. Both are silent, both
                    // are signed, and both survive every later boot because the
                    // damaged tag is what the next edit reads.
                    //
                    // The initializer runs last, so removing and re-adding the
                    // held event's own `i` tags here is the whole fix. Quartz's
                    // rewrite still happens; it is simply overwritten by the
                    // originals. This writer manages no claims of its own —
                    // twitter/mastodon/github stay null above.
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
        // Both directions already claimed for this url — nothing to say. Note
        // this is the only thing checked: another relay's entry, a marker on
        // it, and anything else in the list are somebody else's statements.
        if (listed.any { it.relayUrl == relayUrl && it.type == AdvertisedRelayType.BOTH }) return Outcome.UNCHANGED

        // Ours last, so re-publishing a list that already named it read-only
        // upgrades that one entry in place rather than leaving two `r` tags for
        // one url disagreeing about direction.
        val next = listed.filterNot { it.relayUrl == relayUrl } + AdvertisedRelayInfo(relayUrl, AdvertisedRelayType.BOTH)
        val at = nextCreatedAt(held)
        val event =
            if (held == null) {
                AdvertisedRelayListEvent.create(next, signer, at)
            } else {
                // Keeps every non-`r` tag the held event carried, whoever wrote
                // it. The `r` tags come back through [AdvertisedRelayListEvent.relays],
                // so one naming a url that will not normalize is dropped rather
                // than carried — it named no relay anything could dial.
                AdvertisedRelayListEvent.replaceRelayListWith(held, next, signer, at)
            }
        store.insert(event)
        return Outcome.PUBLISHED
    }

    /**
     * The newest event of [kind] this relay has signed, or null when it has
     * signed none.
     *
     * A replaceable kind should hold exactly one, but the query is not asserted
     * down to that: a store mid-supersede, or one that mirrored an older
     * version of our own event from an upstream, is a state to read past rather
     * than to fail on.
     */
    private suspend fun newest(kind: Int): Event? =
        store
            .query<Event>(Filter(kinds = listOf(kind), authors = listOf(signer.pubKey)))
            .maxByOrNull { it.createdAt }

    /**
     * `max(now, held + 1)`, because a store enforcing replaceable semantics
     * REFUSES an edit that is not strictly newer — and a clock that has not
     * moved since the last write, or one that went backwards across a restart,
     * is ordinary. Silently losing the write to "a newer version exists" is how
     * a boot reports success having published nothing.
     */
    private fun nextCreatedAt(held: Event?): Long = maxOf(nowSeconds(), (held?.createdAt ?: 0L) + 1)
}

/**
 * Publish the relay's own kind 0 and kind 10002 in the background, waiting out
 * an engine that is not answering yet.
 *
 * The wait is the whole reason this is a loop. Both kinds are REPLACEABLE, so
 * publishing decides what to write from what the store already holds, and a
 * cold Vespa that cannot answer must therefore never be taken for a store with
 * nothing in it — that reading is what would replace a profile carrying a
 * `nip05` and a `lud16` with the two fields this file knows about. A failed
 * read is a failed attempt, and the attempt is retried; the work is idempotent,
 * so re-running one that got halfway costs a query.
 *
 * Bounded like every other boot job here: a failure that is not warm-up (a
 * wrong url, a dead cluster) must not leave a coroutine retrying for the life
 * of the process. Serving without a profile is a relay that is harder to
 * discover, not a relay that is down.
 *
 * Called at boot and again whenever a NIP-86 admin RPC rewrites the served
 * document, with the fields it now carries — [RelayProfile] takes the lock, so
 * the two cannot interleave.
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
            // Shutdown is not a failed publish, and swallowing it here would
            // keep this loop alive past the scope that owns it.
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

/** Same shape as the trust reconcile's wait: a cold content node takes minutes, and each attempt is a real query. */
private const val PROFILE_RETRY_MS = 5_000L

private const val PROFILE_MAX_WAIT_MS = 10 * 60 * 1000L
