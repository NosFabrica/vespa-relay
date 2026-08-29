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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.experimental.trustedLists.treasureMap.TrustedListProviderTag
import com.vitorpamplona.quartz.experimental.trustedLists.treasureMap.trustedListProvider
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.serviceProviders
import kotlinx.coroutines.CancellationException
import java.util.concurrent.ConcurrentHashMap

/**
 * WHOSE LISTS A READER HAS ASKED FOR: each observer, plus every service key
 * their own kind-10040 names. [SearchReferenceExpansion] gates the Trusted
 * List and Trusted Assertion families on this set.
 *
 * ## The Map delegates in TWO shapes, and this gate needs both
 *
 * A Treasure Map names a NIP-85 provider per kind AND metric —
 * `["30382:rank", <pubkey>, <relay>]` — which is what `serviceProviders()`
 * reads. Tapestry's Trusted Lists delegate through the same event in a
 * different shape: a GENERIC BARE-KIND entry, `["30392", <pubkey>, <relay>]`,
 * one of which delegates every list of that kind (ADR `tl-treasure-map/0001`).
 * The Map stays a fixed size no matter how many lists the publisher computes,
 * which is the point — list names are never enumerated.
 *
 * Reading only the first shape left the Trusted List half of this gate with no
 * key: a bare-kind entry has no `:`, so NIP-85's parser has never returned one,
 * and 30392-30395 could be admitted only by an observer who signed the lists
 * themselves. Quartz `029c40ebb4` is what makes the second shape readable —
 * it split the two parsers apart, bounding `ServiceProviderTag` to 30382-30385
 * so a NIP-85 consumer is never handed a list delegation, and added
 * [trustedListProvider] for the other side. Both are read here because the
 * question this class answers — "did this reader ask for this signer's
 * computations" — is one question across both specs.
 *
 * NAMED `3039x:<name>` entries are deliberately NOT admitted. The ADR reserves
 * them and says they must drive no behavior until it defines them, and
 * [TrustedListProviderTag.parseGeneric] is where that line is drawn upstream.
 * A gate is the last place to act on a reservation. Asking per kind rather
 * than folding every generic entry in also means a Map that violates the
 * one-entry-per-kind invariant admits the publisher a conformant reader would
 * resolve — the first — rather than everyone who ever appeared under that kind.
 *
 * THE PUBLIC HALF ONLY, for both shapes: half a Map's delegations may be
 * NIP-44 encrypted to its owner, and a relay holds no signer. A reader who
 * keeps a delegation private gets no expansion from it. That is not new here —
 * `serviceProviders()` reads `tags` and has always had the same blind spot —
 * and the direction is the safe one for a gate.
 *
 * ## Why it is cached at all
 *
 * This is a property of the READER, not of the page: their provider list
 * changes when they enrol or drop a service, which is a deliberate act
 * measured in months. Resolving it inside the REQ made every expanded search
 * pay a second store round trip — ~6ms of the ~12ms `SearchExpansionCostBench`
 * measured against a real Vespa — to re-read a document that had not changed
 * since the previous search on the same socket. Cached, only the first search
 * per reader per [ttlMillis] pays it, and the sequential shape that remains
 * (resolve the gate, then look up what it admits) is the one round trip the
 * feature genuinely needs.
 *
 * The cache is safe to SHARE across connections because the lookup is
 * deliberately lensless — `include:spam`, for the reason
 * [SearchReferenceExpansion] gives — so its answer does not depend on who is
 * asking. A lensed lookup could not be shared this way, and turning that token
 * off here would silently make this cache wrong as well as circular.
 *
 * ## Why it goes stale, and by how much
 *
 * [invalidate] is exact for the writes this process sees: a client publishing
 * a new 10040 to this relay gets it applied on their very next search. It
 * cannot see the OTHER writer — the sync process mirrors 10040s into the same
 * store from its own JVM — so [ttlMillis] is the bound that covers those, and
 * it is deliberately short. A minute of staleness on this gate means a
 * newly-enrolled service's lists take up to a minute to start unpacking, and a
 * dropped one's take up to a minute to stop. The store's own [ProviderMap]
 * caches the same document for the same reason and has the same cross-process
 * hole; what it does not have is a TTL, which is why its cache is invalidated
 * on write and never expires.
 *
 * An EMPTY answer IS cached, and that is a real difference from `ProviderMap`,
 * which refuses to: "a relay with no 10040s and one whose engine has not
 * finished serving its corpus return the same empty list". The difference is
 * the TTL. There, an empty cached forever makes a warm-up race permanent;
 * here it costs at most [ttlMillis]. And not caching it would mean the
 * majority case — a reader who has published no provider list at all — is the
 * one case that pays the query on every single search.
 *
 * A FAILED lookup is never cached, empty or otherwise: a store that could not
 * answer must not be recorded as a store that answered "nobody".
 */
internal class EnrolledSigners(
    /**
     * The store recall. Takes filters rather than a pubkey so this stays
     * testable without a store, and so the caller decides which query API the
     * lookup rides — [ObserverBackend] hands in the store's own multi-filter
     * query.
     */
    private val recall: suspend (List<Filter>) -> List<Event>,
    private val ttlMillis: Long = DEFAULT_TTL_MILLIS,
    /** A test seam, and the same one the store uses for its own clocks. */
    private val nowMillis: () -> Long = System::currentTimeMillis,
) {
    private class Entry(
        val signers: Set<HexKey>,
        val expiresAtMillis: Long,
    )

    private val cache = ConcurrentHashMap<HexKey, Entry>()

    /**
     * The admitted signers for [observers] — themselves and their services.
     * Empty for an empty [observers], which is the anonymous read that expands
     * no list at all.
     *
     * One recall for every observer still unresolved, not one each: a REQ
     * naming two observers on two filters is one `authors` list.
     */
    suspend fun of(observers: Set<HexKey>): Set<HexKey> {
        if (observers.isEmpty()) return emptySet()

        val now = nowMillis()
        val signers = HashSet<HexKey>(observers)
        val misses = ArrayList<HexKey>()
        for (observer in observers) {
            val hit = cache[observer]?.takeIf { it.expiresAtMillis > now }
            if (hit == null) misses.add(observer) else signers.addAll(hit.signers)
        }
        if (misses.isEmpty()) return signers

        val lists = read(misses) ?: return signers.also { it.addAll(uncachedFallback(misses)) }

        // Every miss gets an entry, including the readers who turned out to
        // have no list: "this reader has enrolled nobody" is an answer, and
        // the common one.
        val found = HashMap<HexKey, MutableSet<HexKey>>()
        for (observer in misses) found[observer] = HashSet()
        for (list in lists) {
            val services = found[list.pubKey] ?: continue
            list.tags.serviceProviders().forEach { services.add(it.pubkey) }
            TrustedListProviderTag.KINDS.forEach { kind -> list.tags.trustedListProvider(kind)?.let { services.add(it.pubkey) } }
        }
        val expiresAt = now + ttlMillis
        // A crude bound, and it wants to stay crude: the entries are two
        // pubkeys' worth of strings each, the map is only reachable by readers
        // who actually searched, and a relay that has served a million
        // distinct observers inside one TTL has bigger numbers to look at.
        if (cache.size > MAX_ENTRIES) cache.clear()
        for ((observer, services) in found) {
            cache[observer] = Entry(services, expiresAt)
            signers.addAll(services)
        }
        return signers
    }

    /**
     * Drops [pubkey]'s entry, so their next read resolves afresh. Called for
     * every kind-10040 this process accepts — the write path is the only exact
     * signal there is, and it is worth taking because it is what makes a
     * reader publishing a provider list HERE see it work immediately rather
     * than a minute later.
     */
    fun invalidate(pubkey: HexKey) {
        cache.remove(pubkey)
    }

    /**
     * What a failed lookup falls back to: the observers themselves, uncached.
     * A reader's own lists still unpack — that half needs no store at all —
     * and their services' lists stay shut until a read succeeds, which is the
     * conservative direction for a gate.
     */
    private fun uncachedFallback(misses: List<HexKey>): Set<HexKey> = misses.toSet()

    /** The lists, or null when the store could not say — which is not the same as "none". */
    private suspend fun read(observers: List<HexKey>): List<Event>? =
        try {
            recall(
                listOf(
                    // include:spam ON PURPOSE — see [SearchReferenceExpansion]:
                    // reading a reader's own statement of whom they trust
                    // through the trust that statement establishes is circular,
                    // and it fails in the direction that silently removes the
                    // feature. It is also what makes this cache shareable.
                    Filter(
                        kinds = listOf(TrustProviderListEvent.KIND),
                        authors = observers,
                        limit = observers.size,
                        search = "include:spam",
                    ),
                ),
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // One line: a store that has stopped answering produces one of
            // these per search, and the search itself still went out.
            println("search-expansion: provider-list lookup failed, expanding only the reader's own lists: ${e.message}")
            null
        }

    companion object {
        /**
         * How long a reader's enrolment may lag a 10040 written by the OTHER
         * process. Short because this is a gate: a minute of it means a dropped
         * service's lists keep unpacking for up to a minute. Long enough that
         * an interactive session pays the lookup once rather than per search,
         * which is the whole point.
         */
        const val DEFAULT_TTL_MILLIS = 60_000L

        /** Entries held before the map is dropped wholesale. See [of]. */
        private const val MAX_ENTRIES = 100_000
    }
}
