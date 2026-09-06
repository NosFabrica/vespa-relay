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
package com.nosfabrica.vespa.relay.peers

import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withTimeoutOrNull

/**
 * The monitor's verdicts, written to kind 30166 records whose `d` tag is the relay url. Each
 * pass owns its own tags on the record and edits around everyone else's; every verdict tag
 * carries its own measured-at stamp and rules epoch, and [load] drops what is stale under either.
 */
class RelayVerdictRecord(
    private val store: IEventStore,
    private val signer: NostrSigner?,
    private val ttlSeconds: Long = DEFAULT_TTL_SECONDS,
) {
    /** What this monitor has decided and still stands behind. */
    data class Verdicts(
        /** Folded url -> the url that stands in for it. */
        val aliases: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
        /** Urls proven to be their own relay. Never a key in [aliases]. */
        val distinct: Set<NormalizedRelayUrl> = emptySet(),
        /** Urls measured as answering one filter the same way twice. */
        val consistent: Set<NormalizedRelayUrl> = emptySet(),
        /** Urls measured as not doing so. */
        val inconsistent: Set<NormalizedRelayUrl> = emptySet(),
        /** Whether a url answered a NEG-OPEN when the fitness pass asked. Absent is unmeasured, not "no". */
        val speaksNegentropy: Map<NormalizedRelayUrl, Boolean> = emptyMap(),
        /**
         * Urls this monitor stands behind any current verdict about — the fold answer, the
         * stability one, the NIP-77 one, or the fitness grade. A record whose every tag has aged
         * out is not in here: it says what we measured once, not what we measure now.
         */
        val measured: Set<NormalizedRelayUrl> = emptySet(),
    )

    /** One chunked record read, booked as the monitor's. */
    private suspend fun readRecords(filter: Filter): List<Event> =
        storeCall(StoreCalls.CALLER_MONITOR_VERDICTS, StoreCalls.OP_QUERY, StoreCalls.summarise(filter)) {
            store.query<Event>(filter)
        }

    /**
     * Read back every verdict covering [candidates]. Throws when the store cannot answer, and
     * must: [AliasFolding.adopt] forgets every verdict it holds before adopting what comes back.
     */
    suspend fun load(candidates: Collection<NormalizedRelayUrl>): Verdicts {
        val self = signer?.pubKey ?: return Verdicts()
        if (candidates.isEmpty()) return Verdicts()
        val floor = nowSeconds() - ttlSeconds
        val held = Building()
        for (chunk in candidates.map { it.url }.chunked(QUERY_CHUNK)) {
            held.take(
                readRecords(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to chunk))),
                floor,
            )
        }
        return held.verdicts()
    }

    /**
     * The fitness grade this monitor currently stands behind, per url, so an inherited verdict
     * is not re-signed with a fresh measured-at stamp. Stale grades do not appear. Throws when
     * the store cannot answer.
     */
    suspend fun fitnessGrades(candidates: Collection<NormalizedRelayUrl>): Map<NormalizedRelayUrl, StandingGrade> {
        val self = signer?.pubKey ?: return emptyMap()
        if (candidates.isEmpty()) return emptyMap()
        val floor = nowSeconds() - ttlSeconds
        val grades = HashMap<NormalizedRelayUrl, StandingGrade>()
        // Newest wins: a store should hold one record per address, and "should" is not a guarantee.
        val newestAt = HashMap<NormalizedRelayUrl, Long>()
        for (chunk in candidates.map { it.url }.chunked(QUERY_CHUNK)) {
            val held = readRecords(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to chunk)))
            for (event in held) {
                val subject = event.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: continue
                val url = RelayUrlNormalizer.normalizeOrNull(subject) ?: continue
                if (event.createdAt < (newestAt[url] ?: Long.MIN_VALUE)) continue
                val label =
                    event.tags.firstOrNull {
                        it.size > LABEL_NAMESPACE_INDEX && it[0] == LABEL_TAG && it[LABEL_NAMESPACE_INDEX] == FITNESS_NAMESPACE
                    } ?: continue
                if (label.getOrNull(LABEL_EPOCH_INDEX) != FITNESS_EPOCH) continue
                val measuredAt = label.getOrNull(LABEL_MEASURED_AT_INDEX)?.toLongOrNull() ?: continue
                if (measuredAt < floor) continue
                newestAt[url] = event.createdAt
                grades[url] = StandingGrade(label[1], label.getOrNull(LABEL_EVIDENCE_INDEX))
            }
        }
        return grades
    }

    /** The grade and the evidence beside it; `alias` onto a different canonical is a different statement. */
    data class StandingGrade(
        val value: String,
        val evidence: String?,
    )

    /** Every verdict this monitor still stands behind, paged over the corpus. Throws for [load]'s reason. */
    suspend fun loadAll(): Verdicts {
        val self = signer?.pubKey ?: return Verdicts()
        val floor = nowSeconds() - ttlSeconds
        val held = Building()
        RelayDiscovery.scan(
            store,
            Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), since = floor),
            RECORD_PAGE,
            caller = StoreCalls.CALLER_MONITOR_VERDICTS,
        ) { event -> held.take(event, floor) }
        return held.verdicts()
    }

    /** The sets [Verdicts] carries while a walk fills them in. */
    private class Building {
        val aliases = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val distinct = HashSet<NormalizedRelayUrl>()
        val consistent = HashSet<NormalizedRelayUrl>()
        val inconsistent = HashSet<NormalizedRelayUrl>()
        val speaksNegentropy = HashMap<NormalizedRelayUrl, Boolean>()
        val measured = HashSet<NormalizedRelayUrl>()

        fun verdicts() = Verdicts(aliases, distinct, consistent, inconsistent, speaksNegentropy, measured)
    }

    /** A page of records, folded into the sets. */
    private fun Building.take(
        held: List<Event>,
        floor: Long,
    ) {
        for (event in held) take(event, floor)
    }

    /** One record. The verdicts are read independently; a url may carry any subset. */
    private fun Building.take(
        event: Event,
        floor: Long,
    ) {
        val subject = event.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: return
        val from = RelayUrlNormalizer.normalizeOrNull(subject) ?: return
        event.tags.firstOrNull { it.size > 1 && it[0] == SAME_AS_TAG }?.takeIf { current(it, FOLD_EPOCH, floor) }?.get(1)?.let { sameAs ->
            RelayUrlNormalizer.normalizeOrNull(sameAs)?.let { to ->
                if (from == to) distinct += from else aliases[from] = to
                measured += from
            }
        }
        event.tags
            .firstOrNull { it.size > 1 && it[0] == SELF_CONSISTENT_TAG }
            ?.takeIf { current(it, CONSISTENCY_EPOCH, floor) }
            ?.get(1)
            ?.let { answer ->
                when (answer) {
                    CONSISTENT_YES -> {
                        consistent += from
                        measured += from
                    }

                    CONSISTENT_NO -> {
                        inconsistent += from
                        measured += from
                    }

                    // An unreadable answer is no verdict, not "unstable".
                    else -> {
                        Unit
                    }
                }
            }
        // The fitness label too: a relay graded `prime` is the most measured thing on the roster,
        // and it carries neither a fold answer nor a NIP-77 one.
        event.tags
            .firstOrNull { it.size > LABEL_NAMESPACE_INDEX && it[0] == LABEL_TAG && it[LABEL_NAMESPACE_INDEX] == FITNESS_NAMESPACE }
            ?.takeIf { it.getOrNull(LABEL_EPOCH_INDEX) == FITNESS_EPOCH && (it.getOrNull(LABEL_MEASURED_AT_INDEX)?.toLongOrNull() ?: Long.MIN_VALUE) >= floor }
            ?.let { measured += from }
        event.tags
            .firstOrNull { it.size > 1 && it[0] == NIP77_TAG }
            ?.takeIf { current(it, FITNESS_EPOCH, floor) }
            ?.get(1)
            ?.let { answer ->
                when (answer) {
                    "true" -> {
                        speaksNegentropy[from] = true
                        measured += from
                    }

                    "false" -> {
                        speaksNegentropy[from] = false
                        measured += from
                    }

                    else -> {
                        Unit
                    }
                }
            }
    }

    /**
     * Sign and store what a stability pass measured: did this url answer one filter, at one
     * anchor, the same way twice? An unmeasurable url gets no tag rather than a negative one.
     */
    suspend fun publishConsistency(
        url: NormalizedRelayUrl,
        consistent: Boolean,
        first: Int,
        second: Int,
        shared: Int,
        score: Double,
        /** The anchor's age in days, passed in so this record holds no copy of the pass's tuning. */
        anchorDays: Long,
    ): Event? =
        edit(
            url,
            owns = owning(SELF_CONSISTENT_TAG),
            add =
                listOf(
                    arrayOf(
                        SELF_CONSISTENT_TAG,
                        if (consistent) CONSISTENT_YES else CONSISTENT_NO,
                        "$first + $second events at a ${anchorDays}d anchor, $shared shared -> %.3f".format(score),
                        nowSeconds().toString(),
                        CONSISTENCY_EPOCH,
                    ),
                ),
        )

    /**
     * The fitness pass's whole write: the grade as a NIP-32 label under [FITNESS_NAMESPACE],
     * the measured facts, and what the pass learned on the way. NIP-32 fixes index 2 as the
     * namespace, so the label carries evidence, measured-at and epoch one place right of the other tags.
     */
    suspend fun publishFitness(
        url: NormalizedRelayUrl,
        status: String,
        evidence: String,
        pageable: Pair<Boolean, String>?,
        nip77: Pair<Boolean, String>?,
        /** Did the answer match the ask. Null for a dial that got no page to check. */
        compliant: Pair<Boolean, String>? = null,
        facts: RelayFacts = RelayFacts(),
    ): Event? {
        val at = nowSeconds().toString()
        val add =
            buildList {
                add(arrayOf(LABEL_TAG, status, FITNESS_NAMESPACE, evidence, at, FITNESS_EPOCH))
                add(arrayOf(LABEL_NAMESPACE_TAG, FITNESS_NAMESPACE))
                pageable?.let { (yes, why) -> add(arrayOf(PAGEABLE_TAG, if (yes) "true" else "false", why, at, FITNESS_EPOCH)) }
                nip77?.let { (yes, why) -> add(arrayOf(NIP77_TAG, if (yes) "true" else "false", why, at, FITNESS_EPOCH)) }
                compliant?.let { (yes, why) -> add(arrayOf(COMPLIANT_TAG, if (yes) "true" else "false", why, at, FITNESS_EPOCH)) }
                addAll(facts.tags())
            }
        return edit(url, owns = ::ownedByFitness, add = add)
    }

    /** Take the fitness verdict back: this pass's tags leave the record and everyone else's ride through. */
    suspend fun retireFitness(url: NormalizedRelayUrl): Event? = edit(url, owns = ::ownedByFitness, add = emptyList())

    /**
     * Everything the fitness pass replaces on each write. `l` and `L` are shared vocabulary, so
     * only our own namespace inside them is owned; the facts are owned whole even on a pass
     * that learned none, or a changed verdict would carry the old rtt and software.
     */
    private fun ownedByFitness(tag: Array<String>): Boolean =
        when (val name = tag.firstOrNull()) {
            null -> false
            LABEL_TAG -> tag.getOrNull(LABEL_NAMESPACE_INDEX).let { it == null || it == FITNESS_NAMESPACE }
            LABEL_NAMESPACE_TAG -> tag.getOrNull(NAMESPACE_DECLARATION_INDEX).let { it == null || it == FITNESS_NAMESPACE }
            PAGEABLE_TAG, NIP77_TAG, COMPLIANT_TAG -> true
            else -> name in RelayFacts.OWNED
        }

    /** Sign and store one fold; null when there is no signer. The evidence goes in the tag, never the content. */
    suspend fun publish(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): Event? = write(alias, canonical, "$sampled newest events, $shared shared with ${canonical.url}")

    /**
     * The one fold whose evidence is not a containment: a `ws://` url and the `wss://` url of
     * the same endpoint, both of which answered. Its own call so the evidence names that argument.
     */
    suspend fun publishSecureTwin(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        /** True when the pair's windows came through [RelayAliases.GROUP_METADATA_KINDS]. */
        groupList: Boolean = false,
    ): Event? =
        write(
            alias,
            canonical,
            "same endpoint as ${canonical.url} over TLS, both answered; " +
                if (groupList) "$sampled group definitions here" else "$sampled newest events here",
        )

    /** A fold decided on a relay's complete list of groups rather than a slice of its feed. */
    suspend fun publishGroupList(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): Event? = write(alias, canonical, "same group list as ${canonical.url}: $shared of $sampled group definitions shared")

    /** The weakest fold: these urls share a host, every one answered, none would serve anything. */
    suspend fun publishUnreadable(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        urls: Int,
    ): Event? =
        write(
            alias,
            canonical,
            "nothing readable at any of $urls url(s) on this host; folded on the shared name, not on a measurement",
        )

    /**
     * This url was fingerprinted and matched nothing it was compared to, written as `same-as`
     * pointing at its own url. [comparedAgainst] names what was actually held up against it.
     */
    suspend fun publishDistinct(
        url: NormalizedRelayUrl,
        sampled: Int,
        comparedAgainst: String,
        bestShared: Int,
    ): Event? = write(url, url, "$sampled newest events, best $bestShared shared with $comparedAgainst")

    private suspend fun write(
        subject: NormalizedRelayUrl,
        sameAs: NormalizedRelayUrl,
        evidence: String,
    ): Event? =
        edit(
            subject,
            owns = owning(SAME_AS_TAG),
            add = listOf(arrayOf(SAME_AS_TAG, sameAs.url, evidence, nowSeconds().toString(), FOLD_EPOCH)),
        )

    /**
     * Edit a replaceable record: read what is there, keep every tag this writer does not [owns],
     * apply [add], and store it at `max(now, existing + 1)` so the replace is accepted. Bounded
     * whole by [EDIT_DEADLINE_MS], the floor under every writer.
     */
    private suspend fun edit(
        url: NormalizedRelayUrl,
        owns: (Array<String>) -> Boolean,
        add: List<Array<String>>,
    ): Event? {
        val signer = signer ?: return null
        return withTimeoutOrNull(EDIT_DEADLINE_MS) {
            // A failed read aborts the edit; a fresh record would erase other writers' tags.
            val current =
                try {
                    currentRecord(url)
                } catch (e: CancellationException) {
                    throw e
                } catch (_: Exception) {
                    return@withTimeoutOrNull null
                }
            val kept = current?.tags?.filterNot { it.firstOrNull() == "d" || owns(it) }.orEmpty()
            val at = maxOf(nowSeconds(), (current?.createdAt ?: 0L) + 1)
            val template =
                RelayDiscoveryEvent.build(url, current?.content.orEmpty(), at) {
                    for (tag in kept) add(tag)
                    for (tag in add) add(tag)
                }
            // The deadlines work by cancellation, which must propagate.
            try {
                val event = signer.sign(template)
                storeCall(StoreCalls.CALLER_MONITOR_PUBLISH, StoreCalls.OP_INSERT, "kind ${template.kind}, 1 event") {
                    store.insert(event)
                }
                event
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                null
            }
        }
    }

    /** [edit]'s ordinary ownership: these tag names, whole. Wrong for the shared `l`/`L`. */
    private fun owning(vararg names: String): (Array<String>) -> Boolean = { it.firstOrNull() in names }

    /**
     * Is this verdict one we would still act on: under the current rules and within its TTL?
     * Aged by the tag's own stamp, never the event's `createdAt`, which every writer bumps.
     */
    private fun current(
        tag: Array<String>,
        epoch: String,
        floor: Long,
    ): Boolean {
        if (tag.getOrNull(EPOCH_INDEX) != epoch) return false
        val measuredAt = tag.getOrNull(MEASURED_AT_INDEX)?.toLongOrNull() ?: return false
        return measuredAt >= floor
    }

    /** This url's current record, null when nothing holds one, and a throw when the store cannot answer. */
    private suspend fun currentRecord(url: NormalizedRelayUrl): Event? {
        val self = signer?.pubKey ?: return null
        return store
            .query<Event>(
                Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to listOf(url.url))),
            ).maxByOrNull { it.createdAt }
    }

    companion object {
        /** The fold's tag: `["same-as", <url>, <evidence>, <measured-at>, <epoch>]`, pointing at itself for a distinct url. */
        const val SAME_AS_TAG = "same-as"

        /** The stability verdict's tag, same shape; its value is "true"/"false", not the score. */
        const val SELF_CONSISTENT_TAG = "self-consistent"

        const val CONSISTENT_YES = "true"

        const val CONSISTENT_NO = "false"

        /** Where a verdict tag carries the unix second it was measured. Absent reads as stale. */
        private const val MEASURED_AT_INDEX = 3

        /** Where a verdict tag carries the rules it was measured under. */
        private const val EPOCH_INDEX = 4

        /**
         * The version of the fold's decision rules. Bump it in the same commit as any change to
         * what a fingerprint concludes; the cost is a full re-fingerprint of the store.
         */
        const val FOLD_EPOCH = "2"

        /** The same lever for the stability verdict, a separate measurement. */
        const val CONSISTENCY_EPOCH = "1"

        /** NIP-32's label, where the fitness grade lives: `["l", <grade>, <namespace>, <evidence>, <measured-at>, <epoch>]`. */
        const val LABEL_TAG = "l"

        /** NIP-32's namespace declaration, which every `l` on this record needs. */
        const val LABEL_NAMESPACE_TAG = "L"

        /** Whose vocabulary the grade is written in. Named for the judgement, not for us. */
        const val FITNESS_NAMESPACE = "relay.fitness"

        const val LABEL_NAMESPACE_INDEX = 2

        const val NAMESPACE_DECLARATION_INDEX = 1

        /** One place right of [MEASURED_AT_INDEX], because NIP-32 spent index 2 on the namespace. */
        const val LABEL_MEASURED_AT_INDEX = 4

        const val LABEL_EVIDENCE_INDEX = 3

        const val LABEL_EPOCH_INDEX = 5

        /** Where the grade used to live, now the software field; `FitnessPass.retireLegacyGrades` queries it. */
        const val LEGACY_STATUS_TAG = RelayFacts.SOFTWARE_TAG

        /** Measured: does the relay honour `until`, so a paged walk can terminate? */
        const val PAGEABLE_TAG = "pageable"

        /** Measured: did the events it served match the filter? A relay may page perfectly and serve the wrong kind. */
        const val COMPLIANT_TAG = "compliant"

        /** Measured: did it answer a NEG-OPEN? */
        const val NIP77_TAG = "nip77"

        /** The fitness verdict's rules version; `FitnessPass.retireStaleEpochs` takes back older ones at boot. */
        const val FITNESS_EPOCH = "2"

        const val DEFAULT_TTL_SECONDS = 30L * 24 * 60 * 60

        /** The wall clock on one whole [edit]. Wider than `FitnessPass.PUBLISH_DEADLINE_MS`, which must fire first. */
        const val EDIT_DEADLINE_MS = 120_000L

        /** Urls per `#d` query, shared with [RelayDiscovery.undialable]. */
        internal const val QUERY_CHUNK = 500

        private const val RECORD_PAGE = 2_000
    }
}
