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
 * The monitor's verdicts, written to kind 30166 where the next boot and any
 * outbox crawler can read them. The record's `d` tag is the relay url; each
 * pass owns its own tags on it and edits around everyone else's.
 *
 * The fold writes one tag in two forms:
 *
 * ```json
 * ["same-as", "wss://nos.lol/",             "500 newest events, 498 shared with wss://nos.lol/",              "1776038400", "2"]
 * ["same-as", "wss://nostr.ac/v1",          "500 newest events, best 2 shared of 19 peer(s) on this host",     "1776038400", "2"]
 * ```
 *
 * Pointing elsewhere is a fold; pointing at the record's own url says it was
 * measured and is nobody's duplicate. The last two elements are the verdict's
 * own clock (see [current]) and the rules version that produced it (see
 * [FOLD_EPOCH]). `same-as` is an equivalence, so a consumer running
 * union-find over these tags gets the right partition without sharing
 * [RelayAliases.PREFERENCE].
 *
 * Read back with [load], which drops anything older than [ttlSeconds] or
 * measured under superseded rules.
 */
class RelayVerdictRecord(
    private val store: IEventStore,
    private val signer: NostrSigner?,
    private val ttlSeconds: Long = DEFAULT_TTL_SECONDS,
) {
    /**
     * What this monitor has decided and still stands behind. [distinct] is
     * persisted so a boot does not re-fingerprint every non-duplicate.
     */
    data class Verdicts(
        /** Folded url -> the url that stands in for it. */
        val aliases: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
        /** Urls proven to be their own relay. Never a key in [aliases]. */
        val distinct: Set<NormalizedRelayUrl> = emptySet(),
        /** Urls measured as answering one filter the same way twice. */
        val consistent: Set<NormalizedRelayUrl> = emptySet(),
        /** Urls measured as not doing so; the ones the fan-out refuses. */
        val inconsistent: Set<NormalizedRelayUrl> = emptySet(),
        /**
         * Whether a url answered a NEG-OPEN when the fitness pass asked.
         * Absent is unmeasured, not "no": the reader tries and finds out.
         */
        val speaksNegentropy: Map<NormalizedRelayUrl, Boolean> = emptyMap(),
    )

    /** One chunked record read, booked as the monitor's; shared by [load] and [fitnessGrades]. */
    private suspend fun readRecords(filter: Filter): List<Event> =
        storeCall(StoreCalls.CALLER_MONITOR_VERDICTS, StoreCalls.OP_QUERY, StoreCalls.summarise(filter)) {
            store.query<Event>(filter)
        }

    /**
     * Read back every verdict covering [candidates], queried by `#d` because
     * `d` is the only part of these records the tag index answers on.
     *
     * Throws when the store cannot answer, and must go on doing so:
     * [AliasFolding.adopt] forgets every verdict it holds before adopting
     * what comes back, on the promise that a failed read arrives as a failure.
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
     * The fitness grade this monitor currently stands behind, per url, asked
     * before paying for a write. A verdict inherited from another pass must
     * not be re-signed: the measured-at stamp is how a verdict ages, so
     * refreshing what was not tested makes it immortal.
     *
     * A grade that is absent, under a superseded [FITNESS_EPOCH], or past
     * [ttlSeconds] does not appear. Throws when the store cannot answer.
     */
    suspend fun fitnessGrades(candidates: Collection<NormalizedRelayUrl>): Map<NormalizedRelayUrl, StandingGrade> {
        val self = signer?.pubKey ?: return emptyMap()
        if (candidates.isEmpty()) return emptyMap()
        val floor = nowSeconds() - ttlSeconds
        val grades = HashMap<NormalizedRelayUrl, StandingGrade>()
        // Newest wins: a store should hold one record per address, and
        // "should" is not a guarantee this reader can make.
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

    /**
     * What a record says under this pass's namespace: the grade and the
     * evidence beside it. The evidence is part of the identity: `alias` onto
     * a different canonical is a different public statement.
     */
    data class StandingGrade(
        val value: String,
        val evidence: String?,
    )

    /**
     * Every verdict this monitor still stands behind, whatever one cycle
     * discovered. The fold groups the world, not the candidates: a url's
     * siblings can be absent from a candidate set for reasons unrelated to
     * the fold, and a group of one is dropped unresolved. Paged, because this
     * is a corpus; throws for [load]'s reason.
     */
    suspend fun loadAll(): Verdicts {
        val self = signer?.pubKey ?: return Verdicts()
        val floor = nowSeconds() - ttlSeconds
        val held = Building()
        RelayDiscovery.scan(
            store,
            Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), since = floor),
            RECORD_PAGE,
            // The pager is shared with the url round-up; this walk is the monitor's.
            caller = StoreCalls.CALLER_MONITOR_VERDICTS,
        ) { event -> held.take(event, floor) }
        return held.verdicts()
    }

    /** The sets [Verdicts] carries while a walk fills them in, so [load] and [loadAll] read a record one way. */
    private class Building {
        val aliases = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val distinct = HashSet<NormalizedRelayUrl>()
        val consistent = HashSet<NormalizedRelayUrl>()
        val inconsistent = HashSet<NormalizedRelayUrl>()
        val speaksNegentropy = HashMap<NormalizedRelayUrl, Boolean>()

        fun verdicts() = Verdicts(aliases, distinct, consistent, inconsistent, speaksNegentropy)
    }

    /** A page of records, folded into the sets. */
    private fun Building.take(
        held: List<Event>,
        floor: Long,
    ) {
        for (event in held) take(event, floor)
    }

    /** One record. The three verdicts are read independently; a url may carry any subset. */
    private fun Building.take(
        event: Event,
        floor: Long,
    ) {
        val subject = event.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: return
        val from = RelayUrlNormalizer.normalizeOrNull(subject) ?: return
        event.tags.firstOrNull { it.size > 1 && it[0] == SAME_AS_TAG }?.takeIf { current(it, FOLD_EPOCH, floor) }?.get(1)?.let { sameAs ->
            RelayUrlNormalizer.normalizeOrNull(sameAs)?.let { to ->
                if (from == to) distinct += from else aliases[from] = to
            }
        }
        event.tags
            .firstOrNull { it.size > 1 && it[0] == SELF_CONSISTENT_TAG }
            ?.takeIf { current(it, CONSISTENCY_EPOCH, floor) }
            ?.get(1)
            ?.let { answer ->
                when (answer) {
                    CONSISTENT_YES -> consistent += from

                    CONSISTENT_NO -> inconsistent += from

                    // An unreadable answer is no verdict, not "unstable".
                    else -> Unit
                }
            }
        event.tags
            .firstOrNull { it.size > 1 && it[0] == NIP77_TAG }
            ?.takeIf { current(it, FITNESS_EPOCH, floor) }
            ?.get(1)
            ?.let { answer ->
                when (answer) {
                    "true" -> speaksNegentropy[from] = true
                    "false" -> speaksNegentropy[from] = false
                    else -> Unit
                }
            }
    }

    /**
     * Sign and store what a stability pass measured: did this url answer one
     * filter, at one week-old anchor, the same way twice?
     *
     * ```json
     * ["self-consistent", "true",  "500 + 500 events at a 7d anchor, 500 shared -> 1.000", "1776038400", "1"]
     * ["self-consistent", "false", "203 + 179 events at a 7d anchor, 128 shared -> 0.715", "1776038400", "1"]
     * ```
     *
     * Never inferred from `same-as`: a relay may be stable while wearing six
     * aliases. "false" costs a relay its place in the fan-out, so an
     * unmeasurable url gets no tag rather than a negative one.
     */
    suspend fun publishConsistency(
        url: NormalizedRelayUrl,
        consistent: Boolean,
        first: Int,
        second: Int,
        shared: Int,
        score: Double,
        /** The anchor's age in days, the consistency pass's own `ANCHOR_LAG_SECONDS`; passed in so this record holds no copy of that tuning. */
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
                        // The verdict's own clock; see [current].
                        nowSeconds().toString(),
                        CONSISTENCY_EPOCH,
                    ),
                ),
        )

    /**
     * The fitness pass's whole write: the grade a stream filters on, the
     * measured facts a visit reads back, and what the pass learned about the
     * relay on the way.
     *
     * The grade is a NIP-32 label under [FITNESS_NAMESPACE]: `l` is
     * single-letter and therefore indexed, and the namespace is what keeps it
     * clear of other monitors' country and ASN labels on the same record.
     * NIP-32 fixes index 2 as the namespace, so this tag carries evidence,
     * measured-at and epoch one place right of the other verdict tags.
     *
     * The facts are written in the same edit because they were learned in
     * the same pass; a 30166 without `rtt-open` inside the TTL reads as
     * "could not open" to quartz's own convention.
     */
    suspend fun publishFitness(
        url: NormalizedRelayUrl,
        status: String,
        evidence: String,
        pageable: Pair<Boolean, String>?,
        nip77: Pair<Boolean, String>?,
        /** Did the answer match the ask; see [COMPLIANT_TAG]. Defaulted because a dial that got no page has nothing to check. */
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

    /**
     * Take the fitness verdict back: [publishFitness]'s ownership with nothing
     * to add, so this pass's tags and facts leave the record and everyone
     * else's ride through. A verdict taken under rules we no longer apply
     * stops being a verdict, and the url reads as unmeasured.
     */
    suspend fun retireFitness(url: NormalizedRelayUrl): Event? = edit(url, owns = ::ownedByFitness, add = emptyList())

    /**
     * Everything the fitness pass replaces on each write, and nothing else.
     * `l` and `L` are shared vocabulary, so only our own namespace inside
     * them is owned; a label with no namespace is nobody's and is dropped.
     * The measured facts are owned whole even on a pass that learned none,
     * or a changed verdict would carry the old relay's rtt and software.
     */
    private fun ownedByFitness(tag: Array<String>): Boolean =
        when (val name = tag.firstOrNull()) {
            null -> false

            LABEL_TAG -> tag.getOrNull(LABEL_NAMESPACE_INDEX).let { it == null || it == FITNESS_NAMESPACE }

            LABEL_NAMESPACE_TAG -> tag.getOrNull(NAMESPACE_DECLARATION_INDEX).let { it == null || it == FITNESS_NAMESPACE }

            PAGEABLE_TAG, NIP77_TAG, COMPLIANT_TAG -> true

            else -> name in RelayFacts.OWNED
        }

    /**
     * Sign and store one fold. Returns the event so a caller can push it
     * upstream; null when there is no signer. The evidence goes in the tag's
     * third element, never the content, which is the relay's own document.
     */
    suspend fun publish(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): Event? = write(alias, canonical, "$sampled newest events, $shared shared with ${canonical.url}")

    /**
     * The one fold whose evidence is not a containment: a `ws://` url and the
     * `wss://` url of the same host and path, both of which answered. See
     * [RelayAliases.schemeTwins].
     *
     * ```json
     * ["same-as", "wss://nos.lol/", "same endpoint as wss://nos.lol/ over TLS, both answered; 9 newest events here", "1776038400", "2"]
     * ```
     *
     * Its own call because the evidence sentence is the argument actually
     * made; quoting "9 shared" would offer a containment as the reason.
     */
    suspend fun publishSecureTwin(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        /** True when the pair's windows came through [RelayAliases.GROUP_METADATA_KINDS]; changes the noun only. */
        groupList: Boolean = false,
    ): Event? =
        write(
            alias,
            canonical,
            "same endpoint as ${canonical.url} over TLS, both answered; " +
                if (groupList) "$sampled group definitions here" else "$sampled newest events here",
        )

    /**
     * A fold decided on a relay's complete list of groups rather than a slice
     * of its feed. See [RelayAliases.GROUP_METADATA_KINDS].
     *
     * ```json
     * ["same-as", "wss://groups.example/", "same group list as wss://groups.example/: 7 of 7 group definitions shared", "1776038400", "2"]
     * ```
     */
    suspend fun publishGroupList(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): Event? = write(alias, canonical, "same group list as ${canonical.url}: $shared of $sampled group definitions shared")

    /**
     * The weakest thing this monitor says: these urls share a host, every one
     * answered, none would serve anything, so they were treated as one.
     *
     * ```json
     * ["same-as", "wss://x/", "nothing readable at any of 5 url(s) on this host; folded on the shared name, not on a measurement", "1776038400", "2"]
     * ```
     */
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
     * This url was fingerprinted against its host's leader and matched it
     * not, written as `same-as` pointing at its own url. It says "not the
     * leader", not "not any of them"; [comparedAgainst] names what was
     * actually held up against it.
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
     * Edit a replaceable record: read what is there, keep every tag this
     * writer does not [owns], apply [add], and store it one second past
     * whatever it replaced. A record has one address and several writers, so
     * a write built from this writer's tags alone deletes everyone else's.
     * The timestamp is `max(now, existing + 1)` because a store enforcing
     * replaceable semantics rejects an edit that is not newer.
     *
     * Bounded whole by [EDIT_DEADLINE_MS] because the store's client carries
     * no read deadline, and this is the floor under every writer.
     */
    private suspend fun edit(
        url: NormalizedRelayUrl,
        owns: (Array<String>) -> Boolean,
        add: List<Array<String>>,
    ): Event? {
        val signer = signer ?: return null
        return withTimeoutOrNull(EDIT_DEADLINE_MS) {
            // A read the store failed aborts the edit; it is not "no record",
            // or a transient failure signs a fresh record that erases other
            // writers' tags.
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
            // Not `runCatching`: the deadline above and the fitness pass's own
            // per-write clock work by cancellation, which must propagate.
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

    /** [edit]'s ordinary ownership: these tag names, whole. Wrong for the shared `l`/`L`; see [ownedByFitness]. */
    private fun owning(vararg names: String): (Array<String>) -> Boolean = { it.firstOrNull() in names }

    /**
     * Is this verdict one we would still act on: taken under the rules we
     * currently apply, and within its TTL? The tag's own measured-at stamp is
     * aged, never the event's `createdAt`, which every writer of the shared
     * record bumps. A tag without a stamp or with another epoch is stale.
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
        /** The fold's tag, in both forms. This monitor's own; every other NIP-66 consumer skips it. */
        const val SAME_AS_TAG = "same-as"

        /**
         * The stability verdict's tag. Its value is "true"/"false" rather than
         * the score: the score is evidence, the verdict is a decision.
         */
        const val SELF_CONSISTENT_TAG = "self-consistent"

        /** The two values [SELF_CONSISTENT_TAG] is ever written with. */
        const val CONSISTENT_YES = "true"

        const val CONSISTENT_NO = "false"

        /** Where a verdict tag carries the unix second it was measured. Absent reads as stale. */
        private const val MEASURED_AT_INDEX = 3

        /** Where a verdict tag carries the rules it was measured under. */
        private const val EPOCH_INDEX = 4

        /**
         * The version of the fold's decision rules. Bump it in the same commit
         * as any change to what a fingerprint concludes, and every earlier
         * `same-as` reads as no verdict on the next pass. Do not bump it for a
         * change that leaves the conclusion alone: the cost is a full
         * re-fingerprint of the store.
         */
        const val FOLD_EPOCH = "2"

        /** The same lever for the stability verdict, versioned separately because it is a separate measurement. */
        const val CONSISTENCY_EPOCH = "1"

        /** NIP-32's label, where the fitness grade lives: single-letter, so `"#l": ["prime"]` is a whole relay list. */
        const val LABEL_TAG = "l"

        /** NIP-32's namespace declaration, which every `l` on this record needs. */
        const val LABEL_NAMESPACE_TAG = "L"

        /**
         * Whose vocabulary the grade is written in. Named for the judgement,
         * not for us, so another monitor may publish [Verdict] values under
         * it and be understood.
         */
        const val FITNESS_NAMESPACE = "relay.fitness"

        /** Where an `["l", <value>, <namespace>]` carries the namespace. */
        const val LABEL_NAMESPACE_INDEX = 2

        /** Where an `["L", <namespace>]` carries the one it declares. */
        const val NAMESPACE_DECLARATION_INDEX = 1

        /**
         * One place right of [MEASURED_AT_INDEX], because NIP-32 spent index 2
         * on the namespace. Using the fold's constant here would age the grade
         * by its evidence string.
         */
        const val LABEL_MEASURED_AT_INDEX = 4

        /** Where the label carries the sentence published beside the grade; see [StandingGrade]. */
        const val LABEL_EVIDENCE_INDEX = 3

        const val LABEL_EPOCH_INDEX = 5

        /**
         * Where the grade used to live, now the software field. Kept because
         * [FitnessPass.retireLegacyGrades] queries this tag to find old grades.
         */
        const val LEGACY_STATUS_TAG = RelayFacts.SOFTWARE_TAG

        /** Measured: does the relay honour `until`, so a paged walk can terminate? */
        const val PAGEABLE_TAG = "pageable"

        /**
         * Measured: did the events it served match the filter that asked for
         * them? Separate from [PAGEABLE_TAG]: a relay may page perfectly and
         * still serve the wrong kind.
         */
        const val COMPLIANT_TAG = "compliant"

        /** Measured: did it answer a NEG-OPEN, so reconcile is on the table? */
        const val NIP77_TAG = "nip77"

        /**
         * The fitness verdict's rules version. Never read by a consumer;
         * written so [FitnessPass.retireStaleEpochs] can take back, at boot,
         * the verdicts this build would no longer draw. Bump it in the same
         * commit as the rule change.
         */
        const val FITNESS_EPOCH = "2"

        /** Thirty days: one probe per url, and a host that splits into several relays is noticed within a month. */
        const val DEFAULT_TTL_SECONDS = 30L * 24 * 60 * 60

        /**
         * The wall clock on one whole [edit]. Deliberately wider than
         * `FitnessPass.PUBLISH_DEADLINE_MS`, which is an instrument and must
         * fire first; this is the floor under writers with none.
         */
        const val EDIT_DEADLINE_MS = 120_000L

        /** Urls per `#d` query. Shared with [RelayDiscovery.undialable] so one query shape has one width. */
        internal const val QUERY_CHUNK = 500

        /** Records held in memory while [loadAll] walks the corpus; bounds a page of it, not the round trips. */
        private const val RECORD_PAGE = 2_000
    }
}
