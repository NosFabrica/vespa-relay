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

import com.nosfabrica.vespa.relay.ingest.StoreWriter
import com.nosfabrica.vespa.relay.progress.StoreCalls
import com.nosfabrica.vespa.relay.progress.storeCall
import com.nosfabrica.vespa.relay.util.fmtDuration
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * The monitor's verdicts, written to kind 30166 records whose `d` tag is the relay url. Each
 * pass owns its own tags on the record and edits around everyone else's; every verdict tag
 * carries its own measured-at stamp and rules epoch, and [load] drops what is stale under either.
 */
class RelayVerdictRecord(
    private val store: IEventStore,
    private val signer: NostrSigner?,
    private val ttlSeconds: Long = DEFAULT_TTL_SECONDS,
    /** Where a record is written; [store] is still where it is read back from. */
    private val writer: StoreWriter = StoreWriter.local(store),
    /** Whose records [load] and [loadAll] read; an edit only ever reads [signer]'s own. */
    private val readFrom: List<String> = listOfNotNull(signer?.pubKey),
    /** The wall clock on each store query [load] and [loadAll] make; past it the read throws [ReadTimeout]. */
    private val readDeadlineMs: Long = READ_DEADLINE_MS,
) {
    /** A verdict read the store did not answer within [deadlineMs]; an ordinary failure, so a caller skips as on any other. */
    class ReadTimeout(
        val deadlineMs: Long,
    ) : Exception("the read outlived its ${fmtDuration(deadlineMs)} deadline")

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
        /**
         * Urls a record of ours exists for AT ALL, whatever its tags now say. The corpus this
         * monitor covers, which is a question about the config; [measured] is a question about
         * freshness, and an epoch bump or a lapsed TTL empties that without narrowing this.
         */
        val recorded: Set<NormalizedRelayUrl> = emptySet(),
        /** The fitness grade this monitor currently stands behind, per url; stale grades are absent. */
        val fitness: Map<NormalizedRelayUrl, StandingGrade> = emptyMap(),
        /** Urls carrying a fold answer of ours that is no longer current: an older epoch, or past the TTL. */
        val staleFolds: Set<NormalizedRelayUrl> = emptySet(),
    )

    /** One chunked record read, booked as the monitor's and bounded by [readDeadlineMs]. */
    private suspend fun readRecords(filter: Filter): List<Event> =
        withTimeoutOrNull(readDeadlineMs) {
            storeCall(StoreCalls.CALLER_MONITOR_VERDICTS, StoreCalls.OP_QUERY, StoreCalls.summarise(filter)) {
                store.query<Event>(filter)
            }
        } ?: throw ReadTimeout(readDeadlineMs)

    /**
     * Read back every verdict covering [candidates]. Throws when the store cannot answer, and
     * must: a pass that reads nothing back skips its batch rather than acting on an empty set.
     */
    suspend fun load(candidates: Collection<NormalizedRelayUrl>): Verdicts {
        if (readFrom.isEmpty() || candidates.isEmpty()) return Verdicts()
        val floor = nowSeconds() - ttlSeconds
        val held = Building()
        val own = ownSweep()
        if (own != null) {
            val (asked, kept) = own.split(candidates.mapTo(LinkedHashSet()) { it.url })
            held.take(kept, floor)
            for (page in readByD(asked)) held.take(page, floor)
            return held.verdicts()
        }
        for (page in readByD(candidates.map { it.url })) held.take(page, floor)
        return held.verdicts()
    }

    /** The records of these `d` values, chunked, each chunk's newest per url, in chunk order once all have answered. */
    private suspend fun readByD(subjects: Collection<String>): List<Collection<Event>> {
        if (subjects.isEmpty()) return emptyList()
        val permits = Semaphore(LOAD_CONCURRENCY)
        return coroutineScope {
            subjects
                .chunked(QUERY_CHUNK)
                .map { chunk ->
                    async {
                        permits.withPermit {
                            newestPerUrl(readRecords(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = readFrom, tags = mapOf("d" to chunk))))
                        }
                    }
                }.awaitAll()
        }
    }

    /**
     * [load] over one short job, each url's records read from the store once and held, so a later
     * load over urls already read is built from what was held, as a store read of them would be.
     * For one coroutine at a time.
     */
    inner class Held internal constructor() {
        /** Subject -> the records a `#d` read of it returned, empty for none. */
        private val bySubject = HashMap<String, List<Event>>()

        /** [load]'s answer over [candidates], reading only the urls not read before. Throws like [load]. */
        suspend fun load(candidates: Collection<NormalizedRelayUrl>): Verdicts {
            if (readFrom.isEmpty() || candidates.isEmpty()) return Verdicts()
            val subjects = candidates.mapTo(LinkedHashSet()) { it.url }
            val missing = subjects.filter { it !in bySubject }
            if (missing.isNotEmpty()) {
                val found = HashMap<String, MutableList<Event>>()
                val wanted = missing.toHashSet()
                for (page in readByD(missing)) {
                    for (event in page) {
                        // Under every subject it answers for, as the `#d` filter matched it.
                        for (tag in event.tags) if (tag.size > 1 && tag[0] == "d" && tag[1] in wanted) found.getOrPut(tag[1]) { ArrayList() } += event
                    }
                }
                for (subject in missing) bySubject[subject] = found[subject].orEmpty()
            }
            val floor = nowSeconds() - ttlSeconds
            val held = Building()
            for (subject in subjects) bySubject[subject]?.let { held.take(it, floor) }
            return held.verdicts()
        }
    }

    /** A fresh [Held], for one job's reads. */
    fun held(): Held = Held()

    /** One record per url: with several signers the newest speaks for it, so no url mixes two monitors' tags. */
    private fun newestPerUrl(records: List<Event>): Collection<Event> {
        if (readFrom.size < 2) return records
        val newest = HashMap<String, Event>(records.size)
        for (event in records) {
            val subject = subjectOf(event) ?: continue
            if (event.createdAt > (newest[subject]?.createdAt ?: Long.MIN_VALUE)) newest[subject] = event
        }
        return newest.values
    }

    /**
     * The fitness grade this monitor currently stands behind, per url, so an inherited verdict
     * is not re-signed with a fresh measured-at stamp. Stale grades do not appear. Throws when
     * the store cannot answer.
     */
    suspend fun fitnessGrades(candidates: Collection<NormalizedRelayUrl>): Map<NormalizedRelayUrl, StandingGrade> = load(candidates).fitness

    /** The grade and the evidence beside it; `alias` onto a different canonical is a different statement. */
    data class StandingGrade(
        val value: String,
        val evidence: String?,
        /** The unix second the grade was measured, from the label's own stamp. */
        val measuredAt: Long? = null,
    )

    /** Every verdict this monitor still stands behind, paged over the corpus. Throws for [load]'s reason. */
    suspend fun loadAll(): Verdicts {
        if (readFrom.isEmpty()) return Verdicts()
        val floor = nowSeconds() - ttlSeconds
        val held = Building()
        val own = ownSweep()
        if (own != null) {
            held.take(ownSince(own, floor), floor)
            return held.verdicts()
        }
        // Buffered only when several signers can name one url; one signer's records stream through.
        val several = if (readFrom.size > 1) ArrayList<Event>() else null
        RelayDiscovery.scan(
            store,
            Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = readFrom, since = floor),
            RECORD_PAGE,
            caller = StoreCalls.CALLER_MONITOR_VERDICTS,
            pageDeadlineMs = readDeadlineMs,
        ) { event -> if (several != null) several += event else held.take(event, floor) }
        several?.let { held.take(newestPerUrl(it), floor) }
        return held.verdicts()
    }

    /**
     * Every record of ours stamped at or after [since]: off the sweep's walk while one is under way,
     * else one paged walk of the store. Throws when the store cannot answer.
     */
    suspend fun ownRecordsSince(since: Long): List<Event> {
        val self = signer?.pubKey ?: return emptyList()
        sweepRecords()?.let { return ownSince(it, since) }
        return ownRecordsIn(store, self, since)
    }

    /** [own]'s records stamped at or after [since], those of urls an edit failed on read afresh. */
    private suspend fun ownSince(
        own: OwnRecords,
        since: Long,
    ): List<Event> {
        val (asked, kept) = own.split(null)
        return (kept + readByD(asked).flatten()).filter { it.createdAt >= since }
    }

    /**
     * Our own records as one paged walk read them, with every edit since folded in: what a sweep reads
     * in place of the store. A url whose edit failed is [unknown], since the store may or may not hold it.
     */
    private class OwnRecords(
        /** Raw `d` value -> our newest record for it. */
        val byD: ConcurrentHashMap<String, Event>,
    ) {
        val unknown: MutableSet<String> = ConcurrentHashMap.newKeySet()

        /** Of [subjects] (null for all), the ones to read afresh, and the records held for the rest. */
        fun split(subjects: Collection<String>?): Pair<List<String>, List<Event>> {
            val asked = ArrayList<String>()
            val held = ArrayList<Event>()
            if (subjects == null) {
                asked += unknown
                for ((d, event) in byD) if (d !in unknown) held += event
            } else {
                for (d in subjects) if (d in unknown) asked += d else byD[d]?.let(held::add)
            }
            return asked to held
        }
    }

    /** One sweep's [OwnRecords], walked on first use. */
    private class Sweep {
        val walk = Mutex()

        @Volatile
        var records: OwnRecords? = null
    }

    @Volatile
    private var sweep: Sweep? = null

    /**
     * Serve the reads of our own records from one paged walk, taken on the first read, until [endSweep].
     * Called around one sweep, or the boot retirement before any sweep, so nothing else writes our records meanwhile.
     */
    fun beginSweep() {
        if (signer != null) sweep = Sweep()
    }

    fun endSweep() {
        sweep = null
    }

    /** The sweep's records, walked now if this is the first read; null outside a sweep. Throws when the walk fails. */
    private suspend fun sweepRecords(): OwnRecords? {
        val s = sweep ?: return null
        s.records?.let { return it }
        return s.walk.withLock { s.records ?: walkOwn().also { s.records = it } }
    }

    /** [sweepRecords], for a reader whose signers are exactly our own. */
    private suspend fun ownSweep(): OwnRecords? = if (readFrom == listOfNotNull(signer?.pubKey)) sweepRecords() else null

    private suspend fun walkOwn(): OwnRecords {
        val self = checkNotNull(signer).pubKey
        val byD = ConcurrentHashMap<String, Event>()
        RelayDiscovery.scan(
            store,
            Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self)),
            RECORD_PAGE,
            caller = StoreCalls.CALLER_MONITOR_VERDICTS,
            pageDeadlineMs = readDeadlineMs,
        ) { event ->
            val d = subjectOf(event) ?: return@scan
            // The newest, as a `#d` read of one url takes it.
            byD.merge(d, event) { held, next -> if (next.createdAt > held.createdAt) next else held }
        }
        return OwnRecords(byD)
    }

    /** What an edit of [url] did to the sweep's records: its signed record, or null when the store may not hold it. */
    private fun settled(
        url: NormalizedRelayUrl,
        event: Event?,
    ) {
        val own = sweep?.records ?: return
        if (event == null) {
            own.unknown += url.url
        } else {
            own.byD[url.url] = event
            own.unknown -= url.url
        }
    }

    /** The sets [Verdicts] carries while a walk fills them in. */
    private class Building {
        val aliases = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val distinct = HashSet<NormalizedRelayUrl>()
        val consistent = HashSet<NormalizedRelayUrl>()
        val inconsistent = HashSet<NormalizedRelayUrl>()
        val speaksNegentropy = HashMap<NormalizedRelayUrl, Boolean>()
        val measured = HashSet<NormalizedRelayUrl>()
        val recorded = HashSet<NormalizedRelayUrl>()
        val fitness = HashMap<NormalizedRelayUrl, StandingGrade>()
        val staleFolds = HashSet<NormalizedRelayUrl>()

        /** Newest wins: a store should hold one record per address, and "should" is not a guarantee. */
        val fitnessFrom = HashMap<NormalizedRelayUrl, Long>()

        fun verdicts() = Verdicts(aliases, distinct, consistent, inconsistent, speaksNegentropy, measured, recorded, fitness, staleFolds)
    }

    /** A page of records, folded into the sets. */
    private fun Building.take(
        held: Collection<Event>,
        floor: Long,
    ) {
        for (event in held) take(event, floor)
    }

    /** One record. The verdicts are read independently; a url may carry any subset. */
    private fun Building.take(
        event: Event,
        floor: Long,
    ) {
        val from = subjectUrlOf(event) ?: return
        // Before any currency check: the record existing is what says this url is in our corpus.
        recorded += from
        event.tags.firstOrNull { it.size > 1 && it[0] == SAME_AS_TAG }?.let { tag ->
            if (!current(tag, FOLD_EPOCH, floor)) {
                staleFolds += from
                return@let
            }
            RelayUrlNormalizer.normalizeOrNull(tag[1])?.let { to ->
                if (from == to) distinct += from else aliases[from] = to
                measured += from
            }
        }
        currentAnswer(event, SELF_CONSISTENT_TAG, CONSISTENCY_EPOCH, floor)?.let { yes ->
            if (yes) consistent += from else inconsistent += from
            measured += from
        }
        // The fitness label too: a relay graded `prime` is the most measured thing on the roster,
        // and it carries neither a fold answer nor a NIP-77 one.
        currentFitnessLabel(event, floor)
            ?.let { label ->
                measured += from
                if (event.createdAt >= (fitnessFrom[from] ?: Long.MIN_VALUE)) {
                    fitnessFrom[from] = event.createdAt
                    fitness[from] =
                        StandingGrade(label[1], label.getOrNull(LABEL_EVIDENCE_INDEX), label.getOrNull(LABEL_MEASURED_AT_INDEX)?.toLongOrNull())
                }
            }
        currentAnswer(event, NIP77_TAG, FITNESS_EPOCH, floor)?.let { yes ->
            speaksNegentropy[from] = yes
            measured += from
        }
    }

    /**
     * [event]'s `true`/`false` answer on its [name] tag, if current under [epoch]; null when absent, stale,
     * or unreadable, which is no verdict rather than a "no".
     */
    private fun currentAnswer(
        event: Event,
        name: String,
        epoch: String,
        floor: Long,
    ): Boolean? =
        when (
            event.tags
                .firstOrNull { it.size > 1 && it[0] == name }
                ?.takeIf { current(it, epoch, floor) }
                ?.get(1)
        ) {
            "true" -> true
            "false" -> false
            else -> null
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
                        "$first + $second events at a ${anchorDays}d anchor, $shared shared -> %.3f".format(Locale.ROOT, score),
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
        pageable: Fact?,
        nip77: Fact?,
        /** Did the answer match the ask. Null for a dial that got no page to check. */
        compliant: Fact? = null,
        facts: RelayFacts = RelayFacts(),
        /** When the grade was measured; a write retried after the store recovered keeps its measurement's time. */
        measuredAt: Long = nowSeconds(),
    ): Event? {
        val at = measuredAt.toString()
        val add =
            buildList {
                add(arrayOf(LABEL_TAG, status, FITNESS_NAMESPACE, evidence, at, FITNESS_EPOCH))
                add(arrayOf(LABEL_NAMESPACE_TAG, FITNESS_NAMESPACE))
                pageable?.let { (yes, why) -> add(arrayOf(PAGEABLE_TAG, if (yes) "true" else "false", why, at, FITNESS_EPOCH)) }
                nip77?.let { (yes, why) ->
                    add(arrayOf(NIP77_TAG, if (yes) "true" else "false", why, at, FITNESS_EPOCH))
                    // The same measured fact again, as a NIP-32 label, because NIP-01 indexes
                    // single-letter tags ONLY: `nip77` above can never be a `#` filter, so a
                    // stream could not select on it and would be left with the relay's own
                    // NIP-11 claim — which is the thing the monitor exists to not believe.
                    // Measured: 11 of the 37 relays advertising NIP-77 declined our NEG-OPEN.
                    // Its own namespace, so `#l` on the grade and `#l` on this stay separable
                    // and a stream ANDs them with `gatedBy`.
                    add(arrayOf(LABEL_TAG, if (yes) SPEAKS_NEGENTROPY else REFUSES_NEGENTROPY, NEGENTROPY_NAMESPACE, why, at, FITNESS_EPOCH))
                    add(arrayOf(LABEL_NAMESPACE_TAG, NEGENTROPY_NAMESPACE))
                }
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
            LABEL_TAG -> tag.getOrNull(LABEL_NAMESPACE_INDEX).let { it == null || it in OWNED_NAMESPACES }
            LABEL_NAMESPACE_TAG -> tag.getOrNull(NAMESPACE_DECLARATION_INDEX).let { it == null || it in OWNED_NAMESPACES }
            PAGEABLE_TAG, NIP77_TAG, COMPLIANT_TAG -> true
            else -> name in RelayFacts.OWNED
        }

    /**
     * Sign and store one fold answer: [subject] is the same server as [sameAs], or its own relay when the
     * two are one url. Null when there is no signer. The evidence goes in the tag, never the content.
     */
    suspend fun publishFold(
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
     * Read this url's record, rewrite it through quartz's [RelayDiscoveryEvent.edit] (every tag this
     * writer does not [owns] kept, [add] appended, stamped strictly newer), sign and store it. Bounded
     * whole by [EDIT_DEADLINE_MS]. A second edit of [url] while one is under way throws: both would
     * read the same record, and the later write would erase the earlier's tags.
     */
    private suspend fun edit(
        url: NormalizedRelayUrl,
        owns: (Array<String>) -> Boolean,
        add: List<Array<String>>,
    ): Event? {
        val signer = signer ?: return null
        // Said on stderr too: a pass's writer counts a throw as a declined write.
        check(editing.add(url)) { "two verdict edits of ${url.url} overlapped".also { System.err.println("monitor: BUG — $it") } }
        var written: Event? = null
        try {
            written = editOnce(url, owns, add, signer)
            return written
        } finally {
            // In the finally, so a cut edit, which may have landed, is read afresh too.
            settled(url, written)
            editing.remove(url)
        }
    }

    /** The urls an [edit] is under way on. */
    private val editing: MutableSet<NormalizedRelayUrl> = ConcurrentHashMap.newKeySet()

    /** [edit]'s read, rewrite, sign and insert, once the url is its own. */
    private suspend fun editOnce(
        url: NormalizedRelayUrl,
        owns: (Array<String>) -> Boolean,
        add: List<Array<String>>,
        signer: NostrSigner,
    ): Event? =
        withTimeoutOrNull(EDIT_DEADLINE_MS) {
            // A failed read aborts the edit; a fresh record would erase other writers' tags.
            val current =
                try {
                    currentRecord(url)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    editFailed(url, "read", e)
                    return@withTimeoutOrNull null
                }
            val template =
                RelayDiscoveryEvent.edit(url, current?.asDiscovery(), owns, createdAt = nowSeconds()) {
                    for (tag in add) add(tag)
                }
            // The deadlines work by cancellation, which must propagate.
            val event =
                try {
                    signer.sign(template)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    editFailed(url, "sign", e)
                    return@withTimeoutOrNull null
                }
            try {
                storeCall(StoreCalls.CALLER_MONITOR_PUBLISH, StoreCalls.OP_INSERT, "kind ${template.kind}, 1 event") {
                    writer.insert(event)
                }
                event
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                editFailed(url, "insert", e)
                null
            }
        }

    /** One failed [edit], logged at most once a minute with the count since; the caller sees only null. */
    private fun editFailed(
        url: NormalizedRelayUrl,
        stage: String,
        e: Exception,
    ) {
        val failed = editFailures.incrementAndGet()
        val now = System.currentTimeMillis()
        val last = editFailureLoggedAt.get()
        if (now - last < EDIT_FAILURE_LOG_MS || !editFailureLoggedAt.compareAndSet(last, now)) return
        System.err.println(
            "router: verdict edit for ${url.url} FAILED at $stage — ${e.javaClass.simpleName}: ${e.message?.take(200)} " +
                "($failed failed edit(s) so far)",
        )
    }

    /** Every [edit] that failed at a read, a signature or an insert since boot. */
    private val editFailures = AtomicLong()

    private val editFailureLoggedAt = AtomicLong()

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

    /** [this] as the typed record quartz edits; a store that hands back a plain [Event] still keeps every tag. */
    private fun Event.asDiscovery(): RelayDiscoveryEvent = this as? RelayDiscoveryEvent ?: RelayDiscoveryEvent(id, pubKey, createdAt, tags, content, sig)

    /** This url's current record, null when nothing holds one, and a throw when the store cannot answer. */
    private suspend fun currentRecord(url: NormalizedRelayUrl): Event? {
        val self = signer?.pubKey ?: return null
        val own = sweepRecords()
        if (own != null && url.url !in own.unknown) return own.byD[url.url]
        val filter = Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to listOf(url.url)))
        return storeCall(StoreCalls.CALLER_MONITOR_PUBLISH, StoreCalls.OP_QUERY, StoreCalls.summarise(filter)) {
            store.query<Event>(filter)
        }.maxByOrNull { it.createdAt }
    }

    companion object {
        /** The url a record is about, its `d` tag as written. */
        fun subjectOf(event: Event): String? = event.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1)

        /**
         * [subjectOf] as a key to verdicts: any url quartz normalizes, `.onion` included, since a read
         * answers about urls its caller already holds. [RelayDiscovery] turns a subject into a url to dial instead.
         */
        private fun subjectUrlOf(event: Event): NormalizedRelayUrl? = subjectOf(event)?.let(RelayUrlNormalizer::normalizeOrNull)

        /** Every record [self] signed that [store] holds stamped at or after [since], in one paged walk. */
        suspend fun ownRecordsIn(
            store: IEventStore,
            self: String,
            since: Long,
        ): List<Event> {
            val found = ArrayList<Event>()
            RelayDiscovery.scan(store, Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), since = since), RelayDiscovery.SCAN_PAGE) { found += it }
            return found
        }

        /** The fitness pass's own tag: a NIP-32 label under [FITNESS_NAMESPACE], told apart by namespace since `l` is shared vocabulary. */
        fun isFitnessLabel(tag: Array<String>): Boolean = tag.size > LABEL_NAMESPACE_INDEX && tag[0] == LABEL_TAG && tag[LABEL_NAMESPACE_INDEX] == FITNESS_NAMESPACE

        /**
         * [event]'s fitness label if it is one we would still act on: under [FITNESS_EPOCH] and measured
         * since [floor] by its own stamp, never the record's `createdAt`, which every writer bumps.
         */
        fun currentFitnessLabel(
            event: Event,
            floor: Long,
        ): Array<String>? =
            event.tags.firstOrNull(::isFitnessLabel)?.takeIf {
                it.getOrNull(LABEL_EPOCH_INDEX) == FITNESS_EPOCH &&
                    (it.getOrNull(LABEL_MEASURED_AT_INDEX)?.toLongOrNull() ?: Long.MIN_VALUE) >= floor
            }

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
        const val FOLD_EPOCH = "3"

        /** The same lever for the stability verdict, a separate measurement. */
        const val CONSISTENCY_EPOCH = "1"

        /** NIP-32's label, where the fitness grade lives: `["l", <grade>, <namespace>, <evidence>, <measured-at>, <epoch>]`. */
        const val LABEL_TAG = "l"

        /** NIP-32's namespace declaration, which every `l` on this record needs. */
        const val LABEL_NAMESPACE_TAG = "L"

        /** Whose vocabulary the grade is written in. Named for the judgement, not for us. */
        const val FITNESS_NAMESPACE = "relay.fitness"

        /**
         * The NIP-32 namespace for what the monitor MEASURED about NIP-77, kept apart from the
         * grade so a stream can filter on either. Both are `l` tags, and NIP-01 ORs the values
         * inside one `#l` — so "prime AND speaks negentropy" is two sources, the grade on
         * `relaySource` and this on `gatedBy`, which intersects them.
         */
        const val NEGENTROPY_NAMESPACE = "relay.negentropy"

        /** Answered a NEG-OPEN when the fitness pass asked. NEVER the relay's own NIP-11 claim. */
        const val SPEAKS_NEGENTROPY = "negentropy"

        /** Declined it. A relay with no measurement carries neither, which is not a "no". */
        const val REFUSES_NEGENTROPY = "no-negentropy"

        /** Every label namespace the fitness pass replaces on each write. */
        private val OWNED_NAMESPACES = setOf(FITNESS_NAMESPACE, NEGENTROPY_NAMESPACE)

        const val LABEL_NAMESPACE_INDEX = 2

        const val NAMESPACE_DECLARATION_INDEX = 1

        /** One place right of [MEASURED_AT_INDEX], because NIP-32 spent index 2 on the namespace. */
        const val LABEL_MEASURED_AT_INDEX = 4

        const val LABEL_EVIDENCE_INDEX = 3

        const val LABEL_EPOCH_INDEX = 5

        /** Where the grade used to live, now the software field; `FitnessRetirement.retireStale` looks for it. */
        const val LEGACY_STATUS_TAG = RelayFacts.SOFTWARE_TAG

        /** Measured: does the relay honour `until`, so a paged walk can terminate? */
        const val PAGEABLE_TAG = "pageable"

        /** Measured: did the events it served match the filter? A relay may page perfectly and serve the wrong kind. */
        const val COMPLIANT_TAG = "compliant"

        /** Measured: did it answer a NEG-OPEN? */
        const val NIP77_TAG = "nip77"

        /** The fitness verdict's rules version; `FitnessRetirement.retireStale` takes back older ones at boot. */
        const val FITNESS_EPOCH = "3"

        const val DEFAULT_TTL_SECONDS = 30L * 24 * 60 * 60

        /** How long a `dead` verdict holds a url out of the passes and the fold. */
        const val DEAD_TTL_SECONDS = 24L * 60 * 60

        /** The wall clock on one whole [edit]. Wider than `WriteLimits.PUBLISH_DEADLINE_MS`, which must fire first. */
        const val EDIT_DEADLINE_MS = 120_000L

        private const val EDIT_FAILURE_LOG_MS = 60_000L

        /** Urls per `#d` query, shared with [RelayDiscovery.undialable]. */
        internal const val QUERY_CHUNK = 500

        /** `#d` queries one [load] has in flight at once. */
        internal const val LOAD_CONCURRENCY = 4

        /**
         * The wall clock on one verdict query. The passes read under the monitor's pass gate, so a
         * query that never answers would stop every pass behind it.
         */
        const val READ_DEADLINE_MS = 60_000L

        private const val RECORD_PAGE = 2_000
    }
}
