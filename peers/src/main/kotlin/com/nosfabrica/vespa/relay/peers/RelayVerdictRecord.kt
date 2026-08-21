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

import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.RelayDiscoveryEvent

/**
 * The NIP-66 half of [RelayAliases]: a fold verdict, written down where the
 * next boot — and anyone else running an outbox crawler — can read it.
 *
 * This is the same monitor that already signs "I could not reach this relay",
 * saying the other thing a dial can prove: which urls are ONE relay. It rides on
 * kind 30166, whose `d` tag is already the relay url, and adds one tag in two
 * forms:
 *
 * ```json
 * ["same-as", "wss://nos.lol/",             "500 newest events, 498 shared with wss://nos.lol/",              "1776038400", "2"]
 * ["same-as", "wss://nostr.ac/v1",          "500 newest events, best 2 shared of 19 peer(s) on this host",     "1776038400", "2"]
 * ```
 *
 * The last two elements are the verdict's own clock (see [current]) and the
 * version of the rules that produced it (see [FOLD_EPOCH]).
 *
 * The first says this url and that url are the same relay. The second — where
 * the value IS the record's own url — says it was measured and found equivalent
 * to nothing but itself, which is trivially true and therefore safe for a
 * reader that does not know the tag.
 *
 * **`same-as` rather than `redirect`, which this used to be called.** A redirect
 * is a directed edge carrying authority: the server told you to go elsewhere,
 * and the url you asked for is not the endpoint. Both halves are false here —
 * the relay said no such thing, we measured it, and the alias serves perfectly
 * well. What a fingerprint establishes is an EQUIVALENCE, and equivalence is
 * symmetric: a consumer running union-find over these tags gets the right
 * partition without having to share our opinion about which member to dial.
 * That opinion is [RelayAliases.PREFERENCE] and it stays ours.
 *
 * Unknown tags are ignored by every other NIP-66 consumer, so a monitor that
 * has never heard of this reads the record as an ordinary relay observation.
 *
 * Three reasons the verdict lives in the store as an event rather than in a
 * state file beside the bands:
 *
 *  - it is a claim about a relay, which is what kind 30166 IS, and the monitor
 *    is already the thing in this process licensed to make those;
 *  - 30166 is addressable, so re-probing a url REPLACES its verdict instead of
 *    appending — the store does the deduplication a file would need code for;
 *  - it is served. An operator can ask this relay why it stopped syncing a url
 *    and get a signed answer with the evidence in it.
 *
 * Read back with [load], which drops anything older than [ttlSeconds]: a url
 * that is a duplicate today may be a distinct relay in a month, and a verdict
 * nobody re-measures is a relay silently missing from the fan-out. It drops
 * anything measured by SUPERSEDED RULES too — see [FOLD_EPOCH], which is the
 * lever for forcing exactly that.
 */
class RelayVerdictRecord(
    private val store: IEventStore,
    private val signer: NostrSigner?,
    private val ttlSeconds: Long = DEFAULT_TTL_SECONDS,
) {
    /**
     * Both halves of what this monitor has decided and still stands behind.
     *
     * [Verdicts.aliases] are the folds; [Verdicts.distinct] are the urls a probe
     * cleared as their own relay. The second is why this returns a pair rather
     * than a map: without persisting "measured, and it is nobody's duplicate",
     * every boot re-fingerprints all the NON-duplicates forever — 59 of them in
     * the live run against a store already holding 128 folds.
     */
    data class Verdicts(
        /** Folded url -> the url that stands in for it. */
        val aliases: Map<NormalizedRelayUrl, NormalizedRelayUrl> = emptyMap(),
        /** Urls proven to be their own relay. Never a key in [aliases]. */
        val distinct: Set<NormalizedRelayUrl> = emptySet(),
        /** Urls measured as answering one filter the same way twice. */
        val consistent: Set<NormalizedRelayUrl> = emptySet(),
        /** Urls measured as NOT doing so — the ones the fan-out refuses. */
        val inconsistent: Set<NormalizedRelayUrl> = emptySet(),
        /**
         * …and urls a pass ASKED and could not decide, by the reason it could
         * not — see [publishUnmeasured].
         *
         * Neither a verdict nor the absence of one: it is the record of an
         * attempt. `consistent` and `inconsistent` say what the relay does;
         * this says why nobody can tell yet, and a url in here is a url the
         * next pass still dials.
         *
         * What it buys is a CORPUS that survives its passes. The stats card's
         * breakdown of "no verdict" used to be the last run's own findings,
         * which meant it described whatever the pass happened to touch and went
         * blank between passes — and, on a card carrying both a sweep's row and
         * a fast lane tick's, drew every reason twice. Read back from the store
         * it is one row per url, deduplicated by the record's own `d` tag,
         * true between passes and for as long as the record stands.
         */
        val unmeasured: Map<NormalizedRelayUrl, String> = emptyMap(),
        /**
         * Whether a url ANSWERED a NEG-OPEN when the fitness pass asked, by
         * url. Absent means unmeasured — no verdict, an expired one, or a
         * deployment reading someone else's — and unmeasured is not "no": the
         * reader tries and finds out, which costs one round trip, where
         * guessing "no" would give up negentropy for a relay that speaks it.
         *
         * The pass has always published this and nothing has ever read it.
         * What it decides is which of the two re-checks of the past a relay
         * gets: reconcile it on `negentropySyncThePastSeconds`, or re-fetch it
         * on `refetchThePastSeconds`.
         */
        val speaksNegentropy: Map<NormalizedRelayUrl, Boolean> = emptyMap(),
    )

    /**
     * Read back every verdict covering [candidates].
     *
     * Queried by `#d` rather than walked, because `d` is a single-letter tag
     * and therefore the only part of these records the tag index can answer on
     * — `same-as` is not queryable and has to be read off the event.
     * [candidates] bounds the query to the urls this cycle actually discovered.
     *
     * The two forms are told apart by comparing the tag's value to the record's
     * own `d`: pointing elsewhere is a fold, pointing at itself is the cleared
     * verdict. Normalising both sides first, so `wss://nos.lol` and
     * `wss://nos.lol/` cannot read as a fold of a url onto itself.
     *
     * **THROWS when the store cannot answer, and must go on doing so.** A
     * fan-out of 16,000 urls is 30-odd chunks and this used to swallow a failed
     * one into an empty result — while [AliasFolding.adopt] forgets every
     * verdict it holds before adopting what comes back, precisely on the promise
     * that a failed read arrives as a failure. One unlucky query therefore
     * unfolded up to [QUERY_CHUNK] urls for that cycle: they were dialled as
     * their own relays, re-probed for a verdict already published, and nothing
     * anywhere said so. A partial answer is not "no verdict", and the only
     * reading that keeps the fold honest is to let the caller keep what it has.
     */
    suspend fun load(candidates: Collection<NormalizedRelayUrl>): Verdicts {
        val self = signer?.pubKey ?: return Verdicts()
        if (candidates.isEmpty()) return Verdicts()
        val floor = nowSeconds() - ttlSeconds
        val held = Building()
        for (chunk in candidates.map { it.url }.chunked(QUERY_CHUNK)) {
            held.take(
                store.query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to chunk))),
                floor,
            )
        }
        return held.verdicts()
    }

    /**
     * Read back EVERY verdict this monitor still stands behind, whatever any one
     * cycle happens to have discovered.
     *
     * [load]'s `#d` bound is the right read in front of a fan-out — it asks
     * about the urls being dialled and nothing else. It is the wrong read for
     * [AliasFolding.measure], because a duplicate is a property of a url NEXT TO
     * another one and the candidate set is not the whole neighbourhood: a url's
     * siblings on the same host can be absent from it for reasons that have
     * nothing to do with the fold — held out as known dead, dropped from a relay
     * list since, discovered by a stream that has since been reconfigured. A
     * group assembled from candidates alone then has one member, [RelayAliases.unresolved]
     * drops it for being a group of one, and the new url is dialled as its own
     * relay forever while a signed record naming its survivor sits in the store
     * unread.
     *
     * So the fold groups the WORLD and re-measures the groups a new url landed
     * in. That costs one unbounded-by-`#d` query per pass — a pass that already
     * spends minutes on sockets — and nothing per url.
     *
     * Bounded by [ttlSeconds] on the store's own clock as well as by the tag's:
     * a record last written before the floor cannot carry a tag stamped after it
     * (every write re-signs the record whole), so this drops only records
     * [current] would have refused anyway, and it keeps the query off the
     * records of every url that has aged out.
     *
     * THROWS for [load]'s reason, and the caller's fallback is the same: a store
     * that cannot answer is not a store saying "no verdict".
     *
     * PAGED, because the whole point is that this asks for a corpus rather than
     * for a list: [load] is bounded by the caller's candidates and this is
     * bounded by nothing, so a single unlimited query materializes every record
     * this router has ever signed at once — five figures of events on the
     * deployment that needed the feature, held whole while the tags are read off
     * them. [RelayDiscovery.scan] walks the same filter a page at a time for
     * exactly this reason, and the verdicts it accumulates are two small maps.
     */
    suspend fun loadAll(): Verdicts {
        val self = signer?.pubKey ?: return Verdicts()
        val floor = nowSeconds() - ttlSeconds
        val held = Building()
        RelayDiscovery.scan(
            store,
            Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), since = floor),
            RECORD_PAGE,
        ) { event -> held.take(event, floor) }
        return held.verdicts()
    }

    /**
     * The four sets [Verdicts] carries, while a walk is still filling them in.
     *
     * Its own type so [load] and [loadAll] read a record the SAME way: they
     * differ in which records they ask the store for and in nothing else, and a
     * second copy of the tag parsing is a second place for the fold and the
     * stability verdict to drift apart.
     */
    private class Building {
        val aliases = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val distinct = HashSet<NormalizedRelayUrl>()
        val consistent = HashSet<NormalizedRelayUrl>()
        val inconsistent = HashSet<NormalizedRelayUrl>()
        val unmeasured = HashMap<NormalizedRelayUrl, String>()
        val speaksNegentropy = HashMap<NormalizedRelayUrl, Boolean>()

        fun verdicts() = Verdicts(aliases, distinct, consistent, inconsistent, unmeasured, speaksNegentropy)
    }

    /** A page of records, folded into the sets above — see [Building]. */
    private fun Building.take(
        held: List<Event>,
        floor: Long,
    ) {
        for (event in held) take(event, floor)
    }

    /** One record, read the one way both loads read it — see [Building]. */
    private fun Building.take(
        event: Event,
        floor: Long,
    ) {
        val subject = event.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: return
        val from = RelayUrlNormalizer.normalizeOrNull(subject) ?: return
        // Two independent verdicts on one record, read independently: a url may
        // carry a fold, a stability answer, both or neither, and an early exit
        // for a missing `same-as` used to drop the whole event — which would
        // make every stability verdict on a url that was never folded invisible.
        event.tags.firstOrNull { it.size > 1 && it[0] == SAME_AS_TAG }?.takeIf { current(it, FOLD_EPOCH, floor) }?.get(1)?.let { sameAs ->
            RelayUrlNormalizer.normalizeOrNull(sameAs)?.let { to ->
                if (from == to) distinct += from else aliases[from] = to
            }
        }
        event.tags
            .firstOrNull { it.size > 1 && it[0] == SELF_CONSISTENT_TAG }
            ?.takeIf { current(it, CONSISTENCY_EPOCH, floor) }
            ?.let { tag ->
                when (tag[1]) {
                    CONSISTENT_YES -> consistent += from

                    CONSISTENT_NO -> inconsistent += from

                    // NOT a verdict, and read into its own map for that
                    // reason: the pass asked and could not decide, and the
                    // WHY is the evidence element rather than a value of its
                    // own — see [publishUnmeasured]. A row carrying no
                    // evidence is dropped rather than counted namelessly,
                    // since the reason is the entire content of this state.
                    CONSISTENT_UNMEASURED -> tag.getOrNull(EVIDENCE_INDEX)?.takeIf { it.isNotBlank() }?.let { unmeasured[from] = it }

                    // An answer this writer does not recognise is not a
                    // verdict. Ignored rather than guessed at: guessing
                    // "unstable" would drop a relay on a tag we cannot read.
                    else -> Unit
                }
            }
        // The third independent verdict, on the fitness pass's own epoch: did
        // this url answer a NEG-OPEN? Same rule as the two above — an
        // unreadable value is no verdict, so the reader is left to try.
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
     * A separate tag from `same-as` and never inferred from it: they answer
     * different questions about the same url, and a relay may be perfectly
     * stable while wearing six aliases, or a unique endpoint that cannot be
     * measured twice. [edit] owns each tag independently, so writing one leaves
     * the other — and everyone else's — exactly where it was.
     *
     * "false" is the one verdict here that costs a relay its place in the
     * fan-out, which is why nothing writes it from silence: an unmeasurable url
     * gets NO tag at all rather than a negative one. See [RelayConsistency].
     */
    suspend fun publishConsistency(
        url: NormalizedRelayUrl,
        consistent: Boolean,
        first: Int,
        second: Int,
        shared: Int,
        score: Double,
        /**
         * How old the anchor the two reads were taken at was, in days — the
         * consistency pass's own `ANCHOR_LAG_SECONDS`, passed in rather than
         * read from it.
         *
         * A record in :peers cannot reach into the pass that measures, and
         * should not: the constant is that pass's tuning, documented beside the
         * two-run probe that fixed it, and a second copy here is the shape that
         * comes to disagree with the measurement it describes.
         */
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
                        // See [current]: the record's own createdAt is bumped by
                        // quartz's monitor on every connection, so a verdict has
                        // to carry the moment it was MEASURED or it never ages.
                        nowSeconds().toString(),
                        // And which rules measured it — see [CONSISTENCY_EPOCH].
                        CONSISTENCY_EPOCH,
                    ),
                ),
        )

    /**
     * Write down that the gate ASKED and could not decide — and WHY, where the
     * why is ours to say out loud.
     *
     * ```json
     * ["self-consistent", "unmeasured", "never answered a REQ: the TLS handshake failed", "1776038400", "1"]
     * ["self-consistent", "unmeasured", "too few events to judge on",                     "1776038400", "1"]
     * ```
     *
     * ## Why this exists at all
     *
     * An unmeasurable url used to get NO tag, on the rule stated beside
     * [publishConsistency] and still true: nothing writes a `false` from
     * silence, because that verdict costs a relay its place in the fan-out and
     * silence is not evidence of instability. That rule is about `false`. It
     * left a second question unanswered — "why has this url no verdict" — and
     * the only record of the answer was the pass's own memory, published on the
     * stats card as whatever the LAST RUN happened to find.
     *
     * Which meant the corpus could not be described. Four thousand urls with no
     * stability verdict is the normal state of a discovered network and it
     * divides into completely different problems — dead hosts, auth walls,
     * relays holding nine events — with completely different fixes, and the
     * card's breakdown of them went blank between passes, described only the
     * urls one run touched, and drew every reason twice on a router running
     * both a sweep and a fast lane. Addressable records fix all three at once:
     * one row per url keyed by `d`, replaced rather than appended, standing
     * between passes for as long as the record does.
     *
     * ## What it will and will not say
     *
     * [reason] is the pass's own sentence for the finding, and only findings
     * that are CLAIMS ABOUT THE RELAY reach here — see
     * `ConsistencyPass.Unmeasured.publishable`. A url our transport would not
     * carry, a probe that threw, a job that ran out our wall clock: those are
     * facts about this router, they are signed by nobody, and they read back as
     * "nothing recorded", which is exactly what the store knows about them.
     *
     * The value is [CONSISTENT_UNMEASURED] and not a fourth tag, because it is
     * the same question's answer: `self-consistent` is what this monitor thinks
     * of a relay's stability, and "we asked and could not tell" is one of the
     * things it can think. It also means [edit]'s ownership takes the previous
     * answer away, which is right — a verdict that has aged out and a url that
     * has since stopped answering must not leave a stale `true` standing beside
     * the reason it could not be re-measured.
     */
    suspend fun publishUnmeasured(
        url: NormalizedRelayUrl,
        reason: String,
    ): Event? =
        edit(
            url,
            owns = owning(SELF_CONSISTENT_TAG),
            add =
                listOf(
                    arrayOf(
                        SELF_CONSISTENT_TAG,
                        CONSISTENT_UNMEASURED,
                        reason,
                        // See [current]: the record's own createdAt is bumped by
                        // quartz's monitor on every connection, so this has to
                        // carry the moment it was ATTEMPTED or it never ages.
                        nowSeconds().toString(),
                        CONSISTENCY_EPOCH,
                    ),
                ),
        )

    /**
     * The fitness pass's whole write: the grade a stream filters on, the two
     * measured facts a visit reads back, and everything the pass learned about
     * the relay on the way there.
     *
     * ## The grade is a NIP-32 LABEL, and it used to squat `s`
     *
     * It has to be single-letter — only those are indexed, and the grade is the
     * one value streams FILTER on. It used to take `s` for that reason alone,
     * which was a straight collision: `s` is where every monitor in the wild
     * publishes the relay's SOFTWARE (`git+https://github.com/hoytech/strfry.git`
     * on 172 of 400 records sampled off `nos.lol`), so our records said
     * `s: dead` where a reader expected a repository url — and this monitor
     * could not publish the software field at all.
     *
     * NIP-32 is the seam that exists for precisely this: [LABEL_TAG] carries an
     * opinion, [LABEL_NAMESPACE_TAG] says whose vocabulary it is written in, and
     * a reader who does not know that vocabulary skips it. So the grade rides
     * `l` under [FITNESS_NAMESPACE], beside the country and ASN labels other
     * monitors already put on the same record, and `s` goes back to meaning what
     * everyone else means by it.
     *
     * **NIP-32 fixes index 2 as the namespace**, so this one tag carries the
     * house shape one place to the right of the others — value, namespace,
     * evidence, measured-at, epoch. [LABEL_MEASURED_AT_INDEX] is why that is
     * spelled out rather than shared with [MEASURED_AT_INDEX].
     *
     * ## What else it writes, and why it is the same edit
     *
     * `n`, `R` and the two rtts are MEASURED on the dial this pass already
     * paid for; `s` and `N` are read off the relay's NIP-11 document. Written
     * here rather than by a second writer because they are facts about the
     * same url learned in the same pass, and a record is cheaper to reason
     * about with one author per address than with two racing edits.
     *
     * They are also what makes our records legible to anyone else. A 30166
     * carrying no `rtt-open` inside the TTL is read by quartz's own convention
     * as "checked, could not open" — so every record this monitor signed,
     * `prime` ones included, told every foreign crawler applying that rule
     * that the relay was unreachable.
     */
    suspend fun publishFitness(
        url: NormalizedRelayUrl,
        status: String,
        evidence: String,
        pageable: Pair<Boolean, String>?,
        nip77: Pair<Boolean, String>?,
        facts: RelayFacts = RelayFacts(),
    ): Event? {
        val at = nowSeconds().toString()
        val add =
            buildList {
                add(arrayOf(LABEL_TAG, status, FITNESS_NAMESPACE, evidence, at, FITNESS_EPOCH))
                add(arrayOf(LABEL_NAMESPACE_TAG, FITNESS_NAMESPACE))
                pageable?.let { (yes, why) -> add(arrayOf(PAGEABLE_TAG, if (yes) "true" else "false", why, at, FITNESS_EPOCH)) }
                nip77?.let { (yes, why) -> add(arrayOf(NIP77_TAG, if (yes) "true" else "false", why, at, FITNESS_EPOCH)) }
                addAll(facts.tags())
            }
        return edit(url, owns = ::ownedByFitness, add = add)
    }

    /**
     * Take the fitness verdict back: the same ownership as [publishFitness]
     * with nothing to add, so this pass's tags leave the record and everyone
     * else's — the fold's `same-as`, the gate's `self-consistent`, another
     * monitor's label under another namespace — ride through untouched.
     *
     * This is how a rules change reaches readers now. The epoch used to be
     * checked on every read, which meant every consumer of our records had to
     * know our versioning scheme existed and no foreign NIP-66 monitor could
     * ever satisfy it. Retracting the claim ourselves is the same guarantee
     * stated where it belongs: a verdict taken under rules we no longer apply
     * stops being a verdict, and the url reads as one we have not measured —
     * which is exactly the state that gets it re-measured.
     *
     * The measured facts go with it. They were taken on the dial that produced
     * the verdict, so a retraction that kept them would leave an rtt and a
     * software string standing as current readings of a url nothing has
     * measured since.
     */
    suspend fun retireFitness(url: NormalizedRelayUrl): Event? = edit(url, owns = ::ownedByFitness, add = emptyList())

    /**
     * Everything the fitness pass replaces on each write — and NOTHING else,
     * which is why this is a predicate rather than the name set the other
     * writers use.
     *
     * `l` and `L` are shared vocabulary: a foreign monitor labels the same relay
     * with its country and its ASN, and a future pass of ours may label it under
     * a namespace of its own. Owning the tag NAME would delete all of that on
     * every sweep. Owning our own namespace inside it deletes exactly our own
     * previous answer, which is what a replaceable record's writer is entitled
     * to. A bare `l` or `L` carrying no namespace is nobody's vocabulary — no
     * reader can attribute it, so there is no writer it could be taken from —
     * and it is dropped rather than carried forward for the life of the
     * record.
     *
     * The measured facts are owned WHOLE and unconditionally, including on a
     * pass that learned none of them. A verdict that changed makes the old
     * facts claims about a different relay, and carrying them forward would pin
     * `pageable true`, a 40ms rtt and strfry's version to a url that has been
     * dead for a week. That is also what retires the old grades: `s` is in
     * [RelayFacts.OWNED] as the software field now, so `[s, dead]` is replaced
     * the first time this pass re-measures the url.
     */
    private fun ownedByFitness(tag: Array<String>): Boolean =
        when (val name = tag.firstOrNull()) {
            null -> false

            // A LABEL WITH NO NAMESPACE is nobody's vocabulary and cannot be
            // read by anyone, including us. Dropped as malformed rather than
            // carried forward forever, which is the one case where owning a
            // shared tag by name is right: there is no other writer it could
            // belong to.
            LABEL_TAG -> tag.getOrNull(LABEL_NAMESPACE_INDEX).let { it == null || it == FITNESS_NAMESPACE }

            LABEL_NAMESPACE_TAG -> tag.getOrNull(NAMESPACE_DECLARATION_INDEX).let { it == null || it == FITNESS_NAMESPACE }

            PAGEABLE_TAG, NIP77_TAG -> true

            else -> name in RelayFacts.OWNED
        }

    /**
     * Sign and store one verdict. Returns the event so a caller can push it
     * upstream; null when there is no signer, which is also when the router
     * runs without a NIP-66 monitor at all.
     *
     * The evidence goes in the tag's third element — not the content, which
     * belongs to the relay's own NIP-11-ish document and is carried across
     * edits untouched. Nothing parses the evidence; it is there because this is
     * a public statement about somebody else's server and the reader deserves
     * to see what it rests on.
     */
    suspend fun publish(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): Event? = write(alias, canonical, "$sampled newest events, $shared shared with ${canonical.url}")

    /**
     * Sign and store the one fold whose evidence is not a containment: a `ws://`
     * url and the `wss://` url of the same host and path, both of which
     * answered — see [RelayAliases.schemeTwins].
     *
     * ```json
     * ["same-as", "wss://nos.lol/", "same endpoint as wss://nos.lol/ over TLS, both answered; 9 newest events here", "1776038400", "2"]
     * ```
     *
     * It goes out through [write] like every other fold, so it carries the same
     * clock and the same rules version and expires on the same terms — a fold
     * whose evidence is different is still a fold.
     *
     * A separate call rather than [publish] with the numbers filled in, because
     * the numbers would be a lie by implication. These pairs are folded
     * PRECISELY where the windows could not decide — nine events on both sides,
     * or a twin whose own window was taken a month ago — so quoting "9 shared"
     * beside a `same-as` would offer a containment as the reason when the reason
     * is that the two urls name one endpoint and both of them spoke. The reader
     * of a signed month-long claim about somebody else's server deserves the
     * argument that was actually made.
     */
    suspend fun publishSecureTwin(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        /**
         * True when the pair's windows came through
         * [RelayAliases.GROUP_METADATA_KINDS] — see [publishGroupList] for why
         * that changes the noun. The ARGUMENT is unchanged (the two urls name one
         * endpoint and both answered); only "newest events" would be the wrong
         * name for a relay's list of groups.
         */
        groupList: Boolean = false,
    ): Event? =
        write(
            alias,
            canonical,
            "same endpoint as ${canonical.url} over TLS, both answered; " +
                if (groupList) "$sampled group definitions here" else "$sampled newest events here",
        )

    /**
     * Sign and store a fold decided on a relay's LIST OF GROUPS rather than on a
     * slice of its event feed — see [RelayAliases.GROUP_METADATA_KINDS].
     *
     * ```json
     * ["same-as", "wss://groups.example/", "same group list as wss://groups.example/: 7 of 7 group definitions shared", "1776038400", "2"]
     * ```
     *
     * A separate call rather than [publish], for the reason
     * [publishSecureTwin] is one: the numbers are true but the SENTENCE is not.
     * "7 newest events, 7 shared" invites a reader to check it against a
     * general window and find seven events where the relay serves thousands,
     * and to read the fold as resting on a sample far thinner than the one it
     * actually rests on — which is a relay's COMPLETE list of groups, nothing
     * withheld. The number is the same either way; what changes is whether the
     * reader can tell what was measured.
     */
    suspend fun publishGroupList(
        alias: NormalizedRelayUrl,
        canonical: NormalizedRelayUrl,
        sampled: Int,
        shared: Int,
    ): Event? = write(alias, canonical, "same group list as ${canonical.url}: $shared of $sampled group definitions shared")

    /**
     * Sign and store the weakest thing this monitor says: these urls share a host,
     * every one of them answered, none of them would serve anything, so they were
     * treated as one.
     *
     * ```json
     * ["same-as", "wss://x/", "nothing readable at any of 5 url(s) on this host; folded on the shared name, not on a measurement", "1776038400", "2"]
     * ```
     *
     * **The evidence says "not on a measurement" in so many words, and that is
     * the point.** Every other form here quotes a number taken off the wire. This
     * one has none to quote — it is a default applied in the absence of evidence
     * — and a reader of a signed claim about somebody else's server is owed that
     * distinction plainly rather than left to infer it from a missing figure.
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
     * Sign and store the other verdict: this url was fingerprinted against the
     * other urls on its host and matched none of them.
     *
     * Written as `same-as` pointing at the record's OWN url, which is a true
     * statement rather than a placeholder — the equivalence class of this relay
     * contains only itself, as far as this measurement saw.
     *
     * **What it does not claim.** Every url in a group is compared to the
     * group's leader, not to each other, so this says "not the leader" and not
     * "not any of them". Two paths on a host that are duplicates OF EACH OTHER
     * but not of the leader are both recorded distinct and both keep getting
     * dialled. That is a property of leader-based grouping, present within a
     * single pass as much as across boots, and persisting the verdict neither
     * causes it nor makes it worse.
     *
     * [comparedAgainst] therefore names what was ACTUALLY held up against this
     * url — the leader's own url for a member, a count of members for the
     * leader. It once said "of N peers on this host", which counted
     * comparisons that never happened in a signed, month-long statement about
     * somebody else's server.
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
            // The measurement's own clock — see [current] for why the event's
            // cannot be used — and the rules it was taken under, see
            // [FOLD_EPOCH].
            add = listOf(arrayOf(SAME_AS_TAG, sameAs.url, evidence, nowSeconds().toString(), FOLD_EPOCH)),
        )

    /**
     * Edit a replaceable record: read what is there, keep everything this
     * writer does not own, apply [add], and store it one second past whatever
     * it replaced.
     *
     * **A replaceable event has one address and more than one writer, so
     * writing is always an edit.** NIP-66's relay record is addressed by
     * `d` = the relay url, and the passive monitor updates it every time a
     * connection is opened. A writer that builds the record from its own tags
     * alone silently deletes everyone else's — measured here, `[d, n,
     * rtt-open]` became `[d, redirect]` — and nothing about the result looks
     * wrong: still signed, still a valid NIP-66 record, just saying less than
     * it did. Anything that reads that record downstream loses information it
     * had no way to know was ever there.
     *
     * [owns] decides which tags this writer is allowed to replace. Everything
     * else is carried forward untouched, whoever wrote it and whatever it
     * means. A PREDICATE rather than a set of names because ownership is not
     * always a whole tag name: NIP-32's `l` carries every labeller's opinion,
     * and the fitness pass may replace only its own namespace's — see
     * [ownedByFitness]. [owning] is the name-set form the other writers use.
     *
     * The timestamp is `max(now, existing + 1)` rather than `now`, because a
     * store enforcing replaceable semantics REJECTS an edit that is not newer
     * — and two writers seconds apart, or a clock that has not moved, are
     * ordinary. Silently losing the write to `replaced: a newer version
     * exists` is how a repair pass reports success having done nothing.
     */
    private suspend fun edit(
        url: NormalizedRelayUrl,
        owns: (Array<String>) -> Boolean,
        add: List<Array<String>>,
    ): Event? {
        val signer = signer ?: return null
        val current = currentRecord(url)
        val kept = current?.tags?.filterNot { it.firstOrNull() == "d" || owns(it) }.orEmpty()
        val at = maxOf(nowSeconds(), (current?.createdAt ?: 0L) + 1)
        val template =
            RelayDiscoveryEvent.build(url, current?.content.orEmpty(), at) {
                for (tag in kept) add(tag)
                for (tag in add) add(tag)
            }
        return runCatching {
            val event = signer.sign(template)
            store.insert(event)
            event
        }.getOrNull()
    }

    /**
     * The ordinary form of [edit]'s ownership: these tag NAMES, whole.
     *
     * Right for every tag this monitor is the only possible writer of. Wrong
     * for `l`/`L`, which are shared — see [ownedByFitness].
     */
    private fun owning(vararg names: String): (Array<String>) -> Boolean = { it.firstOrNull() in names }

    /**
     * Is this VERDICT one we would still act on: taken under the rules we
     * currently apply, and within its TTL?
     *
     * ## The rules half
     *
     * A verdict is a measurement, and a measurement means what the procedure
     * that took it meant. The fold's procedure has changed repeatedly —
     * comparing a host's urls to each other rather than only to whichever one
     * led, refusing to call a url distinct on a window too thin to say so,
     * proving the yardstick before making a negative claim — and each of those
     * changed what the SAME dials would have concluded. A record signed before
     * one of them is not a stale reading of the current rule; it is a reading of
     * a different rule, and no amount of waiting makes it agree.
     *
     * Left to the TTL alone, those verdicts stand for a month, and the fold
     * spends that month faithfully applying conclusions it would no longer draw
     * — with no way to tell from outside which url is folded on today's evidence
     * and which on last week's. [FOLD_EPOCH] is the lever: bump it in the same
     * commit as the rule change, and every verdict from before it reads as no
     * verdict at all, which is precisely the state that makes
     * [RelayAliases.unresolved] hand the group back and [AliasFolding.measure]
     * re-take it.
     *
     * A tag carrying no epoch is such a record by definition — nothing has ever
     * written one but an older build — so it reads stale rather than being
     * guessed at.
     *
     * ## The clock half
     *
     * **The two are not the same clock, and reading the record's was a bug that
     * made half these verdicts immortal.** Kind 30166 is addressable and shared:
     * quartz's own `RelayMonitor` rewrites the record for every relay this
     * client connects to, on a 5-minute flush, carrying our tags forward
     * untouched. So `event.createdAt` tracks the last time we TALKED to the
     * relay, not the last time we MEASURED it — and for any relay still in the
     * fan-out that is always minutes ago.
     *
     * The effect was exactly backwards from what the TTL is for. A relay we
     * REFUSED is never dialled again, so nothing refreshes its record, so it
     * ages out and is re-measured on schedule — that half worked. A relay we
     * KEPT is dialled constantly, so its record never aged, so its verdict was
     * never re-taken: measure once, trust forever. A relay that degrades after
     * passing would never have been caught, which is the whole case the monthly
     * re-measure exists for. The same applied to a fold's `same-as`: a folded
     * url expires, the canonical it folded onto does not.
     *
     * So the measurement stamps its OWN time into the tag, and that is what is
     * aged.
     *
     * **A tag without one used to fall back to the event's clock, and that
     * fallback was the same trap wearing a different hat.** It was written for
     * the records that predate the stamp, on the reasoning that the event's
     * clock is the only reading available for them — but the event's clock is
     * exactly the one that is bumped every time we connect, so the fallback made
     * those records IMMORTAL for the whole population it matters for: a relay
     * still in the fan-out is dialled constantly, its record is rewritten
     * constantly, and its pre-stamp verdict could therefore never age out under
     * any TTL. Nothing would ever have re-measured them. An unstamped verdict is
     * now simply stale, which costs one re-measure per url and is the only
     * reading that terminates.
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

    /** This url's current record, or null when nothing holds one yet. */
    private suspend fun currentRecord(url: NormalizedRelayUrl): Event? {
        val self = signer?.pubKey ?: return null
        val held: List<Event> =
            runCatching {
                store.query<Event>(
                    Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to listOf(url.url))),
                )
            }.getOrNull().orEmpty()
        return held.maxByOrNull { it.createdAt }
    }

    companion object {
        /**
         * The tag that carries the verdict, in both forms. Not a NIP-66 tag —
         * this monitor defines it — so it is spelled out rather than
         * abbreviated, and every other consumer skips it as an unknown tag.
         *
         * Named for the relation it states rather than the action we take on
         * it: `same-as` is an equivalence, which is what a matching fingerprint
         * proves, while `redirect` (what this was called first) would smuggle
         * in both an instruction the relay never gave and our own opinion about
         * which member of the class to dial.
         */
        const val SAME_AS_TAG = "same-as"

        /**
         * The tag carrying the stability verdict — also this monitor's own, also
         * skipped as unknown by every other NIP-66 consumer.
         *
         * Its value is a plain "true"/"false" rather than the score, because the
         * score is evidence and the verdict is a decision: a reader applying a
         * different bar to our number would be making a claim we did not make.
         * The number is in the third element where the rest of the evidence goes.
         */
        const val SELF_CONSISTENT_TAG = "self-consistent"

        /** The two VERDICTS [SELF_CONSISTENT_TAG] is written with. */
        const val CONSISTENT_YES = "true"

        const val CONSISTENT_NO = "false"

        /**
         * …and the third value, which is not a verdict — see
         * [publishUnmeasured].
         *
         * Safe for a reader that only knows the other two: this file already
         * ignores an unrecognised value rather than guessing at it, on the rule
         * that a tag we cannot read is no verdict, and every other NIP-66
         * consumer skips the whole tag as unknown. So a `false` never appears
         * where a relay was merely unreachable, in our store or in anybody
         * else's.
         */
        const val CONSISTENT_UNMEASURED = "unmeasured"

        /**
         * Where a verdict tag carries its evidence — the sentence a human reads
         * to see what the value above rests on.
         *
         * Read back for exactly one value: [CONSISTENT_UNMEASURED], where the
         * evidence IS the state. `500 + 500 events at a 7d anchor` explains a
         * `true`; `never answered a REQ: the TLS handshake failed` is the whole
         * content of an `unmeasured`, and a row without one is dropped.
         */
        private const val EVIDENCE_INDEX = 2

        /**
         * Where a verdict tag carries the unix second it was MEASURED.
         *
         * Absent on records written before this existed, which [current] reads
         * as stale — see there for why the fallback it replaced could not work.
         */
        private const val MEASURED_AT_INDEX = 3

        /** Where a verdict tag carries the rules it was measured under. */
        private const val EPOCH_INDEX = 4

        /**
         * The version of the FOLD's decision rules — the whole content of
         * "force a re-measure", in one character.
         *
         * **Bump it in the same commit as any change to what a fingerprint
         * concludes**, and every `same-as` written before that commit reads as
         * no verdict at all on the next pass: [RelayAliases.unresolved] hands
         * the group back, [AliasFolding.measure] re-dials it under the new
         * rules, and [edit] replaces the old tag with the new answer. Nothing
         * has to be deleted, no operator has to intervene, and a second router
         * signing with the same key converges the moment it runs the same build.
         *
         * Do NOT bump it for a change that leaves the conclusion alone —
         * logging, budget, ordering, the socket refcount. The cost is a full
         * re-fingerprint of the store — one pass, [AliasFolding.DEFAULT_DIAL_CONCURRENCY]
         * at a time, however many urls that is — and while it runs every
         * un-re-measured url is dialled unfolded. That is the correct price for
         * a rule change and pure waste for anything else.
         *
         * **2** — everything published to date was measured under rules since
         * corrected in three ways that change verdicts: a host's urls are now
         * compared to each other and not only to whichever one led (which had
         * been signing genuine duplicates as distinct relays, six at a time on
         * `haven.calva.dev` alone), any url on the host may hold the ruler
         * rather than only the preferred one (which abandoned whole foldable
         * hosts), and a yardstick must reproduce its own window before a
         * negative claim is signed. Epoch 1 is every record written before this
         * element existed; it is not spelled anywhere, because nothing needs to
         * name it to reject it.
         */
        const val FOLD_EPOCH = "2"

        /**
         * The same lever for the STABILITY verdict, versioned separately
         * because it is a separate measurement with a separate cost — bumping
         * the fold must not re-dial every relay for a consistency answer that
         * has not changed.
         *
         * **1** — [ConsistencyPass]'s rules have not changed since they shipped.
         * The records already in a store still have to be re-taken once, since
         * they carry no epoch, and the ones older than the measured-at stamp
         * were never going to expire on their own anyway — see [current]. From
         * here they age normally.
         */
        const val CONSISTENCY_EPOCH = "1"

        /**
         * NIP-32's label, which is where the fitness grade lives — the one tag
         * streams FILTER on, and single-letter because only those are indexed:
         * `"#l": ["prime"]` is a whole relay list.
         *
         * See [Verdict] for the vocabulary and [FITNESS_NAMESPACE]
         * for what stops it colliding with anyone else's.
         */
        const val LABEL_TAG = "l"

        /** NIP-32's namespace declaration, which every `l` on this record needs. */
        const val LABEL_NAMESPACE_TAG = "L"

        /**
         * Whose vocabulary the grade is written in.
         *
         * NAMED FOR THE JUDGEMENT, NOT FOR US. A monitor's opinion is only worth
         * publishing if somebody else can act on it, and `nosfabrica.*` or
         * `vespa.*` would say the answer is about our deployment rather than
         * about the relay. `relay.fitness` says what was measured, so a crawler,
         * an archiver or a client picking read relays can use the same records
         * without adopting our stack — and a second monitor may publish grades
         * under this namespace and be understood without asking us anything.
         *
         * It also names the SHAPE: everything under this namespace is one of
         * [Verdict]'s values, so a reader who knows the namespace
         * knows the whole vocabulary.
         */
        const val FITNESS_NAMESPACE = "relay.fitness"

        /** Where an `["l", <value>, <namespace>]` carries the namespace. */
        const val LABEL_NAMESPACE_INDEX = 2

        /** …and where an `["L", <namespace>]` carries the one it declares. */
        const val NAMESPACE_DECLARATION_INDEX = 1

        /**
         * The house shape — evidence, measured-at, epoch — one place right of
         * where the other verdict tags carry it, because NIP-32 has already
         * spent index 2 on the namespace.
         *
         * Spelled out rather than shared with [MEASURED_AT_INDEX] precisely
         * because they differ by one: a reader that used the fold's constant
         * here would age the grade by its own evidence string, which parses to
         * null and reads as "this record does not say".
         */
        const val LABEL_MEASURED_AT_INDEX = 4

        const val LABEL_EPOCH_INDEX = 5

        /**
         * Where the grade used to live — now the SOFTWARE field, which is what
         * it always meant to everyone else.
         *
         * Sampled live off `relay.nostr.watch` and `nos.lol`, 12 monitors, 800
         * records: 539 carry `s` and every value is a repository url
         * (`git+https://github.com/hoytech/strfry.git`, `nostr-rs-relay`,
         * `haven`). NIP-66 itself defines no `s` at all, so nothing in the spec
         * ever stopped this monitor writing `s: dead` where a reader expected
         * strfry's git url; only the wild made it a collision, and it is one all
         * the same.
         *
         * The constant survives the move because the MIGRATION needs it: every
         * record in this store still carries a grade here — measured, 4,000 of
         * 4,000 — and [FitnessPass.retireLegacyGrades] queries exactly this tag
         * to find them. The fitness writer owns it either way, so a url the
         * pass re-measures has its stale grade replaced by the real software
         * string without the migration having to reach it first.
         */
        const val LEGACY_STATUS_TAG = RelayFacts.SOFTWARE_TAG

        /** Measured: does the relay honour `until`, i.e. can a paged walk terminate? */
        const val PAGEABLE_TAG = "pageable"

        /** Measured: did it answer a NEG-OPEN, i.e. is reconcile on the table? */
        const val NIP77_TAG = "nip77"

        /**
         * The fitness verdict's own rules version, separate from the fold's
         * and the consistency pass's for the same reason those two are
         * separate: each is its own measurement with its own re-take cost.
         *
         * Unlike those two it is never READ by a consumer — it is written so
         * that [FitnessPass.retireStaleEpochs] can find, at the next boot, the
         * verdicts this build would no longer draw and take them back. Bump it
         * in the same commit as the rule change and that is the whole
         * migration; nothing downstream has to learn that an epoch exists,
         * which is what lets a stream read a foreign monitor's records at all.
         *
         * **1** — the vocabulary and checks as first shipped.
         */
        const val FITNESS_EPOCH = "1"

        /**
         * Thirty days. Long enough that the probe is a one-off per url rather
         * than a recurring cost, short enough that a host which splits one
         * endpoint into several real relays is noticed within a month.
         */
        const val DEFAULT_TTL_SECONDS = 30L * 24 * 60 * 60

        /**
         * Urls per `#d` query. The fan-out is five figures wide; the filter
         * should not be.
         *
         * Shared with [RelayDiscovery.undialable]'s subject-bound read rather
         * than restated there: it is the same query shape against the same
         * records, and two spellings of "how wide may a filter be" is how one
         * of them ends up sized for a store nobody is running.
         */
        internal const val QUERY_CHUNK = 500

        /**
         * Records held in memory at once while [loadAll] walks the corpus.
         *
         * Deliberately smaller than [RelayDiscovery.SCAN_PAGE]: that one pages a
         * relay-list scan whose events are read and dropped, and this walks a
         * kind whose whole population is one record per url this router has
         * measured — the thing being bounded here is a page of somebody's
         * five-figure corpus, not the number of round trips.
         */
        private const val RECORD_PAGE = 2_000
    }
}
