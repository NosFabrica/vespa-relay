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
class RelayAliasRecord(
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
        val stable: Set<NormalizedRelayUrl> = emptySet(),
        /** Urls measured as NOT doing so — the ones the fan-out refuses. */
        val unstable: Set<NormalizedRelayUrl> = emptySet(),
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
        val aliases = HashMap<NormalizedRelayUrl, NormalizedRelayUrl>()
        val distinct = HashSet<NormalizedRelayUrl>()
        val stable = HashSet<NormalizedRelayUrl>()
        val unstable = HashSet<NormalizedRelayUrl>()
        for (chunk in candidates.map { it.url }.chunked(QUERY_CHUNK)) {
            val held: List<Event> =
                store.query<Event>(Filter(kinds = listOf(RelayDiscoveryEvent.KIND), authors = listOf(self), tags = mapOf("d" to chunk)))
            for (event in held) {
                val subject = event.tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: continue
                val from = RelayUrlNormalizer.normalizeOrNull(subject) ?: continue
                // Two independent verdicts on one record, read independently: a
                // url may carry a fold, a stability answer, both or neither, and
                // an early `continue` for a missing `same-as` used to drop the
                // whole event — which would make every stability verdict on a
                // url that was never folded invisible.
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
                            CONSISTENT_YES -> stable += from

                            CONSISTENT_NO -> unstable += from

                            // An answer this writer does not recognise is not a
                            // verdict. Ignored rather than guessed at: guessing
                            // "unstable" would drop a relay on a tag we cannot read.
                            else -> Unit
                        }
                    }
            }
        }
        return Verdicts(aliases, distinct, stable, unstable)
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
    ): Event? =
        edit(
            url,
            owned = setOf(SELF_CONSISTENT_TAG),
            add =
                listOf(
                    arrayOf(
                        SELF_CONSISTENT_TAG,
                        if (consistent) CONSISTENT_YES else CONSISTENT_NO,
                        "$first + $second events at a ${ANCHOR_DAYS}d anchor, $shared shared -> %.3f".format(score),
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
    ): Event? = write(alias, canonical, "same endpoint as ${canonical.url} over TLS, both answered; $sampled newest events here")

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
            owned = setOf(SAME_AS_TAG),
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
     * [owned] is the small set of tag names this writer is allowed to replace.
     * Everything else is carried forward untouched, whoever wrote it and
     * whatever it means.
     *
     * The timestamp is `max(now, existing + 1)` rather than `now`, because a
     * store enforcing replaceable semantics REJECTS an edit that is not newer
     * — and two writers seconds apart, or a clock that has not moved, are
     * ordinary. Silently losing the write to `replaced: a newer version
     * exists` is how a repair pass reports success having done nothing.
     */
    private suspend fun edit(
        url: NormalizedRelayUrl,
        owned: Set<String>,
        add: List<Array<String>>,
    ): Event? {
        val signer = signer ?: return null
        val current = currentRecord(url)
        val kept = current?.tags?.filterNot { it.firstOrNull() == "d" || it.firstOrNull() in owned }.orEmpty()
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

        /** The two values [SELF_CONSISTENT_TAG] is ever written with. */
        const val CONSISTENT_YES = "true"

        const val CONSISTENT_NO = "false"

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
         * re-fingerprint of the store, spread over
         * [AliasFolding.DEFAULT_PROBES_PER_CYCLE] per pass, and while it runs
         * every un-re-measured url is dialled unfolded. That is the correct
         * price for a rule change and pure waste for anything else.
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

        /** How old the stability anchor is, for the evidence string. */
        private const val ANCHOR_DAYS = RelayConsistency.ANCHOR_LAG_SECONDS / (24 * 60 * 60)

        /**
         * Thirty days. Long enough that the probe is a one-off per url rather
         * than a recurring cost, short enough that a host which splits one
         * endpoint into several real relays is noticed within a month.
         */
        const val DEFAULT_TTL_SECONDS = 30L * 24 * 60 * 60

        /** Urls per `#d` query. The fan-out is five figures wide; the filter should not be. */
        private const val QUERY_CHUNK = 500
    }
}
