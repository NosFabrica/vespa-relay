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
package com.nosfabrica.vespa.relay.keywatch

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.hexToByteArray
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import kotlinx.coroutines.delay

class KeyWatchConfig(
    /** The observer whose rank provider ranks the vouchers. */
    val observer: HexKey,
    /** NIP-50 searches that surface published secrets. */
    val keyQueries: List<String> = DEFAULT_KEY_QUERIES,
    /** NIP-50 searches, over profiles and notes, that surface key-change statements. */
    val claimQueries: List<String> = DEFAULT_CLAIM_QUERIES,
    val policy: VouchPolicy = VouchPolicy(),
    /** Under the relay's `max_limit` (5000), with room for COUNT drifting before the REQ. */
    val pageCap: Int = 4000,
    /** How long to wait for a relay answering from part of its index before giving up the pass. */
    val maxDegradedWaitMs: Long = 60 * 60_000L,
    /** The first pause while waiting; doubled each time up to five minutes. */
    val degradedPauseMs: Long = 30_000L,
    /** How many times a phase is redone after a partial answer before the pass is given up. */
    val phaseAttempts: Int = 5,
    /**
     * How far before the previous run's cursor an incremental run starts reading. Events reach a
     * relay after their `created_at` (clients backdate, mirrors catch up), and a filter can only
     * ask by `created_at`, so the overlap catches what arrived late; a periodic full pass catches
     * the rest.
     */
    val overlapSeconds: Long = 2 * 86_400L,
) {
    companion object {
        val DEFAULT_KEY_QUERIES = listOf("nsec1", "ncryptsec1", "privkey", "\"private key\"", "\"secret key\"", "\"seed phrase\"", "mnemonic")

        val DEFAULT_CLAIM_QUERIES =
            listOf(
                "\"new npub\"",
                "\"new account\"",
                "\"new key\"",
                "\"old npub\"",
                "\"old account\"",
                "\"old key\"",
                "\"moved to\"",
                "\"moving to\"",
                "migrating",
                "\"switched to\"",
                "\"dead account\"",
                "compromised",
                "hacked",
                "leaked",
                "\"lost my\"",
                "\"alt account\"",
                "\"main account\"",
                "\"bot run by\"",
                "\"backup account\"",
                "\"nova conta\"",
                "\"conta antiga\"",
                "\"nueva cuenta\"",
                "\"cuenta antigua\"",
                "\"neuer account\"",
                "\"alter account\"",
            ).flatMap { listOf("$it npub", "$it nprofile") }
    }
}

class KeyWatchResult(
    val generatedAt: Long,
    val rankProvider: HexKey,
    val findings: List<Finding>,
    val transitions: List<TransitionVerdict>,
    val eventsRead: Int,
    val cards: (HexKey) -> Card?,
    /** Windows the sweep could not prove it read whole; empty on a complete pass. */
    val gaps: List<String> = emptyList(),
    /** Whether this pass read the whole corpus, or only events from [sweptFrom] on. */
    val full: Boolean = true,
    val sweptFrom: Long = 0,
    /** The newest full pass this result rests on: this one, or the one its prior report carried. */
    val lastFullPassAt: Long = generatedAt,
    /** What is new or changed status since the prior report; empty on a pass without one. */
    val changes: List<Change> = emptyList(),
)

/**
 * One pass over a corpus: find published secrets, read key-change statements, collect who cited
 * them, and decide each key's classes. Reads only; the result goes wherever the caller writes it.
 *
 * Given the [PriorReport] of an earlier pass it reads only what that pass could not have seen:
 * events from its cursor on (less [KeyWatchConfig.overlapSeconds]), and citations of the claims
 * it already knows from the same point. What it already knew is merged, not re-derived, except
 * what can still change: every transition and unconfirmed secondary is decided again from its
 * stored evidence, what arrived since, and today's ranks.
 */
class KeyWatch(
    private val corpus: Corpus,
    private val config: KeyWatchConfig,
    private val log: (String) -> Unit,
) {
    /**
     * Never reports from a partial index. Each phase starts only once the relay answers COUNT (it
     * refuses every COUNT while part of its index is unreachable), and is redone from the start
     * when the relay answered any of its reads partially, or is partial again when it ends. The
     * pass throws, writing nothing, when the relay does not settle.
     */
    suspend fun run(
        nowSeconds: Long,
        prior: PriorReport? = null,
    ): KeyWatchResult {
        val lens =
            steady("rank lens") {
                RankLens.resolve(corpus, config.observer)
                    // Measured: a partial index answered the observer's 10040 with an empty EOSE.
                    ?: if (!corpus.healthy()) {
                        throw RelayDegraded("no kind 10040 for the observer, from a partial index")
                    } else {
                        error("observer ${config.observer} names no 30382:rank provider in a kind 10040")
                    }
            }
        log("ranking through provider ${lens.provider} (observer ${config.observer})")
        val from = prior?.let { (it.sweptUntil - config.overlapSeconds).coerceAtLeast(0) } ?: 0L
        if (prior == null) {
            log("full pass: reading the whole corpus")
        } else {
            log("incremental pass: reading events from $from on, over ${prior.findings.size} findings and ${prior.transitions.size} transitions carried over")
        }
        val sweeper = Sweeper(corpus, config.pageCap, nowSeconds, log)

        val read = HashSet<String>()
        val exposures = mutableListOf<Exposure>()
        val statements = mutableListOf<Statement>()
        val take: suspend (List<Event>) -> Unit = { events ->
            for (e in events) {
                if (!read.add(e.id)) continue
                exposures += KeyMaterialScanner.scan(e)
                statements += StatementReader.read(e)
            }
        }
        // A redone sweep re-reads its query; `take` skips what the first attempt already kept.
        for (q in config.keyQueries) steady("key material `$q`") { sweeper.sweep(Filter(search = q), from, take) }.also { log("key material `$q`: ${read.size} events read so far") }
        for (q in config.claimQueries) steady("claims `$q`") { sweeper.sweep(Filter(kinds = listOf(0, 1), search = q), from, take) }.also { log("claims `$q`: ${read.size} events read so far") }
        val vanishById = mutableMapOf<String, Event>()
        steady("vanish requests") { sweeper.sweep(Filter(kinds = listOf(VANISH_KIND)), from) { page -> page.forEach { vanishById[it.id] = it } } }
        val vanishes = vanishById.values.toList()
        log("read ${read.size} events: ${exposures.size} exposures, ${statements.size} statements, ${vanishes.size} vanish requests")

        val leaks = steady("leak histories") { leakFindings(exposures, lens, prior) }
        log("${leaks.size} leaked keys: ${leaks.count { it.keyClass == KeyClass.COMPROMISED_PUBLIC }} compromised, ${leaks.count { it.keyClass == KeyClass.PUBLIC_FROM_BIRTH }} public from birth")
        val excluded = leaks.map { it.pubkey }.toSet()
        val verdicts = steady("citations") { transitionVerdicts(statements, lens, excluded, sweeper, prior, from) }
        log("${verdicts.size} transitions: ${verdicts.groupingBy { it.status }.eachCount()}")
        val secondaries = steady("secondary keys") { secondaryFindings(statements.filterIsInstance<SecondaryClaim>(), excluded, prior) }
        log("${secondaries.size} secondary keys")

        val findings =
            leaks +
                carried(encryptedKeyFindings(exposures), prior, Basis.ENCRYPTED_SECRET) +
                verdicts.map(::transitionFinding) +
                secondaries +
                carried(vanishes.groupBy { it.pubKey }.map { (pubkey, events) -> vanishFinding(pubkey, events) }, prior, Basis.VANISH)
        steady("rank cards") { lens.load(findings.map { it.pubkey }) }
        if (sweeper.gaps.isNotEmpty()) log("${sweeper.gaps.size} window(s) may be incomplete; listed under `gaps`")
        val changes = prior?.let { Change.between(it.findings, findings) }.orEmpty()
        if (prior != null) log("${changes.size} change(s) since the prior report: ${changes.groupingBy { it.kind }.eachCount()}")
        return KeyWatchResult(
            generatedAt = nowSeconds,
            rankProvider = lens.provider,
            findings = findings,
            transitions = verdicts,
            eventsRead = read.size,
            cards = lens::cardOf,
            gaps = sweeper.gaps.toList(),
            full = prior == null,
            sweptFrom = from,
            lastFullPassAt = prior?.lastFullPassAt ?: nowSeconds,
            changes = changes,
        )
    }

    /** Runs [block] on a whole index: waits for one first, and redoes the block if any read in it was partial. */
    private suspend fun <T> steady(
        phase: String,
        block: suspend () -> T,
    ): T {
        var attempt = 0
        while (true) {
            attempt++
            awaitWholeIndex(phase)
            val outcome =
                try {
                    block().takeIf { corpus.healthy() }
                } catch (e: RelayDegraded) {
                    log("$phase: ${e.message}")
                    null
                }
            if (outcome != null) return outcome
            if (attempt >= config.phaseAttempts) throw IllegalStateException("$phase: the relay answered partially $attempt times; no report written")
            log("$phase: read from a partial index; redoing it once the index is whole (attempt ${attempt + 1} of ${config.phaseAttempts})")
        }
    }

    private suspend fun awaitWholeIndex(phase: String) {
        var waited = 0L
        var pause = config.degradedPauseMs
        while (!corpus.healthy()) {
            if (waited >= config.maxDegradedWaitMs) throw IllegalStateException("$phase: the relay's index stayed partial for ${waited / 60_000} min; no report written")
            log("$phase: the relay is answering from part of its index; waiting ${pause / 1000}s")
            delay(pause)
            waited += pause
            pause = (pause * 2).coerceAtMost(MAX_PAUSE_MS)
        }
    }

    private suspend fun leakFindings(
        exposures: List<Exposure>,
        lens: RankLens,
        prior: PriorReport?,
    ): List<Finding> {
        val byKey = exposures.filter { it.pubkey != null }.groupBy { it.pubkey!! }
        // A published secret stays published: what a prior pass decided is kept, with any new evidence.
        val known =
            prior
                ?.findings
                ?.filter { it.basis == Basis.SECRET }
                ?.associateBy { it.pubkey }
                .orEmpty()
        val carried = known.values.map { p -> byKey[p.pubkey]?.let { LeakClassifier.merge(p, it) } ?: p }
        val fresh = byKey.filterKeys { it !in known }
        lens.load(fresh.keys)
        val leakAt = fresh.mapValues { (_, list) -> list.minOf { it.event.createdAt } }

        val unranked = fresh.keys.filter { lens.cardOf(it) == null }
        log("${fresh.size} newly leaked keys (${known.size} carried over), ${fresh.size - unranked.size} with a rank card; asking the other ${unranked.size} for events before their leak")
        // A life is a day long at least: keys whose secret came out within minutes of their first
        // event (spam generators, and newcomers pasting their nsec into a public chat) never had
        // a reputation to lose. Measured on the first production report.
        val before = authorsWithAny(unranked.map { Filter(authors = listOf(it), until = leakAt.getValue(it) - MIN_LIFE_SECONDS, limit = 1) })
        val classified =
            fresh.map { (pubkey, list) ->
                LeakClassifier.classify(pubkey, list, LeakFacts(hadHistory = lens.cardOf(pubkey) != null || pubkey in before, activeAfter = null))
            }

        // Whether a compromised key still signs is asked again every pass: it is who to warn first.
        val all = carried + classified
        val compromised = all.filter { it.keyClass == KeyClass.COMPROMISED_PUBLIC }
        val after = newestBy(compromised.map { Filter(authors = listOf(it.pubkey), since = it.since!! + 1, limit = 1) })
        return all.map { if (it.keyClass == KeyClass.COMPROMISED_PUBLIC) it.copy(activeAfter = after[it.pubkey] ?: it.activeAfter) else it }
    }

    /** An `ncryptsec` names no key until decrypted; it is the poster's own far more often than not. */
    private fun encryptedKeyFindings(exposures: List<Exposure>): List<Finding> =
        exposures
            .filter { it.form == SecretForm.NCRYPTSEC }
            .groupBy { it.event.pubKey }
            .map { (author, list) ->
                Finding(
                    author,
                    KeyClass.SUSPECTED,
                    Status.CLAIMED,
                    list.minOf { it.event.createdAt },
                    list.map { it.event.id }.distinct().take(10),
                    "posted a password-encrypted key (ncryptsec); as safe as its password",
                    Basis.ENCRYPTED_SECRET,
                )
            }

    private suspend fun transitionVerdicts(
        statements: List<Statement>,
        lens: RankLens,
        excluded: Set<HexKey>,
        sweeper: Sweeper,
        prior: PriorReport?,
        from: Long,
    ): List<TransitionVerdict> {
        val priorByPair = prior?.transitions?.associateBy { it.oldKey to it.newKey }.orEmpty()
        val knownClaims = priorByPair.values.flatMap { t -> t.claims.map { it.event.id } }.toSet()
        val claimsByPair =
            (priorByPair.values.flatMap { it.claims } + statements.filterIsInstance<TransitionClaim>())
                .distinctBy { it.event.id }
                .groupBy { it.oldKey to it.newKey }
        if (claimsByPair.isEmpty()) return emptyList()
        val transitions = claimsByPair.map { (pair, claims) -> pair to Transition(pair.first, pair.second, claims) }.toMap()
        val pairOfClaim = transitions.flatMap { (pair, t) -> t.claims.map { it.event.id to pair } }.toMap()
        val vouches = priorByPair.mapValues { (_, t) -> t.vouches.toMutableList() }.toMutableMap()

        // A claim seen before was cited before: only citations from the cursor on are new. A new
        // claim may have been cited at any time since it was made.
        val (seen, unseen) = pairOfClaim.keys.partition { it in knownClaims }
        log("${transitions.size} transitions over ${pairOfClaim.size} claim events (${unseen.size} new); collecting citations")
        for ((ids, since) in listOf(unseen to 0L, seen to from)) {
            for ((i, chunk) in ids.chunked(IDS_PER_FILTER).withIndex()) {
                val cited = chunk.toSet()
                log("citations of ${if (since == 0L) "new" else "known"} claims ${i * IDS_PER_FILTER + 1}..${i * IDS_PER_FILTER + chunk.size} of ${ids.size}")
                val filters =
                    listOf(
                        Filter(kinds = listOf(6, 16), tags = mapOf("e" to chunk)),
                        Filter(kinds = listOf(1), tags = mapOf("q" to chunk)),
                        Filter(kinds = listOf(1), tags = mapOf("e" to chunk)),
                    )
                for (f in filters) {
                    sweeper.sweep(f, since) { events ->
                        events.forEach { e ->
                            VouchReader.read(e, cited).forEach { (claimId, vouch) -> vouches.getOrPut(pairOfClaim.getValue(claimId)) { mutableListOf() } += vouch }
                        }
                    }
                }
            }
        }
        val byKeys = transitions.keys.associateBy { setOf(it.first, it.second) }
        statements.filterIsInstance<ThirdPartyStatement>().forEach { s ->
            val pair = byKeys[s.keys] ?: return@forEach
            vouches.getOrPut(pair) { mutableListOf() } += Vouch(s.event.pubKey, if (s.against) Stance.AGAINST else Stance.ENDORSE, VouchForm.STATEMENT, s.event)
        }
        val evidence = transitions.keys.associateWith { pair -> vouches[pair].orEmpty().distinctBy { it.event.id } }

        lens.load(transitions.keys.flatMap { listOf(it.first, it.second) } + evidence.values.flatten().map { it.voucher })
        val ranked =
            evidence.values
                .flatten()
                .map { it.voucher }
                .filter { (lens.rankOf(it) ?: 0) >= config.policy.minVoucherRank }
                .toSet()
        val follows = followsOf(ranked)

        val validator = TransitionValidator(config.policy, lens::rankOf, follows::get, excluded)
        // A validation stands unless new evidence goes against it: a voucher's rank drifting below
        // the bar does not undo it, an eligible warning or a rival successor does (both contest it).
        // Under a changed policy everything is decided afresh.
        val samePolicy = prior?.policy == config.policy
        val verdicts =
            transitions.map { (pair, t) ->
                val v = validator.validate(t, evidence.getValue(pair))
                if (samePolicy && v.status == Status.CLAIMED && priorByPair[pair]?.status == Status.VALIDATED) TransitionVerdict(t, Status.VALIDATED, v.vouches) else v
            }
        return TransitionValidator.settleSuccessors(verdicts)
    }

    private fun transitionFinding(v: TransitionVerdict): Finding {
        val t = v.transition
        val endorsers = v.vouches.filter { it.eligible && it.vouch.stance == Stance.ENDORSE }
        val against = v.vouches.count { it.eligible && it.vouch.stance == Stance.AGAINST }
        val signers = t.signers.joinToString(" and ") { if (it == Signer.OLD_KEY) "the old key" else "the new key" }
        return Finding(
            pubkey = t.oldKey,
            keyClass = t.reason.oldKeyClass(),
            status = v.status,
            since = t.firstClaimAt,
            evidence = (t.claims.map { it.event.id } + endorsers.map { it.vouch.event.id }).distinct().take(20),
            detail = "${t.reason.name.lowercase()} claimed by $signers; ${endorsers.size} eligible endorsement(s), $against against, ${v.vouches.size} citation(s) in all",
            basis = Basis.TRANSITION,
            successor = t.newKey.takeIf { v.status == Status.VALIDATED },
            relatedKey = t.newKey,
        )
    }

    /**
     * A secondary is confirmed when its primary names it back; the primary is the one authority on
     * its own alts. A confirmed one stays confirmed; an unconfirmed one is asked again every pass.
     */
    private suspend fun secondaryFindings(
        claims: List<SecondaryClaim>,
        excluded: Set<HexKey>,
        prior: PriorReport?,
    ): List<Finding> {
        val known =
            prior
                ?.findings
                ?.filter { it.basis == Basis.SECONDARY }
                ?.associateBy { it.pubkey to it.relatedKey!! }
                .orEmpty()
        val fresh = claims.groupBy { it.secondary to it.primary }
        val pairs = known.keys + fresh.keys
        val asked =
            pairs.filter { pair -> pair.second !in excluded && known[pair]?.status != Status.VALIDATED }.associateWith { (secondary, primary) ->
                Filter(authors = listOf(primary), kinds = listOf(0, 1), search = secondary.hexToByteArray().toNpub(), limit = 1)
            }
        val confirmedBy = authorsWithAny(asked.values.toList())
        return pairs.map { pair ->
            val (secondary, primary) = pair
            val before = known[pair]
            val list = fresh[pair].orEmpty()
            val mutual = before?.status == Status.VALIDATED || (pair in asked && primary in confirmedBy)
            Finding(
                secondary,
                KeyClass.SECONDARY,
                if (mutual) Status.VALIDATED else Status.CLAIMED,
                listOfNotNull(before?.since, list.minOfOrNull { it.event.createdAt }).min(),
                (before?.evidence.orEmpty() + list.map { it.event.id }).distinct().take(10),
                if (mutual) "named as secondary, and the primary names it back" else "names a primary that does not name it back",
                Basis.SECONDARY,
                relatedKey = primary,
            )
        }
    }

    /** A fact a prior pass found, joined with what this pass found of the same kind: more evidence, an earlier bound. */
    private fun carried(
        found: List<Finding>,
        prior: PriorReport?,
        basis: Basis,
    ): List<Finding> {
        val known =
            prior
                ?.findings
                ?.filter { it.basis == basis }
                ?.associateBy { it.pubkey }
                .orEmpty()
        val now = found.associateBy { it.pubkey }
        return (known.keys + now.keys).map { pubkey ->
            val a = known[pubkey]
            val b = now[pubkey]
            if (a == null || b == null) {
                (a ?: b)!!
            } else {
                a.copy(since = listOfNotNull(a.since, b.since).min(), evidence = (a.evidence + b.evidence).distinct().take(10))
            }
        }
    }

    private fun vanishFinding(
        pubkey: HexKey,
        events: List<Event>,
    ): Finding {
        val relays = events.flatMap { e -> e.tags.filter { it.size > 1 && it[0] == "relay" }.map { it[1] } }.toSet()
        val scope = if ("ALL_RELAYS" in relays) "every relay" else "${relays.size} relay(s)"
        return Finding(pubkey, KeyClass.VANISHED, Status.PROVEN, events.minOf { it.createdAt }, events.map { it.id }.take(10), "asked $scope to erase it (NIP-62)", Basis.VANISH)
    }

    /** The authors any of [filters] returned something for. */
    private suspend fun authorsWithAny(filters: List<Filter>): Set<HexKey> = corpus.fetch(filters).map { it.pubKey }.toSet()

    private suspend fun newestBy(filters: List<Filter>): Map<HexKey, Long> = corpus.fetch(filters).groupBy { it.pubKey }.mapValues { (_, events) -> events.maxOf { it.createdAt } }

    private suspend fun followsOf(pubkeys: Set<HexKey>): Map<HexKey, Set<HexKey>> =
        pubkeys
            .chunked(FOLLOW_LISTS_PER_FILTER)
            .flatMap { corpus.fetch(listOf(Filter(kinds = listOf(3), authors = it, limit = it.size))) }
            .groupBy { it.pubKey }
            .mapValues { (_, lists) ->
                lists
                    .maxBy { it.createdAt }
                    .tags
                    .filter { it.size > 1 && it[0] == "p" }
                    .map { it[1] }
                    .toSet()
            }

    private companion object {
        const val VANISH_KIND = 62
        const val IDS_PER_FILTER = 200
        const val FOLLOW_LISTS_PER_FILTER = 100
        const val MAX_PAUSE_MS = 5 * 60_000L

        /** How long before its leak a key must have signed something to have been anyone's. */
        const val MIN_LIFE_SECONDS = 86_400L
    }
}
