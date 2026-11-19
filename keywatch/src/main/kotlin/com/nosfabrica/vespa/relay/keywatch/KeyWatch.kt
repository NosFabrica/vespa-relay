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
)

/**
 * One pass over a corpus: find published secrets, read key-change statements, collect who cited
 * them, and decide each key's classes. Reads only; the result goes wherever the caller writes it.
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
    suspend fun run(nowSeconds: Long): KeyWatchResult {
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
        for (q in config.keyQueries) steady("key material `$q`") { sweeper.sweep(Filter(search = q), take) }.also { log("key material `$q`: ${read.size} events read so far") }
        for (q in config.claimQueries) steady("claims `$q`") { sweeper.sweep(Filter(kinds = listOf(0, 1), search = q), take) }.also { log("claims `$q`: ${read.size} events read so far") }
        val vanishById = mutableMapOf<String, Event>()
        steady("vanish requests") { sweeper.sweep(Filter(kinds = listOf(VANISH_KIND))) { page -> page.forEach { vanishById[it.id] = it } } }
        val vanishes = vanishById.values.toList()
        log("read ${read.size} events: ${exposures.size} exposures, ${statements.size} statements, ${vanishes.size} vanish requests")

        val leaks = steady("leak histories") { leakFindings(exposures, lens) }
        log("${leaks.size} leaked keys: ${leaks.count { it.keyClass == KeyClass.COMPROMISED_PUBLIC }} compromised, ${leaks.count { it.keyClass == KeyClass.PUBLIC_FROM_BIRTH }} public from birth")
        val excluded = leaks.map { it.pubkey }.toSet()
        val verdicts = steady("citations") { transitionVerdicts(statements, lens, excluded, sweeper) }
        log("${verdicts.size} transitions: ${verdicts.groupingBy { it.status }.eachCount()}")
        val secondaries = steady("secondary keys") { secondaryFindings(statements.filterIsInstance<SecondaryClaim>(), excluded) }
        log("${secondaries.size} secondary keys")

        val findings =
            leaks +
                encryptedKeyFindings(exposures) +
                verdicts.map(::transitionFinding) +
                secondaries +
                vanishes.groupBy { it.pubKey }.map { (pubkey, events) -> vanishFinding(pubkey, events) }
        steady("rank cards") { lens.load(findings.map { it.pubkey }) }
        if (sweeper.gaps.isNotEmpty()) log("${sweeper.gaps.size} window(s) may be incomplete; listed under `gaps`")
        return KeyWatchResult(nowSeconds, lens.provider, findings, verdicts, read.size, lens::cardOf, sweeper.gaps.toList())
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
    ): List<Finding> {
        val byKey = exposures.filter { it.pubkey != null }.groupBy { it.pubkey!! }
        lens.load(byKey.keys)
        val leakAt = byKey.mapValues { (_, list) -> list.minOf { it.event.createdAt } }

        val unranked = byKey.keys.filter { lens.cardOf(it) == null }
        log("${byKey.size} leaked keys, ${byKey.size - unranked.size} with a rank card; asking the other ${unranked.size} for events before their leak")
        // A life is a day long at least. Measured on the first production report: the typical
        // self-leaking "compromised" key had signed one profile a median 83 seconds before naming
        // itself after its nsec, a key born and burned in a minute that never authenticated anyone.
        val before = authorsWithAny(unranked.map { Filter(authors = listOf(it), until = leakAt.getValue(it) - MIN_LIFE_SECONDS, limit = 1) })
        val withHistory = byKey.keys.filter { lens.cardOf(it) != null || it in before }
        val after = newestBy(withHistory.map { Filter(authors = listOf(it), since = leakAt.getValue(it) + 1, limit = 1) })

        return byKey.map { (pubkey, list) ->
            LeakClassifier.classify(pubkey, list, LeakFacts(hadHistory = pubkey in withHistory, activeAfter = after[pubkey]))
        }
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
                )
            }

    private suspend fun transitionVerdicts(
        statements: List<Statement>,
        lens: RankLens,
        excluded: Set<HexKey>,
        sweeper: Sweeper,
    ): List<TransitionVerdict> {
        val transitions =
            statements
                .filterIsInstance<TransitionClaim>()
                .groupBy { it.oldKey to it.newKey }
                .map { (pair, claims) -> Transition(pair.first, pair.second, claims) }
        if (transitions.isEmpty()) return emptyList()
        val byClaim = transitions.flatMap { t -> t.claims.map { it.event.id to t } }.toMap()
        log("${transitions.size} transitions over ${byClaim.size} claim events; collecting citations")

        val vouches = mutableMapOf<Transition, MutableList<Vouch>>()
        for ((i, chunk) in byClaim.keys.chunked(IDS_PER_FILTER).withIndex()) {
            val cited = chunk.toSet()
            log("citations of claims ${i * IDS_PER_FILTER + 1}..${i * IDS_PER_FILTER + chunk.size} of ${byClaim.size}")
            val filters =
                listOf(
                    Filter(kinds = listOf(6, 16), tags = mapOf("e" to chunk)),
                    Filter(kinds = listOf(1), tags = mapOf("q" to chunk)),
                    Filter(kinds = listOf(1), tags = mapOf("e" to chunk)),
                )
            for (f in filters) {
                sweeper.sweep(f) { events ->
                    events.forEach { e ->
                        VouchReader.read(e, cited).forEach { (claimId, vouch) -> vouches.getOrPut(byClaim.getValue(claimId)) { mutableListOf() } += vouch }
                    }
                }
            }
        }
        val byPair = transitions.associateBy { setOf(it.oldKey, it.newKey) }
        statements.filterIsInstance<ThirdPartyStatement>().forEach { s ->
            val t = byPair[s.keys] ?: return@forEach
            vouches.getOrPut(t) { mutableListOf() } += Vouch(s.event.pubKey, if (s.against) Stance.AGAINST else Stance.ENDORSE, VouchForm.STATEMENT, s.event)
        }

        lens.load(transitions.flatMap { listOf(it.oldKey, it.newKey) } + vouches.values.flatten().map { it.voucher })
        val ranked =
            vouches.values
                .flatten()
                .map { it.voucher }
                .filter { (lens.rankOf(it) ?: 0) >= config.policy.minVoucherRank }
                .toSet()
        val follows = followsOf(ranked)

        val validator = TransitionValidator(config.policy, lens::rankOf, follows::get, excluded)
        return TransitionValidator.settleSuccessors(transitions.map { validator.validate(it, vouches[it].orEmpty()) })
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
            successor = t.newKey.takeIf { v.status == Status.VALIDATED },
            relatedKey = t.newKey,
        )
    }

    /** A secondary is confirmed when its primary names it back; the primary is the one authority on its own alts. */
    private suspend fun secondaryFindings(
        claims: List<SecondaryClaim>,
        excluded: Set<HexKey>,
    ): List<Finding> {
        val pairs = claims.groupBy { it.secondary to it.primary }
        val asked =
            pairs.keys.filter { (_, primary) -> primary !in excluded }.associateWith { (secondary, primary) ->
                Filter(authors = listOf(primary), kinds = listOf(0, 1), search = secondary.hexToByteArray().toNpub(), limit = 1)
            }
        val confirmedBy = authorsWithAny(asked.values.toList())
        return pairs.map { (pair, list) ->
            val (secondary, primary) = pair
            val mutual = pair in asked && primary in confirmedBy
            Finding(
                secondary,
                KeyClass.SECONDARY,
                if (mutual) Status.VALIDATED else Status.CLAIMED,
                list.minOf { it.event.createdAt },
                list.map { it.event.id }.distinct().take(10),
                if (mutual) "named as secondary, and the primary names it back" else "names a primary that does not name it back",
                relatedKey = primary,
            )
        }
    }

    private fun vanishFinding(
        pubkey: HexKey,
        events: List<Event>,
    ): Finding {
        val relays = events.flatMap { e -> e.tags.filter { it.size > 1 && it[0] == "relay" }.map { it[1] } }.toSet()
        val scope = if ("ALL_RELAYS" in relays) "every relay" else "${relays.size} relay(s)"
        return Finding(pubkey, KeyClass.VANISHED, Status.PROVEN, events.minOf { it.createdAt }, events.map { it.id }.take(10), "asked $scope to erase it (NIP-62)")
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
