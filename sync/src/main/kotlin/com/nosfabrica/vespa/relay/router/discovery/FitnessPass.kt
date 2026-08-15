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

import com.nosfabrica.vespa.relay.router.progress.Processors
import com.nosfabrica.vespa.relay.util.nowSeconds
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropyReconcileIds
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * THE FITNESS CERTIFICATE — one measured verdict per url, written where a
 * stream can select on it.
 *
 * ## What `syncable` asserts, and what it deliberately does not
 *
 * A sync stream's relay list is built from one filter over kind-30166 records:
 * `"#s": ["syncable"]`. For that filter to be the WHOLE admission decision, the
 * value has to be a composite: reachable AND answering AND canonical AND
 * consistent AND pageable AND readable by us. Five of the refusals below
 * describe relays that are perfectly alive — an alias serves events, an
 * inconsistent relay answers promptly, a cursor-ignoring relay EOSEs every
 * page — which is why the value is not called "live". Slow is not a refusal
 * (rtt is a fact, not a fault), empty is not a refusal (a drain is the relay's
 * honest answer), and a small message cap is not a refusal (a shape the asks
 * respect).
 *
 * ## Measured on the socket, never read off NIP-11
 *
 * Every verdict here comes from what the relay DID: the ask ladder for whether
 * it answers, the anchored walk for whether it honours `until`, one NEG-OPEN
 * for whether it reconciles, quartz's own NIP-42 flow for whether our auth
 * sticks. NIP-11 documents routinely disagree with the relay's own practice —
 * measured on this corpus: a relay that served a REQ over its declared
 * `max_message_length`, a fleet that publishes no document at all — so nothing
 * that changes a decision is taken from one.
 *
 * ## Why a separate pass, on the monitor's clock
 *
 * The checks a stream needs are exactly the checks that must never sit on its
 * critical path. Run inside the fan-out they delay every pass, so they stay
 * minimal; run here they can be as thorough as the verdict deserves, and the
 * stream's discovery collapses to one indexed query. Same bargain as the fold:
 * verdicts are earned on this clock and only APPLIED on the stream's.
 *
 * ## One writer per tag
 *
 * This pass owns `s`, `pageable` and `nip77` and nothing else; the fold's
 * `same-as` and the consistency pass's tag are read, never written, and
 * everything else on the record is carried forward untouched by
 * [RelayAliasRecord]'s edit. The alias and inconsistency REFUSALS therefore
 * cost no dial at all — those passes already paid for the evidence, and this
 * one turns their standing verdicts into the one value a stream filters on.
 */
class FitnessPass(
    private val record: RelayAliasRecord,
    /**
     * The small-target ladder — see [AliasProbe]. Sized for a verdict, not a
     * fingerprint: [FITNESS_TARGET] events say "answers and pages" as well as
     * five hundred would.
     */
    private val probe: AliasProbe,
    private val client: INostrClient,
    /** The fold's standing verdicts over these candidates — read, never earned here. */
    private val foldedAway: suspend (List<NormalizedRelayUrl>) -> Map<NormalizedRelayUrl, NormalizedRelayUrl>,
    /** The consistency pass's standing refusals — same bargain. */
    private val unstable: suspend (List<NormalizedRelayUrl>) -> Set<NormalizedRelayUrl>,
    val progress: Processors.Handle,
    private val concurrency: Int = AliasFolding.DEFAULT_CONCURRENCY,
) {
    /**
     * The verdict vocabulary. [SYNCABLE] is the only admitting value; every
     * refusal is descriptive, so the record explains itself instead of a
     * worker's log line having to.
     */
    enum class Verdict(
        val value: String,
    ) {
        SYNCABLE("syncable"),

        /** No TCP, no TLS, no websocket — the transport itself said no. */
        DEAD("dead"),

        /** Connected, then nothing: no EOSE, no CLOSED, the window lapsed. */
        SILENT("silent"),

        /** Works fine, and is another record's relay — syncing it doubles every event. */
        ALIAS("alias"),

        /** Two answers to one question — would poison bands and coverage. */
        INCONSISTENT("inconsistent"),

        /** Ignores `until`: a paged walk against it cannot terminate. */
        UNPAGEABLE("unpageable"),

        /** Requires NIP-42 and turned OUR key down. */
        AUTH_REFUSED("auth-refused"),

        /** Answers only shaped queries this router cannot generally send. */
        RESTRICTED("restricted"),
    }

    /** One url's outcome, for the pass's own funnel. */
    private class Outcome(
        val verdict: Verdict,
        val evidence: String,
    )

    /**
     * Measure [candidates] and write a verdict for each. Returns how many
     * events the dials delivered to [onEvent] — a fitness walk is a sync that
     * also decides, same as the fold's.
     */
    suspend fun measure(
        label: String,
        candidates: List<NormalizedRelayUrl>,
        canDial: suspend (NormalizedRelayUrl) -> Boolean,
        onEvent: suspend (Event) -> Unit,
        sockets: AliasFolding.Sockets,
    ): Int {
        if (candidates.isEmpty()) return 0
        progress.begin("measuring fitness")
        val startedMs = System.currentTimeMillis()
        val outcomes = ConcurrentHashMap<NormalizedRelayUrl, Outcome>()
        val downloaded = AtomicInteger()
        try {
            // THE FREE REFUSALS FIRST. Both are standing verdicts other passes
            // already paid dials for; turning them into a status costs a store
            // read and a record edit, never a socket.
            val folded = foldedAway(candidates)
            for ((alias, canonical) in folded) {
                outcomes[alias] = Outcome(Verdict.ALIAS, "folds onto ${canonical.url}")
            }
            val remaining = candidates.filter { it !in folded }
            val shaky = unstable(remaining)
            for (url in shaky) {
                outcomes[url] = Outcome(Verdict.INCONSISTENT, "failed the reproducibility bar; see the consistency tag")
            }

            val toDial = remaining.filter { it !in shaky }
            val gate = Semaphore(concurrency)
            // A week back, for the same reason the consistency pass anchors
            // there: an anchored ask against a settled window is the only way
            // "events above the anchor" can mean "ignored the cursor" rather
            // than "new events arrived mid-walk".
            val anchor = RelayConsistency.settledAnchor(nowSeconds())
            coroutineScope {
                for (url in toDial) {
                    launch {
                        gate.withPermit {
                            val reachable =
                                try {
                                    canDial(url)
                                } catch (e: CancellationException) {
                                    throw e
                                } catch (e: Exception) {
                                    outcomes[url] = Outcome(Verdict.DEAD, "the reachability probe itself failed: ${e.javaClass.simpleName}")
                                    return@withPermit
                                }
                            if (!reachable) {
                                outcomes[url] = Outcome(Verdict.DEAD, "no TCP answer at the pre-probe")
                                return@withPermit
                            }
                            sockets.claim(url)
                            try {
                                outcomes[url] =
                                    dialVerdict(url, anchor) { event ->
                                        downloaded.incrementAndGet()
                                        onEvent(event)
                                    }
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: Exception) {
                                outcomes[url] = Outcome(Verdict.SILENT, "the dial threw ${e.javaClass.simpleName} before the relay said anything")
                            } finally {
                                sockets.release(url)
                            }
                        }
                    }
                }
            }

            // The writes, serial and after the dials: the record edit is a
            // read-modify-write with no CAS, and this pass is its only writer
            // for these tags — see [AliasMonitor] for why passes never overlap.
            for ((url, outcome) in outcomes) {
                record.publishFitness(
                    url = url,
                    status = outcome.verdict.value,
                    evidence = outcome.evidence,
                    pageable = pageableOf(url),
                    nip77 = nip77Of(url),
                )
            }

            report(label, candidates.size, outcomes.values.groupingBy { it.verdict }.eachCount(), startedMs)
        } finally {
            progress.finish()
        }
        return downloaded.get()
    }

    /** Per-url facts collected by [dialVerdict], read back at write time. */
    private val pageableFacts = ConcurrentHashMap<NormalizedRelayUrl, Pair<Boolean, String>>()
    private val nip77Facts = ConcurrentHashMap<NormalizedRelayUrl, Pair<Boolean, String>>()

    private fun pageableOf(url: NormalizedRelayUrl): Pair<Boolean, String>? = pageableFacts.remove(url)

    private fun nip77Of(url: NormalizedRelayUrl): Pair<Boolean, String>? = nip77Facts.remove(url)

    /**
     * The dial itself: the ask ladder for "answers", the anchored events for
     * "honours `until`", one NEG-OPEN for "reconciles". Ladder shape follows
     * [AliasProbe.leaderPrint] — bare filter, then the general kinds, then the
     * NIP-29 group rung — but keeps each rung's transport reason, because a
     * verdict has to say WHY where a fingerprint only has to say whether.
     */
    private suspend fun dialVerdict(
        url: NormalizedRelayUrl,
        anchor: Long,
        onEvent: suspend (Event) -> Unit,
    ): Outcome {
        // Events ABOVE the anchor are the relay answering a question it was not
        // asked: the walk's `until` is the anchor, so an honest relay never
        // sends one. Counted across every rung — the shape of the ask does not
        // change what the cursor means.
        var above = 0
        var seen = 0
        val counting: suspend (Event) -> Unit = { event ->
            seen++
            if (event.createdAt > anchor + ANCHOR_SLACK_SECONDS) above++
            onEvent(event)
        }

        var lastReason: String? = null
        var answered: AliasProbe.Window? = null
        var shape: List<Int>? = null
        for (rung in listOf(null, AliasProbe.FALLBACK_KINDS, RelayAliases.GROUP_METADATA_KINDS)) {
            val window = probe.window(url, anchor, rung, counting)
            if (window.authRefused) {
                return Outcome(Verdict.AUTH_REFUSED, "asked for NIP-42 and rejected our key")
            }
            if (window.ids != null) {
                answered = window
                shape = rung
                break
            }
            lastReason = window.reason ?: lastReason
            // Two silent rungs in a row is a url that is not there — the same
            // early exit [AliasProbe.leaderPrint] makes, for the same Tor-window
            // arithmetic.
            if (rung == AliasProbe.FALLBACK_KINDS && lastReason != null) break
        }

        if (answered == null) {
            // Never spoke, or refused every shape. The transport's own words
            // separate the two — [Silence] makes that call in one place.
            return when (val cause = Silence.of(lastReason)) {
                Silence.TIMEOUT, Silence.RATE_LIMITED, Silence.UNKNOWN -> {
                    if (lastReason == null) {
                        Outcome(Verdict.RESTRICTED, "answered no shape this router can send; see the ask ladder")
                    } else {
                        Outcome(Verdict.SILENT, cause.reason)
                    }
                }

                else -> {
                    Outcome(Verdict.DEAD, cause.reason)
                }
            }
        }

        // The pageable fact, from the same events the ladder already paid for.
        // Vacuously pageable when the anchored window is empty: an EOSE on an
        // empty page is a drain, which is exactly how a paged walk terminates.
        if (above > 0 && above == seen) {
            pageableFacts[url] = false to "$above of $seen events came back above the anchor — the cursor was ignored"
            return Outcome(Verdict.UNPAGEABLE, "every event answered above the `until` it was asked for")
        }
        pageableFacts[url] = true to (if (seen == 0) "empty anchored page, honestly EOSEd" else "$seen events, all at or below the anchor")

        // One NEG-OPEN against a sliver of the window. A normal return —
        // however empty — is the relay speaking NIP-77; the dedicated
        // exception is it declining. Anything else proves nothing and writes
        // nothing, so a flaky moment cannot demote a reconciling relay.
        val sliver = Filter(kinds = shape, since = anchor - NIP77_WINDOW_SECONDS, until = anchor)
        try {
            client.negentropyReconcileIds(url, sliver, emptyList(), idleTimeoutMs = NIP77_IDLE_MS)
            nip77Facts[url] = true to "answered a NEG-OPEN over a ${NIP77_WINDOW_SECONDS / 3600}h window"
        } catch (e: CancellationException) {
            throw e
        } catch (e: NegentropySyncException) {
            nip77Facts[url] = false to "declined the NEG-OPEN: ${e.reason}"
        } catch (_: Exception) {
            // No fact: the failure was ours or the moment's, not the relay's.
        }

        return Outcome(Verdict.SYNCABLE, "answered ${if (seen == 0) "an empty anchored page" else "$seen events"} at a settled anchor")
    }

    private fun report(
        label: String,
        candidates: Int,
        byVerdict: Map<Verdict, Int>,
        startedMs: Long,
    ) {
        val counts = byVerdict.entries.sortedByDescending { it.value }.joinToString { "${it.key.value} x${it.value}" }
        System.err.println(
            "router: fitness [$label] — $candidates candidate(s) in ${(System.currentTimeMillis() - startedMs) / 1000}s: $counts",
        )
        progress.counts {
            Verdict.entries.mapNotNull { v -> byVerdict[v]?.let { Processors.Count(v.value, it.toLong()) } }
        }
    }

    companion object {
        /**
         * Events per fitness ask. A verdict needs "answers and pages", which
         * twenty events prove as well as the fold's five hundred — and this
         * pass dials the whole corpus, so the difference is the pass's cost.
         */
        const val FITNESS_TARGET = 20

        /**
         * How far above the anchor an event may sit before it counts as the
         * relay ignoring the cursor. Publishers' clocks run fast by seconds,
         * not minutes; a relay that ignores `until` answers with its newest
         * events, which sit a WEEK above a settled anchor.
         */
        const val ANCHOR_SLACK_SECONDS = 300L

        /** The NEG-OPEN sliver: one hour is enough to prove the verb. */
        const val NIP77_WINDOW_SECONDS = 3600L

        /**
         * The NEG-OPEN's own idle window, shorter than a transfer's: this is a
         * yes/no about the protocol, and a relay that takes half a minute to
         * say NEG-MSG is answered by the transfer's own fallback anyway.
         */
        const val NIP77_IDLE_MS = 10_000L
    }
}
