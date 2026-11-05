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
package com.nosfabrica.vespa.relay.monitor

import com.nosfabrica.vespa.relay.fold.RelayAliases
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.COMPLIANCE_BUDGET_DIVISOR
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.NIP77_WINDOW_SECONDS
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.NONCOMPLIANT_EVIDENCE
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.STAGE_COMPLIANCE
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.STAGE_HELD
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.STAGE_LADDER
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.STAGE_NIP77
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.STAGE_REACHABILITY
import com.nosfabrica.vespa.relay.monitor.FitnessPass.Companion.STAGE_TLS_CHECK
import com.nosfabrica.vespa.relay.peers.Fact
import com.nosfabrica.vespa.relay.peers.Sockets
import com.nosfabrica.vespa.relay.peers.TorTransport
import com.nosfabrica.vespa.relay.peers.Verdict
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropySyncException
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

/** One url's dial for the fitness pass: the pre-probe, the ask ladder, the second page and the NEG-OPEN, filed on its [DialSlot]. */
internal class FitnessDial(
    private val probe: AliasProbe,
    private val compliance: RelayCompliance,
    private val progress: Processors.Handle,
    private val document: RelayDocument?,
    private val tor: TorTransport?,
    private val tlsCheck: suspend (NormalizedRelayUrl) -> TlsAnswer,
    private val reconcile: suspend (NormalizedRelayUrl, Filter) -> Unit,
    private val nip77DeadlineMs: Long,
    private val reach: suspend (NormalizedRelayUrl) -> Reach,
    private val sockets: Sockets,
) {
    /** The NEG-OPEN's answer, boxed so `withTimeoutOrNull` can tell null from the clock firing. */
    private class Reconciled(
        val fact: Fact?,
    )

    /** What the second page came back with, boxed for [Reconciled]'s reason. */
    private class Paged(
        val window: AliasProbe.Compliance?,
    )

    /** The `compliant` fact for one reading, or null where the bars do not support one. */
    private fun factOf(reading: AliasProbe.Compliance): Fact? =
        when (compliance.decide(reading)) {
            RelayCompliance.Verdict.UNMEASURABLE -> null
            RelayCompliance.Verdict.NONCOMPLIANT -> Fact(false, compliance.evidence(reading))
            RelayCompliance.Verdict.COMPLIANT -> Fact(true, compliance.evidence(reading))
        }

    /** The pre-probe, before any dial permit: true when the url is dialled, else its result is filed on [slot]. */
    suspend fun preProbe(
        url: NormalizedRelayUrl,
        slot: DialSlot,
    ): Boolean {
        progress.holding(url.url, STAGE_REACHABILITY)
        val reached =
            try {
                reach(url)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // The pre-probe throwing is this box failing to ask, not evidence about the server.
                slot.result = DialResult.Unmeasured("the reachability probe itself threw ${e.javaClass.simpleName}")
                return false
            }
        when (reached) {
            // Dialled all the same: `silent` is the dial's word to say, on the dial's own window.
            Reach.REACHABLE, Reach.UNANSWERED -> {
                return true
            }

            Reach.PROVED_UNREACHABLE -> {
                slot.result = DialResult.Graded(FitnessOutcome(Verdict.DEAD, "no TCP answer at the pre-probe", preProbe = true))
            }

            // Our proxy not answering says nothing about the relay behind it, nor about our clearnet dials.
            Reach.TRANSPORT_DOWN -> {
                slot.result = if (tor?.routes(url) == true) DialResult.Deferred(Deferral.TOR_DOWN) else DialResult.Unmeasured("our own transport is not answering")
            }

            Reach.UNEXPLAINED -> {
                slot.result = DialResult.Deferred(Deferral.LOOKUP_UNEXPLAINED)
            }
        }
        return false
    }

    /**
     * One url's dial, with its document asked beside it on [documents], which the batch joins once the
     * permit is back. Only the dial claims a socket; the document is bounded by its own clock.
     */
    suspend fun measureOne(
        url: NormalizedRelayUrl,
        anchor: Long,
        slot: DialSlot,
        documents: CoroutineScope,
    ) {
        // Beside the dial, not before it: the two fail independently, and a host that hangs one hangs both.
        document?.let { doc -> documents.launch { slot.reading = doc.read(url) } }
        progress.holding(url.url, STAGE_LADDER)
        dial(url, anchor, slot)
    }

    /**
     * A url waiting on the fold on a server that answered HTTP, asked once: the fold cannot fingerprint
     * a path its server will not upgrade, so that path would wait forever. Graded only when the refusal
     * earns `dead`; an answer of any other kind, or none, leaves it waiting and writes nothing.
     */
    suspend fun checkHeld(
        url: NormalizedRelayUrl,
        anchor: Long,
        slot: DialSlot,
    ) {
        progress.holding(url.url, STAGE_HELD)
        sockets.claim(url)
        try {
            val window = probe.window(url, anchor, null).also { slot.downloaded += it.compliance.seen }
            val reason = window.reason
            if (window.ids == null && Silence.of(reason) == Silence.UPGRADE) {
                upgradeRefused(reason) {}?.takeIf { it.verdict == Verdict.DEAD }?.let { slot.result = DialResult.Graded(it) }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
            // Our instrument failing on a url that writes nothing either way: it simply goes on waiting.
        } finally {
            sockets.release(url)
        }
    }

    /** The ladder on a claimed socket, its outcome or the reason it has none filed on [slot]. */
    private suspend fun dial(
        url: NormalizedRelayUrl,
        anchor: Long,
        slot: DialSlot,
    ) {
        sockets.claim(url)
        try {
            val outcome = dialVerdict(url, anchor, slot)
            if (outcome != null) {
                slot.result = DialResult.Graded(outcome)
            } else if (slot.result == null) {
                slot.result = DialResult.Unmeasured("no EOSE, no CLOSED and no transport reason on any rung")
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // Our instrument giving up, unless the ladder already settled a verdict or a deferral.
            if (slot.result !is DialResult.Graded && slot.result !is DialResult.Deferred) {
                slot.result = DialResult.Unmeasured("the dial threw ${e.javaClass.simpleName} before the relay said anything")
            }
        } finally {
            sockets.release(url)
        }
    }

    /**
     * A refused websocket upgrade, graded by the status the server answered with: a server error
     * or a CDN's is a moment, not a claim; an auth or payment wall is a live relay that will not
     * serve us; anything else is no relay at that address.
     */
    private fun upgradeRefused(
        raw: String?,
        defer: (Deferral) -> Unit,
    ): FitnessOutcome? {
        val status = Silence.upgradeStatus(raw)
        // Built on the reason: `FitnessPass.answeringServers` recognises a refusal by it.
        val said = status?.let { "${Silence.UPGRADE.reason} with HTTP $it" } ?: Silence.UPGRADE.reason
        return when (status) {
            in 500..599 -> {
                defer(Deferral.SERVER_ERROR)
                null
            }

            401, 402, 403 -> {
                FitnessOutcome(Verdict.RESTRICTED, said)
            }

            else -> {
                FitnessOutcome(Verdict.DEAD, said)
            }
        }
    }

    /**
     * A dial's TLS failure, believed only when our own direct handshake fails on the server too:
     * anything between us and the relay can fail a dial's handshake, and that is not the relay's.
     */
    private suspend fun tlsFailed(
        url: NormalizedRelayUrl,
        defer: (Deferral) -> Unit,
    ): FitnessOutcome? {
        progress.holding(url.url, STAGE_TLS_CHECK)
        val answer =
            try {
                tlsCheck(url)
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                TlsAnswer.UNCHECKED
            }
        return when (answer) {
            TlsAnswer.REJECTED -> {
                FitnessOutcome(Verdict.DEAD, Silence.TLS.reason, tls = true)
            }

            TlsAnswer.ACCEPTED -> {
                defer(Deferral.OUR_TLS)
                null
            }

            TlsAnswer.UNCHECKED -> {
                defer(Deferral.TLS_UNCHECKED)
                null
            }
        }
    }

    /** One rung of the ladder that answered, and the shape it asked. */
    private class Answered(
        val window: AliasProbe.Window,
        val shape: List<Int>?,
    )

    /** What the ask ladder heard, rung by rung. */
    private class Ladder(
        /** The first shape that served events. */
        val served: Answered?,
        /** The first shape answered with an empty EOSE. */
        val empty: Answered?,
        /** Every shape the relay heard and CLOSED, with its words: a refusal is not an empty answer. */
        val refusals: List<String>,
        val authRefused: Boolean,
        /** The last transport word a silent rung left. */
        val lastReason: String?,
    ) {
        /** The rung the grade reads. */
        val chosen: Answered? get() = served ?: empty
    }

    /** Page two below where page one ended. */
    private class SecondPage(
        val floor: Long,
        val window: AliasProbe.Compliance,
    )

    /**
     * The dial itself: the ask ladder for "answers", the anchored events for "honours `until`",
     * one NEG-OPEN for "reconciles". Each rung's transport reason is kept so the verdict can say why.
     */
    private suspend fun dialVerdict(
        url: NormalizedRelayUrl,
        anchor: Long,
        slot: DialSlot,
    ): FitnessOutcome? {
        // Handed over the moment the ladder has earned it, so the per-url deadline cannot cost it.
        val settled: (FitnessOutcome) -> Unit = { slot.result = DialResult.Graded(it) }
        val ladder = walkLadder(url, anchor, slot)
        gradeRefusals(ladder)?.let { return it }
        val answered = ladder.chosen ?: return gradeSilence(url, ladder, slot)
        val walked = answered.window.compliance
        val readMs = answered.window.firstPageMs
        unpageableOnPageOne(walked, readMs)?.let { return it }
        val evidence = evidenceOf(answered)

        // Handed over before any further dial, so the per-url deadline cannot leave the url with no
        // verdict, and already graded on page one so a cut cannot publish a grade page one refutes.
        settled(firstPageGrade(walked, evidence, readMs))

        val second = secondPage(url, answered, slot)
        if (second != null && second.window.seen > 0 && second.window.offWindow == second.window.seen) {
            // The fault the mirror aborts on: the walk cannot advance.
            return FitnessOutcome(
                Verdict.UNPAGEABLE,
                "honoured the anchor and then ignored the cursor: page two came back entirely above it",
                pageable = Fact(false, "page one walked to ${second.floor}; page two asked below it and answered ${second.window.seen} events, all above it"),
                rttReadMs = readMs,
                compliant = factOf(walked + second.window),
            )
        }
        val pageable = second?.let(::pageableOf)

        val checked = walked + (second?.window ?: AliasProbe.Compliance())
        if (compliance.decide(checked) == RelayCompliance.Verdict.NONCOMPLIANT) {
            return FitnessOutcome(
                Verdict.NONCOMPLIANT,
                NONCOMPLIANT_EVIDENCE,
                pageable = pageable,
                rttReadMs = readMs,
                compliant = factOf(checked),
            )
        }
        val compliantFact = factOf(checked)

        // Re-handed with the facts earned so far, in case the NEG-OPEN outlives the url's clock.
        settled(FitnessOutcome(Verdict.PRIME, evidence, pageable = pageable, compliant = compliantFact, rttReadMs = readMs))

        return FitnessOutcome(
            Verdict.PRIME,
            evidence,
            pageable = pageable,
            nip77 = nip77Fact(url, anchor, answered.shape, slot),
            compliant = compliantFact,
            rttReadMs = readMs,
        )
    }

    /** Every shape in [AliasProbe.SHAPES] in turn, until one serves events or the ladder says the url is not there. */
    private suspend fun walkLadder(
        url: NormalizedRelayUrl,
        anchor: Long,
        slot: DialSlot,
    ): Ladder {
        var lastReason: String? = null
        var served: Answered? = null
        var empty: Answered? = null
        val refusals = ArrayList<String>()
        var silent = 0
        // A shape refused for our key: chorus.bonsai.com gates DMs this way and serves profiles openly.
        var authRefused = false
        for (rung in AliasProbe.SHAPES) {
            val window = probe.window(url, anchor, rung).also { slot.downloaded += it.compliance.seen }
            if (window.authRefused && window.ids.isNullOrEmpty()) {
                authRefused = true
                refusals += AUTH_REQUIRED + " " + (window.refusal ?: "rejected our key")
                continue
            }
            val ids = window.ids
            if (ids == null) {
                lastReason = window.reason ?: lastReason
                // filter.nostr.wine answers nothing on that connection once it has refused us, and each
                // further shape would wait out the whole idle window.
                if (authRefused) break
                silent++
                // Two silent rungs in a row is a url that is not there.
                if (rung == AliasProbe.FALLBACK_KINDS && silent == 2) break
                if (rung == AliasProbe.SHAPES.first() && probe.endsLadder(url, window)) break
                continue
            }
            if (ids.isNotEmpty()) {
                served = Answered(window, rung)
                break
            }
            // An indexer refuses a bare filter and serves kind 0; a group host serves 39000 alone.
            val refusal = window.refusal
            if (refusal != null) {
                refusals += refusal
            } else if (empty == null) {
                empty = Answered(window, rung)
            }
        }
        return Ladder(served, empty, refusals, authRefused, lastReason)
    }

    /** The grade refusals alone earn: our key refused with nothing served, or every shape heard and turned down. */
    private fun gradeRefusals(ladder: Ladder): FitnessOutcome? {
        // Refused our key on a shape and served nothing anywhere: what it holds is behind the key.
        if (ladder.served == null && ladder.authRefused) {
            return FitnessOutcome(Verdict.AUTH_REFUSED, "asked for NIP-42 and rejected our key", authRequired = true)
        }
        // It heard every shape and turned each down: nothing we can generally send gets an answer. A
        // shape that went silent instead leaves it unproven, and the silence path decides.
        if (ladder.chosen != null || ladder.refusals.size != AliasProbe.SHAPES.size) return null
        val words = ladder.refusals.first().take(REFUSAL_QUOTE)
        return if (ladder.refusals.all { it.startsWith(AUTH_REQUIRED, ignoreCase = true) }) {
            // After our AUTH: a members-only relay that does not count us as one.
            FitnessOutcome(Verdict.AUTH_REFUSED, "refused every shape it was asked as auth-required: $words", authRequired = true)
        } else {
            FitnessOutcome(Verdict.RESTRICTED, "refused every shape it was asked (${AliasProbe.SHAPES.joinToString { AliasProbe.shapeOf(it) }}): $words")
        }
    }

    /** The url answered no shape: a deferral where some shape was refused, else what the last transport word says. */
    private suspend fun gradeSilence(
        url: NormalizedRelayUrl,
        ladder: Ladder,
        slot: DialSlot,
    ): FitnessOutcome? {
        // A relay that CLOSED one shape spoke, so it is not silent, and the quiet shapes keep it unproven.
        if (ladder.refusals.isNotEmpty()) {
            slot.result = DialResult.Deferred(Deferral.PARTLY_REFUSED)
            return null
        }
        val retryAfterMs = Silence.retryAfterSeconds(ladder.lastReason)?.let { it * 1000 }
        return silenced(url, ladder.lastReason, unmeasured = { slot.result = DialResult.Unmeasured(it) }) { slot.result = DialResult.Deferred(it, retryAfterMs) }
    }

    /** All-or-nothing: a relay that puts only some events above the cursor still advances it. */
    private fun unpageableOnPageOne(
        walked: AliasProbe.Compliance,
        readMs: Long?,
    ): FitnessOutcome? {
        if (walked.offWindow == 0 || walked.offWindow != walked.seen) return null
        return FitnessOutcome(
            Verdict.UNPAGEABLE,
            "every event answered above the `until` it was asked for",
            pageable = Fact(false, "${walked.offWindow} of ${walked.seen} events came back above the anchor — the cursor was ignored"),
            rttReadMs = readMs,
            // Asked of the judge, not asserted: one event above the anchor is below the compliance floors.
            compliant = factOf(walked),
        )
    }

    /** What a `prime` says the url answered. */
    private fun evidenceOf(answered: Answered): String {
        val seen = answered.window.compliance.seen
        return if (seen == 0) {
            "answered every shape it was asked empty, or refused it, at a settled anchor"
        } else {
            "answered $seen events at a settled anchor" + (answered.shape?.let { ", through ${AliasProbe.shapeOf(it)}" }.orEmpty())
        }
    }

    /** The grade page one supports on its own. */
    private fun firstPageGrade(
        walked: AliasProbe.Compliance,
        evidence: String,
        readMs: Long?,
    ): FitnessOutcome =
        if (compliance.decide(walked) == RelayCompliance.Verdict.NONCOMPLIANT) {
            FitnessOutcome(Verdict.NONCOMPLIANT, NONCOMPLIANT_EVIDENCE, rttReadMs = readMs, compliant = factOf(walked))
        } else {
            FitnessOutcome(Verdict.PRIME, evidence, rttReadMs = readMs, compliant = factOf(walked))
        }

    /**
     * The second page, through the rung that answered and under its own clock; a cut claims nothing.
     * Null when it was not asked, was cut, or never answered.
     */
    private suspend fun secondPage(
        url: NormalizedRelayUrl,
        answered: Answered,
        slot: DialSlot,
    ): SecondPage? {
        val shape = answered.shape
        // A group host's metadata is its whole list of groups, whatever the cursor: there is no page two.
        val floor = answered.window.oldestAt.takeIf { shape != RelayAliases.GROUP_METADATA_KINDS }
        val asked =
            if (floor == null) {
                null
            } else {
                progress.holding(url.url, STAGE_COMPLIANCE)
                withTimeoutOrNull(probe.deadlineMs(url) / COMPLIANCE_BUDGET_DIVISOR) {
                    Paged(probe.pageBelow(url, floor - 1, shape)?.also { slot.downloaded += it.seen })
                }
            }
        // Boxed for [Reconciled]'s reason: only the clock firing belongs in the count.
        if (floor != null && asked == null) slot.secondPageCut = true
        val second = asked?.window
        // An empty first page proves the relay answers, not that it can be walked; no claim is published.
        if (second == null) slot.pageUnproven = true
        return second?.let { SecondPage(checkNotNull(floor), it) }
    }

    /** The `pageable` fact a second page that did not ignore the cursor earns. */
    private fun pageableOf(second: SecondPage): Fact =
        if (second.window.seen == 0) {
            // A drain is the strongest answer: a relay ignoring the cursor would have served its newest again.
            Fact(true, "page two below ${second.floor} drained — the walk terminates")
        } else {
            Fact(
                true,
                "walked two pages, cursor advanced past ${second.floor} " +
                    "(${second.window.seen - second.window.offWindow} of ${second.window.seen} event(s) below it)",
            )
        }

    /**
     * One NEG-OPEN against a sliver of the window: a normal return is NIP-77 spoken, the
     * dedicated exception is it declined, and anything else writes nothing.
     */
    private suspend fun nip77Fact(
        url: NormalizedRelayUrl,
        anchor: Long,
        shape: List<Int>?,
        slot: DialSlot,
    ): Fact? {
        val sliver = Filter(kinds = shape, since = anchor - NIP77_WINDOW_SECONDS, until = anchor)
        progress.holding(url.url, STAGE_NIP77)
        val reconciled =
            withTimeoutOrNull(nip77DeadlineMs) {
                Reconciled(
                    try {
                        reconcile(url, sliver)
                        Fact(true, "answered a NEG-OPEN over a ${NIP77_WINDOW_SECONDS / 3600}h window")
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: NegentropySyncException) {
                        Fact(false, "declined the NEG-OPEN: ${e.reason}")
                    } catch (_: Exception) {
                        // No fact: the failure was ours or the moment's, not the relay's.
                        null
                    },
                )
            }
        if (reconciled == null) slot.negOpenCut = true
        return reconciled?.fact
    }

    /**
     * The grade for a url no shape heard from, read off the last transport word. Only a word that is
     * evidence earns `silent`; a rate limit, or words [Silence] does not know, earn nothing.
     */
    private suspend fun silenced(
        url: NormalizedRelayUrl,
        lastReason: String?,
        unmeasured: (String) -> Unit,
        defer: (Deferral) -> Unit,
    ): FitnessOutcome? =
        when (val cause = Silence.of(lastReason)) {
            Silence.TLS -> {
                tlsFailed(url, defer)
            }

            Silence.UPGRADE -> {
                upgradeRefused(lastReason, defer)
            }

            // The pre-probe resolved this name moments ago, so a dial that could not is our resolver.
            Silence.NAME -> {
                unmeasured("the dial could not resolve a name the pre-probe had resolved")
                null
            }

            // A server answered and asked us to come back later, which says nothing about the relay.
            Silence.RATE_LIMITED -> {
                defer(Deferral.RATE_LIMITED)
                null
            }

            Silence.UNKNOWN -> {
                // No words at all is our own socket layer as often as the relay, and is left unmeasured.
                if (lastReason != null) defer(Deferral.UNRECOGNISED)
                null
            }

            // A refusal or route word is text, not proof: the typed pre-probe owns `dead`, and it let this url through.
            Silence.REFUSED, Silence.NO_ROUTE, Silence.TIMEOUT -> {
                FitnessOutcome(Verdict.SILENT, cause.reason)
            }
        }

    private companion object {
        /** NIP-42's machine-readable prefix on a CLOSED. */
        const val AUTH_REQUIRED = "auth-required:"

        /** How much of a relay's own refusal a verdict quotes. */
        const val REFUSAL_QUOTE = 120
    }
}
