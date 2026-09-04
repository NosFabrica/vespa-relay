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

/**
 * Does the relay answer the question it was ASKED — the one thing the stability
 * gate cannot see.
 *
 * ## The gap this fills, stated exactly
 *
 * [RelayConsistency] compares two answers to one filter against EACH OTHER. It
 * catches the relay whose window is a fresh random slice per REQ, and it is
 * blind by construction to the relay whose window is the SAME wrong slice every
 * time: two identical answers score 1.000 containment whatever is in them,
 * because nothing in that comparison ever opens an event. A relay that replies
 * to every ask with its newest firehose — ignoring `kinds`, ignoring `until`,
 * ignoring the `limit` — is perfectly self-consistent and, until this pass
 * existed, perfectly certifiable.
 *
 * So the two are complements and neither implies the other. Consistency asks
 * "is this the same relay twice"; this asks "is this an ANSWER".
 *
 * ## What it costs to leave one in
 *
 * The mirror asks for a kind and a window and stores what comes back. A relay
 * that serves something else is not merely useless to a stream — it is
 * expensive to it: every cycle downloads events the ask did not want, they are
 * verified, offered to the store and mostly refused as duplicates, and the band
 * the cursor covers is a fiction because the events that came back were never
 * the ones inside it. That is [RelayConsistency]'s failure mode reached by a
 * different road, and the same argument applies to the response: exclude rather
 * than downgrade, and let the verdict expire so a relay that starts answering
 * properly comes back on its own.
 *
 * It is also the one refusal here that says something a reader outside this
 * deployment can act on directly. `alias` and `inconsistent` are facts about
 * syncing; "this relay does not honour `kinds`" is a fact about the protocol,
 * and any client picking read relays wants it.
 *
 * ## The bars, and how provisional they are
 *
 * **These numbers have not been taken against the network yet, and the header
 * of [RelayConsistency.ANCHOR_LAG_SECONDS] is the standing warning about
 * exactly that mistake** — a single run there suggested a constant that a
 * second run disproved. `RelayComplianceProbe` is the instrument for these two,
 * it is written, and it asserts nothing; run it more than once before believing
 * either number, and record what it said here when you do.
 *
 * Until then the bars are set where a false positive is expensive and a false
 * negative merely leaves things as they were: TWO bars, both of which have to
 * be crossed. A share alone would refuse a relay that answered three events and
 * got one wrong; a count alone would refuse a firehose that got three wrong out
 * of five hundred. Neither of those is the failure this is for, and the failure
 * this is for crosses both by a mile — a relay ignoring the filter gets
 * essentially ALL of them wrong.
 *
 * ## What is deliberately not a refusal
 *
 * [AliasProbe.Compliance.overLimit]. An over-served event matches the filter; it
 * was simply not asked for yet. It is published as a fact and never graded on
 * — see that field's own header.
 */
class RelayCompliance(
    /**
     * How much of an answer must be off-filter before the answer is not one.
     *
     * Ten percent. Nothing measured stands behind that yet (see the class
     * header); it is chosen as a number no honest relay can plausibly reach —
     * a compliant server is at 0.000 by construction, and the only things known
     * to put a stray event in a window are our own five-minute clock slack and
     * a boundary re-read — while the relay this exists for sits at or near
     * 1.000.
     */
    private val minOffFilterShare: Double = DEFAULT_MIN_OFF_FILTER_SHARE,
    /**
     * …and how many off-filter events it takes at all, whatever the share.
     *
     * The floor that stops a thin answer being a verdict. A relay that returned
     * two events and got one wrong is at 0.500 and has told us almost nothing;
     * the same relay over twenty events is telling us something. Same argument
     * as [RelayConsistency]'s `minSample`, one field over: the cost of missing
     * a bad relay for one more cycle is a cycle, and the cost of refusing a
     * good one is its place in the mirror.
     */
    private val minOffFilterEvents: Int = DEFAULT_MIN_OFF_FILTER_EVENTS,
) {
    /** What one answer proved about the relay that served it. */
    enum class Verdict {
        /** Everything that came back matched the ask, or too little of it did not to say. */
        COMPLIANT,

        /** Enough of it did not match that the relay is not answering the filter. */
        NONCOMPLIANT,

        /**
         * Nothing was proved. An empty answer, or one that never happened.
         *
         * Its own value rather than [COMPLIANT], for [RelayConsistency.Verdict.UNMEASURABLE]'s
         * reason: a relay holding nothing under the ask is not a relay that
         * honoured it, and publishing `compliant true` off a drain would put
         * our name to a claim no event supports.
         */
        UNMEASURABLE,
    }

    /** How much of the answer did not match the ask. Zero when there was no answer. */
    fun share(reading: AliasProbe.Compliance): Double = if (reading.seen == 0) 0.0 else reading.offFilter.toDouble() / reading.seen

    /** What [reading] proves — see [Verdict], and the bars on the class. */
    fun decide(reading: AliasProbe.Compliance): Verdict =
        when {
            reading.seen == 0 -> Verdict.UNMEASURABLE
            reading.offFilter >= minOffFilterEvents && share(reading) >= minOffFilterShare -> Verdict.NONCOMPLIANT
            else -> Verdict.COMPLIANT
        }

    /**
     * The sentence published beside the verdict.
     *
     * **A stray that did not reach the bar is still named.** The bars are
     * provisional (see the class header) and the evidence is how they get
     * re-taken: a corpus of records saying "1 of 20 events came back off-filter"
     * is the measurement that says whether ten percent is the right line, and a
     * record that only said `compliant true` would have thrown it away.
     */
    fun evidence(reading: AliasProbe.Compliance): String {
        if (reading.seen == 0) return "nothing came back to check"
        val faults =
            buildList {
                if (reading.offKind > 0) add("${reading.offKind} of a kind the filter did not ask for")
                if (reading.offWindow > 0) add("${reading.offWindow} above the `until` asked for")
                if (reading.overLimit > 0) add("${reading.overLimit} beyond the `limit` asked for")
            }
        val asked = if (reading.kindsAsked) "`kinds`, `until` and `limit` checked" else "`until` and `limit` checked, no `kinds` was put"
        if (faults.isEmpty()) return "${reading.seen} events, every one matching the ask ($asked)"
        return "${reading.seen} events, ${reading.offFilter} off-filter (${faults.joinToString(", ")}); $asked"
    }

    companion object {
        /** See the constructor parameter of the same name. PROVISIONAL — measure before moving it. */
        const val DEFAULT_MIN_OFF_FILTER_SHARE = 0.10

        /** See the constructor parameter of the same name. PROVISIONAL — measure before moving it. */
        const val DEFAULT_MIN_OFF_FILTER_EVENTS = 3
    }
}
