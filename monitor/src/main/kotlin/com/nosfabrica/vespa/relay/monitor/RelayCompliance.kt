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
 * Does the relay answer the question it was asked: the complement of [RelayConsistency], which
 * compares two answers and never opens an event. A failure excludes rather than downgrades,
 * and both bars must be crossed. [AliasProbe.Compliance.overLimit] is published, never graded on.
 */
class RelayCompliance(
    /** How much of an answer must be off-filter before it is not an answer. */
    private val minOffFilterShare: Double = DEFAULT_MIN_OFF_FILTER_SHARE,
    /** The floor that stops a thin answer being a verdict, whatever the share. */
    private val minOffFilterEvents: Int = DEFAULT_MIN_OFF_FILTER_EVENTS,
) {
    /** What one answer proved about the relay that served it. */
    enum class Verdict {
        /** Everything that came back matched the ask, or too little of it did not to say. */
        COMPLIANT,

        /** Enough of it did not match that the relay is not answering the filter. */
        NONCOMPLIANT,

        /** Nothing was proved: an empty answer, or one that never happened. */
        UNMEASURABLE,
    }

    /** How much of the answer did not match the ask. Zero when there was no answer. */
    fun share(reading: AliasProbe.Compliance): Double = if (reading.seen == 0) 0.0 else reading.offFilter.toDouble() / reading.seen

    fun decide(reading: AliasProbe.Compliance): Verdict =
        when {
            reading.seen == 0 -> Verdict.UNMEASURABLE
            reading.offFilter >= minOffFilterEvents && share(reading) >= minOffFilterShare -> Verdict.NONCOMPLIANT
            else -> Verdict.COMPLIANT
        }

    /** The sentence published beside the verdict. A stray below the bar is still named. */
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
        /** Provisional; measure with `RelayComplianceProbe` before moving it. */
        const val DEFAULT_MIN_OFF_FILTER_SHARE = 0.10

        /** Provisional; measure with `RelayComplianceProbe` before moving it. */
        const val DEFAULT_MIN_OFF_FILTER_EVENTS = 3
    }
}
