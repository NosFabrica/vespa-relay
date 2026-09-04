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
package com.nosfabrica.vespa.relay.sync.heal

/** What one relay's answer to one repair means. */
enum class PushVerdict {
    /** Took it. Whether it also dropped the stale copy is next cycle's question. */
    ACCEPTED,

    /** Refused on policy. The next repair meets the same answer, so the relay is closed for writes and the stale id suppressed now. */
    CLOSED,

    /** Refused transiently. Suppresses nothing and closes nothing. */
    RETRY,

    /** Answered, but the answer says nothing about the relay's willingness. */
    IGNORE,

    /** Never answered. Earns a strike, never a suppression; see [WriteCapability]. */
    SILENT,
}

/**
 * Reads NIP-01's machine-readable `OK` prefix for one question: has this relay
 * said it will not take our repairs? A transient refusal read as policy would
 * suppress ids the relay was about to accept, so `rate-limited:` and `error:`
 * are [PushVerdict.RETRY] and an unknown prefix is [PushVerdict.IGNORE].
 */
object OkClassifier {
    fun classify(
        accepted: Boolean,
        message: String,
        transportFailure: Boolean,
    ): PushVerdict {
        if (accepted) return PushVerdict.ACCEPTED
        if (transportFailure) return PushVerdict.SILENT
        val reason = message.trim().lowercase()
        val prefix = reason.substringBefore(':', missingDelimiterValue = "")
        return when (prefix) {
            "auth-required", "restricted", "blocked" -> PushVerdict.CLOSED

            "rate-limited", "error" -> PushVerdict.RETRY

            "invalid", "pow", "duplicate" -> PushVerdict.IGNORE

            // NIP-01 asks for a prefix; a relay that gives none has told us nothing to act on.
            else -> PushVerdict.IGNORE
        }
    }
}
