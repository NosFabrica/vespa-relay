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
    /** Took it. Whether it also DROPPED the stale copy is next cycle's question. */
    ACCEPTED,

    /**
     * Refused on policy — paid relay, allow list, block list. It will refuse
     * the next one identically, so the stale id it serves is suppressed now and
     * the relay is closed for writes.
     */
    CLOSED,

    /**
     * Refused transiently. Suppress NOTHING and close nothing: a momentary blip
     * must never permanently blind us to a relay that was about to heal.
     */
    RETRY,

    /** Answered, but the answer says nothing about the relay's willingness. */
    IGNORE,

    /** Never answered. Earns a strike, never a suppression — see [WriteCapability]. */
    SILENT,
}

/**
 * NIP-01's machine-readable `OK` reason, read for one question: has this relay
 * told us it will not take our repairs?
 *
 * The dangerous row is `rate-limited:` — and `error:` beside it. Treating a
 * momentary refusal as policy would suppress the very ids a relay was about to
 * accept a repair for, permanently and silently. Unknown prefixes are also
 * treated as nothing, on the same conservatism `Unreachability.proves()`
 * already applies to NIP-66 claims: silence about someone else's server costs a
 * retry, being wrong costs a false statement.
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

            // No recognised prefix at all. NIP-01 asks for one; a relay that
            // does not give one has not told us anything we may act on.
            else -> PushVerdict.IGNORE
        }
    }
}
