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
package com.nosfabrica.vespa.relay.sync

import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.PagedFetchResult

/**
 * A refusal that is the relay's policy for an ask rather than a moment of it: kinds or a shape it
 * will not serve, or an identity it will not accept. A cap on our load or width is never one.
 */
internal object StandingRefusal {
    /** Whether a walk that ended with [end], the relay having [said] so, would end the same way asked again. */
    fun of(
        end: PagedFetchResult.End,
        said: String?,
    ): Boolean {
        if (said != null && OUR_LOAD.containsMatchIn(said)) return false
        return when (end) {
            // Ended after quartz answered the challenge: our identity is the one it will not take.
            PagedFetchResult.End.AUTH_REQUIRED -> true

            PagedFetchResult.End.CLOSED -> said != null && (POLICY.containsMatchIn(said) || KIND_REFUSED.containsMatchIn(said))

            else -> false
        }
    }

    /** NIP-01's machine-readable prefixes for a refusal of what was asked. */
    private val POLICY = Regex("^\\s*(restricted|blocked|invalid|invalid-filter|unsupported|auth-required):", RegexOption.IGNORE_CASE)

    /** The same refusal under a prefix that means nothing, as `error: … kind not allowed: 1984`. */
    private val KIND_REFUSED = Regex("\\bkinds?\\b.*\\bnot (allowed|supported|served)\\b", RegexOption.IGNORE_CASE)

    /** Answers to how much we ask, whatever their prefix: `blocked: already 50 subscriptions from this IP`. */
    private val OUR_LOAD = Regex("rate|too many|subscriptions|slow down|try again|later", RegexOption.IGNORE_CASE)
}
