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
package com.nosfabrica.vespa.relay.router

import com.vitorpamplona.quartz.nip01Core.relay.client.listeners.RelayConnectionListener
import com.vitorpamplona.quartz.nip01Core.relay.client.single.IRelayClient
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip77Negentropy.NegErrMessage
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull

/**
 * Reads a peer's `max_sync_events` out of the refusal it sends when a NEG-OPEN
 * matches too much.
 *
 * The peer is the only source for this number — NIP-11 has no field for it —
 * and it hands it over for free on the first rejection:
 *
 *     ["NEG-ERR", <subId>, "blocked: query matches too many records (2431002 > 1000000)"]
 *     ["NEG-ERR", <subId>, "blocked: too many query results", 1000000]
 *
 * Knowing it turns [NegentropyPager]'s search for a workable window size from a
 * halving ladder into one arithmetic step. Not knowing it is not fatal — the
 * pager halves instead — so everything here is best-effort by construction: an
 * unparseable rejection yields null and nothing is learned.
 *
 * This listens on the CONNECTION rather than being wired into the reconcile
 * call, because quartz's negentropy accessory absorbs an overflow itself (it
 * splits the window and retries) and reports only the reason category up to us.
 * The raw frame carrying the number never surfaces through that path, but it
 * does pass every connection listener on its way in.
 *
 * [onCap] is invoked from the relay's reader coroutine. Keep it to a map write.
 */
internal class NegErrWatcher(
    private val onCap: (NormalizedRelayUrl, Int) -> Unit,
) : RelayConnectionListener {
    override suspend fun onIncomingMessage(
        relay: IRelayClient,
        msgStr: String,
        msg: Message,
    ) {
        // Type check first: this runs for EVERY message on every connection in
        // the router, including each mirrored event.
        if (msg !is NegErrMessage) return
        capOf(msgStr, msg.reason)?.let { onCap(relay.url, it) }
    }

    companion object {
        /** `(2431002 > 1000000)` — the cap is the right-hand side. */
        private val COMPARISON = Regex("""\(\s*\d+\s*>\s*(\d+)\s*\)""")

        /**
         * The peer's cap, or null if this rejection does not carry one.
         *
         * Narrow on purpose, in both directions. The reason must look like a
         * RESULT-SET-SIZE refusal before any number in it is believed: a
         * rate-limit ("too many requests") or an auth refusal can carry digits
         * too, and reading one of those as a negentropy cap would shrink every
         * window against a relay that has no such limit. And the number must be
         * positive — a peer reporting `0` would otherwise wedge the pager at a
         * window size that can never hold anything.
         */
        fun capOf(
            raw: String,
            reason: String,
        ): Int? {
            if (!looksLikeOverflow(reason)) return null
            COMPARISON
                .find(reason)
                ?.groupValues
                ?.get(1)
                ?.toIntOrNull()
                ?.let { if (it > 0) return it }
            // strfry's newer framing puts the cap in a fourth element rather
            // than the prose. Parsed off the raw frame because quartz's parsed
            // NEG-ERR keeps only subId and reason.
            return runCatching {
                Json
                    .parseToJsonElement(raw)
                    .jsonArray
                    .getOrNull(3)
                    ?.jsonPrimitive
                    ?.longOrNull
                    ?.takeIf { it > 0 && it <= Int.MAX_VALUE }
                    ?.toInt()
            }.getOrNull()
        }

        /**
         * Does this reason mean "your query matched more than I will reconcile"?
         *
         * Deliberately the same narrow set of phrasings quartz matches for the
         * same decision, and for the same reason: a quota or rate error that
         * merely SOUNDS like overflow does not shrink when the window shrinks,
         * so acting on it teaches the pager a cap that does not exist.
         */
        fun looksLikeOverflow(reason: String): Boolean =
            reason.contains("too many records", ignoreCase = true) ||
                reason.contains("too many results", ignoreCase = true) ||
                reason.contains("too many query results", ignoreCase = true) ||
                reason.contains("result set too large", ignoreCase = true) ||
                reason.contains("results too large", ignoreCase = true) ||
                reason.contains("max_sync_events", ignoreCase = true)
    }
}
