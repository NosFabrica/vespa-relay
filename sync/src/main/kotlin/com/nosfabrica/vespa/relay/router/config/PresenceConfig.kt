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
package com.nosfabrica.vespa.relay.router.config

/**
 * `presence { }` — the modifier that scopes a stream's `relaySource` to the
 * people SIGNED IN right now, one reader at a time.
 *
 *     authedContent {
 *       dir      = "down"
 *       sync     = "fetch"
 *       filter   = { "kinds": [1, 1111, 30023] }
 *       presence = { pollSeconds = 30 }
 *       relaySource = [
 *         # what they wrote, from their own outbox relays
 *         { select = [ { kind = 10002, tag = "r", marker = "write", authors = "pubkey" } ]
 *           filter = { "kinds": [10002] } },
 *         # …and what mentions them, from their own inbox relays
 *         { select = [ { kind = 10002, tag = "r", marker = "read", "#p" = "pubkey" } ]
 *           filter = { "kinds": [10002] } }
 *       ]
 *     }
 *
 * **It is a SCOPE, not a third source language.** Everything about which tag
 * holds a url, which marker to keep, and what that url should then be asked for
 * is [RelaySelect] and its bindings — the same grammar `profileViaOutbox` and
 * `assertions` are written in, read by the same code. What presence changes is
 * one thing: each source's scan filter is narrowed to `authors = [one signed-in
 * reader]` and re-run every [pollSeconds], instead of walking the whole corpus
 * every six hours.
 *
 * That one change is what the other two sources cannot express at any setting.
 * A reader who signs in is one author among millions and their band is one of
 * thousands; nothing in a rotation over every stored relay list knows they are
 * waiting. It also makes `Slot.EventPubkey` mean something new: on a corpus scan
 * `authors = "pubkey"` pairs each relay with whoever listed it, and here that
 * pubkey is always the reader — so the fan-out is exactly their own outbox
 * asking for exactly their own events.
 *
 * **What a stream may NOT carry with it** is refused in the loader rather than
 * ignored: `refreshSeconds`, `recycleSeconds`, `concurrency` and `authorsPerLeg`
 * belong to the dynamic rotation and mean nothing here — presence is paced by
 * [pollSeconds] and has one author per leg by construction. `exclude` and
 * `maxRelaysPerList` stay where they are and do the same job.
 *
 * @param pollSeconds how often the authed set is re-read and the subscriptions
 *   reconciled against it. This is the whole latency budget of the feature —
 *   the gap between somebody signing in and their relays being dialled — so it
 *   is seconds, not minutes. It also re-resolves the targets of readers already
 *   here, because a 10002 can change while its author is online.
 * @param concurrency how many history catch-ups run at once. Only meaningful
 *   with `sync = "fetch"`; the live tails are sockets, not workers.
 * @param maxRelaysPerReader the most urls one reader may put into the fan-out,
 *   across every source. A relay list is single digits in the wild and five
 *   figures in the pathology this repo has measured; without a cap, one person
 *   publishing a 10,591-entry kind 10002 dials ten thousand relays by signing
 *   in. Distinct from `maxRelaysPerList`, which refuses an implausible EVENT
 *   whole — this bounds what one PERSON costs however many lists they publish.
 */
data class PresenceConfig(
    val pollSeconds: Long = DEFAULT_POLL_SECONDS,
    val concurrency: Int = 4,
    val maxRelaysPerReader: Int = DEFAULT_MAX_RELAYS_PER_READER,
) {
    companion object {
        /**
         * Fast enough that signing in and being mirrored feel like one event,
         * slow enough that the poll is nothing to the relay: at 30s it is one
         * small GET twice a minute, against a process already answering
         * websockets.
         */
        const val DEFAULT_POLL_SECONDS = 30L

        /** The floor, so a misconfigured stream cannot turn the feed into a load generator. */
        const val MIN_POLL_SECONDS = 5L

        /**
         * Generous rather than tight, because the cost of being wrong is
         * asymmetric — too low silently stops mirroring a relay the reader
         * really writes to, too high costs some sockets — and because the alias
         * fold takes the minted-path duplicates out downstream anyway. It is
         * eight per SOURCE-merged set, so an outbox/inbox pair shares it.
         */
        const val DEFAULT_MAX_RELAYS_PER_READER = 8

        /**
         * The knobs that pace the dynamic rotation and mean nothing on a
         * presence stream. Refused at parse time, named here so the loader's
         * message and this list cannot drift.
         */
        val ROTATION_ONLY = listOf("refreshSeconds", "recycleSeconds", "concurrency", "authorsPerLeg")
    }
}
