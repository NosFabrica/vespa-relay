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
 * A stream whose relay list is WHOEVER IS SIGNED IN — the third way a stream
 * gets its urls, beside the hand-written `urls` and the store-scanning
 * `relaySource`.
 *
 *     authedOutbox {
 *       dir      = "down"
 *       sync     = "fetch"
 *       filter   = { "kinds": [1, 1111, 30023] }
 *       presence = { source = "outbox" }
 *     }
 *
 * The other two sources answer "what does this corpus say we should mirror",
 * which is the right question for a corpus and the wrong one for a person. A
 * reader who signs in wants their own writes and their own scores here NOW, and
 * a discovery cycle running every six hours over every stored relay list cannot
 * express that at any setting: they are one author among millions, their band
 * is one of thousands, and nothing in the rotation knows they are waiting.
 *
 * **The set is not knowable in this process.** A NIP-42 AUTH lands on a
 * websocket the RELAY owns, and the mirror has been its own process since the
 * split — so presence crosses the boundary by HTTP exactly as serving latency
 * does: the relay serves `GET /authed` and `AuthedPoller` reads it. That
 * endpoint names clients, unlike every other document either process serves, so
 * it exists only where a token does on both sides.
 *
 * @param source which of a signed-in reader's two relay lists this stream
 *   follows — see [PresenceSource]. One stream, one source, because the two
 *   want completely different filters: their own kinds authored by them, or
 *   kind 30382 authored by their provider.
 * @param pollSeconds how often the authed set is re-read and the subscriptions
 *   reconciled against it. This is the whole latency budget of the feature —
 *   the gap between someone signing in and their relays being dialled — so it
 *   is seconds, not minutes. It also re-resolves the targets of readers already
 *   here, because a 10002 or a 10040 can change while its author is online.
 * @param concurrency how many history catch-ups run at once. Only meaningful
 *   with `sync = "fetch"`; the live tails are sockets, not workers, and are not
 *   paced by it.
 * @param maxRelaysPerReader the most urls one reader may put into the fan-out.
 *   A relay list is single digits in the wild and five figures in the pathology
 *   this repo has measured; without a cap, one person publishing a 10,591-entry
 *   kind 10002 would dial ten thousand relays by signing in.
 * @param exclude urls to skip however a reader names them — the same syntax and
 *   the same matching as a dynamic stream's, and the place to put this relay's
 *   own url so a reader's outbox pointing here does not make us mirror
 *   ourselves.
 */
data class PresenceConfig(
    val source: PresenceSource,
    val pollSeconds: Long = DEFAULT_POLL_SECONDS,
    val concurrency: Int = 4,
    val maxRelaysPerReader: Int = DEFAULT_MAX_RELAYS_PER_READER,
    val exclude: RelayExcludes = RelayExcludes.NONE,
) {
    companion object {
        /**
         * Fast enough that signing in and being mirrored feel like one event,
         * slow enough that the poll is nothing to the relay: at 30s it is one
         * conditional-less GET of a few kilobytes twice a minute, against a
         * process already answering websockets.
         */
        const val DEFAULT_POLL_SECONDS = 30L

        /** The floor, so a misconfigured stream cannot turn the feed into a load generator. */
        const val MIN_POLL_SECONDS = 5L

        /**
         * A real NIP-65 outbox is single digits. This is generous rather than
         * tight because the cost of being wrong is asymmetric — too low silently
         * stops mirroring a relay the reader actually writes to, too high costs
         * some sockets — and because [RelayAliases]' fold takes the minted-path
         * duplicates out downstream anyway. What it exists to stop is the
         * measured pathology: 148 pubkeys publish a kind 10002 of 100 to 10,591
         * entries.
         */
        const val DEFAULT_MAX_RELAYS_PER_READER = 8
    }
}

/**
 * Which of a signed-in reader's own lists a presence stream follows.
 *
 * Both are a chain of exactly one link: read the reader's replaceable event,
 * take the urls out of it, and narrow the stream's filter with what that event
 * paired each url with. What differs is whose events come back — which is why
 * they are two streams in `router.conf` rather than one with a flag.
 */
enum class PresenceSource(
    val wire: String,
) {
    /**
     * Their kind 10002, write side — NIP-65's outbox model, aimed at one person.
     *
     * Each url is asked for the stream's filter narrowed to `authors = [that
     * reader]`, because the relay was named by THEM as where they publish, and a
     * whole outbox relay's worth of everybody's notes is a corpus stream's job
     * and not this one's. That narrowing is also what makes the catch-up cheap:
     * one author's history on one relay, and a band keyed on a filter that
     * changes only when their relay list does.
     */
    OUTBOX("outbox"),

    /**
     * Their kind 10040's ranking providers — the relay AND the key both come out
     * of the reader's own list, which is the whole point.
     *
     * `TrustNotice` tells a reader on login that this relay holds none of their
     * provider's cards; `readiness.js` draws the same gap as a bar. Neither
     * could FIX it: the corpus stream that mirrors NIP-85 assertions discovers
     * providers by scanning stored 10040s on a six-hour cycle, so a reader whose
     * provider nobody here has ever mirrored waits out that cycle before ranked
     * search returns anything at all. This closes the loop — the reader whose
     * scores are missing is, by construction, present.
     *
     * Each `30382:rank` entry gives a (service pubkey, relay hint) pair, and the
     * pair is kept together: the relay is asked for `authors = [that service]`
     * and nothing else. The signer is what ranks — a card is ABOUT its `d` tag
     * but ranks for whoever named its signer — so narrowing by the service is
     * the narrowing that matches how the store reads them.
     */
    SCORES("scores"),
    ;

    companion object {
        fun parse(raw: String): PresenceSource =
            entries.firstOrNull { it.wire.equals(raw.trim(), ignoreCase = true) }
                ?: error("router: unknown presence source '$raw' (expected ${entries.joinToString(" / ") { it.wire }})")
    }
}
