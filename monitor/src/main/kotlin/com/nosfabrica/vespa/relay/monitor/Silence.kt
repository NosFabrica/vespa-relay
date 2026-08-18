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
 * WHY A URL NEVER ANSWERED — read off what the transport said, not guessed.
 *
 * `ConsistencyPass.Unmeasured.SILENT` is the largest bucket a probe pass
 * produces on a discovered corpus, and on its own it says nothing an operator
 * can act on: a hostname that no longer resolves, a port that answers with a
 * refusal, a TLS handshake that fails on an expired certificate and a socket
 * that simply lapses are four different findings, and only the last is worth
 * retrying on its own schedule. The evidence to tell them apart already existed
 * — quartz hands us the socket layer's own message as the terminal reason for
 * the url — and it was read once, for a `startsWith` on the prefix, and dropped.
 *
 * ## Substring matching, and why that is acceptable here
 *
 * The text is formatted by somebody else and is not a contract: it comes from
 * OkHttp and the JDK's socket exceptions through
 * `onCannotConnect(relay, message, filters)`, and its wording differs by
 * platform and by version. Measured live, quartz relays it as
 * `WebSocket Failure: <message> (<ExceptionName>)` — and for a refused port the
 * message is `Failed to connect to localhost/127.0.0.1:1`, which carries no
 * "refused" at all. The class name in the suffix is the whole evidence. Matching on it is a judgement, so it is made HERE,
 * once, where it can be read and corrected — and quartz itself classifies the
 * same strings the same way (`classifyDrainFailure` matches `timeout`,
 * `timed out`, `connect timed out`, `429`, `too many requests`).
 *
 * **What makes it honest is [UNKNOWN].** Nothing is forced into a bucket: text
 * this table does not recognise is counted as unrecognised and SAMPLED to
 * stderr, so the gap is visible on the first pass after a deploy and the table
 * can be extended from real strings rather than from imagination. A
 * misclassification here would be a claim about somebody else's server made
 * from a string we did not read properly, which is exactly the kind of claim
 * `Unreachability.proves` is careful not to make.
 *
 * The causes are deliberately the ones [Unreachability] already enumerates as
 * PROVING unreachability — the same five exceptions, in the same order of
 * confidence — plus the two transient ones quartz names, so the two components
 * can be read against each other.
 */
enum class Silence(
    val reason: String,
) {
    /** `UnknownHostException`: nothing answers for this name any more. */
    NAME("the name does not resolve"),

    /** `ConnectException`: something answered the port and said no. */
    REFUSED("the connection was refused"),

    /** `NoRouteToHostException` / `PortUnreachableException`. */
    NO_ROUTE("no route to the host"),

    /** `SSLHandshakeException` and friends: reachable, and we could not agree on TLS. */
    TLS("the TLS handshake failed"),

    /**
     * Reachable, and what answered is not a websocket. A relay list full of
     * plain web servers is ordinary, and this is what they look like.
     */
    UPGRADE("the websocket upgrade was refused"),

    /** The window lapsed with nothing on it — the one cause here that is worth simply retrying. */
    TIMEOUT("it never answered in time"),

    /** 429, or the words for it. Transient by definition, and our own pacing's business. */
    RATE_LIMITED("we were rate limited"),

    /** Text this table has not been taught. Counted, sampled to stderr, never forced. */
    UNKNOWN("gave up for a reason we do not recognise"),
    ;

    companion object {
        /**
         * The table, in match order. First hit wins, so the specific patterns
         * come before the general ones — `connect timed out` is a timeout and
         * not a refusal, and a rate limit that mentions a timeout is a rate
         * limit.
         */
        private val PATTERNS: List<Pair<Silence, List<String>>> =
            listOf(
                RATE_LIMITED to listOf("429", "too many requests", "rate limit"),
                NAME to
                    listOf(
                        "unknownhost",
                        "unable to resolve",
                        "name or service not known",
                        "nodename nor servname",
                        "no address associated",
                        "temporary failure in name resolution",
                    ),
                NO_ROUTE to listOf("no route to host", "noroutetohost", "network is unreachable", "portunreachable"),
                TLS to listOf("sslhandshake", "sslexception", "sslpeerunverified", "certificate", "trust anchor"),
                UPGRADE to listOf("unexpected response code", "expected http 101", "not a websocket"),
                // TIMEOUT ABOVE REFUSED, because the class name is the only
                // evidence a refusal leaves and it is the weaker of the two: the
                // message quartz relays for a refused port is `Failed to connect
                // to localhost/127.0.0.1:1`, which says nothing, and the verdict
                // rides on the `(ConnectException)` suffix. An explicit timeout
                // in the same string must win over that.
                TIMEOUT to listOf("timeout", "timed out", "sockettimeout"),
                REFUSED to listOf("connection refused", "econnrefused", "connectexception"),
            )

        /**
         * Classify one terminal reason. Null — a walk that ended without the
         * transport saying anything at all — is [UNKNOWN] rather than a cause
         * of its own: it is the same "we do not know", and giving silence about
         * silence its own row would suggest a finding where there is none.
         */
        fun of(raw: String?): Silence {
            val text = raw?.lowercase() ?: return UNKNOWN
            return PATTERNS.firstOrNull { (_, needles) -> needles.any { it in text } }?.first ?: UNKNOWN
        }
    }
}
