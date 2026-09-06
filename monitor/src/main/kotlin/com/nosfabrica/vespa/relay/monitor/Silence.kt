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
 * Why a url never answered, read off the transport's terminal message. The text is not a
 * contract, so text the table does not recognise is [UNKNOWN] and never forced into a bucket.
 */
enum class Silence(
    val reason: String,
) {
    /** `UnknownHostException`. */
    NAME("the name does not resolve"),

    /** `ConnectException`: something answered the port and said no. */
    REFUSED("the connection was refused"),

    /** `NoRouteToHostException` / `PortUnreachableException`. */
    NO_ROUTE("no route to the host"),

    /** `SSLHandshakeException` and friends. */
    TLS("the TLS handshake failed"),

    /** Reachable, but what answered is not a websocket. */
    UPGRADE("the websocket upgrade was refused"),

    /** The window lapsed with nothing on it. */
    TIMEOUT("it never answered in time"),

    /** 429, or the words for it. */
    RATE_LIMITED("we were rate limited"),

    /** Text this table has not been taught. */
    UNKNOWN("gave up for a reason we do not recognise"),
    ;

    companion object {
        /** In match order: first hit wins. */
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
                // Timeout before refused: a refusal's only evidence is the `(ConnectException)` suffix.
                TIMEOUT to listOf("timeout", "timed out", "sockettimeout"),
                REFUSED to listOf("connection refused", "econnrefused", "connectexception"),
            )

        /** Null is [UNKNOWN], not a cause of its own. */
        fun of(raw: String?): Silence {
            val text = raw?.lowercase() ?: return UNKNOWN
            return PATTERNS.firstOrNull { (_, needles) -> needles.any { it in text } }?.first ?: UNKNOWN
        }
    }
}
