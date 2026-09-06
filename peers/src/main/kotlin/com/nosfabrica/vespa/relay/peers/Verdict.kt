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
package com.nosfabrica.vespa.relay.peers

/**
 * The NIP-32 grade vocabulary of this deployment's kind-30166 records: the monitor writes
 * it, the mirror reads it back to build a roster. [PRIME] is the only admitting value; every
 * refusal names what the relay did, not what this router does with it.
 */
enum class Verdict(
    val value: String,
) {
    PRIME("prime"),

    /** The transport itself said no: no TCP, no TLS, no websocket. */
    DEAD("dead"),

    /** Connected, then nothing: no EOSE, no CLOSED, the window lapsed. */
    SILENT("silent"),

    /** Works, and is another record's relay. */
    ALIAS("alias"),

    /** Two answers to one question. */
    INCONSISTENT("inconsistent"),

    /** Ignores `until`: a paged walk against it cannot terminate. */
    UNPAGEABLE("unpageable"),

    /**
     * Answers with events the filter did not ask for. Unlike [INCONSISTENT] this reads the
     * events, so a relay serving the same wrong events every time is caught.
     */
    NONCOMPLIANT("noncompliant"),

    /** Requires NIP-42 and turned this router's key down. */
    AUTH_REFUSED("auth-refused"),

    /** Answers only shaped queries this router cannot generally send. */
    RESTRICTED("restricted"),
}
