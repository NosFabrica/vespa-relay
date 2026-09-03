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

/*
 * WHAT THIS DEPLOYMENT SAYS ABOUT A RELAY — the NIP-32 label vocabulary its
 * kind-30166 records are written in, and the one thing about the monitor the
 * mirror has to understand.
 *
 * In :peers rather than beside the pass that decides it, because it is the
 * CONTRACT between the two planes. The monitor writes these values; the mirror
 * reads them back out of the store to build a roster (`#l = prime`), and
 * `RelayDiscovery` holds a `dead` url out of a candidate set. A vocabulary only
 * one side can name is not a contract.
 *
 * (A block comment: a toplevel KDoc documenting no declaration is what ktlint
 * calls dangling, and it fails the build.)
 */

/**
 * The grade vocabulary. [PRIME] is the only admitting value; every refusal
 * is descriptive, so the record explains itself instead of a worker's log
 * line having to.
 *
 * **`prime`, and it used to be `syncable`.** The old word named OUR use of
 * the relay, on a record published for everyone — a crawler, an archiver, a
 * client choosing read relays all want this same composite, and none of them
 * are syncing. A grade names the relay; what the reader does with a prime
 * one is their business.
 */
enum class Verdict(
    val value: String,
) {
    PRIME("prime"),

    /** No TCP, no TLS, no websocket — the transport itself said no. */
    DEAD("dead"),

    /** Connected, then nothing: no EOSE, no CLOSED, the window lapsed. */
    SILENT("silent"),

    /** Works fine, and is another record's relay — syncing it doubles every event. */
    ALIAS("alias"),

    /** Two answers to one question — would poison bands and coverage. */
    INCONSISTENT("inconsistent"),

    /** Ignores `until`: a paged walk against it cannot terminate. */
    UNPAGEABLE("unpageable"),

    /**
     * Answers with events the filter did not ask for — a wrong kind, a stamp
     * above the `until`.
     *
     * The refusal [INCONSISTENT] structurally cannot make: that one compares
     * two answers to each other, so a relay serving the same wrong events every
     * time passes it. This one reads the events.
     */
    NONCOMPLIANT("noncompliant"),

    /** Requires NIP-42 and turned OUR key down. */
    AUTH_REFUSED("auth-refused"),

    /** Answers only shaped queries this router cannot generally send. */
    RESTRICTED("restricted"),
}
