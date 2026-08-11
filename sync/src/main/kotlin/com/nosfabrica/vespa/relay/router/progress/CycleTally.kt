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
package com.nosfabrica.vespa.relay.router.progress

import java.util.concurrent.atomic.AtomicLong

/**
 * Where every url a cycle took on ENDED UP — a partition, not a tally.
 *
 * ## The number this exists to end
 *
 * A production `/stats.json` reported 16,752 relays discovered and 5,323
 * carrying a band. Nothing anywhere said what happened to the other ~11,400.
 * Every one of them had a disposition the router knew perfectly well at the
 * time — folded onto another url, refused at the TCP probe, on a host already
 * struck out, unreachable, reached-and-nothing-new — and none of it was
 * published, so the only reading available to an operator was "two thirds of
 * the fan-out vanished".
 *
 * The fix is not more counters. It is counters that ADD UP, checked by the
 * thing that publishes them:
 *
 * ```
 * discovered = foldedOntoAnother + taken
 * taken      = delivered + nothingNew + unreachable + transferFailed
 *            + noRoute + hostStruckOut + torUnavailable + pending
 * ```
 *
 * [pending] is what closes the second identity WHILE THE CYCLE RUNS. It is
 * derived, never incremented: whatever has not yet landed in one of the seven
 * terminal outcomes is still in flight, and publishing it as its own member is
 * the difference between a partition a reader can check and a bag of numbers
 * that happens not to sum today. When the cycle ends, `pending` is 0 — and if
 * it is not, [balanced] is false and the document says so rather than letting a
 * miscount pass as a disposition.
 *
 * ## One outcome per url, at most once
 *
 * Every counter here is incremented from exactly one place in `DynamicSync`'s
 * fan-out, on the path that ends that url's participation. The class does not
 * enforce it — a `CycleTally` cannot see the loop — which is precisely why
 * [balanced] is published beside the numbers instead of asserted in a comment.
 *
 * ## Hosts, because urls are not servers
 *
 * 3,272 discovered urls resolved to 850 hosts in the same run: most relay
 * software answers on every path, so a relay list can mint `wss://nos.lol/x`
 * without limit and every count taken over urls is inflated by whatever the
 * fold has not yet decided. [hosts] is the distinct authority count over the
 * urls actually taken, published beside them so the inflation is visible rather
 * than inferred. It is NOT the fold's answer — the fold measures identity and
 * runs on its own clock; this is arithmetic over strings, available immediately
 * and honest about being that.
 */
class CycleTally(
    /** Every url discovery handed this cycle, before anything was dropped. */
    val discovered: Int,
    /** …of those, the ones an alias verdict folded onto another url, never dialled. */
    val foldedOntoAnother: Int,
    /** Distinct authorities among the urls taken — see the class header. */
    val hosts: Int,
) {
    /** The urls this cycle is actually responsible for. */
    val taken: Int get() = (discovered - foldedOntoAnother).coerceAtLeast(0)

    /** Reached it, and it had something we did not. */
    val delivered = AtomicLong()

    /** Reached it, and we were already in sync — a working relay with nothing to give. */
    val nothingNew = AtomicLong()

    /** Never answered: the finding NIP-66 exists for, and the only one published. */
    val unreachable = AtomicLong()

    /** Answered the handshake, then the transfer broke. Not a claim about the relay. */
    val transferFailed = AtomicLong()

    /** The TCP pre-probe was refused or the name did not resolve, so no websocket was opened. */
    val noRoute = AtomicLong()

    /** A sibling url on the same authority struck it out while this one waited for a slot. */
    val hostStruckOut = AtomicLong()

    /** OUR Tor proxy was not answering. A fact about this container, not about their server. */
    val torUnavailable = AtomicLong()

    /** Events this cycle received from upstreams — see [SyncProgress] on what that counts. */
    val received = AtomicLong()

    /** The urls that have reached a terminal outcome. */
    fun settled(): Long =
        delivered.get() + nothingNew.get() + unreachable.get() + transferFailed.get() +
            noRoute.get() + hostStruckOut.get() + torUnavailable.get()

    /**
     * Still in flight — derived, so the seven outcomes plus this one always
     * cover [taken] exactly.
     */
    fun pending(): Long = (taken - settled()).coerceAtLeast(0)

    /**
     * Whether the partition actually holds.
     *
     * Published, not asserted. A double-counted url makes `settled` exceed
     * `taken`, `pending` clamps to 0 to keep a reader's arithmetic from going
     * negative, and this is then the only thing that says the numbers are
     * wrong — which is a far better failure than three panels quietly
     * disagreeing.
     */
    fun balanced(): Boolean = settled() <= taken && foldedOntoAnother <= discovered
}
