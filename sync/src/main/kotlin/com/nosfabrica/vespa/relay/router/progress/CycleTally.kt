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
 * discovered = foldedOntoAnother + excluded + taken
 * taken      = delivered + nothingNew + unreachable + transferFailed
 *            + noRoute + hostStruckOut + knownDead + torUnavailable + pending
 * ```
 *
 * [pending] is what closes the second identity WHILE THE CYCLE RUNS. It is
 * derived, never incremented: whatever has not yet landed in one of the eight
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
    /**
     * …and the ones dropped by config after the fold — a stream's `exclude`
     * list, or this relay's own url, which is in plenty of other people's relay
     * lists.
     *
     * Its own member rather than absorbed into the alias count, which is where
     * it went while that count was inferred from a subtraction. They are
     * different facts with different fixes: one is a duplicate url the router
     * worked out for itself, the other is an operator's instruction being
     * obeyed.
     */
    val excluded: Int = 0,
    /** Distinct authorities among the urls taken — see the class header. */
    val hosts: Int,
    /**
     * Which url folded onto which survivor, for the urls this cycle handled.
     * Summarised rather than published whole — see [foldedOnto].
     */
    private val folded: Map<String, String> = emptyMap(),
) {
    /** The urls this cycle is actually responsible for. */
    val taken: Int get() = (discovered - foldedOntoAnother - excluded).coerceAtLeast(0)

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

    /**
     * A sibling url on the same authority struck it out while this one waited
     * for a slot. Cycle-local: nothing about a strike persists, so this url is
     * dialled again on the next cycle.
     */
    val hostStruckOut = AtomicLong()

    /**
     * Not dialled because an EARLIER run published a signed NIP-66
     * unreachability record for it that is still within its TTL (24h by
     * quartz's default), so this cycle skipped it without asking.
     *
     * Separate from [hostStruckOut] because the two answer "will it try again,
     * and when" in opposite ways, and reported as one number they are exactly
     * as unreadable as the "skipped as dead" they replaced. This one comes back
     * when the record ages out — or immediately, if its host delivers anything.
     */
    val knownDead = AtomicLong()

    /** OUR Tor proxy was not answering. A fact about this container, not about their server. */
    val torUnavailable = AtomicLong()

    /** Events this cycle received from upstreams — see [SyncProgress] on what that counts. */
    val received = AtomicLong()

    /** The urls that have reached a terminal outcome. */
    fun settled(): Long =
        delivered.get() + nothingNew.get() + unreachable.get() + transferFailed.get() +
            noRoute.get() + hostStruckOut.get() + knownDead.get() + torUnavailable.get()

    /**
     * Still in flight — derived, so the eight outcomes plus this one always
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
    fun balanced(): Boolean = settled() <= taken && foldedOntoAnother + excluded <= discovered

    /**
     * WHICH urls were folded away, grouped by the survivor that absorbed them.
     *
     * The count alone answers "how many" and nothing else — and "which server
     * is wearing forty urls" is the question an operator actually has, because
     * it is the one they can do something about. The full list is not
     * publishable: 11,429 folded urls is roughly a megabyte on a document
     * fetched on every poll, the same reason a discovery filter's `authors`
     * never reaches `/stats.json`.
     *
     * So: the biggest [limit] survivors, a couple of examples each, and
     * `omitted` naming what was left out. NO SILENT CAP — a truncated list that
     * does not say it is truncated reads as the whole answer, and the whole
     * answer here is thousands of urls long.
     */
    fun foldedOnto(limit: Int = DEFAULT_FOLD_ROWS): FoldedSummary {
        val bySurvivor = folded.entries.groupBy({ it.value }, { it.key })
        // Ties broken by url so two rollups of the same state publish the same
        // document and a reader can diff them.
        val ranked =
            bySurvivor.entries.sortedWith(
                compareByDescending<Map.Entry<String, List<String>>> { it.value.size }.thenBy { it.key },
            )
        return FoldedSummary(
            onto = ranked.take(limit).map { (survivor, urls) -> Absorbed(survivor, urls.size, urls.sorted().take(DEFAULT_FOLD_EXAMPLES)) },
            omitted = (ranked.size - limit).coerceAtLeast(0),
        )
    }

    /** One survivor and the urls that folded onto it. */
    class Absorbed(
        val relay: String,
        val urls: Int,
        val examples: List<String>,
    )

    /** [foldedOnto]'s answer: the biggest survivors, and how many were left out. */
    class FoldedSummary(
        val onto: List<Absorbed>,
        val omitted: Int,
    )

    companion object {
        /**
         * How many survivors the fold summary names. Twenty covers the shape of
         * every distribution measured here — a few hosts wearing dozens of urls
         * each, then a long tail of ones and twos — without the list becoming a
         * table nobody scrolls.
         */
        const val DEFAULT_FOLD_ROWS = 20

        /** Enough to recognise the pattern (`/alpha`, `/beacon-glyph`) without listing it. */
        const val DEFAULT_FOLD_EXAMPLES = 2
    }
}
