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
package com.nosfabrica.vespa.relay

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * Which relays are worth dialling, within one dynamic cycle.
 *
 * An outbox relay list on a mature store is five figures of urls and most of
 * them are corpses — measured here: 20,340 discovered, a few hundred that ever
 * answer. Every cycle re-dialled all of them, each burning a connect timeout
 * while holding a concurrency permit, so the working relays queued behind hosts
 * that had not existed for years.
 *
 * ## Why failures are counted per AUTHORITY, not per url
 *
 * The outbox model mints one url per user for a filtering relay —
 * `wss://filter.example/npub1aaa`, `/npub1bbb`, hundreds of them — all pointing
 * at one server. A per-url strike counter never reaches a threshold on any
 * single one, so a dead server stays dead forever and is re-tried in full every
 * cycle. Striking the authority (`host[:port]`) is what makes the count add up.
 *
 * The authority is host-only and deliberately does NOT fold a subdomain into its
 * parent: `filter.example` and `example` are different servers, and shedding one
 * must never take out the other.
 *
 * ## Ever-produced wins
 *
 * [produced] overrides [deadHosts] rather than merely clearing the strikes, and
 * the difference matters at this fan-out: with a hundred relays in flight, one
 * worker can push an authority over the threshold at the same instant another is
 * receiving events from it. A host that has ever delivered is never treated as
 * dead for the rest of the cycle, whichever way that race lands.
 *
 * This is behaviour-driven and cycle-local. Nothing here is persisted; see
 * [RelayReachability] for the part that survives a restart.
 */
class RelayHealth(
    private val strikeLimit: Int = DEFAULT_STRIKE_LIMIT,
    // Relays a previous run proved unreachable, and still within their TTL.
    private val knownDead: Set<NormalizedRelayUrl> = emptySet(),
) {
    private val strikes = ConcurrentHashMap<String, Int>()
    private val deadHosts = ConcurrentHashMap.newKeySet<String>()
    private val producedHosts = ConcurrentHashMap.newKeySet<String>()

    /** Relays this cycle actually got something from — worth remembering as live. */
    val reachable: MutableSet<NormalizedRelayUrl> = ConcurrentHashMap.newKeySet()

    /** Relays this cycle could not reach at all. */
    val unreachable: MutableSet<NormalizedRelayUrl> = ConcurrentHashMap.newKeySet()

    /**
     * Skip this relay? True when a previous run proved it dead (and no
     * [produced] since), or when its whole authority has been struck out here.
     */
    fun isDead(url: NormalizedRelayUrl): Boolean {
        val authority = authorityOf(url.url)
        if (authority in producedHosts) return false
        return url in knownDead || authority in deadHosts
    }

    /**
     * This relay connected but delivered nothing before giving up. Count it
     * against its authority and, at [strikeLimit], stop dialling the host.
     */
    fun strike(url: NormalizedRelayUrl): Evicted? {
        unreachable += url
        if (strikeLimit <= 0) return null
        val authority = authorityOf(url.url)
        if (authority in producedHosts || authority in deadHosts) return null
        if (strikes.merge(authority, 1, Int::plus)!! < strikeLimit) return null
        deadHosts += authority
        // The caller publishes this. Three separate urls on one host going
        // silent is a finding about the HOST, and it is the only finding we will
        // ever have about the thousands of sibling urls we now skip without
        // dialling — so a verdict kept private means the monitor says nothing
        // about the relays it just ruled out.
        return Evicted(authority, strikeLimit)
    }

    /** An authority struck out, and the evidence for it. */
    class Evicted(
        val authority: String,
        val strikes: Int,
    )

    /** This relay delivered. Its authority is alive, whatever else happened. */
    fun produced(url: NormalizedRelayUrl) {
        reachable += url
        unreachable -= url
        producedHosts += authorityOf(url.url)
    }

    /** For the cycle's closing line: what was skipped and why. */
    fun evictedHosts(): Int = deadHosts.size

    fun summary(total: Int): String =
        "${reachable.size} live, ${unreachable.size} unreachable, " +
            "${deadHosts.size} host(s) struck out, ${knownDead.size} skipped as known-dead of $total"

    companion object {
        /**
         * Strikes before an authority is dropped for the rest of the cycle. Three
         * because a single timeout is ordinary — a busy relay that never answered
         * one REQ is not a dead one — while three separate urls on the same host
         * all going silent is a server, not a coincidence.
         */
        const val DEFAULT_STRIKE_LIMIT = 3

        /**
         * `host[:port]` — everything between the ws/wss scheme and the first path
         * slash. The port is part of it: two ports on one machine are two relays.
         */
        fun authorityOf(url: String): String {
            val afterScheme =
                when {
                    url.startsWith("wss://") -> url.substring(6)
                    url.startsWith("ws://") -> url.substring(5)
                    else -> url
                }
            val slash = afterScheme.indexOf('/')
            return if (slash >= 0) afterScheme.substring(0, slash) else afterScheme
        }
    }
}
