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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.util.concurrent.ConcurrentHashMap

/**
 * Which relays are worth dialling within one dynamic cycle. Failures are counted per authority
 * (`host[:port]`), never per url, and a host that has delivered this cycle is never dead.
 * Nothing persists past the cycle.
 */
class HostStrikes(
    private val strikeLimit: Int = DEFAULT_STRIKE_LIMIT,
    // Relays a previous run proved unreachable, still within their TTL.
    private val knownDead: Set<NormalizedRelayUrl> = emptySet(),
) {
    private val strikes = ConcurrentHashMap<String, Int>()
    private val deadHosts = ConcurrentHashMap.newKeySet<String>()
    private val producedHosts = ConcurrentHashMap.newKeySet<String>()

    /** Relays this cycle got something from. */
    val reachable: MutableSet<NormalizedRelayUrl> = ConcurrentHashMap.newKeySet()

    /** Relays this cycle could not reach at all. */
    val unreachable: MutableSet<NormalizedRelayUrl> = ConcurrentHashMap.newKeySet()

    fun isDead(url: NormalizedRelayUrl): Boolean = whyDead(url) != null

    /**
     * Why this relay is being skipped, or null. [Skip.KNOWN_DEAD] lasts until our signed `dead`
     * verdict ages out, [Skip.STRUCK_OUT] only this cycle.
     */
    fun whyDead(url: NormalizedRelayUrl): Skip? {
        val authority = authorityOf(url.url)
        // A delivery this cycle outranks both verdicts.
        if (authority in producedHosts) return null
        // The durable reason first: it is the one with the longer reach.
        if (url in knownDead) return Skip.KNOWN_DEAD
        if (authority in deadHosts) return Skip.STRUCK_OUT
        return null
    }

    /** Why a url was not dialled. */
    enum class Skip {
        /** An earlier run's signed `dead` record, still within its TTL. */
        KNOWN_DEAD,

        /** [strikeLimit] sibling urls on this authority went silent during this cycle. */
        STRUCK_OUT,
    }

    /**
     * This relay connected but delivered nothing. Returns the eviction, for the caller to
     * publish, only from the strike that took the host down.
     */
    fun strike(url: NormalizedRelayUrl): Evicted? {
        unreachable += url
        if (strikeLimit <= 0) return null
        val authority = authorityOf(url.url)
        if (authority in producedHosts || authority in deadHosts) return null
        if (strikes.merge(authority, 1, Int::plus)!! < strikeLimit) return null
        // add() decides which concurrent striker publishes; produced is re-checked after winning
        // because a delivery in between outranks a verdict that becomes a signed record.
        if (!deadHosts.add(authority)) return null
        if (authority in producedHosts) return null
        return Evicted(authority, strikeLimit)
    }

    /** An authority struck out, and the evidence for it. */
    class Evicted(
        val authority: String,
        val strikes: Int,
    )

    /** This relay delivered; its authority is alive. */
    fun produced(url: NormalizedRelayUrl) {
        reachable += url
        unreachable -= url
        producedHosts += authorityOf(url.url)
    }

    fun evictedHosts(): Int = deadHosts.size

    fun summary(total: Int): String =
        "${reachable.size} live, ${unreachable.size} unreachable, " +
            "${deadHosts.size} host(s) struck out, ${knownDead.size} skipped as known-dead of $total"

    companion object {
        const val DEFAULT_STRIKE_LIMIT = 3

        /** `host[:port]`, everything between the scheme and the first slash. Two ports are two relays. */
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
