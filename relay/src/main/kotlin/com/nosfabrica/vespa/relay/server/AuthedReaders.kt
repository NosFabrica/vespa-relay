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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

/**
 * WHO IS SIGNED IN RIGHT NOW — the relay's half of the presence sync mode.
 *
 * The router mirrors on a schedule over a relay list read out of the store,
 * which is the right shape for a corpus and the wrong one for a person: a
 * reader who signs in wants their own outbox and their own trust provider
 * pulled *now*, and nothing about a six-hour discovery cycle can express that.
 * The set of people to pull for is only knowable HERE — a NIP-42 AUTH lands on
 * a websocket this process owns — and the mirror is a different process, so the
 * set crosses over HTTP the way [ServingPressure] already does. See
 * `router/presence/PresenceSync` for the other end.
 *
 * **Presence is a property of the CONNECTION, not of the identity.** Quartz
 * accepts an AUTH frame for its whole ten-minute freshness window and a client
 * may replay it, so counting logins would leak; and an identity signed in on
 * two sockets must not disappear when one of them closes. So the state is a
 * connection → identities map, `signedIn` adds and the engine's own
 * `onDisconnect` removes the whole connection, which is why this is a
 * [RelayServerListener] rather than something the auth policy alone drives.
 *
 * **The token is not decoration and is why this class holds one.** Every other
 * document this relay serves is public because it names no client:
 * `/pressure` is a mean latency, `/stats.json` is a fact about stored events —
 * and `corpusStats`' KDoc records the rule that nothing about clients goes in
 * it. This is a list of the people currently reading, which is exactly the
 * thing that rule protects. There is no consumer for it but our own sync
 * process, so the credential is a constructor argument: a registry with no
 * token cannot be built, and `RelayMain` builds one only when the operator set
 * `RELAY_AUTHED_TOKEN`. Unset, no route is mounted and nothing is tracked.
 */
class AuthedReaders(
    /**
     * The shared secret the sync process presents as `Authorization: Bearer …`.
     *
     * Compared with [MessageDigest.isEqual] over the raw bytes rather than with
     * `==`: string equality on the JVM returns at the first differing character,
     * and a caller who can time the answer can walk a secret out of it one byte
     * at a time. Cheap to do right, so it is done right.
     */
    private val token: String,
) : RelayServerListener {
    init {
        require(token.isNotBlank()) { "AuthedReaders needs a non-blank token — it serves a list of the people currently reading." }
    }

    /**
     * connection id → the identities that authenticated on it.
     *
     * More than one is ordinary rather than exotic: an AUTH frame can name
     * several relays and a client may sign in as a second key without
     * reconnecting, and quartz's own `RequestContext.authenticatedUsers` is a
     * set for the same reason.
     *
     * The value is an immutable set replaced under `compute`, so a reader
     * iterating this map during a login never sees a set being mutated — the
     * snapshot below runs on the HTTP thread while logins land on transport
     * ones.
     */
    private val connections = ConcurrentHashMap<Long, Set<HexKey>>()

    /**
     * Record a verified NIP-42 AUTH. Called from the auth policy's `authorize`
     * hook, which runs in front of the client's `OK`, so it does nothing that
     * can block or throw.
     */
    fun signedIn(
        connectionId: Long,
        pubkey: HexKey,
    ) {
        connections.compute(connectionId) { _, held -> if (held == null) setOf(pubkey) else held + pubkey }
    }

    /** The engine's own disconnect. One removal takes every identity on that socket with it. */
    override fun onDisconnect(connectionId: Long) {
        connections.remove(connectionId)
    }

    /** Connections currently holding at least one verified identity. */
    fun connectionCount(): Int = connections.size

    /**
     * Everyone signed in right now, oldest connection first, bounded.
     *
     * ORDER IS THE CONNECTION ID, which quartz mints from one process-wide
     * `AtomicLong` — so it is the order people arrived, and an identity holding
     * two sockets sorts at the older one. That matters only because of the
     * bound: [MAX_READERS] has to cut somewhere, and cutting the NEWEST arrivals
     * means a burst of logins cannot displace the subscriptions the mirror
     * already holds for people who have been here. Sorting by pubkey would have
     * been stable too and would have made who gets mirrored a property of their
     * key, which is worse.
     *
     * The bound exists because this is a JSON body on a poll: presence is
     * unbounded in principle (every socket may authenticate) and the mirror's
     * cost is per reader, so a relay that suddenly has fifty thousand signed-in
     * readers must produce a large steady response and not an enormous one.
     * What is left out is COUNTED rather than dropped silently — the same rule
     * `inFlight` follows — so the far end can say "the feed is truncated"
     * instead of quietly mirroring for a subset it cannot name.
     */
    fun snapshot(): Snapshot {
        // Min connection id per identity: one pass, no second map to keep in
        // step with this one.
        val firstSeen = HashMap<HexKey, Long>()
        connections.forEach { (id, keys) ->
            for (key in keys) {
                val held = firstSeen[key]
                if (held == null || id < held) firstSeen[key] = id
            }
        }
        val ordered = firstSeen.entries.sortedBy { it.value }.map { it.key }
        return Snapshot(
            pubkeys = ordered.take(MAX_READERS),
            omitted = (ordered.size - MAX_READERS).coerceAtLeast(0),
        )
    }

    /**
     * What [snapshot] found: who is here, and how many did not fit.
     *
     * [omitted] is published even at zero. A reader of this document has to be
     * able to tell "nobody was left out" from "this build does not say", and
     * only the presence of the member does that.
     */
    class Snapshot(
        val pubkeys: List<HexKey>,
        val omitted: Int,
    ) {
        fun toJson(): String =
            buildString {
                append("""{"pubkeys":[""")
                pubkeys.forEachIndexed { i, key ->
                    if (i > 0) append(',')
                    append('"').append(key).append('"')
                }
                append("""],"count":""").append(pubkeys.size)
                append(""","omitted":""").append(omitted).append('}')
            }
    }

    /**
     * Does this `Authorization` header carry our token?
     *
     * A missing or malformed header is a plain no, with the same answer and the
     * same cost as a wrong token: the route replies 401 either way and says
     * nothing about which, because "your header was the right shape" is a fact
     * worth nothing to the sync process and worth something to everyone else.
     */
    fun authorizes(header: String?): Boolean {
        val presented = header?.removePrefix(BEARER)?.takeIf { it != header } ?: return false
        val a = presented.toByteArray(Charsets.UTF_8)
        val b = token.toByteArray(Charsets.UTF_8)
        return MessageDigest.isEqual(a, b)
    }

    companion object {
        private const val BEARER = "Bearer "

        /**
         * The most identities one response names.
         *
         * Sized for the case this exists for — a search relay's signed-in
         * readership, which is people rather than crawlers — with room to spare,
         * and small enough that the body stays well under a megabyte of hex.
         * Reached, it is reported through `omitted`, never hidden.
         */
        const val MAX_READERS = 5_000
    }
}
