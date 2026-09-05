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
package com.nosfabrica.vespa.relay.web

import com.vitorpamplona.quartz.nip98HttpAuth.Nip98AuthVerifier
import java.security.SecureRandom
import java.util.Base64

/**
 * WHO MAY READ AN ADMIN DOCUMENT. One verdict per request, so a route can tell
 * "you sent nothing" from "you sent something broken" from "you are not an
 * administrator here" — three different HTTP answers, and collapsing them
 * makes a misconfiguration indistinguishable from an attack.
 */
sealed interface Admitted {
    /** Proven to be one of the configured administrators. */
    class Admin(
        val pubkey: String,
    ) : Admitted

    /** No credentials at all — the ordinary first request from a browser. */
    data object NoCredentials : Admitted

    /** Credentials that did not verify: bad signature, wrong url, expired, replayed. */
    class BadCredentials(
        val reason: String,
    ) : Admitted

    /** Credentials that verified, for somebody who is not an administrator. */
    class NotAdmin(
        val pubkey: String,
    ) : Admitted
}

/** Decides [Admitted] for one request's `Authorization` header. */
fun interface AdminGate {
    suspend fun admit(
        authorization: String?,
        method: String,
        url: String,
    ): Admitted
}

/**
 * The relay's existing definition of an administrator, applied to a GET.
 *
 * NIP-98 AND `RELAY_ADMIN_PUBKEYS` — the same proof and the same list the
 * NIP-86 admin RPC already uses, deliberately, rather than a second
 * credential. One place to add or remove an operator, one thing to leak, and
 * the audit question ("who can read this?") has the same answer as it does for
 * "who can ban a pubkey?".
 *
 * What quartz's verifier checks: the event is kind 27235, signed, recent
 * within tolerance, its `method` tag matches, its `u` tag matches [publicUrl]
 * plus the route's path, and its id has not been seen before. That last one
 * matters here more than it does for NIP-86: this page polls, and a token that
 * could be replayed would be a bearer credential with none of the properties
 * of one.
 *
 * BECAUSE THE TOKEN IS SINGLE-USE, a browser cannot poll with it — it would
 * need a signature per request, which means an extension popup every two
 * seconds. [AdminSessions] is the other half: one signature opens a session,
 * the session carries the polls. A script that would rather sign every request
 * may still do so; both paths end here. Such a script must not sign twice
 * within one second for the same url and method: a NIP-98 event carries no
 * nonce, so those two tokens are the same event with the same id, and the
 * second is a replay.
 *
 * [publicUrl] IS NOT TAKEN FROM THE REQUEST. The `u` tag exists so a token
 * stolen from one service cannot be spent at another, which it cannot do if
 * the server derives the expected url from a header the caller controls. It is
 * an operator setting, and the 401 says what it is so a signer can match it
 * without guessing.
 */
class Nip98AdminGate(
    /** The administrators, 64-hex. EMPTY ADMITS NOBODY — see [PulseGuard]. */
    private val admins: Set<String>,
    /** Origin the tokens are signed against, no trailing slash (e.g. `http://localhost:7780`). */
    val publicUrl: String,
    private val verifier: Nip98AuthVerifier = Nip98AuthVerifier(),
) : AdminGate {
    /** The `u` a token for [path] must carry. Published in the 401 so a signer never has to guess. */
    fun urlFor(path: String): String = publicUrl.trimEnd('/') + path

    /**
     * Whether this deployment is reached over TLS, and so whether the session
     * cookie may be marked `Secure`.
     *
     * From the operator's declared origin rather than the request, because a
     * request's scheme here is the local socket's — this site installs no
     * forwarded-headers plugin, on purpose — so behind a TLS-terminating proxy
     * the request reads `http` and the cookie would go out unmarked in exactly
     * the deployment where the mark matters.
     */
    val servesOverTls: Boolean = publicUrl.startsWith("https://", ignoreCase = true)

    override suspend fun admit(
        authorization: String?,
        method: String,
        url: String,
    ): Admitted {
        if (authorization.isNullOrBlank()) return Admitted.NoCredentials
        // No body: this gate protects GETs and a bodyless POST, so there is no
        // payload hash to bind. A token that carries one still verifies —
        // quartz compares it only against a body it was given.
        return when (val r = verifier.verify(authorization, method, url, null)) {
            is Nip98AuthVerifier.Result.Verified -> {
                if (r.pubkey in admins) Admitted.Admin(r.pubkey) else Admitted.NotAdmin(r.pubkey)
            }

            is Nip98AuthVerifier.Result.Malformed -> {
                Admitted.BadCredentials(r.reason)
            }

            is Nip98AuthVerifier.Result.Missing -> {
                Admitted.NoCredentials
            }

            else -> {
                Admitted.BadCredentials("unrecognised authorization")
            }
        }
    }
}

/**
 * The sessions one NIP-98 signature opens, so a polling page does not need a
 * signature per poll.
 *
 * IN MEMORY, ON PURPOSE. A restart signs everyone out, which is the right
 * default for a page that exists to be read during an incident: no session
 * outlives the process whose counters it was reading, there is no signing key
 * to configure or leak, and nothing about who looked at this page is written
 * to disk.
 *
 * FIXED EXPIRY, NOT SLIDING. A tab left open overnight stops being an open
 * door at [ttlMillis] whatever it was doing; renewing costs one more signature.
 *
 * The token is 256 bits from [SecureRandom] and is never logged. Bounded at
 * [max] live sessions, oldest evicted, so a signer that opens sessions in a
 * loop cannot grow this without bound.
 */
class AdminSessions(
    val ttlMillis: Long = 30 * 60_000L,
    private val max: Int = 64,
    private val now: () -> Long = System::currentTimeMillis,
) {
    private class Session(
        val pubkey: String,
        val expiresAt: Long,
    )

    // Access-ordered so eviction drops the least recently used, and synchronized
    // because Netty answers requests on many threads.
    private val live = LinkedHashMap<String, Session>(16, 0.75f, true)
    private val random = SecureRandom()

    /** A new session for [pubkey]; returns the token to put in the cookie. */
    @Synchronized
    fun open(pubkey: String): String {
        sweep()
        // `isNotEmpty` as well as the bound: a `max` of zero or less would
        // otherwise spin on an empty map and throw out of a route that is
        // supposed to be handing somebody a session.
        while (live.size >= max && live.isNotEmpty()) live.remove(live.keys.first())
        val token = ByteArray(32).also { random.nextBytes(it) }.let { Base64.getUrlEncoder().withoutPadding().encodeToString(it) }
        live[token] = Session(pubkey, now() + ttlMillis)
        return token
    }

    /** Whose session this is, or null when there is none, it expired, or the token is not one of ours. */
    @Synchronized
    fun holder(token: String?): String? {
        val t = token?.takeIf { it.isNotEmpty() } ?: return null
        val s = live[t] ?: return null
        if (s.expiresAt <= now()) {
            live.remove(t)
            return null
        }
        return s.pubkey
    }

    /** End one session. Unknown tokens are a no-op — a logout must never report whether a token existed. */
    @Synchronized
    fun close(token: String?) {
        token?.let { live.remove(it) }
    }

    /** Live sessions, for a test and for a status line. Never the tokens themselves. */
    @Synchronized
    fun size(): Int {
        sweep()
        return live.size
    }

    private fun sweep() {
        val at = now()
        live.entries.removeAll { it.value.expiresAt <= at }
    }
}
