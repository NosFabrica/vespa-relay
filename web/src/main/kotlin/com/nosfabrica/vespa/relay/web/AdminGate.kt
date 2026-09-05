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
 * Who may read an admin document, as one verdict per request, so a route can tell "sent
 * nothing" from "sent something broken" from "not an administrator here": three HTTP answers.
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
 * The relay's existing definition of an administrator, applied to a GET: a NIP-98 token from one
 * of `RELAY_ADMIN_PUBKEYS`, the same proof and list the NIP-86 rpc uses. Tokens are single-use,
 * so a polling page goes through [AdminSessions]; [publicUrl] is never derived from the request.
 */
class Nip98AdminGate(
    /** The administrators, 64-hex. Empty admits nobody. */
    private val admins: Set<String>,
    /** Origin the tokens are signed against, no trailing slash (e.g. `http://localhost:7780`). */
    val publicUrl: String,
    private val verifier: Nip98AuthVerifier = Nip98AuthVerifier(),
) : AdminGate {
    /** The `u` a token for [path] must carry. Published in the 401 so a signer never has to guess. */
    fun urlFor(path: String): String = publicUrl.trimEnd('/') + path

    /**
     * Whether the session cookie may be marked `Secure`, from the operator's declared origin: the
     * request's scheme is the local socket's, since this site installs no forwarded-headers plugin.
     */
    val servesOverTls: Boolean = publicUrl.startsWith("https://", ignoreCase = true)

    override suspend fun admit(
        authorization: String?,
        method: String,
        url: String,
    ): Admitted {
        if (authorization.isNullOrBlank()) return Admitted.NoCredentials
        // No body to bind: this gate protects GETs and a bodyless POST. A token carrying a payload
        // hash still verifies, since quartz compares it only against a body it was given.
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
 * The sessions one NIP-98 signature opens, so a polling page does not need a signature per
 * poll. In memory, so a restart signs everyone out and nothing about who read the page reaches
 * disk; fixed expiry at [ttlMillis], not sliding; bounded at [max] live sessions, oldest evicted.
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
        // `isNotEmpty` as well as the bound, or a `max` of zero would spin on an empty map.
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

    /** Live sessions, never the tokens themselves. */
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
