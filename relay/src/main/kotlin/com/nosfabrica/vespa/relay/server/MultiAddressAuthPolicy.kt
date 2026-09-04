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

import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.AuthCmd
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.OptionalAuthPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult
import com.vitorpamplona.quartz.nip40Expiration.isExpired
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import com.vitorpamplona.quartz.nip42RelayAuth.tags.RelayTag
import com.vitorpamplona.quartz.utils.TimeUtils
import kotlinx.coroutines.CancellationException

/**
 * NIP-42 for a relay that answers at more than one address: the clearnet url
 * and a `.onion` in front of the same port.
 *
 * A client signs the address it dialled. Quartz's [OptionalAuthPolicy] binds
 * one url, and on this relay a failed AUTH is not a locked door but a lost
 * ranking lens, so the hidden service would look fine and rank nothing. The
 * check is restated rather than delegated because the challenge is minted
 * per policy instance; the other conditions are quartz's, in quartz's order,
 * with quartz's messages, pinned by `RelayOnionAuthTest`.
 *
 * Three widenings: any of the addresses satisfies the relay tag; every
 * `relay` tag is considered, safe because [challenge] is per connection; and
 * a scheme's default port folds on both sides, since a hidden service is
 * published on port 80. It is also where a verified AUTH becomes
 * [onAuthenticated].
 */
class MultiAddressAuthPolicy(
    primary: NormalizedRelayUrl,
    alsoAt: Set<NormalizedRelayUrl> = emptySet(),
    /** Told who just signed in and how to reach them. See [TrustNotice]. */
    private val onAuthenticated: AuthNotifier? = null,
) : OptionalAuthPolicy(primary) {
    /** Never empty: [primary] is always in it. */
    private val addresses = (alsoAt + primary).mapTo(HashSet(), NormalizedRelayUrl::withoutDefaultPort)

    /**
     * This connection's channel to the client. A policy is built per
     * connection; `@Volatile` because the AUTH can land on a different
     * coroutine than the connect.
     */
    @Volatile
    private var send: ((Message) -> Unit)? = null

    override fun onConnect(
        scope: RequestContext,
        send: (Message) -> Unit,
    ) {
        // Super first: it sends the challenge, without which [authorize] is never reached.
        super.onConnect(scope, send)
        this.send = send
    }

    /**
     * Identities already told. An AUTH frame stays valid for its whole
     * freshness window, so a client may resend it any number of times; the
     * answer is a property of the identity, paid once per connection.
     */
    private val told = HashSet<String>()

    /**
     * Quartz's post-verification hook, before the `OK` goes out. It must not
     * block, since the client waits on the `OK`, and must not throw, since
     * quartz reads a throw as a failed login.
     */
    override suspend fun authorize(event: RelayAuthEvent) {
        val notify = onAuthenticated ?: return
        val send = send ?: return
        if (!synchronized(told) { told.add(event.pubKey) }) return
        try {
            notify(event.pubKey, send)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            println("auth: post-login notice failed for ${event.pubKey.take(8)}…: ${e.message}")
        }
    }

    override fun accept(cmd: AuthCmd): PolicyResult<AuthCmd> {
        val event = cmd.event

        if (event.isExpired()) {
            return PolicyResult.Rejected("invalid: auth event expired")
        }

        if (!TimeUtils.withinTenMinutes(event.createdAt)) {
            return PolicyResult.Rejected("invalid: created_at is too far from the current time")
        }

        if (event.challenge() != challenge) {
            return PolicyResult.Rejected("invalid: challenge does not match")
        }

        if (event.tags.mapNotNull(RelayTag::parse).none { it.withoutDefaultPort() in addresses }) {
            return PolicyResult.Rejected("invalid: relay url does not match")
        }

        return PolicyResult.Accepted(cmd)
    }
}

/**
 * `ws://host:80/` and `ws://host/` name one endpoint, likewise `wss` and 443.
 * Quartz's normalizer keeps an explicit default port, so the spellings
 * compare unequal. The port only ever follows a bracketed IPv6 host's
 * closing bracket, so `[::1]` survives.
 */
private fun NormalizedRelayUrl.withoutDefaultPort(): NormalizedRelayUrl {
    val port =
        when {
            url.startsWith("ws://") -> ":80"
            url.startsWith("wss://") -> ":443"
            else -> return this
        }
    val authorityStart = url.indexOf("://") + 3
    val slash = url.indexOf('/', authorityStart)
    val authorityEnd = if (slash < 0) url.length else slash
    val portStart = authorityEnd - port.length
    if (portStart <= authorityStart || !url.regionMatches(portStart, port, 0, port.length)) return this
    return NormalizedRelayUrl(url.removeRange(portStart, authorityEnd))
}
