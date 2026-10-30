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

import com.nosfabrica.vespa.relay.util.withoutDefaultPort
import com.vitorpamplona.quartz.nip01Core.core.HexKey
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
 * NIP-42 for a relay that answers at more than one address. Quartz's [OptionalAuthPolicy] binds
 * one url, so its check is restated: any address satisfies the relay tag, every `relay` tag is
 * considered, and a scheme's default port folds. The other conditions are quartz's, pinned by test.
 * Remove the restated check when quartz's `FullAuthPolicy` takes a set of addresses.
 */
class MultiAddressAuthPolicy(
    primary: NormalizedRelayUrl,
    alsoAt: Set<NormalizedRelayUrl> = emptySet(),
    /** Told who just signed in and how to reach them. */
    private val onAuthenticated: AuthNotifier? = null,
) : OptionalAuthPolicy(primary) {
    /** Never empty: [primary] is always in it. */
    private val addresses = (alsoAt + primary).mapTo(HashSet(), NormalizedRelayUrl::withoutDefaultPort)

    /** This connection's channel to the client; `@Volatile` because the AUTH can land on another coroutine. */
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
     * Identities already told: a client may resend a still-valid AUTH any number of times, and the
     * notice is paid once per identity per connection, for at most [MAX_TOLD] identities.
     */
    private val told = HashSet<String>()

    /**
     * Quartz's post-verification hook, before the `OK` goes out. Must not block (the client waits
     * on the `OK`) and must not throw (quartz reads a throw as a failed login).
     */
    override suspend fun authorize(event: RelayAuthEvent) {
        val notify = onAuthenticated ?: return
        val send = send ?: return
        // A client can mint keys without end; past the bound the courtesy stops rather than the set growing.
        if (!synchronized(told) { told.size < MAX_TOLD && told.add(event.pubKey) }) return
        try {
            notify(event.pubKey, send)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            println("auth: post-login notice failed for ${event.pubKey.take(8)}…: ${e.message}")
        }
    }

    /**
     * A key a NIP-98 header proved (NIP-FE) signs the session in as a NIP-42 AUTH would. No notice:
     * over HTTP a NOTICE frame would end the answer, and a one-command session has no later line for it.
     */
    override suspend fun authorizeTransport(pubkey: HexKey): String? = null

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

    companion object {
        /** Identities one connection is told about; real clients sign in as one or two. */
        internal const val MAX_TOLD = 16
    }
}
