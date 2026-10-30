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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.OptionalAuthPolicy
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import kotlinx.coroutines.CancellationException

/**
 * NIP-42 for a relay that answers at more than one address: quartz's [OptionalAuthPolicy] over
 * every address, so an AUTH whose `relay` tags name any of them is accepted, and a sign-in is
 * told to [onAuthenticated] over the connection.
 */
class MultiAddressAuthPolicy(
    primary: NormalizedRelayUrl,
    alsoAt: Set<NormalizedRelayUrl> = emptySet(),
    /** Told who just signed in and how to reach them. */
    private val onAuthenticated: AuthNotifier? = null,
) : OptionalAuthPolicy(linkedSetOf(primary) + alsoAt) {
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
        // A client can mint keys without end; past the bound the courtesy stops rather than the set growing.
        if (!synchronized(told) { told.size < MAX_TOLD && told.add(event.pubKey) }) return
        try {
            notify(event.pubKey, ::sendToClient)
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

    companion object {
        /** Identities one connection is told about; real clients sign in as one or two. */
        internal const val MAX_TOLD = 16
    }
}
