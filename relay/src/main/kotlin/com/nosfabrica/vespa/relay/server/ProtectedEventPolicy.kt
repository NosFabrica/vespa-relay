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

import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix.AUTH_REQUIRED
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix.RESTRICTED
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.EventCmd
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PassThroughPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult
import com.vitorpamplona.quartz.nip70ProtectedEvts.isProtected

/**
 * NIP-70: an event tagged `["-"]` is accepted only from a connection signed in as its author. The
 * challenge is [MultiAddressAuthPolicy]'s, sent on connect. Remove when quartz's relay server enforces NIP-70.
 */
class ProtectedEventPolicy : PassThroughPolicy() {
    /** This connection's context; `@Volatile` because the EVENT can land on another coroutine. */
    @Volatile
    private var scope: RequestContext? = null

    override fun onConnect(
        scope: RequestContext,
        send: (Message) -> Unit,
    ) {
        this.scope = scope
    }

    override fun accept(cmd: EventCmd): PolicyResult<EventCmd> {
        if (!cmd.event.isProtected()) return PolicyResult.Accepted(cmd)
        // Read live: the AUTH may land after the connection opened.
        val signedIn = scope?.authenticatedUsers.orEmpty()
        return when {
            cmd.event.pubKey in signedIn -> PolicyResult.Accepted(cmd)
            signedIn.isEmpty() -> PolicyResult.Rejected(NOT_SIGNED_IN)
            else -> PolicyResult.Rejected(NOT_THE_AUTHOR)
        }
    }

    companion object {
        /** NIP-70's own sentence. */
        internal val NOT_SIGNED_IN = AUTH_REQUIRED.format("this event may only be published by its author")

        /** NIP-42's `restricted:` (signed in, not allowed), which a mirror reads as final rather than retrying. */
        internal val NOT_THE_AUTHOR = RESTRICTED.format("this event may only be published by its author; sign in as its author to publish it")
    }
}
