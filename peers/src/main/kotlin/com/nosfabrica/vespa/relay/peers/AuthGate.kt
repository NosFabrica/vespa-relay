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
package com.nosfabrica.vespa.relay.peers

import com.vitorpamplona.quartz.nip01Core.relay.client.INostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.client.listeners.RelayConnectionListener
import com.vitorpamplona.quartz.nip01Core.relay.client.single.IRelayClient
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.ClosedMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.OkMessage
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.signers.NostrSigner
import com.vitorpamplona.quartz.nip77Negentropy.NegErrMessage
import kotlinx.coroutines.CoroutineScope

/**
 * Which relays a [PeerClient] answers NIP-42 challenges on, decided from what each relay said when
 * it refused us. Without one the client answers every challenge, as the monitor's does.
 */
interface AuthGate {
    /** A refusal the gate reads: the relay's sentence, and whether what it refused was a read or an EVENT. */
    class Refusal(
        val said: String,
        /** A REQ, COUNT or NEG-OPEN was refused; a NOTICE, which names nothing, is taken as one. False for an EVENT's `OK`. */
        val ofRead: Boolean,
    )

    /** An `auth-required:` or `restricted:` refusal from [url], heard before the responder acts on it. */
    fun heard(
        url: NormalizedRelayUrl,
        refusal: Refusal,
    )

    /** Whether to sign [url]'s challenge. */
    fun mayAuthenticate(url: NormalizedRelayUrl): Boolean

    companion object {
        /** The refusal a message carries, when it is one an [AuthGate] reads. */
        fun refusalIn(msg: Message): Refusal? {
            val said =
                when (msg) {
                    is ClosedMessage -> msg.message
                    is NegErrMessage -> msg.reason
                    is OkMessage -> msg.message.takeUnless { msg.success }
                    is NoticeMessage -> msg.message
                    else -> null
                } ?: return null
            val prefix = MachineReadablePrefix.parse(said)
            if (prefix != MachineReadablePrefix.AUTH_REQUIRED && prefix != MachineReadablePrefix.RESTRICTED) return null
            return Refusal(said, ofRead = msg !is OkMessage)
        }
    }
}

/**
 * Hands [gate] every refusal, and signs in at once where one opened the gate on a refusal quartz's
 * responder does not re-authenticate on by itself (it does on a CLOSED). Registered on the client
 * before the responder, so the gate has decided by the time the responder asks it.
 */
internal class AuthGateListener(
    private val gate: AuthGate,
    private val reauthenticate: (NormalizedRelayUrl) -> Unit,
) : RelayConnectionListener {
    override suspend fun onIncomingMessage(
        relay: IRelayClient,
        msgStr: String,
        msg: Message,
    ) {
        val refusal = AuthGate.refusalIn(msg) ?: return
        val before = gate.mayAuthenticate(relay.url)
        gate.heard(relay.url, refusal)
        if (!before && msg !is ClosedMessage && gate.mayAuthenticate(relay.url)) reauthenticate(relay.url)
    }
}

/** [client]'s NIP-42 responder for [signer], answering only where [gate] lets it, and the listener feeding [gate]. */
class GatedResponder(
    client: INostrClient,
    scope: CoroutineScope,
    signer: NostrSigner,
    gate: AuthGate?,
) {
    // Registered before the responder, which reads the gate on the same refusal.
    val listener: RelayConnectionListener? = gate?.let { g -> AuthGateListener(g) { url -> authenticator.reauthenticate(url) }.also(client::addConnectionListener) }

    val authenticator: RelayAuthenticator =
        RelayAuthenticator(client, scope) { url, template, _ -> if (gate?.mayAuthenticate(url) == false) emptyList() else listOf(signer.sign(template)) }
}
