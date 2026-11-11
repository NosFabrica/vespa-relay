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
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.NoticeMessage
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PassThroughPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyStack
import java.util.concurrent.atomic.AtomicLong

/**
 * One connection's NOTICE that its results came from part of the index, sent at most once per
 * [everyMillis] however many partial reads it makes. A policy only to be handed the connection's send.
 */
class PartialNotice(
    private val everyMillis: Long = EVERY_MS,
    private val now: () -> Long = System::currentTimeMillis,
) : PassThroughPolicy() {
    @Volatile
    private var send: ((Message) -> Unit)? = null

    private val lastSent = AtomicLong(Long.MIN_VALUE)

    override fun onConnect(
        scope: RequestContext,
        send: (Message) -> Unit,
    ) {
        this.send = send
    }

    /** Sends the notice unless this connection was told within [everyMillis]. */
    fun tell() {
        val send = send ?: return
        val at = now()
        val last = lastSent.get()
        if (last != Long.MIN_VALUE && at - last < everyMillis) return
        if (lastSent.compareAndSet(last, at)) send(NoticeMessage(MESSAGE))
    }

    companion object {
        /** A feed client reads in bursts; one line per burst is enough. */
        const val EVERY_MS = 10 * 60 * 1000L

        internal const val MESSAGE = "partial: part of this relay's index is temporarily unreachable, so results may be incomplete"

        /** The connection's notice in [policy]: itself, or a member of a [PolicyStack], nested too (quartz stacks its limits on ours). */
        fun of(policy: IRelayPolicy): PartialNotice? = policy as? PartialNotice ?: (policy as? PolicyStack)?.policies?.firstNotNullOfOrNull(::of)
    }
}
