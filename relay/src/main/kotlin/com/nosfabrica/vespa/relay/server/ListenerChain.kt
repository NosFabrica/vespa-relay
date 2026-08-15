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

import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener

/**
 * Every connection listener, behind the one slot quartz's engine has.
 *
 * `RelayServerBase` takes a single [RelayServerListener], which was enough
 * while [ConnectionCountListener] was the only one — a debug switch nobody else
 * competed for. [AuthedReaders] needs the same disconnect, and it is not a
 * debug switch: with one slot, turning on `LOG_CONNECTIONS` would have silently
 * unhooked presence and left the mirror holding subscriptions for readers who
 * had gone. That is the "configured component silently inert" failure in its
 * worst spelling, so the slot takes a list instead.
 *
 * A listener that throws is not allowed to cost its siblings their callback:
 * these run on the transport's own path, and one bad listener taking the others
 * down would be a connection accounting bug with no trace. Each is called in a
 * `runCatching`.
 */
internal class ListenerChain(
    private val listeners: List<RelayServerListener>,
) : RelayServerListener {
    override fun onConnect(connectionId: Long) {
        listeners.forEach { runCatching { it.onConnect(connectionId) } }
    }

    override fun onDisconnect(connectionId: Long) {
        listeners.forEach { runCatching { it.onDisconnect(connectionId) } }
    }

    companion object {
        /**
         * The one listener for [parts], skipping the nulls a composition root
         * produces for the switches nobody turned on.
         *
         * Zero and one are not wrapped: `None` is what the engine already
         * expects for "nothing installed", and a single listener wrapped in a
         * chain would only make its stack traces longer.
         */
        fun of(vararg parts: RelayServerListener?): RelayServerListener {
            val live = parts.filterNotNull()
            return when (live.size) {
                0 -> RelayServerListener.None
                1 -> live.first()
                else -> ListenerChain(live)
            }
        }
    }
}
