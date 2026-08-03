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
package com.nosfabrica.vespa.relay

import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import java.util.concurrent.atomic.AtomicInteger

/**
 * A minimal observability hook: tracks the live websocket connection count and
 * (when [log] is on) prints it on each connect/disconnect. [count] is readable
 * for tests or a future metrics endpoint. This replaces the default
 * [RelayServerListener.None] only when the operator opts in (`LOG_CONNECTIONS`).
 */
class ConnectionCountListener(
    private val log: Boolean = true,
) : RelayServerListener {
    private val active = AtomicInteger(0)

    override fun onConnect(connectionId: Long) {
        val now = active.incrementAndGet()
        if (log) println("relay connections: $now (+)")
    }

    override fun onDisconnect(connectionId: Long) {
        val now = active.decrementAndGet()
        if (log) println("relay connections: $now (-)")
    }

    fun count(): Int = active.get()
}
