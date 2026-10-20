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

import com.nosfabrica.vespa.relay.ingest.MirrorDelete
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.OkMessage
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.EventCommandHandler
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext

/**
 * Answers the sync process's [MirrorDelete] requests: the retraction audit's deletes, run here
 * because the relay is the only writer. Every other event passes through untouched. The request
 * is consumed (answered, never stored or fanned out), and [purge] runs before the `OK`, so an
 * `OK true` means the events are gone.
 */
class MirrorDeleteHandler(
    private val relayPubkey: HexKey,
    private val purge: suspend (Filter) -> Unit,
    private val nowSeconds: () -> Long = { System.currentTimeMillis() / 1000 },
) : EventCommandHandler {
    override suspend fun handle(
        event: Event,
        ctx: RequestContext,
    ): OkMessage? {
        if (event.kind != MirrorDelete.KIND) return null
        val filter = MirrorDelete.parse(event, relayPubkey, nowSeconds()).getOrElse { return OkMessage(event.id, false, "restricted: ${it.message}") }
        purge(filter)
        return OkMessage(event.id, true, "")
    }
}
