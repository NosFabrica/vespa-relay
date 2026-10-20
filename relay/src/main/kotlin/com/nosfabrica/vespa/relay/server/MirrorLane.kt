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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.EventCmd
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PassThroughPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import java.util.concurrent.ConcurrentHashMap

/**
 * The relay's second write lane, for what the sync process forwards (`SYNC_INGEST=relay`).
 *
 * The sync process signs in over NIP-42 with `RELAY_NSEC`, the relay's own key, and publishes
 * what it mirrors as ordinary EVENTs. They pass the same policy stack as any client's, by design:
 * a mirrored event meets the same bans, allowlists and limits as a published one. What the lane
 * changes is only WHERE an accepted event queues: in [ingest], a pipeline of its own, so a
 * backfill filling its queue never makes a client's publish wait for room, and its yield to
 * `ServingPressure` happens in this process. Both lanes end in the one store and the one
 * `LiveEventStore`, so a mirrored event is fanned out to live subscribers like any other.
 *
 * Quartz hands the store an EVENT without its connection, so the lane is chosen by a tag: [policy]
 * runs per connection, LAST in the stack, and tags an accepted event's id when its connection
 * has signed in as [relayPubkey]; [LanedIngest] claims the tag on the submit that follows, in the
 * same coroutine. A tag is claimed exactly once, since `LiveEventStore` hands every event to its
 * ingest. An EVENT sent before the AUTH completes simply takes the client lane: same rules,
 * another queue.
 */
class MirrorLane(
    private val relayPubkey: HexKey,
    /** The mirror's own pipeline. */
    val ingest: EventIngest,
) {
    private val tagged: MutableSet<String> = ConcurrentHashMap.newKeySet()

    /** A fresh per-connection tagging policy; it must be the stack's last, so a tag means accepted. */
    fun policy(): IRelayPolicy = Tagger()

    /** True once per tagged event: whether [event] goes to [ingest]. */
    internal fun claim(event: Event): Boolean = tagged.remove(event.id)

    /** [ingest] for what this lane tagged, [clients] for everything else. */
    fun over(clients: EventIngest): EventIngest = LanedIngest(clients, this)

    private inner class Tagger : PassThroughPolicy() {
        @Volatile private var connection: RequestContext? = null

        override fun onConnect(
            scope: RequestContext,
            send: (Message) -> Unit,
        ) {
            connection = scope
        }

        override fun accept(cmd: EventCmd): PolicyResult<EventCmd> {
            // Read live: the AUTH may land after the connection opened.
            if (connection?.authenticatedUsers?.contains(relayPubkey) == true) tagged.add(cmd.event.id)
            return PolicyResult.Accepted(cmd)
        }
    }
}

/** Routes a submit to the mirror lane when [MirrorLane.policy] tagged it, else to [clients]. */
private class LanedIngest(
    private val clients: EventIngest,
    private val lane: MirrorLane,
) : EventIngest {
    override suspend fun submit(
        event: Event,
        skipVerify: Boolean,
        onComplete: (IEventStore.InsertOutcome) -> Unit,
    ) = (if (lane.claim(event)) lane.ingest else clients).submit(event, skipVerify, onComplete)

    override fun hasBacklog(): Boolean = clients.hasBacklog() || lane.ingest.hasBacklog()

    override fun close() {
        clients.close()
        lane.ingest.close()
    }
}
