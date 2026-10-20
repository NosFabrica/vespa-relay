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
package com.nosfabrica.vespa.relay.ingest

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.OptimizedJsonMapper
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.signers.EventTemplate
import kotlinx.coroutines.CancellationException
import kotlin.math.abs

/**
 * The sync process asking the relay to delete events, the one write it makes that no Nostr
 * command expresses: the retraction audit deletes what an upstream no longer holds, by id,
 * without the authors' NIP-09 requests. Once the relay is the only writer, the sync cannot run
 * that delete itself, so it sends this event on the socket it forwards its downloads on.
 *
 * An EPHEMERAL kind, so nothing can store it, signed by the relay's own key (`RELAY_NSEC`, which
 * both processes hold), whose content is the delete's filter as JSON. The relay answers it in
 * its event-command hook, before any policy, and only when [parse] accepts it: the right kind,
 * signed by the relay key, fresh, and naming explicit ids, at most [MAX_IDS], so a request can
 * never stand for the whole corpus.
 */
object MirrorDelete {
    /** Private to this deployment: no NIP assigns it. In the ephemeral range, 20000-29999. */
    const val KIND = 29_391

    /** The ids one request may name; the audit deletes in chunks of fewer. */
    const val MAX_IDS = 1_000

    /** How far a request's clock may stand from the relay's, as NIP-98 allows its tokens. */
    const val FRESH_SECONDS = 60L

    /** The unsigned request for [filter], stamped [createdAt]. */
    fun template(
        filter: Filter,
        createdAt: Long = System.currentTimeMillis() / 1000,
    ): EventTemplate<Event> = EventTemplate(createdAt, KIND, emptyArray(), filter.toJson())

    /**
     * The filter a request asks to delete, or the reason it is refused. [relayPubkey] is the only
     * signer accepted; the signature is checked here, since the relay's hook runs ahead of the
     * policy chain that would otherwise check it.
     */
    fun parse(
        event: Event,
        relayPubkey: String,
        nowSeconds: Long = System.currentTimeMillis() / 1000,
    ): Result<Filter> {
        if (event.kind != KIND) return refused("not a mirror delete request")
        if (event.pubKey != relayPubkey) return refused("only this relay's own key may ask it to delete")
        if (abs(nowSeconds - event.createdAt) > FRESH_SECONDS) return refused("the request is not fresh")
        if (!event.verify()) return refused("bad event signature or id")
        val filter =
            try {
                (OptimizedJsonMapper.fromJsonToCommand("""["REQ","delete",${event.content}]""") as? ReqCmd)?.filters?.singleOrNull()
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                null
            } ?: return refused("the content is not one filter")
        val ids = filter.ids
        if (ids.isNullOrEmpty()) return refused("a delete must name its ids")
        if (ids.size > MAX_IDS) return refused("a delete may name at most $MAX_IDS ids")
        return Result.success(filter)
    }

    private fun refused(reason: String): Result<Filter> = Result.failure(IllegalArgumentException(reason))
}
