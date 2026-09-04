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
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix.AUTH_REQUIRED
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.Message
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.Command
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.CountCmd
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PassThroughPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import com.vitorpamplona.quartz.utils.Hex

/**
 * Every read says whose eyes it is read through. A REQ or COUNT from a
 * connection that has not authenticated is answered only if each filter
 * names a lens (`observer:<64-hex>`) or waives one (`include:spam`); anything
 * else is refused with `auth-required:`, and signing in is the third way.
 *
 * This store applies a lens as a filter and has no house observer, so a read
 * with no lens is the whole corpus with trust switched off. That is a
 * legitimate ask, and it must be asked for rather than got by saying nothing.
 *
 * Not authentication: both tokens work on a socket that signs nothing, so
 * `limitation.auth_required` stays false. Not a write gate. NIP-77 is gated
 * too, because quartz runs a NEG-OPEN's filters through `accept(ReqCmd)`;
 * an anonymous peer must declare `include:spam` to mirror from here.
 *
 * All filters or none: a subscription's filters are ORed, so one undeclared
 * filter beside a declared one would serve the undeclared question in full.
 * The parse is quartz's own [SearchQuery], the parser the store maps with.
 */
class LensRequiredPolicy : PassThroughPolicy() {
    /**
     * This connection's context. `@Volatile` because the REQ that reads it can
     * land on a different coroutine than the connect that wrote it. The set
     * inside is quartz's own and grows as AUTHs land.
     */
    @Volatile
    private var scope: RequestContext? = null

    override fun onConnect(
        scope: RequestContext,
        send: (Message) -> Unit,
    ) {
        this.scope = scope
    }

    override fun accept(cmd: ReqCmd): PolicyResult<ReqCmd> = gate(cmd, cmd.filters)

    override fun accept(cmd: CountCmd): PolicyResult<CountCmd> = gate(cmd, cmd.filters)

    private fun <T : Command> gate(
        cmd: T,
        filters: List<Filter>,
    ): PolicyResult<T> =
        when {
            // Signed in: the connection's own pubkey is the lens, applied by ObserverBackend.
            scope?.authenticatedUsers?.isNotEmpty() == true -> PolicyResult.Accepted(cmd)

            filters.all(Filter::declaresLens) -> PolicyResult.Accepted(cmd)

            else -> PolicyResult.Rejected(NO_LENS)
        }

    companion object {
        /**
         * The refusal names all three ways out. `auth-required:` is the half
         * NIP-42 clients act on; the prose is for a person reading a CLOSED.
         */
        internal val NO_LENS =
            AUTH_REQUIRED.format(
                "this relay answers through a web of trust and has no house observer to lend you. " +
                    "Sign in (NIP-42), or name whose trust ranks this read with the NIP-50 `observer:<64-hex pubkey>` " +
                    "token, or ask for the whole corpus unranked with `include:spam`.",
            )
    }
}

/** Whether this filter names a usable lens or waives one. See [observerLens]. */
internal fun Filter.declaresLens(): Boolean {
    val parsed = SearchQuery.parse(search ?: return false)
    return parsed.includeSpam || observerLens() != null
}

/**
 * The pubkey this filter names as its lens, or null. The one reading of the
 * `observer:` token in this module. Must be 64 hex, the store's own
 * acceptance test, or `observer:npub1…` would pass here and rank nothing there.
 */
internal fun Filter.observerLens(): HexKey? =
    search
        ?.takeIf { it.isNotEmpty() }
        ?.let { SearchQuery.parse(it).extension(OBSERVER) }
        ?.lowercase()
        ?.takeIf { it.length == 64 && Hex.isHex64(it) }

private const val OBSERVER = "observer"
