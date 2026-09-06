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
 * Every read says whose eyes it is read through: an unauthenticated REQ or COUNT is answered only
 * if every filter names a lens (`observer:<64-hex>`) or waives one (`include:spam`), because a
 * subscription's filters are ORed. NIP-77 passes through the same gate.
 */
class LensRequiredPolicy : PassThroughPolicy() {
    /** This connection's context; `@Volatile` because the REQ can land on another coroutine. */
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
        /** Names all three ways out; `auth-required:` is the half NIP-42 clients act on. */
        internal val NO_LENS =
            AUTH_REQUIRED.format(
                "this relay answers through a web of trust and has no house observer to lend you. " +
                    "Sign in (NIP-42), or name whose trust ranks this read with the NIP-50 `observer:<64-hex pubkey>` " +
                    "token, or ask for the whole corpus unranked with `include:spam`.",
            )
    }
}

/** Whether this filter names a usable lens or waives one. */
internal fun Filter.declaresLens(): Boolean {
    val parsed = SearchQuery.parse(search ?: return false)
    return parsed.includeSpam || observerLens() != null
}

/**
 * The pubkey this filter names as its lens, or null. Must be 64 hex, the store's own acceptance
 * test, or `observer:npub1…` would pass here and rank nothing there.
 */
internal fun Filter.observerLens(): HexKey? =
    search
        ?.takeIf { it.isNotEmpty() }
        ?.let { SearchQuery.parse(it).extension(OBSERVER) }
        ?.lowercase()
        ?.takeIf { it.length == 64 && Hex.isHex64(it) }

private const val OBSERVER = "observer"
