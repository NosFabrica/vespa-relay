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
 * EVERY READ SAYS WHOSE EYES IT IS READ THROUGH. A REQ or a COUNT from a
 * connection that has not authenticated is answered only if each of its
 * filters names a lens — NIP-50 `observer:<64-hex>` — or waives one with
 * `include:spam`. Anything else is refused with `auth-required:` — and that
 * prefix is not a formality: signing a NIP-42 AUTH and asking again is the
 * THIRD way to be answered, the one where the connection itself becomes the
 * lens.
 *
 * WHY A RELAY WOULD DO THAT. This store treats a web-of-trust lens as a
 * FILTER, and it has no house observer to fall back on ([ObserverBackend] says
 * why). So a read with no lens is not "the same answers, unranked" — it is a
 * DIFFERENT question: the whole corpus, spam and all, in text-relevance or
 * recency order, with the trust this relay exists to apply switched off
 * entirely. The engine does not even send `min_rank` without an observer to
 * anchor it (the store's `EventYql`), so the default trust floor a search
 * carries is silently inert. That answer is a legitimate thing to want, and
 * `include:spam` is how a client asks for it. What it must not be is what a
 * client gets by SAYING NOTHING — a search relay whose unstated default is
 * "no trust at all" misreports itself to every client that never heard of
 * this feature, and the reader has no way to tell the two corpora apart.
 *
 * WHAT IT IS NOT. Not authentication for reads: `include:spam` and
 * `observer:` both work on a socket that never signs anything, because trust
 * scores here are public and any client may rank through any lens. The
 * NIP-11 `limitation.auth_required` therefore stays FALSE — it would claim a
 * door that is not locked. And not a write gate: EVENT, AUTH and NIP-77
 * NEG-OPEN are untouched. Negentropy reconciles IDS, not content, and the
 * REQ that fetches what it named is gated like any other; gating it too would
 * break relay-to-relay mirroring for nothing.
 *
 * WHAT IT COSTS A CLIENT THAT COMPLIES: nothing but the token. Measured
 * against the staging deployment on 2026-08-22, one anonymous socket, three
 * REQs — `{kinds:[1],limit:5}` and the same filter carrying
 * `search:"include:spam"` returned the SAME five ids, while the same filter
 * carrying `observer:460c25…` returned five different ones. The waiver is free
 * because the store maps a termless `include:spam` to plain recall, and the
 * lens resolves on a socket that signed nothing because the scores are public.
 * That is why the page can stamp a whole connection (`web/shared/lens.js`)
 * rather than reason about it per ask.
 *
 * ALL FILTERS OR NONE. A subscription's filters are ORed, so one undeclared
 * filter beside a declared one would serve the undeclared question in full.
 * The refusal names the whole REQ rather than dropping filters out of it: a
 * client that gets back fewer answers than it asked for cannot tell that from
 * a quiet corpus.
 *
 * The parse is quartz's own [SearchQuery], the very parser the store maps
 * with, so the gate cannot come to a different reading of a token than the
 * query planner does — including the lexing rules that make `"include:spam"`
 * in quotes a phrase and `-observer:…` an exclusion rather than either being
 * a way through.
 */
class LensRequiredPolicy : PassThroughPolicy() {
    /**
     * This connection's context, captured from the only hook handed one.
     * `@Volatile` for the same reason [MultiAddressAuthPolicy] holds its
     * `send` that way: the REQ that reads it can land on a different transport
     * coroutine than the connect that wrote it. The SET inside is quartz's own
     * and grows as AUTHs land, so this reads the live auth state rather than a
     * snapshot of it.
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
            // Signed in: the connection's own pubkey IS the lens, applied by
            // ObserverBackend. Nothing to declare.
            scope?.authenticatedUsers?.isNotEmpty() == true -> PolicyResult.Accepted(cmd)

            filters.all(Filter::declaresLens) -> PolicyResult.Accepted(cmd)

            else -> PolicyResult.Rejected(NO_LENS)
        }

    companion object {
        /**
         * The refusal, and the whole of the client's way out — the three ways,
         * in the order a client can act on them. `auth-required:` is the
         * machine-readable half NIP-42 clients already act on (ours is
         * `shared/relay.js`, which authenticates and resends), and the prose is
         * for the reader of a `CLOSED` frame in a console, who is the person
         * this default exists to inform.
         */
        internal val NO_LENS =
            AUTH_REQUIRED.format(
                "this relay answers through a web of trust and has no house observer to lend you. " +
                    "Sign in (NIP-42), or name whose trust ranks this read with the NIP-50 `observer:<64-hex pubkey>` " +
                    "token, or ask for the whole corpus unranked with `include:spam`.",
            )
    }
}

/**
 * Does this filter say whose eyes it is read through?
 *
 * `observer:` must be a USABLE lens — 64 hex, the store's own acceptance test
 * ([com.nosfabrica.vespa.eventstore.mapping] drops anything else) — or
 * `observer:npub1…` would pass the gate here and rank nothing there, which is
 * the silent no-lens read this policy exists to stop.
 */
internal fun Filter.declaresLens(): Boolean {
    val parsed = SearchQuery.parse(search ?: return false)
    return parsed.includeSpam || parsed.extension(OBSERVER)?.lowercase()?.let(Hex::isHex64) == true
}

/** The NIP-50 extension naming the pubkey whose web of trust ranks a read. */
private const val OBSERVER = "observer"
