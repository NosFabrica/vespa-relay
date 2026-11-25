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
package com.nosfabrica.vespa.relay.sync

import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11Fetcher
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap

/**
 * Which relays this mirror reads with NIP-50's `include:spam`, the waiver a web-of-trust relay
 * answers with its whole corpus: those whose NIP-11 `nip50` names it, and those that refused us
 * naming it. Every other relay is asked exactly what the stream says, because a relay without
 * NIP-50 refuses any filter carrying `search` (strfry: `unrecognised filter item`).
 */
internal class LensWaivers(
    /** The relay's NIP-11 `nip50` list; null when there is no document to read. */
    private val nip50Of: suspend (NormalizedRelayUrl) -> List<String>?,
    private val log: (String) -> Unit = System.err::println,
) {
    private val learned = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** Each relay's advertisement as last read; a missing document reads as no waiver. */
    private val advertised = ConcurrentHashMap<NormalizedRelayUrl, Boolean>()

    /** How many relays are read with the waiver; published as `waivedRelays`. */
    val waived: Int get() = (learned + advertised.filterValues { it }.keys).size

    /** Whether reads of [url] carry the waiver. */
    suspend fun waives(url: NormalizedRelayUrl): Boolean {
        if (url in learned) return true
        val says = nip50Of(url)?.any { it.trim() == NIP50_FEATURE } == true
        if (advertised.put(url, says) != says && says) log("router: ${url.url} advertises `$NIP50_FEATURE` (NIP-11); reading it with the waiver")
        return says
    }

    /** [filter] as it goes to [url]. */
    suspend fun onWire(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): Filter = if (waives(url)) waived(filter) else filter

    /** [filters] as they go to [url]. */
    suspend fun onWire(
        url: NormalizedRelayUrl,
        filters: List<Filter>,
    ): List<Filter> = if (waives(url)) filters.map(::waived) else filters

    /**
     * Reads a refusal. True only when it newly teaches the waiver, the caller's signal to re-walk:
     * an `auth-required:` or `restricted:` sentence naming `include:spam` as the way in.
     */
    fun learn(
        url: NormalizedRelayUrl,
        said: String?,
    ): Boolean {
        if (said == null || !namesWaiver(said) || !learned.add(url)) return false
        log("router: ${url.url} refused us naming `include:spam`; asking with it from here")
        return true
    }

    companion object {
        /** How NIP-11 lists the waiver, in the `"<class> <token>"` spelling vespa-relay and noswhere use. */
        const val NIP50_FEATURE = "ext include:spam"

        /** A NIP-11 read may hold a visit's first ask this long; past it the relay is read without. */
        const val NIP11_DEADLINE_MS = 10_000L

        /** A waiver no relay advertises; for tests and probes that dial nothing. */
        val NONE = LensWaivers({ null })

        /** The NIP-11 `nip50` list off [fetcher], bounded by [NIP11_DEADLINE_MS]; null on any failure. */
        fun nip50Off(fetcher: Nip11Fetcher): suspend (NormalizedRelayUrl) -> List<String>? =
            { url ->
                try {
                    withTimeoutOrNull(NIP11_DEADLINE_MS) { fetcher.fetch(url) }?.nip50
                } catch (e: CancellationException) {
                    throw e
                } catch (_: Exception) {
                    null
                }
            }

        internal fun namesWaiver(said: String): Boolean {
            val prefix = MachineReadablePrefix.parse(said)
            return (prefix == MachineReadablePrefix.AUTH_REQUIRED || prefix == MachineReadablePrefix.RESTRICTED) && said.contains(TOKEN)
        }

        /**
         * [filter] with `include:spam` among its search extensions, every term, phrase and other
         * extension kept. Appended to the stream's own string where that reads back the same; a
         * string the append would change (an unclosed quote swallows it) is rebuilt canonically.
         */
        internal fun waived(filter: Filter): Filter {
            val search = filter.search
            if (search.isNullOrBlank()) return filter.copy(search = TOKEN)
            val parsed = SearchQuery.parse(search)
            if (parsed.includeSpam) return filter
            val appended = "$search $TOKEN"
            if (sameApartFromWaiver(parsed, SearchQuery.parse(appended))) return filter.copy(search = appended)
            val rebuilt = SearchQuery(parsed.terms, parsed.extensions + (SearchQuery.INCLUDE to SearchQuery.SPAM), parsed.phrases, parsed.notPhrases, parsed.notTerms)
            return filter.copy(search = rebuilt.toSearchString())
        }

        private fun sameApartFromWaiver(
            was: SearchQuery,
            now: SearchQuery,
        ): Boolean =
            now.includeSpam &&
                now.terms == was.terms &&
                now.phrases == was.phrases &&
                now.notPhrases == was.notPhrases &&
                now.notTerms == was.notTerms &&
                now.extensions - SearchQuery.INCLUDE == was.extensions - SearchQuery.INCLUDE

        private const val TOKEN = "${SearchQuery.INCLUDE}:${SearchQuery.SPAM}"
    }
}
