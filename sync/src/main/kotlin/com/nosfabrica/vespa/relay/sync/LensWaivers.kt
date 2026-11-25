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

import com.nosfabrica.vespa.eventstore.ReadShape
import com.nosfabrica.vespa.relay.peers.AuthGate
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.MachineReadablePrefix
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import java.util.concurrent.ConcurrentHashMap

/**
 * How this mirror reads each relay: anonymously, verbatim, until a refusal says otherwise. A
 * refusal naming NIP-50's `include:spam` (a web-of-trust relay's lens gate) teaches the waiver,
 * which then rides every read of that relay; any other `auth-required:` refusal, or a NIP-67 EOSE
 * `auth` hint, signs us in there.
 * Anonymous first because a gated relay answers a signed-in key through that key's lens, empty for
 * ours and never refused, so only an anonymous read hears the gate. Never the waiver unasked: a
 * relay without NIP-50 refuses any filter carrying `search` (strfry: `unrecognised filter item`).
 * Held in memory for the process; a restart relearns each on its first refusal.
 */
internal class LensWaivers(
    /** Relays kept through [retain] whatever the roster says: the declared up and down urls. */
    private val pinned: Set<NormalizedRelayUrl> = emptySet(),
    private val log: (String) -> Unit = System.err::println,
) : AuthGate {
    private val waiving = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /** Where we answer NIP-42, and whether a refused read asked for it: only that says the relay gates how it reads us. */
    private val signingIn = ConcurrentHashMap<NormalizedRelayUrl, SignIn>()

    private enum class SignIn { TO_WRITE, TO_READ }

    /** How many relays are read with the waiver; published as `waivedRelays`. */
    val waived: Int get() = waiving.size

    /** Whether reads of [url] carry the waiver. */
    fun waives(url: NormalizedRelayUrl): Boolean = url in waiving

    /**
     * Signed in on [url] because it refused a read, and no waiver learned there: a relay that gates by
     * trust would answer our key through its lens, empty and unrefused. Its absences are not to be acted
     * on. A sign-in an EVENT's refusal asked for says nothing of how the relay reads.
     */
    fun mayBeLensed(url: NormalizedRelayUrl): Boolean = signingIn[url] == SignIn.TO_READ && url !in waiving

    /** How [url] is read now; an answer from it stands for a later decision only while this does. */
    fun stance(url: NormalizedRelayUrl): Stance = Stance(waives(url), mayAuthenticate(url), mayBeLensed(url))

    /** Whether reads carry the waiver, whether we sign in, and whether a refused read made us. */
    data class Stance(
        val waived: Boolean,
        val signedIn: Boolean,
        val lensed: Boolean,
    )

    /** [filter] as it goes to [url]. */
    fun onWire(
        url: NormalizedRelayUrl,
        filter: Filter,
    ): Filter = if (waives(url)) waived(filter) else filter

    /** [filters] as they go to [url]. */
    fun onWire(
        url: NormalizedRelayUrl,
        filters: List<Filter>,
    ): List<Filter> = if (waives(url)) filters.map(::waived) else filters

    override fun heard(
        url: NormalizedRelayUrl,
        refusal: AuthGate.Refusal,
    ) {
        val said = refusal.said
        if (learn(url, said) || !refusal.asksAuth) return
        val why = if (refusal.ofRead) SignIn.TO_READ else SignIn.TO_WRITE
        var changed = false
        // A read's reason outranks a write's: the relay then gates reads too.
        signingIn.compute(url) { _, before -> if (before != null && before >= why) before else why.also { changed = true } }
        if (changed) {
            val what = if (refusal.ofRead) "read" else "write"
            log("router: ${url.url} asks us to sign in to $what (${said?.take(120) ?: "an EOSE `auth` hint"}); answering its NIP-42 challenge from here")
        }
    }

    override fun mayAuthenticate(url: NormalizedRelayUrl): Boolean = signingIn.containsKey(url)

    /**
     * Reads a refusal. True when it names the waiver as the way in (an `auth-required:` or
     * `restricted:` sentence naming `include:spam`): the caller's signal to ask again with it.
     */
    fun learn(
        url: NormalizedRelayUrl,
        said: String?,
    ): Boolean {
        if (said == null || !namesWaiver(said)) return false
        if (waiving.add(url)) log("router: ${url.url} refused us naming `include:spam`; asking with it from here")
        return true
    }

    /**
     * Forgets the waiver once [url] rejects the `search` field itself, the one evidence that the
     * waiver is what it refuses. True when the caller should ask again without it.
     */
    fun unlearn(
        url: NormalizedRelayUrl,
        said: String?,
    ): Boolean {
        if (said == null || !REFUSES_SEARCH.containsMatchIn(said) || !waiving.remove(url)) return false
        log("router: ${url.url} refused the `search` field (${said.take(120)}); asking it verbatim from here")
        return true
    }

    /** Forgets relays off [urls] and [pinned]. */
    fun retain(urls: Set<NormalizedRelayUrl>) {
        waiving.removeIf { it !in urls && it !in pinned }
        signingIn.keys.removeIf { it !in urls && it !in pinned }
    }

    companion object {
        /** The waiver, alone: a search with no words, which quartz keeps on every by-id fetch. */
        const val TOKEN = "${SearchQuery.INCLUDE}:${SearchQuery.SPAM}"

        /** A registry of its own each call, for tests and probes. */
        fun none(): LensWaivers = LensWaivers()

        internal fun namesWaiver(said: String): Boolean {
            val prefix = MachineReadablePrefix.parse(said)
            return (prefix == MachineReadablePrefix.AUTH_REQUIRED || prefix == MachineReadablePrefix.RESTRICTED) && said.contains(TOKEN)
        }

        /**
         * A refusal of the `search` field itself, as strfry (`unrecognised filter item: search`) and
         * NIP-50-less relays word it; never one about what a search says (`unsupported search extension`).
         */
        private val REFUSES_SEARCH =
            Regex(
                "(?i)(?:unrecogni[sz]ed|unknown|unsupported|invalid)\\s+(?:filter\\s+)?(?:item|field|key)s?\\W{0,3}search\\b" +
                    "|\\bsearch\\W{0,3}(?:is\\s+)?(?:not\\s+supported|unsupported)",
            )

        /**
         * [filter] with `include:spam` among its search extensions, every term, phrase and other
         * extension kept; unchanged when it names its own lens (`observer:<hex>`, as the store reads it). Appended to the stream's own string where that reads back the same; a
         * string the append would change (an unclosed quote swallows it) is rebuilt canonically.
         * `include` holds one value in quartz's grammar, the last written, so a stream's own
         * `include:<other>` gives way to the waiver.
         */
        internal fun waived(filter: Filter): Filter {
            val search = filter.search
            if (search.isNullOrBlank()) return filter.copy(search = TOKEN)
            // A stream naming its own lens chose its floor, and a gate does not refuse it.
            if (ReadShape.of(search).observer != null) return filter
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
    }
}
