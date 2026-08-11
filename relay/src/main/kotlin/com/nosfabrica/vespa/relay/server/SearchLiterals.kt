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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

// NIP-50's minus is ONE minus. A search token that opens with a RUN of dashes is
// text somebody typed, and this rewrites it into the quoted spelling that says
// so — before the grammar downstream can read it as an exclusion.
//
// What it is for. `--------------06:30--------------` is the format an hourly
// bot posts in, and searching search-staging for it returned the newest 40
// events in the store, none of them containing anything like it. The path:
// quartz's `SearchQuery.parse` files ANY 2+ character token starting with `-`
// under `notTerms` with every leading dash stripped, so the typed string became
// the exclusion `06:30--------------`; the query was then left with no positive
// term, `hasText` came out false, and the store's FilterMapping sent
// `search = null` — a plain-recall filter. A text search was answered with a
// recency feed. Measured, all three of `--------------06:30--------------`,
// `-06:30` and `--------------` returned the SAME feed, which is what identified
// the minus rather than the tokenizer as the cause.
//
// The rule. One leading `-` is the operator; two or more are punctuation.
// `-bitcoin` still excludes bitcoin, and `-"phrase"` still excludes the phrase.
//
// Why quotes rather than trimming the dashes off. A bare token with leading
// dashes cannot exist in the grammar — the parse strips them unconditionally —
// so quoting is the only spelling that carries the literal through intact, and
// it lands on semantics both upstreams already define: a phrase is an ADJACENCY
// requirement, and a phrase whose content the index cannot hold is
// provably-no-match rather than match-all (quartz's SearchQuery KDoc says so in
// as many words, and the store's `containsPhrase` returns false on an empty
// token list). So the typed string reaches the engine as the adjacency `06 30`,
// which is the most any query can ask of this index: Vespa's tokenizer keeps no
// punctuation, and `"06:30"` and `"--------------06:30--------------"` return
// the same set on staging — the dashes and the colon are simply not in the
// index to be required.
//
// What this does NOT fix. `06 30` is also how `2026-06-30` tokenizes, so a
// literal-string search still competes on relevance with every ISO date in the
// corpus, and a low-trust author's note stays below them (the note above is
// recall-reachable — it comes back at #1 under a `since` window — but sits
// below 500 without one). Ranking a punctuation literal is not something a
// query rewrite can do. Answering with a feed that has nothing to do with the
// query is, and that is the part that lives here.

/**
 * The dash-led literals in every filter's `search` quoted, or this same list
 * when there is nothing to quote.
 *
 * Applied on every read path, negentropy included: the store honours `search`
 * in `snapshotIdsForNegentropy` (it falls through to `index.search(q)` for any
 * filter carrying one), so a NEG session and a REQ must not disagree about what
 * the same filter means.
 *
 * The no-op case must not allocate — most reads carry no `search` at all, and a
 * search without a dash in it cannot contain a dash-led token.
 */
internal fun List<Filter>.quotingDashedLiterals(): List<Filter> {
    if (none { it.search?.contains('-') == true }) return this
    var changed = false
    val out =
        map { filter ->
            val search = filter.search ?: return@map filter
            val quoted = quoteDashedLiterals(search)
            if (quoted === search) {
                filter
            } else {
                changed = true
                filter.copy(search = quoted)
            }
        }
    return if (changed) out else this
}

/**
 * [search] with each dash-led token wrapped in quotes; the same instance back
 * when no token qualifies.
 *
 * The walk mirrors quartz's `liftQuotedSpans` on where a quoted span may open —
 * only at a token boundary, running to the next quote or to the end of the
 * string — because the two have to agree about which characters are inside one.
 * Spans are copied through untouched: what is already a phrase needs nothing
 * from this, and rewriting inside one would nest quotes and break the lex. A
 * quote MID-token is an ordinary character there and stays one here, which is
 * also why a token holding a `"` is left alone rather than wrapped — wrapping it
 * would split the token in two at the inner quote.
 */
internal fun quoteDashedLiterals(search: String): String {
    // The one case where a token that is nothing but dashes gets quoted: it is
    // the whole query, so quoting is the difference between "nothing matched"
    // and the match-all feed. Beside real words a dash run is decoration —
    // `bitcoin --` must keep searching for bitcoin, not become unsatisfiable —
    // so there it drops out exactly as it does today.
    val sole = search.trim()
    var out: StringBuilder? = null
    var i = 0
    while (i < search.length) {
        val c = search[i]
        if (c.isWhitespace()) {
            out?.append(c)
            i++
            continue
        }
        // At a token boundary. `"…"` and `-"…"` are already phrases: skip the
        // span whole, closing quote included.
        if (c == '"' || (c == '-' && i + 1 < search.length && search[i + 1] == '"')) {
            val opened = i + if (c == '-') 2 else 1
            val close = search.indexOf('"', opened)
            val end = if (close < 0) search.length else close + 1
            out?.append(search, i, end)
            i = end
            continue
        }
        var end = i
        while (end < search.length && !search[end].isWhitespace()) end++
        val token = search.substring(i, end)
        if (isTypedLiteral(token, alone = token == sole)) {
            val to = out ?: StringBuilder(search.length + 2).append(search, 0, i).also { out = it }
            to.append('"').append(token).append('"')
        } else {
            out?.append(token)
        }
        i = end
    }
    return out?.toString() ?: search
}

/**
 * Is [token] a literal somebody typed rather than NIP-50 syntax?
 *
 * Two or more leading dashes, and then either something an index could hold —
 * the reader's word, decorated — or nothing else in the query to search for
 * ([alone]). The second arm is what makes a query of only punctuation answer
 * "nothing matched": without it `----` parses to no terms at all and the read
 * falls through to plain recall.
 */
private fun isTypedLiteral(
    token: String,
    alone: Boolean,
): Boolean =
    token.startsWith("--") &&
        '"' !in token &&
        (alone || token.any(Char::isLetterOrDigit))
