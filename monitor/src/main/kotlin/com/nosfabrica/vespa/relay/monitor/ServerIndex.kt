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
package com.nosfabrica.vespa.relay.monitor

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * The urls the monitor knows, indexed by server, so the urls sharing a server with a batch are
 * found without walking the whole set. Immutable: a change is a new index.
 */
class ServerIndex(
    val urls: Set<NormalizedRelayUrl>,
) {
    /** Built on first use, once per index rather than once per caller. */
    private val byServer: Map<String, List<NormalizedRelayUrl>> by lazy { urls.groupBy { RelayAliases.authorityOf(it.url) } }

    operator fun contains(url: NormalizedRelayUrl): Boolean = url in urls

    fun isEmpty(): Boolean = urls.isEmpty()

    /** The known urls outside [candidates] that share a server with one of them. */
    fun matesOf(candidates: Collection<NormalizedRelayUrl>): List<NormalizedRelayUrl> {
        if (urls.isEmpty()) return emptyList()
        val asked = candidates.toHashSet()
        return candidates
            .mapTo(HashSet()) { RelayAliases.authorityOf(it.url) }
            .flatMap { byServer[it].orEmpty() }
            .filter { it !in asked }
    }

    /** This index with [more] known too. */
    operator fun plus(more: Collection<NormalizedRelayUrl>): ServerIndex = ServerIndex(urls + more)

    companion object {
        val EMPTY = ServerIndex(emptySet())
    }
}
