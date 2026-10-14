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

import com.nosfabrica.vespa.relay.peers.RelayVerdictRecord
import com.nosfabrica.vespa.relay.progress.Processors
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/** Which servers carry more than one `prime` url, and what the fold holds about each of those urls. */
internal object PrimeCensus {
    /** [prime] grouped by server, read against the fold's answers in [held]. */
    fun of(
        prime: Collection<NormalizedRelayUrl>,
        held: RelayVerdictRecord.Verdicts,
    ): Processors.SharedHosts {
        val canonicals = held.aliases.values.toHashSet()

        fun foldOf(url: NormalizedRelayUrl): String =
            when (url) {
                in held.aliases -> Processors.FOLD_ALIAS
                in canonicals -> Processors.FOLD_CANONICAL
                in held.distinct -> Processors.FOLD_DISTINCT
                in held.staleFolds -> Processors.FOLD_STALE
                else -> Processors.FOLD_NONE
            }

        val shared = prime.groupBy { RelayAliases.authorityOf(it.url) }.filterValues { it.size > 1 }
        val rows =
            shared.map { (host, urls) ->
                val folds = urls.sortedBy { it.url }.map { Processors.FoldOf(it.url, foldOf(it)) }
                // Proven are the urls the fold measured as their own relay; at most one of the rest is owed.
                val unproven = folds.count { it.fold != Processors.FOLD_CANONICAL && it.fold != Processors.FOLD_DISTINCT } > 1
                Triple(host, folds, unproven)
            }
        val top =
            rows
                .sortedWith(compareBy({ !it.third }, { -it.second.size }, { it.first }))
                .take(Processors.MAX_SHARED_HOSTS)
                .map { (host, folds, _) -> Processors.SharedHost(host, folds.size, folds.take(Processors.MAX_SHARED_HOST_URLS)) }
        return Processors.SharedHosts(
            hosts = shared.size,
            urls = shared.values.sumOf { it.size },
            unproven = rows.count { it.third },
            top = top,
        )
    }
}
