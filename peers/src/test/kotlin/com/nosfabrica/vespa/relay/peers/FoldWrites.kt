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
package com.nosfabrica.vespa.relay.peers

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/** A containment fold, as the monitor words one; the record signs whatever evidence it is handed. */
internal suspend fun RelayVerdictRecord.publish(
    alias: NormalizedRelayUrl,
    canonical: NormalizedRelayUrl,
    sampled: Int,
    shared: Int,
): Event? = publishFold(alias, canonical, "$sampled newest events, $shared shared with ${canonical.url}")

/** A url measured as its own relay, as the monitor words one. */
internal suspend fun RelayVerdictRecord.publishDistinct(
    url: NormalizedRelayUrl,
    sampled: Int,
    comparedAgainst: String,
    bestShared: Int,
): Event? = publishFold(url, url, "$sampled newest events, best $bestShared shared with $comparedAgainst")
