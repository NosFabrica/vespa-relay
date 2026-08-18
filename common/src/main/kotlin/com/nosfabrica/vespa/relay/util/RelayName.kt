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
package com.nosfabrica.vespa.relay.util

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer

/**
 * One relay, spelled one way — so the distribution counts relays and not
 * the strings people typed.
 *
 * A grouping over the relay tag returns the `r` values BYTE FOR BYTE as their
 * authors signed them, and relay lists are hand-written: `wss://nos.lol`
 * and `wss://nos.lol/` are the same relay and were two rows, each holding
 * part of that relay's lists. That does not merely look untidy, it
 * MISRANKS a table sorted by count — a relay split three ways across a
 * trailing slash, a capitalised host and an explicit `:443` sits below
 * relays it actually outnumbers, and `total` counts spellings.
 *
 * Quartz's [RelayUrlNormalizer] is the same one the router dials with and
 * the same one [com.nosfabrica.vespa.relay.config.RelayAddresses] admits
 * urls through, so this panel now identifies a relay the way the rest of
 * the stack does rather than inventing a third rule.
 *
 * A url the normalizer REJECTS is kept as its trimmed self rather than
 * dropped. This panel is a census of what our users' lists name, and a
 * silently vanishing row would understate `total` while leaving the
 * heading claiming otherwise — junk in the corpus is a finding, not
 * something for this function to hide.
 *
 * The trailing slash comes off AFTER normalising, never instead of it.
 * That ordering is the whole safety argument: the normalizer is what
 * decides two spellings are one relay, and trimming a suffix from its
 * output is a deterministic rename applied to every member of a group at
 * once, so it can tidy how a relay is displayed but can never split one
 * back into two.
 */
fun canonicalRelay(raw: String): String {
    val trimmed = raw.trim()
    val normalized = RelayUrlNormalizer.normalizeOrNull(trimmed)?.url ?: trimmed
    // "wss://nos.lol/" is what the normalizer emits for a bare host; the
    // slash is noise in a column of urls read side by side.
    return normalized.removeSuffix("/").ifEmpty { trimmed }
}
