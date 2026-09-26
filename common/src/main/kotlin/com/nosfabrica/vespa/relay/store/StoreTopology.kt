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
package com.nosfabrica.vespa.relay.store

import com.nosfabrica.vespa.eventstore.runtime.DEFAULT_PROVIDER_REFRESH_MILLIS
import com.nosfabrica.vespa.eventstore.runtime.WriterTopology

/**
 * What this deployment tells the store about its writers at every `VespaEventStore.open()`.
 * Strict is the only exact answer when the relay and the sync process write the same authors.
 */
val STORE_WRITERS: WriterTopology = WriterTopology.SHARED_STRICT

/** A day: a longer window is a list that waits a day to apply, which `0` already says more honestly. */
const val MAX_PROVIDER_REFRESH_SECONDS: Long = 86_400L

/** The variable [providerRefreshSeconds] reads — one name for both processes, so compose hands it to each. */
const val PROVIDER_REFRESH_ENV = "TRUST_PROVIDER_REFRESH_SECONDS"

/**
 * How often each process rebuilds the store's kind-10040 provider pass, the cache every observer's
 * lens and Trusted List gate read. Both processes need it, for opposite reasons: the relay so that a
 * provider list the sync mirrored in becomes a reader's lens and delegation, the sync so that cards it
 * mirrors for a service first named on the relay's socket are projected at all. Without it each pass
 * learned only from its own process's writes, and on staging (2026-09-26) 24 of 47 recent observers
 * resolved no lens and got empty ranked pages (NosFabrica/vespa-eventstore#145, #243 here).
 *
 * Defaults to the store's 60 s, which is the window docs/configuration.md promises. Each tick is one
 * read of every stored 10040 (hundreds). `0` turns it off and brings the old behaviour back: a list
 * applies only when this process writes a 10040 itself or restarts. A value that does not parse, is
 * negative, or exceeds a day stops the boot rather than quietly falling back — the bound also keeps
 * the store's seconds-to-millis conversion from overflowing into a negative interval, which it reads
 * as "off".
 */
fun providerRefreshSeconds(env: Map<String, String>): Long =
    env[PROVIDER_REFRESH_ENV]?.trim()?.takeIf { it.isNotEmpty() }?.let {
        it.toLongOrNull()?.takeIf { n -> n in 0..MAX_PROVIDER_REFRESH_SECONDS }
            ?: error(
                "$PROVIDER_REFRESH_ENV='$it' is not a number of seconds up to $MAX_PROVIDER_REFRESH_SECONDS. " +
                    "Use 0 to turn the refresh off, or unset it for the default.",
            )
    } ?: (DEFAULT_PROVIDER_REFRESH_MILLIS / 1000)
