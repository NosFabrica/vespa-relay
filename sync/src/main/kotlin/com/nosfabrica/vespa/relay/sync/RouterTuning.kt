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

/**
 * How long a sync-plane transfer may sit silent before it is abandoned. An idle clock, not a
 * deadline: it resets on every message, so a relay still delivering is never cut off.
 */
internal const val NEG_IDLE_MS = 30_000L

/**
 * How long one relay may deliver nothing across a run of asks before the rest are left for
 * the next visit. Reset by every event that arrives, like [NEG_IDLE_MS].
 */
internal const val LEG_QUIET_GIVE_UP_MS = 10 * NEG_IDLE_MS

/**
 * How many times one leg may be narrowed and re-walked inside one visit. A cost bound, not a
 * convergence one: the learned cap outlives the visit.
 */
internal const val MAX_NARROWINGS = 3

/** How often progress lines and phase reports refresh. */
internal const val PROGRESS_INTERVAL_MS = 15_000L
