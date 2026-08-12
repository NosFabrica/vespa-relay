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
package com.nosfabrica.vespa.relay.router

/**
 * Idle time a transfer may sit silent before it is abandoned.
 *
 * IDLE, not a deadline: the clock resets on every message, so a relay that is
 * still delivering is never cut off however long its history takes. A
 * wall-clock deadline could only ever fire on the healthy case — one once
 * truncated four working upstreams at exactly its 4h mark.
 */
internal const val NEG_IDLE_MS = 30_000L

/**
 * How long one relay may deliver NOTHING before the rest of its asks are left
 * for the next pass.
 *
 * [NEG_IDLE_MS] bounds a single ask; this bounds the SEQUENCE of them. A stream
 * with `authorsPerLeg` set asks one relay once per author chunk, and a relay
 * that answers each chunk with a full idle window costs `chunks * NEG_IDLE_MS`
 * of a transfer slot and a socket — measured at 5h00m on one url, of which
 * 4h56m arrived nothing.
 *
 * Ten idle windows, so an ordinary run of empty-but-prompt chunks never reaches
 * it and a relay must be genuinely silent for five minutes to be given up on.
 *
 * Still not a deadline, and that distinction is the one the comment above is
 * about: this clock is reset by every event that arrives, so it cannot fire on a
 * leg that is working, however long that leg runs.
 */
internal const val LEG_QUIET_GIVE_UP_MS = 10 * NEG_IDLE_MS

/** How often progress lines and phase reports refresh. */
internal const val PROGRESS_INTERVAL_MS = 15_000L
