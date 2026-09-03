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
 * Idle time a transfer may sit silent before it is abandoned.
 *
 * IDLE, not a deadline: the clock resets on every message, so a relay that is
 * still delivering is never cut off however long its history takes. A
 * wall-clock deadline could only ever fire on the healthy case — one once
 * truncated four working upstreams at exactly its 4h mark.
 *
 * **THE SYNC PLANE'S BUDGET, AND THE MONITOR PLANE HAS A DIFFERENT ONE.** Every
 * caller of this constant is a sync-plane transfer — `VisitPool`,
 * `StaticBackfill`, `RetractionAudit`, `UpstreamPush`, `NegentropyPager` — while
 * the probe passes derive theirs per url from `connectionTimeout` (20s by
 * default) through [probeIdleMs], plus the Tor circuit budget where the url
 * needs one. The two are deliberately different numbers, because they are
 * sizing different things: this one is the silence a relay is allowed WHILE
 * DELIVERING a history that may run for hours, and the probe's is the silence a
 * relay is allowed while answering a single twenty-event ask that has no
 * business taking longer than a handshake.
 *
 * The passes also carry something no caller of this constant does — a hard
 * per-url wall clock, [AliasProbe.deadlineMs], sized as a multiple of the probe
 * window above. That is not a disagreement with the paragraph above either: a
 * probe is not a transfer, so cutting one costs a re-measure next pass rather
 * than a truncated history, and a probe pass BLOCKS the roster every stream is
 * built from while it runs.
 */
internal const val NEG_IDLE_MS = 30_000L

/**
 * How long one relay may deliver NOTHING before the rest of its asks are left
 * for the next pass.
 *
 * [NEG_IDLE_MS] bounds a single ask; this bounds the SEQUENCE of them. A stream
 * with author-bound asks visits one relay once per bound author, and a relay
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

/**
 * How many times ONE leg may be narrowed and re-walked inside a single visit
 * when a relay refuses it on filter width — see [FilterWidths].
 *
 * Three, and the number is a cost bound rather than a convergence one. A relay
 * that STATES its limit (`too many kinds (max 100)`) is under it on the first
 * retry, so this never bites there. A relay that only says the ask was too wide
 * is halved, and from this router's 139-kind `contentViaOutbox` ask that is
 * seven halvings to reach one kind — so the bound stops a single visit from
 * paying all seven, each of which re-walks the chunks that already succeeded.
 *
 * It costs nothing in convergence because the cap OUTLIVES THE VISIT: the pool
 * keeps what it learned, so the next visit starts three halvings in and the
 * relay is inside its limit within a handful of visits rather than never.
 */
internal const val MAX_NARROWINGS = 3

/** How often progress lines and phase reports refresh. */
internal const val PROGRESS_INTERVAL_MS = 15_000L
