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
package com.nosfabrica.vespa.relay.maintenance

import com.nosfabrica.vespa.eventstore.WriterTopology

/**
 * What this deployment tells the store about its writers, at every
 * `VespaEventStore.open()` — the one fact neither process can work out for
 * itself, and the one the store's NIP-09/NIP-62 fast path turns on.
 *
 * There are two writers here and they are split by ROLE, not by owner: the
 * relay serves clients, the sync process mirrors upstreams, and both touch the
 * same authors. That is the one shape the guard-owner cache cannot be exact
 * for — the router stores tombstones the relay's copy never hears about, and
 * clients publish deletions the router's copy never hears about, so a cached
 * instance proves to itself that an author has no tombstone and admits the
 * event it covers.
 *
 * [WriterTopology.SHARED] would bound that to a refresh interval, and it is
 * still the wrong trade here: the rebuild is a corpus-wide visit (hours on
 * ours), so the window is set by the rebuild rather than by the interval you
 * configure, and the exposure it leaves is one-directional — a covered event is
 * admitted, stored and served, and nothing repairs it afterwards, because
 * re-delivering the tombstone hits the dedup gate before it can sweep again.
 * `SINGLE_WRITER` is simply false for us.
 *
 * The cost of being strict is measured and small: per-event `insert()` ~143 →
 * ~137 ev/s (−4.5%) with p50 unchanged, and no measurable difference on
 * `batchInsert`, because the store fires the dup and both guard probes
 * concurrently. Ingest throughput was never what the cache bought — it bought
 * engine read headroom, which is not worth a deleted note coming back.
 */
val STORE_WRITERS: WriterTopology = WriterTopology.SHARED_STRICT
