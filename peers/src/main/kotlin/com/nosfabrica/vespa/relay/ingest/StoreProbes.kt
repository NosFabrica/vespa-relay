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
package com.nosfabrica.vespa.relay.ingest

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.relay.ingest.refused.RefusalSink
import com.nosfabrica.vespa.relay.pressure.ServingPressure
import kotlinx.coroutines.CoroutineScope

/** A pure membership read, which is all ingest needs to skip verifying a duplicate. */
fun VespaEventStore.knownIdsProbe(): suspend (List<String>) -> Set<String> = engine::existingIds

/** The stored winner per author of one plain replaceable kind, by the store's own NIP-01 rule. */
fun VespaEventStore.newestVersionsProbe(): suspend (Int, List<String>) -> Map<String, AddressVersion> = { kind, authors -> engine.newestVersions(kind, authors).mapValues { AddressVersion(it.value.createdAt, it.value.id) } }

/** The pipeline over [store] with both drop-probes wired, as both processes run it. Not started. */
fun probedIngestPipeline(
    store: VespaEventStore,
    tuning: IngestTuning,
    scope: CoroutineScope,
    audit: ParseAudit? = null,
    servingPressure: ServingPressure? = null,
    refusals: RefusalSink = RefusalSink.None,
    bootName: String? = null,
): IngestPipeline = IngestPipeline(store, tuning, audit, servingPressure, scope, store.knownIdsProbe(), store.newestVersionsProbe(), refusals, bootName = bootName)
