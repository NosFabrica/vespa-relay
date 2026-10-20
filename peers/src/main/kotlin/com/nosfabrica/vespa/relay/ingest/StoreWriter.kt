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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.IEventStore

/**
 * Where the sync process's writes go: every write it makes, and only those, so a single switch
 * (`SYNC_INGEST`) moves them all. [local] is the store itself, the shape every deployment had
 * before; [RelayWriter] forwards them to the serving relay, which then is the only process that
 * writes the index. Reads never go through here: the probes, the band clocks and the audits keep
 * reading the store directly in either mode.
 */
interface StoreWriter : AutoCloseable {
    /** One line for the boot log: where the writes go. */
    val description: String

    /** [IEventStore.batchInsert]'s contract: one outcome per event, in order. */
    suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome>

    /** [IEventStore.insert]'s contract: returns once stored, throws on any refusal. */
    suspend fun insert(event: Event)

    /** [IEventStore.delete] for a filter that names its ids, the only delete the sync makes. */
    suspend fun delete(filter: Filter)

    override fun close() = Unit

    companion object {
        /** The store's own write path, unchanged. */
        fun local(store: IEventStore): StoreWriter =
            object : StoreWriter {
                override val description = "straight into the store"

                override suspend fun batchInsert(events: List<Event>) = store.batchInsert(events)

                override suspend fun insert(event: Event) = store.insert(event)

                override suspend fun delete(filter: Filter) = store.delete(filter)
            }
    }
}
