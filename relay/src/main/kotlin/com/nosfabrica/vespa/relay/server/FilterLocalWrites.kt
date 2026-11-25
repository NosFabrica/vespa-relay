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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventIngest
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionRequestEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.UserAssertionEvent

/**
 * The relay's [LiveEventStore]: quartz's default hold before EOSE, and NEG-OPEN snapshots kept across
 * every write [isFilterLocalWrite] says leaves the trust gate alone.
 */
internal fun liveEventStore(
    store: IEventStore,
    ingest: EventIngest,
) = LiveEventStore(store, ingest, HELD_BEFORE_EOSE, ::isFilterLocalWrite)

/** Quartz's two-argument `LiveEventStore` default, which it keeps private. */
private const val HELD_BEFORE_EOSE = 1_000

/**
 * Quartz's `writeIsFilterLocal` for this store: false for a write that can change what the trust gate
 * lets through, which drops every cached NEG-OPEN snapshot. The store's `NostrSemanticsStore.touchesTrust`
 * (vespa-eventstore), except that a kind 5 naming only addresses of other kinds is let through.
 */
internal fun isFilterLocalWrite(event: Event): Boolean =
    when (event.kind) {
        in TRUST_GATE_KINDS, RequestToVanishEvent.KIND -> false
        DeletionRequestEvent.KIND -> deletesNoTrustDocument(event)
        else -> true
    }

/** A card (a cell on its subject's reputation) and a provider list (whose cards count, and its owner's lens): the store's `TrustProjection.TRUST_KINDS`. */
internal val TRUST_GATE_KINDS = setOf(UserAssertionEvent.KIND, TrustProviderListEvent.KIND)

/** The store deletes an `e` target whatever its kind (`Deletions.applyDeletion`), so only `a` targets can be told apart. */
private fun deletesNoTrustDocument(deletion: Event): Boolean =
    deletion.tags.none { it.size > 1 && it[0] == "e" } &&
        deletion.tags.none { it.size > 1 && it[0] == "a" && it[1].substringBefore(':').toIntOrNull().let { kind -> kind == null || kind in TRUST_GATE_KINDS } }
