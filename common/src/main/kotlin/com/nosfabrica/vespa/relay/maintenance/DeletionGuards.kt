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

/**
 * The store's name for "check NIP-09 and NIP-62 against the store on EVERY
 * insert". Its own default is an in-memory cache of the authors that instance
 * has watched store a kind 5/62 — everyone else's inserts then skip the two
 * guard queries, which is exact for a store with ONE writer.
 *
 * This deployment has two. The relay and the sync process open their own store
 * instance against the same Vespa, and the cache is per instance: the router
 * mirrors kind 5 and 62 (see router.conf.example) and the relay's copy never
 * hears about them, so a client could republish an event a mirrored deletion
 * covers and be admitted. NIP-09 and NIP-62 are not optional, and neither is
 * this — [requireDeletionGuards] refuses to boot without it rather than let a
 * deployment decide how much of the protocol it implements.
 */
const val DELETION_GUARD_ENV = "GUARD_OWNERS_DISABLE"

/**
 * Whether [value] turns the store's guard-owner cache OFF, i.e. whether every
 * insert is checked. The store accepts `1` as well as `true`, and the docs
 * prescribe `1`; this mirrors that parse, so a value the store would ignore
 * fails here instead of passing as protection nobody has.
 */
fun deletionGuardsEnforced(value: String?): Boolean = value == "1" || value?.toBooleanStrictOrNull() == true

/**
 * Stop the boot unless every insert is checked, naming what would break.
 *
 * A tripwire, not a setting. The failure it prevents is silent and it is
 * NOT ours to trade away: deleted events come back, and they come back into a
 * relay that answers other people's clients. Both processes call it, because
 * either one admitting a covered event undoes the other's erase.
 */
fun requireDeletionGuards(env: Map<String, String>) {
    if (deletionGuardsEnforced(env[DELETION_GUARD_ENV])) return
    error(
        "$DELETION_GUARD_ENV must be 1 here. Two processes write this store — the relay and the sync process — " +
            "and the store's guard-owner cache is per instance, so each would skip the NIP-09/NIP-62 checks for " +
            "authors whose deletion the OTHER one stored: a deleted event republished by a client, or re-mirrored " +
            "from an upstream, would be admitted. The image and the Gradle run tasks set it; unset or 0 means " +
            "something else launched this process.",
    )
}
