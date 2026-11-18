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
package com.nosfabrica.vespa.relay.keywatch

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.hexToByteArray
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.add
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlinx.serialization.json.putJsonObject

/**
 * The run as one JSON document. Public keys, classes, statuses, time bounds and event ids, never
 * a secret: nothing upstream of it holds one.
 *
 * Keys are ordered by what needs a person first: a public secret on a key that is still signing,
 * then by rank.
 */
object Report {
    fun toJson(
        result: KeyWatchResult,
        relay: String,
        observer: HexKey,
        policy: VouchPolicy,
    ): JsonObject =
        buildJsonObject {
            put("generatedAt", result.generatedAt)
            put("relay", relay)
            put("observer", observer)
            put("rankProvider", result.rankProvider)
            put("eventsRead", result.eventsRead)
            putJsonObject("policy") {
                put("minVoucherRank", policy.minVoucherRank)
                put("minVouchers", policy.minVouchers)
                put("aboveNewKey", policy.aboveNewKey)
                put("requireKnewOldKey", policy.requireKnewOldKey)
            }
            putJsonObject("summary") {
                result.findings.groupBy { it.keyClass }.toSortedMap().forEach { (keyClass, list) ->
                    putJsonObject(keyClass.name) {
                        list
                            .groupingBy { it.status }
                            .eachCount()
                            .toSortedMap()
                            .forEach { (s, n) -> put(s.name, n) }
                    }
                }
            }
            put("keys", keys(result))
            put("transitions", transitions(result))
        }

    private fun keys(result: KeyWatchResult): JsonArray {
        val byKey = result.findings.groupBy { it.pubkey }
        val ordered =
            byKey.entries.sortedWith(
                compareByDescending<Map.Entry<HexKey, List<Finding>>> { (_, f) -> f.any { it.keyClass == KeyClass.COMPROMISED_PUBLIC && it.activeAfter != null } }
                    .thenByDescending { (key, _) -> result.cards(key)?.rank ?: -1 },
            )
        return buildJsonArray {
            ordered.forEach { (pubkey, findings) ->
                addJsonObject {
                    put("pubkey", pubkey)
                    put("npub", npub(pubkey))
                    result.cards(pubkey)?.let { card ->
                        card.rank?.let { put("rank", it) }
                        card.followers?.let { put("followers", it) }
                    }
                    putJsonArray("findings") {
                        findings.sortedBy { it.keyClass }.forEach { f ->
                            addJsonObject {
                                put("class", f.keyClass.name)
                                put("status", f.status.name)
                                f.since?.let { put("since", it) }
                                put("detail", f.detail)
                                f.successor?.let { put("successor", it) }
                                f.relatedKey?.let { put("relatedKey", it) }
                                f.activeAfter?.let { put("activeAfter", it) }
                                putJsonArray("evidence") { f.evidence.forEach { add(it) } }
                            }
                        }
                    }
                }
            }
        }
    }

    private fun transitions(result: KeyWatchResult): JsonArray =
        buildJsonArray {
            result.transitions.sortedBy { it.status }.forEach { v ->
                val t = v.transition
                addJsonObject {
                    put("oldKey", t.oldKey)
                    put("newKey", t.newKey)
                    put("reason", t.reason.name)
                    put("status", v.status.name)
                    put("firstClaimAt", t.firstClaimAt)
                    putJsonArray("signedBy") { t.signers.sorted().forEach { add(it.name) } }
                    putJsonArray("claims") { t.claims.forEach { add(it.event.id) } }
                    putJsonArray("vouches") {
                        v.vouches.forEach { vv ->
                            addJsonObject {
                                put("voucher", vv.vouch.voucher)
                                vv.rank?.let { put("rank", it) }
                                put("stance", vv.vouch.stance.name)
                                put("form", vv.vouch.form.name)
                                put("event", vv.vouch.event.id)
                                put("knewOldKey", vv.knewOldKey)
                                put("eligible", vv.eligible)
                                vv.ineligible?.let { put("ineligible", it) }
                            }
                        }
                    }
                }
            }
        }

    private fun npub(hex: HexKey) = runCatching { hex.hexToByteArray().toNpub() }.getOrDefault("")
}
