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
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
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
            put("formatVersion", FORMAT_VERSION)
            put("generatedAt", result.generatedAt)
            put("pass", if (result.full) "full" else "incremental")
            put("sweptFrom", result.sweptFrom)
            put("sweptUntil", result.generatedAt)
            put("lastFullPassAt", result.lastFullPassAt)
            put("relay", relay)
            put("observer", observer)
            put("rankProvider", result.rankProvider)
            put("eventsRead", result.eventsRead)
            putJsonArray("gaps") { result.gaps.forEach { add(it) } }
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
            putJsonArray("changes") {
                result.changes.forEach { c ->
                    addJsonObject {
                        put("pubkey", c.pubkey)
                        put("npub", npub(c.pubkey))
                        put("class", c.keyClass.name)
                        put("kind", c.kind.name)
                        c.from?.let { put("from", it.name) }
                        put("to", c.to.name)
                        c.relatedKey?.let { put("relatedKey", it) }
                        c.activeAfter?.let { put("activeAfter", it) }
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
                                put("basis", f.basis.name)
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
                    putJsonArray("claims") {
                        t.claims.forEach { c ->
                            addJsonObject {
                                put("id", c.event.id)
                                put("author", c.event.pubKey)
                                put("at", c.event.createdAt)
                                put("signedBy", c.signedBy.name)
                                put("reason", c.reason.name)
                            }
                        }
                    }
                    putJsonArray("vouches") {
                        v.vouches.forEach { vv ->
                            addJsonObject {
                                put("voucher", vv.vouch.voucher)
                                vv.rank?.let { put("rank", it) }
                                put("stance", vv.vouch.stance.name)
                                put("form", vv.vouch.form.name)
                                put("event", vv.vouch.event.id)
                                put("at", vv.vouch.event.createdAt)
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

    /** Bumped whenever a later pass could not rebuild its state from an older report. */
    const val FORMAT_VERSION = 2

    /**
     * The state an earlier report leaves for the next pass, or null with the reason logged when it
     * cannot be built on: another format, relay or observer (ranks, and so verdicts, are per
     * observer), or a full pass older than [fullEverySeconds] (a `created_at` cursor misses events
     * that arrive backdated; only a full pass finds those).
     */
    fun read(
        json: JsonObject,
        relay: String,
        observer: HexKey,
        nowSeconds: Long,
        fullEverySeconds: Long,
        log: (String) -> Unit,
    ): PriorReport? {
        val version = json.int("formatVersion")
        val lastFull = json.long("lastFullPassAt")
        val why =
            when {
                version != FORMAT_VERSION -> "it is format ${version ?: "1"}, this pass writes $FORMAT_VERSION"
                json.str("relay") != relay -> "it read ${json.str("relay")}, this pass reads $relay"
                json.str("observer") != observer -> "it ranked through observer ${json.str("observer")}, this pass through $observer"
                lastFull == null || nowSeconds - lastFull >= fullEverySeconds -> "its last full pass is ${lastFull?.let { (nowSeconds - it) / 86_400 } ?: "?"} day(s) old"
                else -> null
            }
        if (why != null) {
            log("prior report not built on: $why; running a full pass")
            return null
        }
        val policy = json.obj("policy")!!
        return PriorReport(
            relay = relay,
            observer = observer,
            policy =
                VouchPolicy(
                    minVoucherRank = policy.int("minVoucherRank")!!,
                    minVouchers = policy.int("minVouchers")!!,
                    aboveNewKey = policy.bool("aboveNewKey")!!,
                    requireKnewOldKey = policy.bool("requireKnewOldKey")!!,
                ),
            sweptUntil = json.long("sweptUntil")!!,
            lastFullPassAt = lastFull!!,
            findings =
                json.arr("keys").flatMap { k ->
                    val key = k.jsonObject
                    key.arr("findings").map { it.jsonObject.toFinding(key.str("pubkey")!!) }
                },
            transitions = json.arr("transitions").map { it.jsonObject.toTransition() },
        )
    }

    private fun JsonObject.toFinding(pubkey: HexKey) =
        Finding(
            pubkey = pubkey,
            keyClass = KeyClass.valueOf(str("class")!!),
            status = Status.valueOf(str("status")!!),
            since = long("since"),
            evidence = arr("evidence").map { it.jsonPrimitive.content },
            detail = str("detail")!!,
            basis = Basis.valueOf(str("basis")!!),
            successor = str("successor"),
            relatedKey = str("relatedKey"),
            activeAfter = long("activeAfter"),
        )

    private fun JsonObject.toTransition(): PriorTransition {
        val oldKey = str("oldKey")!!
        val newKey = str("newKey")!!
        return PriorTransition(
            oldKey = oldKey,
            newKey = newKey,
            status = Status.valueOf(str("status")!!),
            claims =
                arr("claims").map {
                    val c = it.jsonObject
                    TransitionClaim(oldKey, newKey, Reason.valueOf(c.str("reason")!!), Signer.valueOf(c.str("signedBy")!!), EventRef(c.str("id")!!, c.str("author")!!, c.long("at")!!))
                },
            vouches =
                arr("vouches").map {
                    val v = it.jsonObject
                    val voucher = v.str("voucher")!!
                    Vouch(voucher, Stance.valueOf(v.str("stance")!!), VouchForm.valueOf(v.str("form")!!), EventRef(v.str("event")!!, voucher, v.long("at")!!))
                },
        )
    }

    private fun JsonObject.str(key: String) = (this[key] as? JsonPrimitive)?.contentOrNull

    private fun JsonObject.long(key: String) = (this[key] as? JsonPrimitive)?.longOrNull

    private fun JsonObject.int(key: String) = (this[key] as? JsonPrimitive)?.intOrNull

    private fun JsonObject.bool(key: String) = (this[key] as? JsonPrimitive)?.booleanOrNull

    private fun JsonObject.obj(key: String) = this[key] as? JsonObject

    private fun JsonObject.arr(key: String) = (this[key] as? JsonArray).orEmpty()
}
