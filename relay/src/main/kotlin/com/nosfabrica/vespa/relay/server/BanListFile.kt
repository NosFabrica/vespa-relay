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

import com.nosfabrica.vespa.relay.util.writeDurably
import com.vitorpamplona.quartz.nip86RelayManagement.server.BanStore
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.io.File

/** File persistence for the NIP-86 [BanStore]. Only the moderation lists are durable. */
object BanListFile {
    private val json = Json { prettyPrint = true }

    /**
     * Seeds [banStore] from [path] if the file exists. One unreadable entry refuses the boot: serving
     * with no bans, then overwriting the file on the next mutation, would lose every one of them.
     */
    fun loadInto(
        path: String,
        banStore: BanStore,
    ) {
        val file = File(path)
        if (!file.exists()) return
        val root =
            try {
                Json.parseToJsonElement(file.readText())
            } catch (e: Exception) {
                refuse(path, "it is not JSON (${e::class.simpleName})")
            }
        val doc = root as? JsonObject ?: refuse(path, "it is not a JSON object")
        banStore.seedFromSnapshot(
            doc.pairs(path, "bannedPubkeys"),
            doc.pairs(path, "allowedPubkeys"),
            doc.pairs(path, "bannedEvents"),
            doc.ints(path, "allowedKinds"),
            doc.ints(path, "disallowedKinds"),
        )
    }

    /** Writes [banStore]'s lists to [path] durably. Synchronized: two writers would share one `.tmp` path. */
    @Synchronized
    fun save(
        path: String,
        banStore: BanStore,
    ) {
        val doc =
            buildJsonObject {
                putPairs("bannedPubkeys", banStore.listBannedPubkeys())
                putPairs("allowedPubkeys", banStore.listAllowedPubkeys())
                putPairs("bannedEvents", banStore.listBannedEvents())
                putInts("allowedKinds", banStore.listAllowedKinds())
                putInts("disallowedKinds", banStore.listDisallowedKinds())
            }
        try {
            writeDurably(File(path), json.encodeToString(JsonObject.serializer(), doc))
        } catch (e: Exception) {
            // Not thrown: the ban is already enforced in memory. It vanishes on restart, so said aloud.
            System.err.println("nip86: could not persist ban lists to $path (${e.message}) — the change holds until restart; the previous file is kept")
        }
    }

    private fun refuse(
        path: String,
        why: String,
    ): Nothing =
        error(
            "RELAY_STATE_FILE $path cannot be read as a ban list: $why. Refusing to start with no bans enforced; " +
                "fix the entry, or move the file aside to start with empty lists.",
        )

    private fun JsonObject.listAt(
        path: String,
        key: String,
    ): List<JsonElement> =
        when (val value = this[key]) {
            null -> emptyList()
            is JsonArray -> value
            else -> refuse(path, "$key is not a list")
        }

    private fun JsonObject.pairs(
        path: String,
        key: String,
    ): List<Pair<String, String>> =
        listAt(path, key).mapIndexed { i, element ->
            val entry = element as? JsonObject
            val value = entry?.text("value")?.takeIf { it.isNotEmpty() }
            val reason = entry?.let { if (it["reason"] == null) "" else it.text("reason") }
            if (value == null || reason == null) refuse(path, "$key[$i] is not {\"value\": \"…\", \"reason\": \"…\"}")
            value to reason
        }

    private fun JsonObject.ints(
        path: String,
        key: String,
    ): List<Int> =
        listAt(path, key).mapIndexed { i, element ->
            (element as? JsonPrimitive)?.takeUnless { it.isString }?.intOrNull ?: refuse(path, "$key[$i] is not a kind number")
        }

    private fun JsonObject.text(key: String): String? = (this[key] as? JsonPrimitive)?.takeIf { it.isString }?.content
}

/** A NIP-86 [BanStore], backed by [stateFile] when one is given: seeded on boot, rewritten on mutation. */
fun openBanStore(stateFile: String?): BanStore {
    // Blank as well as null: compose's `${RELAY_STATE_FILE:-}` delivers "" for unset.
    if (stateFile.isNullOrBlank()) return BanStore {}
    // The onChange callback needs the store it belongs to.
    var store: BanStore? = null
    val bs = BanStore { store?.let { BanListFile.save(stateFile, it) } }
    store = bs
    BanListFile.loadInto(stateFile, bs)
    return bs
}

private fun JsonObjectBuilder.putPairs(
    key: String,
    pairs: List<Pair<String, String?>>,
) = putJsonArray(key) {
    pairs.forEach { (value, reason) ->
        add(
            buildJsonObject {
                put("value", value)
                put("reason", reason ?: "")
            },
        )
    }
}

private fun JsonObjectBuilder.putInts(
    key: String,
    ints: List<Int>,
) = putJsonArray(key) { ints.forEach { add(it) } }
