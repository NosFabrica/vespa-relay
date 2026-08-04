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

import com.vitorpamplona.quartz.nip86RelayManagement.server.BanStore
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
 * File persistence for the NIP-86 [BanStore], so bans/allows survive a
 * restart. Only the moderation lists are durable — runtime NIP-11 edits reset
 * to the environment-configured identity on restart.
 */
object BanListFile {
    private val json = Json { prettyPrint = true }

    /** Seed [banStore] from [path] if the file exists and parses; otherwise no-op. */
    fun loadInto(
        path: String,
        banStore: BanStore,
    ) {
        val file = File(path)
        if (!file.exists()) return
        val root = runCatching { Json.parseToJsonElement(file.readText()).jsonObject }.getOrNull() ?: return
        banStore.seedFromSnapshot(
            root.pairs("bannedPubkeys"),
            root.pairs("allowedPubkeys"),
            root.pairs("bannedEvents"),
            root.ints("allowedKinds"),
            root.ints("disallowedKinds"),
        )
    }

    /** Write [banStore]'s current lists to [path] atomically (temp file + move). */
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
        val target = File(path)
        target.absoluteFile.parentFile?.mkdirs()
        val tmp = File(path + ".tmp")
        tmp.writeText(
            json.encodeToString(
                kotlinx.serialization.json.JsonObject
                    .serializer(),
                doc,
            ),
        )
        runCatching {
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }.recoverCatching {
            // Some filesystems don't support ATOMIC_MOVE; fall back to a plain replace.
            Files.move(tmp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }
    }
}

/**
 * Open a NIP-86 [BanStore], optionally backed by a state file: seeds from it
 * on boot and rewrites it on every mutation. With no path, purely in-memory.
 */
fun openBanStore(stateFile: String?): BanStore {
    if (stateFile == null) return BanStore {}
    // The onChange callback needs the store it belongs to; wire it after
    // construction through a nullable holder.
    var store: BanStore? = null
    val bs = BanStore { store?.let { BanListFile.save(stateFile, it) } }
    store = bs
    BanListFile.loadInto(stateFile, bs)
    return bs
}

private fun kotlinx.serialization.json.JsonObjectBuilder.putPairs(
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

private fun kotlinx.serialization.json.JsonObjectBuilder.putInts(
    key: String,
    ints: List<Int>,
) = putJsonArray(key) { ints.forEach { add(it) } }

private fun kotlinx.serialization.json.JsonObject.pairs(key: String): List<Pair<String, String>> =
    this[key]
        ?.jsonArray
        ?.map { element ->
            val obj = element.jsonObject
            (obj["value"]?.jsonPrimitive?.content ?: "") to (obj["reason"]?.jsonPrimitive?.content ?: "")
        }.orEmpty()

private fun kotlinx.serialization.json.JsonObject.ints(key: String): List<Int> = this[key]?.jsonArray?.map { it.jsonPrimitive.int }.orEmpty()
