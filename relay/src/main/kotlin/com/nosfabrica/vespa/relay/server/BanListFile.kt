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
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull
import java.io.File
import java.io.IOException
import java.nio.file.Files

/**
 * File persistence for the NIP-86 [BanStore], in quartz's whole-state codec ([BanStore.toJson]).
 * The lists are checked here first, so a hand-edited file is refused on the same faults as ever.
 */
object BanListFile {
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
        val text =
            try {
                Files.readString(file.toPath())
            } catch (e: IOException) {
                refuse(path, "it cannot be read (${e::class.simpleName}: ${e.message}); $OWNER_HINT")
            }
        val root =
            try {
                Json.parseToJsonElement(text)
            } catch (e: Exception) {
                refuse(path, "it is not JSON (${e::class.simpleName})")
            }
        val doc = root as? JsonObject ?: refuse(path, "it is not a JSON object")
        doc.requireTyped(path)
        try {
            banStore.seedFromJson(text)
        } catch (e: IllegalArgumentException) {
            refuse(path, e.message ?: "it is not a ban list")
        }
    }

    /** Writes [banStore]'s whole state to [path] durably. Synchronized: two writers would share one `.tmp` path. */
    @Synchronized
    fun save(
        path: String,
        banStore: BanStore,
    ) {
        try {
            writeDurably(File(path), banStore.toJson())
        } catch (e: Exception) {
            // Not thrown: the ban is already enforced in memory. It vanishes on restart, so said aloud.
            System.err.println(
                "nip86: could not persist ban lists to $path (${e::class.simpleName}: ${e.message}) — the change holds until restart; " +
                    "the previous file is kept. If it is a permissions error, $OWNER_HINT",
            )
        }
    }

    /** The image's user; a bind-mounted state directory must be its to read and write. */
    private const val OWNER_HINT = "the relay runs as uid 10001, so the file and its directory must be that user's (on the host: sudo chown -R 10001:10001 <dir>)"

    private val ENTRY_LISTS = listOf("bannedPubkeys", "allowedPubkeys", "bannedEvents", "allowedEvents")

    private val KIND_LISTS = listOf("allowedKinds", "disallowedKinds")

    /**
     * The lists as this relay has always refused them, each fault named: quartz's decoder also takes
     * a quoted kind or a null reason. Remove when quartz's `seedFromJson` refuses both.
     */
    private fun JsonObject.requireTyped(path: String) {
        for (key in KIND_LISTS) {
            listAt(path, key).forEachIndexed { i, element ->
                (element as? JsonPrimitive)?.takeUnless { it.isString }?.intOrNull ?: refuse(path, "$key[$i] is not a kind number")
            }
        }
        for (key in ENTRY_LISTS) {
            listAt(path, key).forEachIndexed { i, element ->
                val entry = element as? JsonObject
                val value = entry?.text("value")?.takeIf { it.isNotEmpty() }
                val reason = entry?.let { if (it["reason"] == null) "" else it.text("reason") }
                if (value == null || reason == null) refuse(path, "$key[$i] is not {\"value\": \"…\", \"reason\": \"…\"}")
            }
        }
    }

    private fun JsonObject.listAt(
        path: String,
        key: String,
    ): List<JsonElement> =
        when (val value = this[key]) {
            null -> emptyList()
            is JsonArray -> value
            else -> refuse(path, "$key is not a list")
        }

    private fun JsonObject.text(key: String): String? = (this[key] as? JsonPrimitive)?.takeIf { it.isString }?.content

    private fun refuse(
        path: String,
        why: String,
    ): Nothing =
        error(
            "RELAY_STATE_FILE $path cannot be read as a ban list: $why. Refusing to start with no bans enforced; " +
                "fix the entry, or move the file aside to start with empty lists.",
        )
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
