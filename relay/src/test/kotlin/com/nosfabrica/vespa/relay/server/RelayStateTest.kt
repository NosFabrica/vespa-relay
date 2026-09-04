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

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RelayStateTest {
    private val a = "a".repeat(64)
    private val b = "b".repeat(64)

    @Test
    fun `ban state survives a save and reload into a fresh store`() {
        val file = File.createTempFile("relay-state", ".json").apply { delete() }
        try {
            // openBanStore persists on every mutation.
            val first = openBanStore(file.path)
            first.banPubkey(a, "spam")
            first.allowKind(1)
            first.disallowKind(4)
            assertTrue(file.exists(), "a mutation should have written the state file")

            val second = openBanStore(file.path)
            assertTrue(second.isBanned(a))
            assertFalse(second.isBanned(b))
            assertEquals(listOf(1), second.listAllowedKinds())
            assertEquals(listOf(4), second.listDisallowedKinds())
        } finally {
            file.delete()
            File(file.path + ".tmp").delete()
        }
    }

    @Test
    fun `a missing state file is a clean empty start`() {
        val store = openBanStore("/nonexistent/does-not-exist.json")
        assertFalse(store.isBanned(a))
    }

    @Test
    fun `a corrupt state file starts empty instead of taking the relay down`() {
        // Three shapes, because they throw from different places.
        for ((label, text) in listOf(
            "not json at all" to "{ this is not json",
            "json, but not an object" to """["bannedPubkeys"]""",
            // A valid object whose fields are the wrong shape throws from every accessor.
            "an object with the wrong field types" to """{"bannedPubkeys":"nope","allowedKinds":{"a":1}}""",
        )) {
            val file = File.createTempFile("relay-state-corrupt", ".json")
            try {
                file.writeText(text)
                val store = openBanStore(file.path)
                assertFalse(store.isBanned(a), "$label: should start with empty lists")
                // Still usable, so the next admin rpc rewrites the file.
                store.banPubkey(a, "spam")
                assertTrue(openBanStore(file.path).isBanned(a), "$label: the file should have been rewritten")
            } finally {
                file.delete()
                File(file.path + ".tmp").delete()
            }
        }
    }
}
