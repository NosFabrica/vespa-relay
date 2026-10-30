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
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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

    /** An admin told `allowevent` applied is still told the truth after a restart. */
    @Test
    fun `an event allow-listed by an admin survives a restart, as a ban does`() {
        val file = File.createTempFile("relay-state-allowed", ".json").apply { delete() }
        try {
            openBanStore(file.path).allowEvent(a, "false positive")
            val reloaded = openBanStore(file.path)
            assertTrue(reloaded.isAllowedEvent(a))
            assertFalse(reloaded.isBannedEvent(a))
        } finally {
            file.delete()
            File(file.path + ".tmp").delete()
        }
    }

    /** The NIP-86 call is answered after the ban applies; a disk that refuses the write may not undo either. */
    @Test
    fun `a write the disk refuses keeps the ban in force and the previous file whole`() {
        val file = File.createTempFile("relay-state-refused", ".json").apply { delete() }
        val blocker = File(file.path + ".tmp")
        try {
            val store = openBanStore(file.path)
            store.banPubkey(a, "spam")
            // A directory where the temp file goes: opening it for writing fails as a full disk would.
            assertTrue(blocker.mkdir())
            store.banPubkey(b, "spam")

            assertTrue(store.isBanned(b), "the ban the caller was told about is enforced")
            val reloaded = openBanStore(file.path)
            assertTrue(reloaded.isBanned(a), "the last good write survives the failed one")
            assertFalse(reloaded.isBanned(b))
        } finally {
            file.delete()
            blocker.delete()
        }
    }

    @Test
    fun `a missing state file is a clean empty start`() {
        val store = openBanStore("/nonexistent/does-not-exist.json")
        assertFalse(store.isBanned(a))
    }

    /** Starting unmoderated, then overwriting the file on the next ban, would lose every ban it held. */
    @Test
    fun `an unreadable state file refuses the boot, names what is wrong, and is left as it was`() {
        val good = """{"value":"$a","reason":"spam"}"""
        for ((text, named) in listOf(
            "{ this is not json" to "not JSON",
            """["bannedPubkeys"]""" to "not a JSON object",
            """{"bannedPubkeys":"nope"}""" to "bannedPubkeys is not a list",
            """{"bannedPubkeys":[$good,{"reason":"no value"}]}""" to "bannedPubkeys[1]",
            """{"bannedPubkeys":[$good],"allowedKinds":[1,"seven"]}""" to "allowedKinds[1] is not a kind number",
            """{"bannedPubkeys":[$good],"disallowedKinds":[4,1.5]}""" to "disallowedKinds[1] is not a kind number",
            // Quartz's decoder takes both of these; the file is refused on them as it always was.
            """{"allowedKinds":["1"]}""" to "allowedKinds[0] is not a kind number",
            """{"bannedPubkeys":[{"value":"$a","reason":null}]}""" to "bannedPubkeys[0]",
            """{"allowedEvents":[{"value":""}]}""" to "allowedEvents[0]",
        )) {
            val file = File.createTempFile("relay-state-corrupt", ".json")
            try {
                file.writeText(text)
                val refused = assertFailsWith<IllegalStateException>(text) { openBanStore(file.path) }
                val message = refused.message.orEmpty()
                assertContains(message, file.path)
                assertContains(message, named)
                assertEquals(text, file.readText(), "the evidence is not overwritten")
            } finally {
                file.delete()
            }
        }
    }

    /** A permissions error is the operator's chown, not a corrupt file to move aside. */
    @Test
    fun `a state file the relay cannot read is refused as unreadable, with the owner to fix, not as bad JSON`() {
        // A directory where the file goes fails the read as a permissions error would, even for root.
        val dir = File.createTempFile("relay-state-unreadable", ".json").apply { delete() }
        try {
            assertTrue(dir.mkdir())
            val message = assertFailsWith<IllegalStateException> { openBanStore(dir.path) }.message.orEmpty()
            assertContains(message, "cannot be read")
            assertContains(message, "uid 10001")
            assertFalse("not JSON" in message, message)
        } finally {
            dir.delete()
        }
    }

    @Test
    fun `a file with every list well formed loads, and a missing list is an empty one`() {
        val file = File.createTempFile("relay-state-partial", ".json")
        try {
            file.writeText("""{"bannedPubkeys":[{"value":"$a"}],"allowedKinds":[1]}""")
            val store = openBanStore(file.path)
            assertTrue(store.isBanned(a), "a reason is optional")
            assertEquals(listOf(1), store.listAllowedKinds())
            assertTrue(store.listDisallowedKinds().isEmpty())
        } finally {
            file.delete()
        }
    }
}
