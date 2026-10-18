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
package com.nosfabrica.vespa.relay.util

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** A durable write leaves exactly the new content and no temp file; a quarantine keeps the old bytes. */
class DurableFilesTest {
    @Test
    fun `a write replaces the target and leaves no temp file behind`() {
        val dir = Files.createTempDirectory("durable").toFile()
        val target = dir.resolve("nested/state.json")
        writeDurably(target, "one")
        writeDurably(target, "two")
        assertEquals("two", target.readText())
        assertEquals(listOf("state.json"), target.parentFile.list()!!.toList())
    }

    @Test
    fun `a quarantined file keeps its bytes under a new name and frees the original`() {
        val dir = Files.createTempDirectory("durable").toFile()
        val target = dir.resolve("state.json").apply { writeText("{not json") }
        val aside = assertNotNull(quarantine(target, nowMillis = 42))
        assertFalse(target.exists())
        assertEquals("state.json.corrupt-42", aside.name)
        assertEquals("{not json", aside.readText())
    }

    @Test
    fun `nothing to quarantine is null`() {
        val dir = Files.createTempDirectory("durable").toFile()
        assertNull(quarantine(dir.resolve("absent.json")))
        assertTrue(dir.list()!!.isEmpty())
    }
}
